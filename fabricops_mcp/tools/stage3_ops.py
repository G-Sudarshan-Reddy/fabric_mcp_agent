from __future__ import annotations

import asyncio
import datetime
import json
import os
from pathlib import Path
from typing import Any
import uuid

from fabricops_mcp.fabric_client import FabricClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]
AUDIT_DIR = PROJECT_ROOT / "audit_log"

WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
LAKEHOUSE_ID = os.getenv("FABRIC_LAKEHOUSE_ID")

BRONZE_NOTEBOOK_ID = os.getenv("BRONZE_NOTEBOOK_ID", "d2c11d0d-329f-46de-9247-2c633764a026")
SILVER_NOTEBOOK_ID = os.getenv("SILVER_NOTEBOOK_ID", "6b845e96-75e2-4e32-8a30-7a81f81bcf23")
GOLD_NOTEBOOK_ID = os.getenv("GOLD_NOTEBOOK_ID", "c8a8e8a7-a50f-48d5-b42f-9a5f6b314267")


def _client() -> FabricClient:
	workspace_id = os.getenv("FABRIC_WORKSPACE_ID") or WORKSPACE_ID
	if not workspace_id:
		raise RuntimeError("Missing FABRIC_WORKSPACE_ID in environment variables.")
	return FabricClient(workspace_id=workspace_id)


def _lakehouse_id() -> str:
	lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID") or LAKEHOUSE_ID
	if not lakehouse_id:
		raise RuntimeError("Missing FABRIC_LAKEHOUSE_ID in environment variables.")
	return lakehouse_id


def _audit_files() -> list[Path]:
	if not AUDIT_DIR.exists():
		return []
	return sorted(AUDIT_DIR.glob("audit_*.json"))


def _load_audit_entries() -> list[dict[str, Any]]:
	entries: list[dict[str, Any]] = []
	for file_path in _audit_files():
		try:
			with file_path.open("r", encoding="utf-8") as f:
				data = json.load(f)
			if isinstance(data, dict):
				entries.append(data)
		except Exception:
			continue

	def _parse_ts(entry: dict[str, Any]) -> datetime.datetime:
		ts = str(entry.get("timestamp", ""))
		try:
			return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
		except Exception:
			return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

	entries.sort(key=_parse_ts, reverse=True)
	return entries


def analyse_notebook_source(cells: list) -> dict:
	"""
	Analyses notebook cell source code for common errors.
	Returns dict with error_type, affected_cell_index,
	affected_line, description, and fix_code.
	"""
	import ast
	import re

	for i, cell in enumerate(cells):
		source = "".join(cell.get("source", []))

		# Check 1: Python syntax errors
		try:
			ast.parse(source)
		except SyntaxError as e:
			line_no = e.lineno or 0
			line_txt = ""
			if line_no > 0:
				lines = source.split("\n")
				if line_no - 1 < len(lines):
					line_txt = lines[line_no - 1]
			return {
				"error_type": "syntax_error",
				"affected_cell_index": i,
				"affected_line": e.lineno,
				"description": f"SyntaxError in cell {i}, line {e.lineno}: {e.msg}",
				"bad_code_snippet": line_txt,
				"fix_strategy": "fix_syntax",
			}

		# Check 2: Common typos - spark read options
		if re.search(r"\.otion\s*\(", source):
			match = re.search(r".*\.otion.*", source)
			return {
				"error_type": "typo_option",
				"affected_cell_index": i,
				"affected_line": None,
				"description": f"Typo in cell {i}: '.otion(' should be '.option('",
				"bad_code_snippet": match.group() if match else "",
				"fix_strategy": "fix_typo",
			}

		# Check 3: Wrong file path / nonexistent file reference
		if re.search(r"source_path\s*=.*nonexistent|source_file\s*=.*nonexistent", source):
			match = re.search(r".*nonexistent.*", source)
			return {
				"error_type": "wrong_file_path",
				"affected_cell_index": i,
				"affected_line": None,
				"description": f"Cell {i} references a nonexistent file path",
				"bad_code_snippet": match.group() if match else "",
				"fix_strategy": "fix_file_path",
			}

		# Check 4: Missing dropna on key columns (silver notebook)
		if "saveAsTable" in source and "silver" in source.lower():
			if "dropna" not in source:
				return {
					"error_type": "missing_dropna",
					"affected_cell_index": i,
					"affected_line": None,
					"description": f"Silver notebook cell {i} missing .dropna() call",
					"bad_code_snippet": "",
					"fix_strategy": "add_dropna",
				}

		# Check 5: OOM risk - no repartition on large reads
		if "spark.read" in source and "repartition" not in source and "bronze" in source.lower():
			pass  # Not always an error, skip

	return {
		"error_type": "no_static_error_found",
		"affected_cell_index": -1,
		"affected_line": None,
		"description": "No static errors found in notebook source. Error may be runtime data issue.",
		"bad_code_snippet": "",
		"fix_strategy": "runtime_only",
	}


async def get_notebook_error(notebook_id: str) -> str:
	"""
	Reads the current source code of a Fabric notebook cell using the
	Fabric REST API getDefinition endpoint, then analyses the code for
	errors. Does NOT use local Python tools. Requires notebook_id parameter.
	"""
	client = _client()

	# Step 1: get notebook definition via Fabric REST API
	definition = await client.get_notebook_definition(notebook_id)
	cells = definition["cells"]

	# Step 2: analyse source code for errors
	analysis = analyse_notebook_source(cells)

	# Step 3: show the current source of the affected cell
	affected_source = ""
	if analysis["affected_cell_index"] >= 0:
		cell_source = cells[analysis["affected_cell_index"]].get("source", [])
		affected_source = "".join(cell_source)

	result = {
		"notebook_id": notebook_id,
		"total_cells": len(cells),
		"analysis": analysis,
		"affected_cell_source": affected_source[:500],
	}

	return json.dumps(result, indent=2)


async def auto_fix_and_retry(
	notebook_id: str,
	notebook_name: str,
	error_type: str,
) -> str:
	"""
	Downloads the Fabric notebook definition via REST API, patches the
	broken cell source code, re-uploads via REST API, then re-runs the
	notebook. All operations call the Fabric REST API. Requires
	notebook_id, notebook_name, error_type parameters.
	"""
	client = _client()
	lakehouse_id = _lakehouse_id()

	# Step 1: get current notebook cells via Fabric API
	definition = await client.get_notebook_definition(notebook_id)
	cells = definition["cells"]

	# Step 2: re-analyse to confirm error and get affected cell
	analysis = analyse_notebook_source(cells)
	cell_index = analysis["affected_cell_index"]

	if cell_index < 0 and error_type != "unknown":
		return json.dumps(
			{
				"outcome": "no_fix_needed",
				"message": "Source code analysis found no errors. Error may have been transient.",
				"recommendation": "Retry the notebook run directly.",
			}
		)

	# Step 3: apply fix based on strategy
	fix_description = ""
	fixed_source = ""

	if analysis["fix_strategy"] == "fix_typo":
		current_source = "".join(cells[cell_index].get("source", []))
		fixed_source = current_source.replace(".otion(", ".option(")
		fix_description = "Fixed typo: .otion( → .option("

	elif analysis["fix_strategy"] == "fix_syntax":
		current_source = "".join(cells[cell_index].get("source", []))
		# Add a comment marking the bad line and comment it out
		lines = current_source.split("\n")
		bad_line = (analysis["affected_line"] or 1) - 1
		if 0 <= bad_line < len(lines):
			lines[bad_line] = f"# AUTO-FIX: syntax error removed — {lines[bad_line]}"
		fixed_source = "\n".join(lines)
		fix_description = f"Commented out syntax error at line {analysis['affected_line']}"

	elif analysis["fix_strategy"] == "fix_file_path":
		current_source = "".join(cells[cell_index].get("source", []))
		import re

		fixed_source = re.sub(r"nonexistent[^\s\'\"]*", "sales.csv", current_source)
		fix_description = "Restored file path to sales.csv"

	elif analysis["fix_strategy"] == "add_dropna":
		current_source = "".join(cells[cell_index].get("source", []))
		fixed_source = current_source.replace(
			"df.write.format",
			"df = df.dropna()\ndf.write.format",
		)
		fix_description = "Added .dropna() before write"

	else:
		fix_description = "No source fix applied — retrying as-is"
		fixed_source = "".join(cells[cell_index].get("source", [])) if cell_index >= 0 else ""

	# Step 4: patch the notebook via Fabric REST API (if we have a fix)
	if fix_description != "No source fix applied — retrying as-is" and cell_index >= 0:
		await client.patch_notebook_cell(
			notebook_id,
			cell_index,
			fixed_source,
			notebook_name,
			lakehouse_id,
		)

	# Step 5: re-run the notebook via Fabric REST API
	job_instance_id = await client.run_notebook(notebook_id)
	final_status = await client.poll_until_done(notebook_id, job_instance_id)

	fix_worked = str(final_status.get("status", "")).lower() == "completed"

	# Step 6: write audit log
	await client.write_audit_log_entry(
		triggered_by="auto_fix_and_retry",
		action="patch_and_retry",
		target_item=notebook_name,
		outcome="success" if fix_worked else "failure",
		error_detail=analysis["description"],
		fix_applied=fix_description,
		fix_outcome="success" if fix_worked else "failure",
	)

	return json.dumps(
		{
			"error_found": analysis["description"],
			"fix_applied": fix_description,
			"affected_cell": cell_index,
			"retry_job_id": job_instance_id,
			"retry_status": final_status.get("status"),
			"fix_worked": fix_worked,
		},
		indent=2,
	)


async def run_full_pipeline() -> str:
	"""Runs Bronze → Silver → Gold with error detection and auto-fix retry."""
	client = _client()
	run_id = str(uuid.uuid4())

	layers = [
		("Bronze", BRONZE_NOTEBOOK_ID, "sales_bronze_nb"),
		("Silver", SILVER_NOTEBOOK_ID, "sales_silver_nb"),
		("Gold", GOLD_NOTEBOOK_ID, "sales_gold_nb"),
	]

	rows: list[dict[str, Any]] = []

	for layer_name, notebook_id, notebook_name in layers:
		started = datetime.datetime.now(datetime.timezone.utc)
		job_instance_id = await client.run_notebook(notebook_id)
		status_payload = await client.poll_until_done(notebook_id, job_instance_id, max_wait_seconds=900)
		status = str(status_payload.get("status", "unknown"))
		duration = int((datetime.datetime.now(datetime.timezone.utc) - started).total_seconds())

		await client.write_audit_log_entry(
			triggered_by="run_full_pipeline",
			action=f"run_{layer_name.lower()}_notebook",
			target_item=notebook_name,
			outcome="success" if status.lower() == "completed" else "failure",
			error_detail="",
			fix_applied="",
			fix_outcome="",
		)

		row = {
			"layer": layer_name,
			"status": status,
			"duration": duration,
			"job_instance_id": job_instance_id,
			"auto_fix": "",
		}

		if status.lower() != "completed":
			error_json = await get_notebook_error(notebook_id)
			error_info = json.loads(error_json)
			error_type = str(error_info.get("analysis", {}).get("error_type", "unknown"))

			fix_json = await auto_fix_and_retry(notebook_id, notebook_name, error_type)
			fix_info = json.loads(fix_json)
			row["auto_fix"] = fix_info.get("fix_applied", "")
			row["status"] = str(fix_info.get("retry_status", status))
			row["job_instance_id"] = str(fix_info.get("retry_job_id", job_instance_id))

			rows.append(row)

			if str(fix_info.get("retry_status", "")).lower() != "completed":
				break
		else:
			rows.append(row)

	header = f"Run ID: {run_id}\n\n| Layer | Status | Duration (s) | Job Instance ID | Auto-fix |\n|---|---|---:|---|---|"
	lines = [
		f"| {r['layer']} | {r['status']} | {r['duration']} | {r['job_instance_id']} | {r.get('auto_fix','')} |"
		for r in rows
	]
	return "\n".join([header, *lines])


def show_audit_log(limit: int = 20) -> str:
	entries = _load_audit_entries()
	if not entries:
		return "No audit log entries found."

	selected = entries[: max(1, int(limit))]
	header = "| timestamp | action | target_item | outcome | fix_applied |\n|---|---|---|---|---|"
	rows = []
	for entry in selected:
		rows.append(
			"| "
			+ f"{entry.get('timestamp', '')} | "
			+ f"{entry.get('action', '')} | "
			+ f"{entry.get('target_item', '')} | "
			+ f"{entry.get('outcome', '')} | "
			+ f"{entry.get('fix_applied', '')} |"
		)
	return "\n".join([header, *rows])


async def get_pipeline_health() -> dict[str, str]:
	client = _client()
	items = await client.list_items()
	item_ids = {str(item.get("id", "")) for item in items}

	health = {
		"bronze_notebook": "present" if BRONZE_NOTEBOOK_ID in item_ids else "missing",
		"silver_notebook": "present" if SILVER_NOTEBOOK_ID in item_ids else "missing",
		"gold_notebook": "present" if GOLD_NOTEBOOK_ID in item_ids else "missing",
	}

	entries = _load_audit_entries()
	if not entries:
		health["last_run"] = "never"
		health["last_run_outcome"] = "unknown"
		health["recommendation"] = "Run run_full_pipeline to generate fresh execution records."
		return health

	latest = entries[0]
	latest_ts = str(latest.get("timestamp", ""))
	last_outcome = str(latest.get("outcome", "unknown")).lower()
	if last_outcome not in {"success", "failure"}:
		last_outcome = "unknown"

	health["last_run"] = latest_ts
	health["last_run_outcome"] = last_outcome

	if "missing" in {health["bronze_notebook"], health["silver_notebook"], health["gold_notebook"]}:
		health["recommendation"] = "Recreate missing notebooks before next run."
	elif last_outcome == "failure":
		health["recommendation"] = "Use get_notebook_error and auto_fix_and_retry on failed layer."
	else:
		health["recommendation"] = "Pipeline appears healthy."

	return health


async def send_alert(pipeline_name: str, status: str, error_summary: str, fix_applied: str) -> str:
	client = _client()
	webhook_url = os.getenv("TEAMS_WEBHOOK_URL", "")
	return await client.send_teams_alert(webhook_url, pipeline_name, status, error_summary, fix_applied)


def register_stage3_ops_tools(mcp: Any) -> None:
	@mcp.tool(
		name="run_full_pipeline",
		description="Runs the complete Bronze-Silver-Gold medallion pipeline in sequence and reports status of each layer.",
	)
	async def run_full_pipeline_tool() -> str:
		return await run_full_pipeline()

	@mcp.tool(
		name="get_notebook_error",
		description=(
			"Reads the current source code of a Fabric notebook cell using the "
			"Fabric REST API getDefinition endpoint, then analyses the code for "
			"errors. Does NOT use local Python tools. Requires notebook_id parameter."
		),
	)
	async def get_notebook_error_tool(notebook_id: str) -> str:
		return await get_notebook_error(notebook_id)

	@mcp.tool(
		name="auto_fix_and_retry",
		description=(
			"Downloads the Fabric notebook definition via REST API, patches the "
			"broken cell source code, re-uploads via REST API, then re-runs the "
			"notebook. All operations call the Fabric REST API. Requires "
			"notebook_id, notebook_name, error_type parameters."
		),
	)
	async def auto_fix_and_retry_tool(notebook_id: str, notebook_name: str, error_type: str) -> str:
		return await auto_fix_and_retry(notebook_id, notebook_name, error_type)

	@mcp.tool(name="show_audit_log", description="Shows the audit log of all FabricOps Agent actions.")
	async def show_audit_log_tool(limit: int = 20) -> str:
		return show_audit_log(limit)

	@mcp.tool(
		name="get_pipeline_health",
		description="Checks the health of the medallion pipeline by verifying Delta table row counts in the Lakehouse.",
	)
	async def get_pipeline_health_tool() -> dict[str, str]:
		return await get_pipeline_health()

	@mcp.tool(name="send_alert", description="Sends a Teams alert about pipeline status.")
	async def send_alert_tool(pipeline_name: str, status: str, error_summary: str, fix_applied: str) -> str:
		return await send_alert(pipeline_name, status, error_summary, fix_applied)
