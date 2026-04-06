from __future__ import annotations

import base64
import io
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from fabricops_mcp.auth import get_token
from fabricops_mcp.fabric_client import FabricClient


WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
LAKEHOUSE_ID = os.getenv("FABRIC_LAKEHOUSE_ID")
SQL_ENDPOINT_URL = os.getenv("FABRIC_SQL_ENDPOINT_URL")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_DIR = PROJECT_ROOT / "fabricops_mcp" / "templates"


def _require_workspace_id() -> str:
	workspace_id = os.getenv("FABRIC_WORKSPACE_ID") or WORKSPACE_ID
	if not workspace_id:
		raise RuntimeError("Missing FABRIC_WORKSPACE_ID in environment variables.")
	return workspace_id


def _require_lakehouse_id() -> str:
	lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID") or LAKEHOUSE_ID
	if not lakehouse_id:
		raise RuntimeError("Missing FABRIC_LAKEHOUSE_ID in environment variables.")
	return lakehouse_id


def _build_client() -> FabricClient:
	return FabricClient(workspace_id=_require_workspace_id())


def _resolve_local_csv(file_path: str) -> Path | None:
	candidates = [
		PROJECT_ROOT / file_path,
		PROJECT_ROOT / "fabricops_mcp" / file_path,
		Path.cwd() / file_path,
	]
	for candidate in candidates:
		if candidate.exists() and candidate.is_file():
			return candidate
	return None


def _map_dtype_to_inferred(dtype_name: str) -> str:
	dtype = dtype_name.lower()
	if "int" in dtype:
		return "int"
	if "float" in dtype:
		return "double"
	if "bool" in dtype:
		return "boolean"
	if "datetime" in dtype or "date" in dtype:
		return "date"
	return "string"


def _spark_cast_for(inferred_type: str) -> str:
	lookup = {
		"int": "int",
		"integer": "int",
		"long": "long",
		"double": "double",
		"float": "double",
		"decimal": "double",
		"number": "double",
		"boolean": "boolean",
		"date": "date",
		"timestamp": "timestamp",
		"string": "string",
	}
	return lookup.get(str(inferred_type).lower(), "string")


def _pick_key_columns(columns: list[dict[str, Any]]) -> list[str]:
	keys: list[str] = []
	for col in columns:
		name = str(col.get("name", ""))
		inferred = str(col.get("inferred_type", "")).lower()
		lname = name.lower()
		if inferred in {"string", "object", "text"} and any(
			token in lname for token in ("id", "name", "code", "key")
		):
			keys.append(name)

	if keys:
		return keys

	for col in columns:
		if str(col.get("inferred_type", "")).lower() in {"string", "object", "text"}:
			return [str(col.get("name"))]

	return [str(columns[0].get("name"))] if columns else []


def _pick_aggregation_column(columns: list[dict[str, Any]]) -> str:
	for preferred in ("double", "float"):
		for col in columns:
			if str(col.get("inferred_type", "")).lower() == preferred:
				return str(col.get("name"))

	for fallback in ("decimal", "number", "int", "integer", "long"):
		for col in columns:
			if str(col.get("inferred_type", "")).lower() == fallback:
				return str(col.get("name"))

	return str(columns[0].get("name")) if columns else "value"


def _pick_group_by_columns(columns: list[dict[str, Any]], key_columns: list[str]) -> list[str]:
	tokens = ("region", "category", "type", "date", "country", "state", "city", "month", "year")
	group_by: list[str] = []

	for col in columns:
		name = str(col.get("name", ""))
		inferred = str(col.get("inferred_type", "")).lower()
		lname = name.lower()
		if inferred in {"string", "date", "timestamp"} and any(token in lname for token in tokens):
			group_by.append(name)

	if group_by:
		return group_by

	if key_columns:
		return key_columns[:2]

	for col in columns:
		if str(col.get("inferred_type", "")).lower() in {"string", "date", "timestamp"}:
			return [str(col.get("name"))]

	return [str(columns[0].get("name"))] if columns else ["category"]


def _load_templates() -> Environment:
	return Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False)


def _code_to_ipynb_base64(code: str) -> str:
	"""Wrap Python code into a minimal valid ipynb JSON and return base64 payload."""
	notebook_obj = {
		"cells": [
			{
				"cell_type": "code",
				"execution_count": None,
				"metadata": {"language": "python"},
				"outputs": [],
				"source": [line + "\n" for line in code.splitlines()],
			}
		],
		"metadata": {
			"language_info": {
				"name": "python",
			}
		},
		"nbformat": 4,
		"nbformat_minor": 5,
	}
	ipynb_text = json.dumps(notebook_obj, ensure_ascii=False)
	return base64.b64encode(ipynb_text.encode("utf-8")).decode("utf-8")


async def _write_run_log(
	item_id: str,
	item_name: str,
	job_instance_id: str,
	status_payload: dict[str, Any],
	duration_seconds: int,
) -> None:
	if not SQL_ENDPOINT_URL:
		return

	status = str(status_payload.get("status", "Unknown")).replace("'", "''")
	failure_reason = str(status_payload.get("failureReason") or "").replace("'", "''")
	start_time_utc = str(status_payload.get("startTimeUtc") or "")
	end_time_utc = str(status_payload.get("endTimeUtc") or "")
	now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
	workspace_id = _require_workspace_id().replace("'", "''")

	item_id_sql = item_id.replace("'", "''")
	item_name_sql = item_name.replace("'", "''")
	job_id_sql = job_instance_id.replace("'", "''")

	sql = (
		"INSERT INTO pipeline_run_log "
		"(captured_at, workspace_id, item_id, item_name, job_instance_id, status, start_time_utc, end_time_utc, failure_reason, duration_seconds) VALUES "
		f"(TIMESTAMP '{now_utc}', '{workspace_id}', '{item_id_sql}', '{item_name_sql}', '{job_id_sql}', '{status}', "
		f"'{start_time_utc}', '{end_time_utc}', '{failure_reason}', {int(duration_seconds)})"
	)

	token = await get_token()
	headers = {
		"Authorization": f"Bearer {token}",
		"Content-Type": "application/json",
	}

	async with httpx.AsyncClient(timeout=60.0) as client:
		response = await client.post(SQL_ENDPOINT_URL, headers=headers, json={"statement": sql})

	if response.status_code < 200 or response.status_code >= 300:
		raise RuntimeError(
			"Failed writing run log to pipeline_run_log: "
			f"status={response.status_code}, body={response.text}"
		)


async def inspect_csv_schema(file_path: str) -> dict[str, Any]:
	"""Inspects a CSV file in the Fabric Lakehouse and returns column names and inferred data types."""
	client = _build_client()
	items = await client.list_items()
	control_lh = next(
		(
			item
			for item in items
			if str(item.get("type")) == "Lakehouse" and str(item.get("displayName")) == "control_lh"
		),
		None,
	)

	local_file = _resolve_local_csv(file_path)
	if not local_file:
		return {
			"columns": [],
			"row_count_estimate": 0,
			"message": (
				f"CSV '{file_path}' was not found on local disk. "
				"Please provide column names and types in schema_json."
			),
			"control_lakehouse_found": bool(control_lh),
			"control_lakehouse_id": (control_lh or {}).get("id"),
		}

	with local_file.open("r", encoding="utf-8") as f:
		csv_text = f.read()

	df = pd.read_csv(io.StringIO(csv_text))
	columns = [
		{"name": name, "inferred_type": _map_dtype_to_inferred(str(dtype))}
		for name, dtype in df.dtypes.items()
	]

	return {
		"columns": columns,
		"row_count_estimate": int(len(df)),
		"control_lakehouse_found": bool(control_lh),
		"control_lakehouse_id": (control_lh or {}).get("id"),
	}


async def create_medallion_notebooks(
	table_name: str,
	source_file: str,
	schema_json: str,
	lakehouse_id: str,
) -> dict[str, str]:
	"""Creates Bronze, Silver, and Gold PySpark notebooks in Fabric from templates based on a CSV schema."""
	client = _build_client()

	try:
		schema_obj = json.loads(schema_json)
	except json.JSONDecodeError as exc:
		raise ValueError(f"schema_json is not valid JSON: {exc}") from exc

	columns = schema_obj.get("columns", []) if isinstance(schema_obj, dict) else []
	if not columns:
		raise ValueError("schema_json must include a 'columns' array with name and inferred_type entries.")

	key_columns = _pick_key_columns(columns)
	aggregation_column = _pick_aggregation_column(columns)
	group_by_columns = _pick_group_by_columns(columns, key_columns)
	column_casts = {
		str(col.get("name")): _spark_cast_for(str(col.get("inferred_type", "string")))
		for col in columns
		if col.get("name")
	}

	env = _load_templates()

	bronze_template = env.get_template("bronze_notebook.py.j2")
	silver_template = env.get_template("silver_notebook.py.j2")
	gold_template = env.get_template("gold_notebook.py.j2")

	bronze_code = bronze_template.render(
		table_name=table_name,
		source_file=source_file,
		lakehouse_id=lakehouse_id,
		bronze_table_name=f"{table_name}_bronze",
	)

	silver_code = silver_template.render(
		table_name=table_name,
		source_file=source_file,
		lakehouse_id=lakehouse_id,
		bronze_table_name=f"{table_name}_bronze",
		silver_table_name=f"{table_name}_silver",
		key_columns=key_columns,
		column_casts=column_casts,
	)

	gold_code = gold_template.render(
		table_name=table_name,
		source_file=source_file,
		lakehouse_id=lakehouse_id,
		silver_table_name=f"{table_name}_silver",
		gold_table_name=f"{table_name}_gold",
		group_by_columns=group_by_columns,
		aggregation_column=aggregation_column,
	)

	bronze_encoded = _code_to_ipynb_base64(bronze_code)
	silver_encoded = _code_to_ipynb_base64(silver_code)
	gold_encoded = _code_to_ipynb_base64(gold_code)

	bronze_item = await client.create_notebook_with_definition(
		name=f"{table_name}_bronze_nb",
		description=f"Bronze notebook for {table_name}",
		base64_code=bronze_encoded,
	)
	bronze_notebook_id = str(bronze_item.get("id", ""))
	bronze_notebook_name = f"{table_name}_bronze_nb"
	await client.attach_lakehouse_to_notebook(
		bronze_notebook_id,
		bronze_notebook_name,
		lakehouse_id,
	)
	silver_item = await client.create_notebook_with_definition(
		name=f"{table_name}_silver_nb",
		description=f"Silver notebook for {table_name}",
		base64_code=silver_encoded,
	)
	silver_notebook_id = str(silver_item.get("id", ""))
	silver_notebook_name = f"{table_name}_silver_nb"
	await client.attach_lakehouse_to_notebook(
		silver_notebook_id,
		silver_notebook_name,
		lakehouse_id,
	)
	gold_item = await client.create_notebook_with_definition(
		name=f"{table_name}_gold_nb",
		description=f"Gold notebook for {table_name}",
		base64_code=gold_encoded,
	)
	gold_notebook_id = str(gold_item.get("id", ""))
	gold_notebook_name = f"{table_name}_gold_nb"
	await client.attach_lakehouse_to_notebook(
		gold_notebook_id,
		gold_notebook_name,
		lakehouse_id,
	)

	return {
		"bronze_id": bronze_notebook_id,
		"silver_id": silver_notebook_id,
		"gold_id": gold_notebook_id,
		"table_name": table_name,
	}


async def deploy_pipeline(
	pipeline_name: str,
	bronze_notebook_id: str,
	silver_notebook_id: str,
	gold_notebook_id: str,
) -> str:
	"""Creates a Fabric DataPipeline that runs Bronze, Silver, and Gold notebooks in sequence."""
	client = _build_client()
	workspace_id = _require_workspace_id()

	pipeline_json = {
		"name": pipeline_name,
		"properties": {
			"activities": [
				{
					"name": "Run_Bronze",
					"type": "TridentNotebook",
					"typeProperties": {
						"notebookId": bronze_notebook_id,
						"workspaceId": workspace_id,
					},
				},
				{
					"name": "Run_Silver",
					"type": "TridentNotebook",
					"dependsOn": [
						{
							"activity": "Run_Bronze",
							"dependencyConditions": ["Succeeded"],
						}
					],
					"typeProperties": {
						"notebookId": silver_notebook_id,
						"workspaceId": workspace_id,
					},
				},
				{
					"name": "Run_Gold",
					"type": "TridentNotebook",
					"dependsOn": [
						{
							"activity": "Run_Silver",
							"dependencyConditions": ["Succeeded"],
						}
					],
					"typeProperties": {
						"notebookId": gold_notebook_id,
						"workspaceId": workspace_id,
					},
				},
			],
		}
	}

	pipeline_item = await client.create_pipeline(pipeline_name)
	pipeline_id = str(pipeline_item.get("id", ""))
	if not pipeline_id:
		raise RuntimeError(f"Pipeline creation response missing id: {pipeline_item}")

	encoded_definition = base64.b64encode(json.dumps(pipeline_json).encode("utf-8")).decode("utf-8")
	status_code = await client.update_pipeline_definition(pipeline_id, encoded_definition)
	if status_code != 200:
		raise RuntimeError(f"Pipeline definition update failed. Expected 200, got {status_code}")

	return pipeline_id


async def run_notebook_tool(
	notebook_id: str,
	notebook_name: str,
	max_wait_seconds: int = 600,
) -> dict[str, Any]:
	"""Runs a specific Fabric notebook and waits for completion. Returns status and duration."""
	client = _build_client()
	_ = _require_lakehouse_id()

	started = time.monotonic()
	job_instance_id = await client.run_notebook(notebook_id)
	status_payload = await client.poll_until_done(notebook_id, job_instance_id, max_wait_seconds)
	duration_seconds = int(time.monotonic() - started)

	try:
		await _write_run_log(
			item_id=notebook_id,
			item_name=notebook_name,
			job_instance_id=job_instance_id,
			status_payload=status_payload,
			duration_seconds=duration_seconds,
		)
	except Exception:
		# Non-blocking logging failure.
		pass

	return {
		"status": status_payload.get("status"),
		"duration_seconds": duration_seconds,
		"job_instance_id": job_instance_id,
	}


async def get_run_status_tool(item_id: str, job_instance_id: str) -> dict[str, Any]:
	"""Gets the current status of a notebook or pipeline run."""
	client = _build_client()
	return await client.get_job_status(item_id, job_instance_id)


def register_stage2_tools(mcp: Any) -> None:
	@mcp.tool(
		name="inspect_csv_schema",
		description="Inspects a CSV file in the Fabric Lakehouse and returns column names and inferred data types.",
	)
	async def inspect_csv_schema_tool(file_path: str) -> dict[str, Any]:
		return await inspect_csv_schema(file_path)

	@mcp.tool(
		name="create_medallion_notebooks",
		description="Creates Bronze, Silver, and Gold PySpark notebooks in Fabric from templates based on a CSV schema.",
	)
	async def create_medallion_notebooks_tool(
		table_name: str,
		source_file: str,
		schema_json: str,
		lakehouse_id: str,
	) -> dict[str, str]:
		return await create_medallion_notebooks(table_name, source_file, schema_json, lakehouse_id)

	@mcp.tool(
		name="deploy_pipeline",
		description="Creates a Fabric DataPipeline that runs Bronze, Silver, and Gold notebooks in sequence.",
	)
	async def deploy_pipeline_tool(
		pipeline_name: str,
		bronze_notebook_id: str,
		silver_notebook_id: str,
		gold_notebook_id: str,
	) -> str:
		return await deploy_pipeline(pipeline_name, bronze_notebook_id, silver_notebook_id, gold_notebook_id)

	@mcp.tool(
		name="run_notebook_tool",
		description="Runs a specific Fabric notebook and waits for completion. Returns status and duration.",
	)
	async def run_notebook_stage2_tool(
		notebook_id: str,
		notebook_name: str,
		max_wait_seconds: int = 600,
	) -> dict[str, Any]:
		return await run_notebook_tool(notebook_id, notebook_name, max_wait_seconds)

	@mcp.tool(
		name="get_run_status_tool",
		description="Gets the current status of a notebook or pipeline run.",
	)
	async def get_run_status_stage2_tool(item_id: str, job_instance_id: str) -> dict[str, Any]:
		return await get_run_status_tool(item_id, job_instance_id)
