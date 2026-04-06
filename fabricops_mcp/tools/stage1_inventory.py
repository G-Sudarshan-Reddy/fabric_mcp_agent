from __future__ import annotations

import asyncio
import os
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import httpx

from fabricops_mcp.auth import get_token
from fabricops_mcp.fabric_client import FabricClient


ALLOWED_ITEM_TYPES = {"Lakehouse", "Notebook", "DataPipeline"}


def _format_items_markdown(items: list[dict[str, Any]]) -> str:
	header = "| Name | Type | Item ID |\n|---|---|---|"
	rows = []
	for item in items:
		name = str(item.get("displayName", ""))
		item_type = str(item.get("type", ""))
		item_id = str(item.get("id", ""))
		rows.append(f"| {name} | {item_type} | {item_id} |")
	return "\n".join([header, *rows])


async def write_inventory_snapshot(items: list[dict[str, Any]], workspace_id: str) -> None:
	"""
	Appends workspace item rows to the `workspace_inventory` Delta table.

	This uses a SQL endpoint REST execution pattern when configured:
	  - FABRIC_SQL_ENDPOINT_URL: SQL statement execution endpoint URL
		(example placeholder pattern: https://.../sql/statements)

	If endpoint configuration is missing, this function raises a readable error.
	"""
	sql_endpoint = os.getenv("FABRIC_SQL_ENDPOINT_URL")
	if not sql_endpoint:
		raise RuntimeError(
			"Inventory snapshot write skipped: FABRIC_SQL_ENDPOINT_URL is not configured. "
			"Set this to your Fabric SQL statement endpoint to enable writes to workspace_inventory."
		)

	captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
	token = await get_token()
	headers = {
		"Authorization": f"Bearer {token}",
		"Content-Type": "application/json",
	}

	values_sql_parts: list[str] = []
	for item in items:
		item_id = str(item.get("id", "")).replace("'", "''")
		item_type = str(item.get("type", "")).replace("'", "''")
		display_name = str(item.get("displayName", "")).replace("'", "''")
		item_name = display_name
		workspace_id_sql = workspace_id.replace("'", "''")
		values_sql_parts.append(
			"("  # noqa: ISC003
			f"TIMESTAMP '{captured_at}', "
			f"'{workspace_id_sql}', "
			f"'{item_id}', "
			f"'{item_type}', "
			f"'{item_name}', "
			f"'{display_name}'"
			")"
		)

	if not values_sql_parts:
		return

	sql = (
		"INSERT INTO workspace_inventory "
		"(captured_at, workspace_id, item_id, item_type, item_name, display_name) VALUES "
		+ ", ".join(values_sql_parts)
	)

	async with httpx.AsyncClient(timeout=60.0) as client:
		response = await client.post(sql_endpoint, headers=headers, json={"statement": sql})

	if response.status_code < 200 or response.status_code >= 300:
		raise RuntimeError(
			"Failed writing inventory snapshot to workspace_inventory: "
			f"status={response.status_code}, body={response.text}"
		)


def register_stage1_tools(mcp: Any) -> None:
	@mcp.tool(
		name="list_workspace_items",
		description=(
			"Lists all items in the FabricOps-Dev Fabric workspace and saves a snapshot "
			"to the Control Lakehouse inventory table."
		),
	)
	async def list_workspace_items() -> str:
		try:
			client = FabricClient()
			items = await client.list_items()

			if not items:
				return "Workspace is empty. No items found."

			markdown_table = _format_items_markdown(items)

			try:
				await write_inventory_snapshot(items, client.workspace_id)
				return markdown_table
			except Exception as write_err:
				return (
					f"{markdown_table}\n\n"
					f"> Warning: Items were listed, but snapshot write failed: {write_err}"
				)
		except Exception as err:
			return f"Could not list workspace items: {err}"

	@mcp.tool(
		name="create_fabric_item",
		description=(
			"Creates a new item in the FabricOps-Dev workspace. "
			"Supported types: Lakehouse, Notebook, DataPipeline."
		),
	)
	async def create_fabric_item(item_type: str, display_name: str) -> str:
		try:
			if item_type not in ALLOWED_ITEM_TYPES:
				allowed = ", ".join(sorted(ALLOWED_ITEM_TYPES))
				return (
					f"Invalid item_type '{item_type}'. "
					f"Supported values are: {allowed}."
				)

			if not re.fullmatch(r"[A-Za-z0-9_]+", display_name):
				return (
					"Invalid display_name. Use alphanumeric characters and underscores only (no spaces)."
				)

			client = FabricClient()
			created = await client.create_item(item_type, display_name)
			item_id = created.get("id") or created.get("itemId") or "unknown"

			await asyncio.sleep(10)
			return f"Created {item_type} '{display_name}' successfully. Item ID: {item_id}"
		except Exception as err:
			return f"Could not create Fabric item: {err}"

	@mcp.tool(
		name="describe_workspace",
		description=(
			"Returns a full markdown report of the FabricOps-Dev workspace — item counts "
			"by type, full item list, and a summary of recent pipeline runs."
		),
	)
	async def describe_workspace() -> str:
		try:
			client = FabricClient()
			items = await client.list_items()

			if not items:
				return "# Workspace Report\n\nWorkspace is empty. No items found."

			counts = Counter(str(item.get("type", "Unknown")) for item in items)
			counts_lines = ["## Item Counts by Type"]
			for item_type, count in sorted(counts.items()):
				counts_lines.append(f"- {item_type}: {count}")

			table = _format_items_markdown(items)

			pipelines = [i for i in items if str(i.get("type")) == "DataPipeline"]
			pipeline_lines = ["## Recent Pipeline Runs Summary"]
			if pipelines:
				pipeline_lines.append(
					"- Pipeline run history is not yet persisted in Stage 1. "
					"Use Stage 2 logging to populate `pipeline_run_log`."
				)
				pipeline_lines.append(f"- Pipelines currently in workspace: {len(pipelines)}")
			else:
				pipeline_lines.append("- No DataPipeline items currently present.")

			try:
				await write_inventory_snapshot(items, client.workspace_id)
				snapshot_note = ""
			except Exception as write_err:
				snapshot_note = (
					"\n\n> Warning: Inventory snapshot write failed while generating report: "
					f"{write_err}"
				)

			return (
				"# Workspace Report\n\n"
				+ "\n".join(counts_lines)
				+ "\n\n## Full Item List\n"
				+ table
				+ "\n\n"
				+ "\n".join(pipeline_lines)
				+ snapshot_note
			)
		except Exception as err:
			return f"Could not describe workspace: {err}"
