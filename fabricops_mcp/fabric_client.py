from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any
from uuid import uuid4

import httpx
from dotenv import load_dotenv

from fabricops_mcp.auth import get_token


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

logger = logging.getLogger(__name__)


class FabricClient:
	"""Async Fabric REST API client."""

	BASE_URL = "https://api.fabric.microsoft.com/v1"

	def __init__(self, workspace_id: str | None = None) -> None:
		self.workspace_id = workspace_id or os.getenv("FABRIC_WORKSPACE_ID")
		self.base_url = self.BASE_URL
		if not self.workspace_id:
			raise ValueError(
				"Missing workspace_id. Provide it explicitly or set FABRIC_WORKSPACE_ID in environment variables."
			)

	async def _request(
		self,
		method: str,
		path: str,
		*,
		json_body: dict[str, Any] | None = None,
	) -> httpx.Response:
		token = await get_token()
		headers = {
			"Authorization": f"Bearer {token}",
			"Content-Type": "application/json",
		}

		url = f"{self.BASE_URL}{path}"
		async with httpx.AsyncClient(timeout=60.0) as client:
			response = await client.request(method, url, headers=headers, json=json_body)

		if response.status_code < 200 or response.status_code >= 300:
			raise RuntimeError(
				f"Fabric API request failed: {method} {url} | "
				f"status={response.status_code} | body={response.text}"
			)

		return response

	async def list_items(self) -> list[dict[str, Any]]:
		"""GET /workspaces/{workspaceId}/items."""
		response = await self._request("GET", f"/workspaces/{self.workspace_id}/items")
		data = response.json()
		return data.get("value", data)

	async def create_item(self, item_type: str, display_name: str) -> dict[str, Any]:
		"""POST /workspaces/{workspaceId}/items."""
		body = {"displayName": display_name, "type": item_type}
		response = await self._request("POST", f"/workspaces/{self.workspace_id}/items", json_body=body)
		return response.json()

	async def get_workspace_info(self) -> dict[str, Any]:
		"""GET /workspaces/{workspaceId}."""
		response = await self._request("GET", f"/workspaces/{self.workspace_id}")
		return response.json()

	async def create_notebook(self, display_name: str, base64_definition: str) -> dict[str, Any]:
		"""Create a notebook item with inline base64 definition payload."""
		body = {
			"displayName": display_name,
			"type": "Notebook",
			"definition": {
				"format": "ipynb",
				"parts": [
					{
						"path": "notebook-content.py",
						"payload": base64_definition,
						"payloadType": "InlineBase64",
					}
				],
			},
		}
		response = await self._request("POST", f"/workspaces/{self.workspace_id}/items", json_body=body)
		return response.json()

	async def run_job(
		self,
		item_id: str,
		job_type: str = "RunNotebook",
		params: dict[str, Any] | None = None,
	) -> str:
		"""POST /workspaces/{workspaceId}/items/{itemId}/jobs/instances and return operation ID."""
		body: dict[str, Any] = {"jobType": job_type}
		if params:
			body["executionData"] = params

		response = await self._request(
			"POST",
			f"/workspaces/{self.workspace_id}/items/{item_id}/jobs/instances",
			json_body=body,
		)

		location = response.headers.get("Location") or response.headers.get("location")
		if not location:
			payload = response.json()
			operation_id = payload.get("operationId") or payload.get("id")
			if operation_id:
				return str(operation_id)
			raise RuntimeError("Run job succeeded but no operation ID found in response headers/body.")

		return location.rstrip("/").split("/")[-1]

	async def get_item_definition(self, item_id: str) -> dict[str, Any]:
		"""GET /workspaces/{workspaceId}/items/{itemId}/getDefinition."""
		response = await self._request("GET", f"/workspaces/{self.workspace_id}/items/{item_id}/getDefinition")
		return response.json()

	async def update_item_definition(
		self,
		item_id: str,
		base64_definition: str,
		item_type: str,
	) -> dict[str, Any]:
		"""POST /workspaces/{workspaceId}/items/{itemId}/updateDefinition."""
		body = {
			"type": item_type,
			"definition": {
				"format": "ipynb",
				"parts": [
					{
						"path": "notebook-content.py",
						"payload": base64_definition,
						"payloadType": "InlineBase64",
					}
				],
			},
		}
		response = await self._request(
			"POST",
			f"/workspaces/{self.workspace_id}/items/{item_id}/updateDefinition",
			json_body=body,
		)
		return response.json()

	async def create_notebook_with_definition(
		self,
		name: str,
		description: str,
		base64_code: str,
	) -> dict[str, Any]:
		"""
		POST /workspaces/{workspaceId}/notebooks with inline notebook definition.

		If the API returns 202, poll the Location URL every 10 seconds until
		the operation status is not Running, then return the created notebook item.
		"""
		platform_json = (
			'{"$schema":"https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",'
			f'"metadata":{{"type":"Notebook","displayName":{json.dumps(name)}}},'
			'"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}'
		)
		base64_platform = base64.b64encode(platform_json.encode("utf-8")).decode("utf-8")

		body = {
			"displayName": name,
			"description": description,
			"definition": {
				"format": "ipynb",
				"parts": [
					{
						"path": "notebook-content.py",
						"payload": base64_code,
						"payloadType": "InlineBase64",
					},
					{
						"path": ".platform",
						"payload": base64_platform,
						"payloadType": "InlineBase64",
					},
				],
			},
		}

		response = await self._request(
			"POST",
			f"/workspaces/{self.workspace_id}/notebooks",
			json_body=body,
		)

		if response.status_code != 202:
			payload = response.json()
			if isinstance(payload, dict) and payload.get("id"):
				return payload
			if isinstance(payload, dict) and isinstance(payload.get("item"), dict):
				item = payload["item"]
				if item.get("id"):
					return item
			# Fallback: return payload as-is for callers to inspect.
			return payload if isinstance(payload, dict) else {"value": payload}

		location = response.headers.get("Location") or response.headers.get("location")
		if not location:
			raise RuntimeError("Notebook creation returned 202 but no Location header was provided.")

		while True:
			operation_payload = await self._request_absolute("GET", location)
			status = str(operation_payload.get("status", ""))

			if status == "Running":
				await asyncio.sleep(10)
				continue

			if status in {"Succeeded", "Completed"}:
				for key in ("item", "result"):
					candidate = operation_payload.get(key)
					if isinstance(candidate, dict) and candidate.get("id"):
						return candidate

				if operation_payload.get("id"):
					return operation_payload

				notebooks = [
					item
					for item in await self.list_items()
					if str(item.get("type")) == "Notebook" and str(item.get("displayName")) == name
				]
				if notebooks:
					return notebooks[-1]

				raise RuntimeError(
					"Notebook creation completed but created item could not be resolved from operation payload."
				)

			raise RuntimeError(
				f"Notebook creation did not complete successfully. status={status}, payload={operation_payload}"
			)

	async def run_notebook(self, notebook_id: str) -> str:
		"""
		Trigger a notebook run using the correct jobType=RunNotebook endpoint.
		Returns the job_instance_id extracted from the Location response header.
		"""
		token = await get_token()
		url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"

		async with httpx.AsyncClient() as client:
			response = await client.post(
				url,
				headers={
					"Authorization": f"Bearer {token}",
					"Content-Type": "application/json"
				},
				timeout=30
			)

		if response.status_code not in (200, 202):
			raise RuntimeError(
				f"Failed to start notebook run. Status: {response.status_code}. "
				f"Body: {response.text}"
			)

		location = response.headers.get("Location", "")
		if not location:
			raise RuntimeError("Notebook run started but no Location header returned — cannot poll status.")

		# Extract job_instance_id from Location URL
		# Format: .../workspaces/{wsId}/items/{notebookId}/jobs/instances/{jobInstanceId}
		job_instance_id = location.rstrip("/").split("/")[-1]
		return job_instance_id

	async def get_job_status(self, item_id: str, job_instance_id: str) -> dict:
		token = await get_token()
		url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{item_id}/jobs/instances/{job_instance_id}"

		async with httpx.AsyncClient() as client:
			response = await client.get(
				url,
				headers={"Authorization": f"Bearer {token}"},
				timeout=30
			)

		if response.status_code != 200:
			raise RuntimeError(
				f"Failed to get job status. Status: {response.status_code}. Body: {response.text}"
			)

		return response.json()

	async def poll_until_done(
		self,
		item_id: str,
		job_instance_id: str,
		max_wait_seconds: int = 600,
	) -> dict[str, Any]:
		"""
		Poll job status every 15 seconds until terminal status or timeout.
		Terminal statuses: Completed, Failed, Cancelled.
		"""
		start = time.monotonic()
		terminal_statuses = {"Completed", "Failed", "Cancelled"}

		while True:
			status_payload = await self.get_job_status(item_id, job_instance_id)
			status = str(status_payload.get("status"))
			elapsed = int(time.monotonic() - start)
			print(f"Job status: {status} ({elapsed}s elapsed)")

			if status in terminal_statuses:
				return status_payload

			if elapsed >= max_wait_seconds:
				raise TimeoutError(
					f"Job did not complete within {max_wait_seconds} seconds. Last status: {status}"
				)

			await asyncio.sleep(15)

	async def create_pipeline(self, name: str) -> dict[str, Any]:
		"""
		POST /workspaces/{workspaceId}/items with DataPipeline type.
		If response is 202, poll Location until complete and return created item.
		"""
		body = {"displayName": name, "type": "DataPipeline"}
		response = await self._request(
			"POST",
			f"/workspaces/{self.workspace_id}/items",
			json_body=body,
		)

		if response.status_code != 202:
			payload = response.json()
			if isinstance(payload, dict) and payload.get("id"):
				return payload
			if isinstance(payload, dict) and isinstance(payload.get("item"), dict):
				item = payload["item"]
				if item.get("id"):
					return item
			return payload if isinstance(payload, dict) else {"value": payload}

		location = response.headers.get("Location") or response.headers.get("location")
		if not location:
			raise RuntimeError("Pipeline creation returned 202 but no Location header was provided.")

		while True:
			operation_payload = await self._request_absolute("GET", location)
			status = str(operation_payload.get("status", ""))

			if status == "Running":
				await asyncio.sleep(10)
				continue

			if status in {"Succeeded", "Completed"}:
				for key in ("item", "result"):
					candidate = operation_payload.get(key)
					if isinstance(candidate, dict) and candidate.get("id"):
						return candidate

				if operation_payload.get("id"):
					return operation_payload

				pipelines = [
					item
					for item in await self.list_items()
					if str(item.get("type")) == "DataPipeline" and str(item.get("displayName")) == name
				]
				if pipelines:
					return pipelines[-1]

				raise RuntimeError(
					"Pipeline creation completed but created item could not be resolved from operation payload."
				)

			raise RuntimeError(
				f"Pipeline creation did not complete successfully. status={status}, payload={operation_payload}"
			)

	async def update_pipeline_definition(self, item_id: str, base64_pipeline_json: str) -> int:
		"""
		POST /workspaces/{workspaceId}/items/{itemId}/updateDefinition for DataPipeline.
		Returns HTTP 200 on success.
		"""
		body = {
			"definition": {
				"parts": [
					{
						"path": "pipeline-content.json",
						"payload": base64_pipeline_json,
						"payloadType": "InlineBase64",
					}
				]
			}
		}

		response = await self._request(
			"POST",
			f"/workspaces/{self.workspace_id}/items/{item_id}/updateDefinition",
			json_body=body,
		)
		return response.status_code

	async def attach_lakehouse_to_notebook(self, notebook_id: str, notebook_name: str, lakehouse_id: str) -> None:
		"""
		Attaches a Lakehouse as the default for a notebook by updating
		the .platform definition part.
		"""
		import json, base64

		platform_config = {
			"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
			"metadata": {
				"type": "Notebook",
				"displayName": notebook_name
			},
			"config": {
				"version": "2.0",
				"logicalId": "00000000-0000-0000-0000-000000000000",
				"defaultLakehouse": {
					"known_lakehouses": [{"id": lakehouse_id}],
					"default_lakehouse": lakehouse_id,
					"default_lakehouse_name": "control_lh"
				}
			}
		}

		platform_b64 = base64.b64encode(
			json.dumps(platform_config).encode()
		).decode()

		token = await get_token()
		url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{notebook_id}/updateDefinition"

		async with httpx.AsyncClient() as client:
			response = await client.post(
				url,
				headers={
					"Authorization": f"Bearer {token}",
					"Content-Type": "application/json"
				},
				json={
					"definition": {
						"parts": [
							{
								"path": ".platform",
								"payload": platform_b64,
								"payloadType": "InlineBase64"
							}
						]
					}
				},
				timeout=30
			)

		if response.status_code not in (200, 202):
			raise RuntimeError(
				f"Failed to attach Lakehouse to notebook. "
				f"Status: {response.status_code}. Body: {response.text}"
			)

	async def get_notebook_definition(self, notebook_id: str) -> dict[str, Any]:
		"""
		POST /workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition
		No request body needed.

		If response is 202, poll Location header until complete.
		Decode the base64 payload from notebook-content.py or notebook-content.ipynb.
		Returns normalized structure with cells and raw content.
		"""
		response = await self._request(
			"POST",
			f"/workspaces/{self.workspace_id}/notebooks/{notebook_id}/getDefinition",
		)

		if response.status_code == 202:
			location = response.headers.get("Location") or response.headers.get("location")
			if not location:
				raise RuntimeError("Notebook getDefinition returned 202 but no Location header was provided.")

			operation_location = location
			while True:
				operation_payload = await self._request_absolute("GET", operation_location)
				status = str(operation_payload.get("status", ""))

				if status == "Running":
					await asyncio.sleep(3)
					continue

				if status in {"Succeeded", "Completed"}:
					payload = operation_payload
					break

				raise RuntimeError(
					f"Notebook getDefinition failed. status={status}, payload={operation_payload}"
				)

			definition = payload.get("definition") if isinstance(payload, dict) else None
			result = payload.get("result") if isinstance(payload, dict) else None
			if not isinstance(definition, dict) and not isinstance(result, dict):
				result_location = operation_location
				if not result_location.rstrip("/").endswith("/result"):
					result_location = result_location.rstrip("/") + "/result"
				payload = await self._request_absolute("GET", result_location)
		else:
			payload = response.json()

		definition = payload.get("definition") if isinstance(payload, dict) else None
		if not isinstance(definition, dict):
			result = payload.get("result") if isinstance(payload, dict) else None
			if isinstance(result, dict):
				definition = result.get("definition")

		if not isinstance(definition, dict):
			raise RuntimeError(f"Notebook definition payload missing definition object: {payload}")

		parts = definition.get("parts")
		if not isinstance(parts, list):
			raise RuntimeError(f"Notebook definition payload missing parts array: {payload}")

		target_part = next(
			(
				p
				for p in parts
				if isinstance(p, dict)
				and str(p.get("path")) in {"notebook-content.py", "notebook-content.ipynb"}
			),
			None,
		)
		if not target_part:
			raise RuntimeError("Could not find notebook-content.py or notebook-content.ipynb in definition parts.")

		payload_b64 = target_part.get("payload")
		if not isinstance(payload_b64, str) or not payload_b64:
			raise RuntimeError("Notebook content part payload is missing or empty.")

		content_path = str(target_part.get("path") or "")
		decoded_text = base64.b64decode(payload_b64).decode("utf-8")

		if content_path.endswith(".ipynb"):
			ipynb_obj = json.loads(decoded_text)
			cells = ipynb_obj.get("cells", []) if isinstance(ipynb_obj, dict) else []
			raw_ipynb = decoded_text
		else:
			# notebook-content.py fallback: represent as a single Python cell.
			py_lines = decoded_text.splitlines(keepends=True)
			cells = [
				{
					"cell_type": "code",
					"metadata": {"language": "python"},
					"source": py_lines,
				}
			]
			ipynb_obj = {
				"cells": cells,
				"metadata": {},
				"nbformat": 4,
				"nbformat_minor": 5,
			}
			raw_ipynb = json.dumps(ipynb_obj, ensure_ascii=False)

		return {
			"cells": cells if isinstance(cells, list) else [],
			"raw_ipynb": raw_ipynb,
			"raw_content": decoded_text,
			"content_path": content_path or "notebook-content.ipynb",
		}

	async def patch_notebook_cell(
		self,
		notebook_id: str,
		cell_index: int,
		new_source: str,
		notebook_name: str,
		lakehouse_id: str,
	) -> dict[str, Any]:
		"""
		Patch one notebook cell source and update definition while preserving .platform.
		Re-attaches the lakehouse after patching.
		"""
		current = await self.get_notebook_definition(notebook_id)
		raw_ipynb = current.get("raw_ipynb", "")
		if not isinstance(raw_ipynb, str) or not raw_ipynb:
			raise RuntimeError("Failed to retrieve notebook raw ipynb content for patching.")

		ipynb_obj = json.loads(raw_ipynb)
		cells = ipynb_obj.get("cells")
		if not isinstance(cells, list):
			raise RuntimeError("Notebook content does not include a valid cells array.")

		if cell_index < 0 or cell_index >= len(cells):
			raise IndexError(f"cell_index {cell_index} out of range (cells={len(cells)}).")

		if not isinstance(cells[cell_index], dict):
			raise RuntimeError(f"Target cell at index {cell_index} is not a valid cell object.")

		cells[cell_index]["source"] = [new_source]

		content_path = str(current.get("content_path") or "notebook-content.ipynb")
		if content_path.endswith(".py"):
			new_payload_text = "".join(cells[cell_index].get("source", []))
		else:
			new_payload_text = json.dumps(ipynb_obj, ensure_ascii=False)

		new_b64 = base64.b64encode(new_payload_text.encode("utf-8")).decode("utf-8")

		# Fetch full definition again to preserve .platform payload if present.
		definition_response = await self._request(
			"POST",
			f"/workspaces/{self.workspace_id}/notebooks/{notebook_id}/getDefinition",
		)

		if definition_response.status_code == 202:
			location = definition_response.headers.get("Location") or definition_response.headers.get("location")
			if not location:
				raise RuntimeError("Notebook getDefinition returned 202 but no Location header was provided.")
			operation_location = location
			while True:
				definition_payload = await self._request_absolute("GET", operation_location)
				status = str(definition_payload.get("status", ""))
				if status == "Running":
					await asyncio.sleep(3)
					continue
				if status in {"Succeeded", "Completed"}:
					break
				raise RuntimeError(
					f"Notebook getDefinition failed during patch. status={status}, payload={definition_payload}"
				)

			definition_obj = definition_payload.get("definition") if isinstance(definition_payload, dict) else None
			result_obj = definition_payload.get("result") if isinstance(definition_payload, dict) else None
			if not isinstance(definition_obj, dict) and not isinstance(result_obj, dict):
				result_location = operation_location
				if not result_location.rstrip("/").endswith("/result"):
					result_location = result_location.rstrip("/") + "/result"
				definition_payload = await self._request_absolute("GET", result_location)
		else:
			definition_payload = definition_response.json()

		definition_obj = definition_payload.get("definition") if isinstance(definition_payload, dict) else None
		if not isinstance(definition_obj, dict):
			result = definition_payload.get("result") if isinstance(definition_payload, dict) else None
			if isinstance(result, dict):
				definition_obj = result.get("definition")

		if not isinstance(definition_obj, dict):
			raise RuntimeError("Unable to read notebook definition for preserving .platform part.")

		existing_parts = definition_obj.get("parts") if isinstance(definition_obj, dict) else None
		if not isinstance(existing_parts, list):
			existing_parts = []

		platform_part = next(
			(
				p
				for p in existing_parts
				if isinstance(p, dict)
				and str(p.get("path")) == ".platform"
			),
			None,
		)

		parts: list[dict[str, Any]] = [
			{
				"path": content_path,
				"payload": new_b64,
				"payloadType": "InlineBase64",
			}
		]

		if isinstance(platform_part, dict) and platform_part.get("payload"):
			parts.append(
				{
					"path": ".platform",
					"payload": str(platform_part.get("payload")),
					"payloadType": str(platform_part.get("payloadType") or "InlineBase64"),
				}
			)

		token = await get_token()
		url = f"{self.base_url}/workspaces/{self.workspace_id}/notebooks/{notebook_id}/updateDefinition"

		async with httpx.AsyncClient() as client:
			update_response = await client.post(
				url,
				headers={
					"Authorization": f"Bearer {token}",
					"Content-Type": "application/json",
				},
				json={
					"definition": {
						"parts": parts,
					}
				},
				timeout=30,
			)

		if update_response.status_code not in (200, 202):
			raise RuntimeError(
				f"Failed to patch notebook cell. Status: {update_response.status_code}. "
				f"Body: {update_response.text}"
			)

		return {
			"patched_cell_index": cell_index,
			"new_source_preview": new_source[:100],
		}

	async def trigger_pipeline(self, pipeline_id: str) -> str:
		"""
		POST /workspaces/{workspaceId}/items/{pipelineId}/jobs/instances?jobType=Pipeline
		No request body. Returns job_instance_id from Location header.
		"""
		token = await get_token()
		url = (
			f"{self.base_url}/workspaces/{self.workspace_id}/items/"
			f"{pipeline_id}/jobs/instances?jobType=Pipeline"
		)

		async with httpx.AsyncClient() as client:
			response = await client.post(
				url,
				headers={
					"Authorization": f"Bearer {token}",
					"Content-Type": "application/json",
				},
				timeout=30,
			)

		if response.status_code not in (200, 202):
			raise RuntimeError(
				f"Failed to trigger pipeline run. Status: {response.status_code}. Body: {response.text}"
			)

		location = response.headers.get("Location", "")
		if not location:
			raise RuntimeError("Pipeline run started but no Location header returned — cannot poll status.")

		job_instance_id = location.rstrip("/").split("/")[-1]
		return job_instance_id

	async def write_audit_log_entry(
		self,
		triggered_by: str,
		action: str,
		target_item: str,
		outcome: str,
		error_detail: str = "",
		fix_applied: str = "",
		fix_outcome: str = "",
	) -> str:
		"""
		Write an audit log JSON file to local audit_log/ folder and return event_id.
		"""
		event_id = str(uuid4())
		timestamp_utc = datetime.now(timezone.utc).isoformat()

		audit_entry = {
			"event_id": event_id,
			"timestamp": timestamp_utc,
			"triggered_by": triggered_by,
			"action": action,
			"target_item": target_item,
			"outcome": outcome,
			"error_detail": error_detail,
			"fix_applied": fix_applied,
			"fix_outcome": fix_outcome,
		}

		audit_dir = _PROJECT_ROOT / "audit_log"
		audit_dir.mkdir(parents=True, exist_ok=True)

		timestamp_for_name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
		file_path = audit_dir / f"audit_{timestamp_for_name}_{event_id}.json"

		with file_path.open("w", encoding="utf-8") as f:
			json.dump(audit_entry, f, ensure_ascii=False, indent=2)

		return event_id

	async def send_teams_alert(
		self,
		webhook_url: str | None,
		pipeline_name: str,
		status: str,
		error_summary: str,
		fix_applied: str,
	) -> str:
		"""
		Send a Teams MessageCard alert and return status string.
		"""
		if not webhook_url:
			logger.warning("Teams webhook not configured — alert skipped")
			return "Teams webhook not configured — alert skipped"

		payload = {
			"@type": "MessageCard",
			"@context": "http://schema.org/extensions",
			"summary": "FabricOps Alert",
			"themeColor": "FF0000" if status == "failure" else "00FF00",
			"title": f"FabricOps Agent — {pipeline_name} {status.upper()}",
			"sections": [
				{
					"facts": [
						{"name": "Pipeline", "value": pipeline_name},
						{"name": "Status", "value": status},
						{"name": "Time (UTC)", "value": datetime.now(timezone.utc).isoformat()},
						{"name": "Error", "value": error_summary or "None"},
						{"name": "Fix applied", "value": fix_applied or "None"},
					]
				}
			],
		}

		try:
			async with httpx.AsyncClient() as client:
				response = await client.post(webhook_url, json=payload, timeout=30)
			if response.status_code < 200 or response.status_code >= 300:
				return f"Failed to send alert. Status: {response.status_code}. Body: {response.text}"
			return "Alert sent"
		except Exception as exc:
			return f"Failed to send alert: {exc}"

	async def _request_absolute(
		self,
		method: str,
		url: str,
		*,
		json_body: dict[str, Any] | None = None,
	) -> dict[str, Any]:
		"""Issue an authenticated request to an absolute URL and return JSON body."""
		token = await get_token()
		headers = {
			"Authorization": f"Bearer {token}",
			"Content-Type": "application/json",
		}

		async with httpx.AsyncClient(timeout=60.0) as client:
			response = await client.request(method, url, headers=headers, json=json_body)

		if response.status_code < 200 or response.status_code >= 300:
			raise RuntimeError(
				f"Fabric API request failed: {method} {url} | "
				f"status={response.status_code} | body={response.text}"
			)

		if not response.text:
			return {}

		try:
			payload = response.json()
		except Exception as exc:
			raise RuntimeError(f"Expected JSON response from {url}, got: {response.text}") from exc

		if isinstance(payload, dict):
			return payload

		return {"value": payload}
