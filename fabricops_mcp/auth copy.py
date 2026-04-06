

from __future__ import annotations

import os
from pathlib import Path

import msal

#Prompt:
# Rewrite fabricops_mcp/auth.py completely to use DefaultAzureCredential 
# from azure-identity instead of MSAL Device Code Flow.

# The new file should:

# 1. Import DefaultAzureCredential from azure.identity
# 2. Define an async get_token() function that:
#    - Creates a DefaultAzureCredential() instance
#    - Calls .get_token("https://api.fabric.microsoft.com/.default")
#    - Returns the token string (result.token)
#    - Wraps everything in try/except and raises a clear RuntimeError if 
#      it fails with message: 
#      "Authentication failed. Please sign in via the Microsoft Fabric 
#       VS Code extension or run 'az login' in the terminal."

# 3. Update requirements.txt:
#    - Remove: msal
#    - Add: azure-identity>=1.15.0

# 4. Delete .token_cache.json if it exists

# Show me the complete new auth.py file when done.

SCOPE = ["https://api.fabric.microsoft.com/.default"]


def _project_root() -> Path:
	# fabricops_mcp/auth.py -> project root is two levels up
	return Path(__file__).resolve().parent.parent


def _cache_file_path() -> Path:
	return _project_root() / ".token_cache.json"


def _load_cache() -> msal.SerializableTokenCache:
	cache = msal.SerializableTokenCache()
	cache_path = _cache_file_path()
	if cache_path.exists():
		cache.deserialize(cache_path.read_text(encoding="utf-8"))
	return cache


def _save_cache(cache: msal.SerializableTokenCache) -> None:
	if cache.has_state_changed:
		_cache_file_path().write_text(cache.serialize(), encoding="utf-8")


async def get_token() -> str:
	"""Acquire a Fabric access token using MSAL Device Code Flow with local cache."""
	tenant_id = os.getenv("FABRIC_TENANT_ID")
	if not tenant_id:
		raise ValueError("Missing required environment variable: FABRIC_TENANT_ID")

	authority = f"https://login.microsoftonline.com/{tenant_id}"
	cache = _load_cache()

	app = msal.PublicClientApplication(
		client_id="871c010f-5e61-4fb1-83ac-98610a7e9110",  # Microsoft Power BI / Fabric public client app
		authority=authority,
		token_cache=cache,
	)

	accounts = app.get_accounts()
	result = None
	if accounts:
		result = app.acquire_token_silent(SCOPE, account=accounts[0])

	if not result or "access_token" not in result:
		flow = app.initiate_device_flow(scopes=SCOPE)
		if "user_code" not in flow:
			raise RuntimeError(f"Failed to initiate device flow: {flow}")

		verification_uri = flow.get("verification_uri") or flow.get("verification_uri_complete")
		user_code = flow.get("user_code")

		print("\nFabric authentication required.")
		if verification_uri:
			print(f"Open this URL: {verification_uri}")
		print(f"Enter this code: {user_code}\n")

		result = app.acquire_token_by_device_flow(flow)

	_save_cache(cache)

	access_token = result.get("access_token") if isinstance(result, dict) else None
	if not access_token:
		error = result.get("error") if isinstance(result, dict) else "unknown_error"
		error_description = (
			result.get("error_description") if isinstance(result, dict) else "No error description provided"
		)
		raise RuntimeError(f"Token acquisition failed: {error} - {error_description}")

	return access_token
