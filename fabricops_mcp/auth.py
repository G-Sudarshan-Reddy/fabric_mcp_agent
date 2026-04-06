from __future__ import annotations

from azure.identity import DefaultAzureCredential


async def get_token() -> str:
	"""Acquire a Fabric access token using Azure Identity default credential chain."""
	try:
		credential = DefaultAzureCredential()
		result = credential.get_token("https://api.fabric.microsoft.com/.default")
		return result.token
	except Exception as exc:
		raise RuntimeError(
			"Authentication failed. Please sign in via the Microsoft Fabric "
			"VS Code extension or run 'az login' in the terminal."
		) from exc