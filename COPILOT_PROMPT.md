# FabricOps Agent — GitHub Copilot Developer Brief

> **Read this entire file before writing a single line of code.**
> This is your specification. Everything you build must follow this document exactly.
> Do not invent architecture. Do not add features not listed here. Ask before deviating.

---

## Your role and the team

| Role | Who | What they do |
|---|---|---|
| Project Manager | Sudarshan | Gives requirements, tests outputs, approves each stage |
| Solution Architect | Claude (AI) | Wrote this document. Defines what gets built and how |
| Senior Developer | You (GitHub Copilot) | Build exactly what is specified here. Nothing more, nothing less |

**Your job is to be a precise, careful developer — not an architect.**
When the spec says "use MSAL Device Code Flow", you use MSAL Device Code Flow.
When the spec says "do not create a Service Principal", you do not suggest a Service Principal.
Always confirm your understanding of a task before building it.

---

## Critical constraints — read before touching any code

### Licence limitation
- The Fabric workspace `1st Demo - Energy` runs on **Power BI Pro** — it **cannot** host Lakehouses, Notebooks, or Pipelines
- All project work happens in a **new workspace called `FabricOps-Dev`** running on a **Fabric Trial capacity**
- The trial workspace ID will be provided in `.env` — never hardcode it

### Authentication constraint
- **No Service Principal. No Azure AD App Registration.** These require IT admin access that is not available.
- Use **MSAL Device Code Flow** exclusively
- On first run, the server prints a URL + code to the terminal → user opens browser → logs in → token cached to `.token_cache.json`
- `.token_cache.json` must be in `.gitignore`
- Token scope: `https://api.fabric.microsoft.com/.default`

### Admin access
- Admin access to ONE workspace only: `FabricOps-Dev` (trial)
- No tenant admin, no Azure portal, no other workspaces
- Do not build any feature that requires cross-workspace access or tenant-level settings

---

## Repository structure — create exactly this

```
fabricops-agent/
├── ARCHITECTURE.md
├── COPILOT_PROMPT.md          ← this file
├── .env.example
├── .gitignore
├── requirements.txt
├── fabricops_mcp/
│   ├── __init__.py
│   ├── server.py
│   ├── auth.py
│   ├── fabric_client.py
│   └── tools/
│       ├── __init__.py
│       ├── stage1_inventory.py
│       ├── stage2_pipeline.py
│       └── stage3_ops.py
│   └── templates/
│       ├── bronze_notebook.py.j2
│       ├── silver_notebook.py.j2
│       └── gold_notebook.py.j2
├── control_lakehouse/
│   └── schema.sql
└── .vscode/
    └── mcp.json
```

---

## Environment variables

**`.env.example`** — create this file exactly:
```
FABRIC_WORKSPACE_ID=your-workspace-id-here
FABRIC_TENANT_ID=your-tenant-id-here
AZURE_OPENAI_ENDPOINT=your-azure-openai-endpoint-here
AZURE_OPENAI_KEY=your-azure-openai-key-here
TEAMS_WEBHOOK_URL=your-teams-incoming-webhook-url-here
```

**`.gitignore`** — must include:
```
.env
.token_cache.json
__pycache__/
*.pyc
.venv/
```

**`requirements.txt`**:
```
fastmcp>=0.9.0
msal>=1.24.0
httpx>=0.27.0
openai>=1.30.0
python-dotenv>=1.0.0
jinja2>=3.1.0
```

---

## Shared foundation — build first, used by all stages

### `fabricops_mcp/auth.py`

Build an async function `get_token()` that:
1. Loads tenant ID from environment variable `FABRIC_TENANT_ID`
2. Creates an `msal.PublicClientApplication` with a `SerializableTokenCache` backed by `.token_cache.json`
3. Attempts silent token acquisition first (from cache)
4. If no cached token, initiates Device Code Flow: prints the `user_code` and `verification_uri` to stdout
5. Waits for the user to complete browser login
6. Saves the updated cache to `.token_cache.json`
7. Returns the access token string

Scope: `["https://api.fabric.microsoft.com/.default"]`

### `fabricops_mcp/fabric_client.py`

Build an async class `FabricClient` using `httpx.AsyncClient`:
- Constructor takes `workspace_id: str` (from env var `FABRIC_WORKSPACE_ID`)
- Base URL: `https://api.fabric.microsoft.com/v1`
- All methods call `get_token()` from `auth.py` and set `Authorization: Bearer {token}` header
- All methods are `async`
- Raise descriptive exceptions on non-2xx responses — include status code and response body in the error message

Required methods (add more as stages require):
```python
async def list_items(self) -> list[dict]
    # GET /workspaces/{workspaceId}/items
    # Returns list of {id, type, displayName, workspaceId}

async def create_item(self, item_type: str, display_name: str) -> dict
    # POST /workspaces/{workspaceId}/items
    # Body: {"displayName": display_name, "type": item_type}
    # Returns created item dict

async def get_workspace_info(self) -> dict
    # GET /workspaces/{workspaceId}
    # Returns workspace metadata

async def create_notebook(self, display_name: str, base64_definition: str) -> dict
    # POST /workspaces/{workspaceId}/items
    # Body: {"displayName": display_name, "type": "Notebook", "definition": {"format": "ipynb", "parts": [{"path": "notebook-content.py", "payload": base64_definition, "payloadType": "InlineBase64"}]}}

async def run_job(self, item_id: str, job_type: str = "RunNotebook", params: dict = None) -> str
    # POST /workspaces/{workspaceId}/items/{itemId}/jobs/instances
    # Returns the operationId from Location header

async def get_job_status(self, item_id: str, operation_id: str) -> dict
    # GET /workspaces/{workspaceId}/items/{itemId}/jobs/instances/{operationId}
    # Returns {status, startTimeUtc, endTimeUtc, failureReason}

async def get_item_definition(self, item_id: str) -> dict
    # GET /workspaces/{workspaceId}/items/{itemId}/getDefinition
    # Returns item definition including notebook content

async def update_item_definition(self, item_id: str, base64_definition: str, item_type: str) -> dict
    # POST /workspaces/{workspaceId}/items/{itemId}/updateDefinition
    # Used for patching notebook cells
```

### `control_lakehouse/schema.sql`

Three Delta table DDL statements (to be run in a Fabric Notebook against the Control Lakehouse):

```sql
CREATE TABLE IF NOT EXISTS workspace_inventory (
    captured_at TIMESTAMP,
    workspace_id STRING,
    item_id STRING,
    item_type STRING,
    item_name STRING,
    display_name STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id STRING,
    pipeline_name STRING,
    stage STRING,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status STRING,
    rows_in LONG,
    rows_out LONG,
    error_message STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ops_audit_log (
    event_id STRING,
    timestamp TIMESTAMP,
    triggered_by STRING,
    action STRING,
    target_item STRING,
    outcome STRING,
    error_detail STRING,
    fix_applied STRING,
    fix_outcome STRING
) USING DELTA;
```

### `.vscode/mcp.json`

```json
{
  "servers": {
    "fabricops": {
      "type": "stdio",
      "command": "python",
      "args": ["-m", "fabricops_mcp.server"],
      "cwd": "${workspaceFolder}"
    }
  }
}
```

### `fabricops_mcp/server.py`

Create a FastMCP server:
```python
from fastmcp import FastMCP
from fabricops_mcp.tools.stage1_inventory import register_stage1_tools
from fabricops_mcp.tools.stage2_pipeline import register_stage2_tools
from fabricops_mcp.tools.stage3_ops import register_stage3_tools

mcp = FastMCP("FabricOps Agent")

register_stage1_tools(mcp)
register_stage2_tools(mcp)
register_stage3_tools(mcp)

if __name__ == "__main__":
    mcp.run()
```

Each tool module exports a `register_*_tools(mcp)` function that decorates and registers its tools onto the provided FastMCP instance.

---

## Stage 1 tools — `fabricops_mcp/tools/stage1_inventory.py`

### Tool: `list_workspace_items`

```
Description for Copilot: "Lists all items in the FabricOps-Dev Fabric workspace and saves a snapshot to the Control Lakehouse inventory table."
Parameters: none
Returns: markdown-formatted table string of all items (id, type, displayName)
Side effect: writes rows to workspace_inventory Delta table via Fabric SQL endpoint
```

Implementation notes:
- Call `FabricClient().list_items()`
- Format result as a markdown table with columns: Name, Type, Item ID
- Also call `write_inventory_snapshot(items)` which appends rows to workspace_inventory (implement this as a helper that calls the Fabric Load Table API or runs a SQL INSERT via the Lakehouse SQL endpoint)
- If the workspace has zero items, return: "Workspace is empty. No items found."

### Tool: `create_fabric_item`

```
Description: "Creates a new item in the FabricOps-Dev workspace. Supported types: Lakehouse, Notebook, DataPipeline."
Parameters:
  - item_type: str  (must be one of: "Lakehouse", "Notebook", "DataPipeline")
  - display_name: str  (alphanumeric and underscores only, no spaces)
Returns: confirmation string with the new item's ID
```

Implementation notes:
- Validate `item_type` against allowed list — return helpful error if invalid
- Call `FabricClient().create_item(item_type, display_name)`
- Return: f"Created {item_type} '{display_name}' successfully. Item ID: {item_id}"
- Wait 10 seconds after creation before returning (Lakehouse provisioning takes ~10s)

### Tool: `describe_workspace`

```
Description: "Returns a full markdown report of the FabricOps-Dev workspace — item counts by type, full item list, and a summary of recent pipeline runs."
Parameters: none
Returns: formatted markdown report string
```

Implementation notes:
- Call `list_workspace_items()` internally
- Group items by type and show counts
- Format as a clean markdown document with sections

---

## Stage 2 tools — `fabricops_mcp/tools/stage2_pipeline.py`

> Build Stage 2 only after PM signs off on Stage 1.

### Tool: `inspect_csv_schema`

```
Description: "Inspects a CSV file in the FabricOps-Dev Lakehouse and returns the column names and inferred data types."
Parameters:
  - lakehouse_id: str
  - file_path: str  (relative path within Files section, e.g. "raw/sales.csv")
Returns: JSON string with {"columns": [{"name": str, "inferred_type": str}]}
```

Implementation notes:
- Create a temporary Fabric Notebook via `create_fabric_item()`
- The notebook content runs: `df = spark.read.option("header","true").option("inferSchema","true").csv(f"Files/{file_path}")` then `print(df.dtypes)` as JSON
- Run the notebook, poll status until complete, extract output
- Delete the temporary notebook after extracting the result
- Parse and return the schema as a dict

### Tool: `create_notebook`

```
Description: "Creates a PySpark notebook in the FabricOps-Dev workspace from a template."
Parameters:
  - display_name: str
  - template_name: str  ("bronze", "silver", or "gold")
  - template_vars: dict  (variables to substitute into the Jinja2 template)
Returns: notebook item ID string
```

Implementation notes:
- Load the appropriate Jinja2 template from `fabricops_mcp/templates/`
- Render the template with `template_vars`
- Base64-encode the rendered Python string
- Call `FabricClient().create_notebook(display_name, base64_encoded_content)`
- Return the created notebook's item ID

### Notebook templates

**`templates/bronze_notebook.py.j2`**:
```python
# Bronze layer — raw ingestion
# Generated by FabricOps Agent for table: {{ table_name }}
from pyspark.sql.functions import current_timestamp, lit

source_path = "Files/{{ source_file }}"
target_table = "{{ bronze_table_name }}"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
df = df.withColumn("_ingested_at", current_timestamp())
df = df.withColumn("_source_file", lit("{{ source_file }}"))

row_count = df.count()
assert row_count > 0, f"Bronze load failed: zero rows read from {source_path}"

df.write.format("delta").mode("overwrite").saveAsTable(target_table)
print(f"Bronze complete: {row_count} rows written to {target_table}")
```

**`templates/silver_notebook.py.j2`**:
```python
# Silver layer — cleansing and validation
# Generated by FabricOps Agent for table: {{ table_name }}
from pyspark.sql.functions import col, trim

source_table = "{{ bronze_table_name }}"
target_table = "{{ silver_table_name }}"
key_columns = {{ key_columns }}

df = spark.read.format("delta").table(source_table)
df = df.dropDuplicates()
df = df.dropna(subset=key_columns)
{% for col_name, col_type in column_casts.items() %}
df = df.withColumn("{{ col_name }}", col("{{ col_name }}").cast("{{ col_type }}"))
{% endfor %}

row_count = df.count()
assert row_count > 0, f"Silver transform failed: zero rows after cleansing from {source_table}"

df.write.format("delta").mode("overwrite").saveAsTable(target_table)
print(f"Silver complete: {row_count} rows written to {target_table}")
```

**`templates/gold_notebook.py.j2`**:
```python
# Gold layer — aggregation and serving
# Generated by FabricOps Agent for table: {{ table_name }}
from pyspark.sql.functions import sum, count, avg, col

source_table = "{{ silver_table_name }}"
target_table = "{{ gold_table_name }}"
group_by_cols = {{ group_by_columns }}
agg_col = "{{ aggregation_column }}"

df = spark.read.format("delta").table(source_table)
gold_df = df.groupBy(group_by_cols).agg(
    count("*").alias("record_count"),
    sum(col(agg_col)).alias(f"total_{agg_col}"),
    avg(col(agg_col)).alias(f"avg_{agg_col}")
)

row_count = gold_df.count()
assert row_count > 0, f"Gold aggregation failed: zero rows output from {source_table}"

gold_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
print(f"Gold complete: {row_count} rows written to {target_table}")
```

### Tool: `deploy_pipeline`

```
Description: "Creates a Fabric DataPipeline that chains Bronze, Silver, and Gold notebooks in sequence."
Parameters:
  - pipeline_name: str
  - bronze_notebook_id: str
  - silver_notebook_id: str
  - gold_notebook_id: str
Returns: pipeline item ID string
```

Implementation notes:
- Build a Fabric Pipeline definition JSON with three Notebook activities in sequence
- Each activity's `dependsOn` points to the previous activity
- Create via `FabricClient().create_item("DataPipeline", pipeline_name)` then update its definition

### Tool: `run_notebook`

```
Description: "Runs a Fabric Notebook and waits for completion. Returns status and row counts if available."
Parameters:
  - notebook_id: str
  - max_wait_seconds: int  (default: 300)
Returns: dict with {status, duration_seconds, output_preview}
```

Implementation notes:
- Call `FabricClient().run_job(notebook_id)`
- Poll `get_job_status()` every 15 seconds until status is not "Running" or max_wait_seconds exceeded
- Write result to `pipeline_run_log` table
- Return structured result

### Tool: `get_run_status`

```
Description: "Gets the current status of a notebook or pipeline run."
Parameters:
  - item_id: str
  - run_id: str
Returns: dict with {status, started_at, ended_at, error_message}
```

---

## Stage 3 tools — `fabricops_mcp/tools/stage3_ops.py`

> Build Stage 3 only after PM signs off on Stage 2.

### Tool: `trigger_pipeline`

```
Description: "Triggers a named DataPipeline in the FabricOps-Dev workspace, waits for completion, and writes the result to the audit log."
Parameters:
  - pipeline_name: str
Returns: summary string with status, duration, and next steps if failed
```

Implementation notes:
- Call `list_workspace_items()` to find the pipeline ID by name
- Call `run_job()` on the pipeline
- Poll every 30 seconds until complete
- Call `write_audit_log()` with full details
- If failed: immediately call `get_notebook_error()` and include error summary in return value

### Tool: `get_notebook_error`

```
Description: "Extracts and categorises the error from a failed notebook run."
Parameters:
  - notebook_id: str
  - run_id: str
Returns: dict with {error_type, error_message, failing_cell_index, traceback, fix_recommendation}
```

Error type classification logic (implement as a function `classify_error(traceback: str) -> str`):
```python
if "AnalysisException" in tb and ("cannot resolve" in tb or "UNRESOLVED_COLUMN" in tb):
    return "schema_mismatch"
elif "NOT NULL constraint" in tb or "NullPointerException" in tb:
    return "null_constraint"
elif "FileNotFoundException" in tb or "Path does not exist" in tb:
    return "file_not_found"
elif "OutOfMemoryError" in tb or "GC overhead" in tb or "SparkOutOfMemoryError" in tb:
    return "spark_oom"
else:
    return "unknown"
```

### Tool: `patch_notebook_cell`

```
Description: "Replaces a specific cell in a Fabric Notebook with new PySpark code."
Parameters:
  - notebook_id: str
  - cell_index: int
  - new_code: str
Returns: confirmation string
```

Implementation notes:
- Call `FabricClient().get_item_definition(notebook_id)` to download current notebook content
- Base64-decode the notebook content to get the Python/ipynb source
- Replace the code in cell at `cell_index` with `new_code`
- Base64-encode the modified content
- Call `FabricClient().update_item_definition(notebook_id, new_base64, "Notebook")`
- Return confirmation with cell index and first 100 chars of new code

### Auto-fix dispatcher

Implement `auto_fix(error_type: str, error_context: dict, notebook_id: str) -> dict`:

```python
fix_strategies = {
    "schema_mismatch": fix_schema_mismatch,    # re-inspect schema, update .select() in bronze notebook cell 0
    "null_constraint": fix_null_constraint,     # add .dropna(subset=[failing_col]) in silver notebook cell 2
    "spark_oom":       fix_spark_oom,           # add .repartition(8) before failing transformation
    "unknown":         fix_with_llm,            # call Azure OpenAI, apply suggested fix
    "file_not_found":  fix_not_possible,        # return helpful message only
}
```

For `fix_with_llm`: call Azure OpenAI chat completions with this system prompt:
```
You are a PySpark expert. Given a notebook error traceback, return ONLY a single corrected Python code cell that fixes the error. Return raw Python code only, no markdown, no explanation.
```
User message: the full traceback from `get_notebook_error`.

### Tool: `retry_run`

```
Description: "Re-runs a notebook after a fix has been applied and reports the outcome."
Parameters:
  - notebook_id: str
  - previous_run_id: str
Returns: dict with {previous_status, new_status, fix_worked: bool}
```

### Tool: `write_audit_log`

```
Description: "Writes an entry to the ops_audit_log Delta table."
Parameters: full audit entry dict matching ops_audit_log schema
Returns: None (side effect only)
```

Implementation notes:
- Use the Fabric Load Table API or SQL Analytics Endpoint INSERT
- Always call this tool before returning from any Stage 3 tool that modifies Fabric state

### Tool: `send_teams_alert`

```
Description: "Sends a formatted alert card to a Microsoft Teams channel via incoming webhook."
Parameters:
  - pipeline_name: str
  - status: str
  - error_summary: str
  - fix_applied: str
Returns: "Alert sent" or error message
```

Implementation notes:
- Read `TEAMS_WEBHOOK_URL` from environment variables
- If not set, log a warning and return "Teams webhook not configured — skipping alert"
- POST a simple Adaptive Card JSON to the webhook URL
- Card should include: pipeline name, status (coloured), error summary, fix applied, timestamp

---

## Build order and sign-off gates

```
Step 1: Create repo structure and all empty files
Step 2: Build shared foundation (auth.py, fabric_client.py, schema.sql, server.py, mcp.json)
Step 3: Build Stage 1 tools → STOP and tell PM "Stage 1 ready for testing"
Step 4: Wait for PM sign-off on Stage 1
Step 5: Build Stage 2 tools and templates → STOP and tell PM "Stage 2 ready for testing"
Step 6: Wait for PM sign-off on Stage 2
Step 7: Build Stage 3 tools → STOP and tell PM "Stage 3 ready for testing"
Step 8: Wait for PM sign-off on Stage 3
```

**Never start the next stage until the current stage is signed off by Sudarshan.**

---

## How to start building — first prompt to use

After creating this file in your VS Code project, open GitHub Copilot Chat in Agent mode and paste:

```
You are a Senior Data and AI Engineer working on the FabricOps Agent project.
Read COPILOT_PROMPT.md in full before doing anything.
Then begin Step 1: create the complete repository structure with all empty files and placeholder comments.
Do not write any implementation code yet — just the structure.
Confirm when done and list every file you created.
```

---

## Useful reference links

- Fabric REST API docs: https://learn.microsoft.com/en-us/rest/api/fabric/
- FastMCP docs: https://github.com/jlowin/fastmcp
- MSAL Python docs: https://github.com/AzureAD/microsoft-authentication-library-for-python
- Fabric Notebook API: https://learn.microsoft.com/en-us/rest/api/fabric/notebook/
- Manage Lakehouse via API: https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-api

---

*Document prepared by Claude (Solution Architect) for Sudarshan Gopal (Project Manager)*
*Developer: GitHub Copilot*
*Version: 1.0 — aligned to Power BI Pro + Fabric Trial access constraints*
