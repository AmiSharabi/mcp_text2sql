# MCP Text2SQL PoC (Local + Cloud Tunnel)

Read-only SQL Server tool server with guardrails, built with FastAPI + SQLAlchemy.

## Implemented tools
- `get_schema`:
  - returns `tables`, `views`, and detected foreign-key `relationships` with data types
- `execute_readonly_sql`:
  - SELECT-only execution with guardrails
  - enforces `TOP` and max table/query complexity
- `explain_reasoning`
- `preview_table`:
  - returns first 5 rows for a requested table

## Endpoints
- `POST /tools/get_schema`
- `POST /tools/execute_readonly_sql`
- `POST /tools/explain_reasoning`
- `POST /tools/preview_table`

## SQL safety limits
- single statement only
- SELECT-only root query
- denylist for write/dangerous SQL keywords
- `SELECT INTO` blocked
- auto-add `TOP (SQL_MAX_ROWS)` when missing
- max query scope controlled by `SQL_MAX_TABLES` (tables/joins/subqueries/CTEs)

## Security check
- Tool endpoints support API-key auth via `MCP_API_KEY`.
- Send one of:
  - `x-api-key: <MCP_API_KEY>`
  - `Authorization: Bearer <MCP_API_KEY>`
- If `MCP_API_KEY` is empty, auth is disabled (local dev mode).

## Setup
1. `python -m venv venv`
2. `venv\\Scripts\\python.exe -m pip install -r requirements.txt`
3. Copy `.env.example` -> `.env` and set DB values.
4. Run: `venv\\Scripts\\python.exe mcp_server.py`

## Cloud GPT access
Cloud GPT cannot access `127.0.0.1` directly. Expose your local server through HTTPS tunnel (example ngrok):
1. `ngrok http 8000`
2. Use generated `https://...` URL in GPT/MCP connector.
3. Add auth header `x-api-key` with `MCP_API_KEY`.

## Logging
- JSONL: `logs/events.jsonl`
- CSV export: `venv\\Scripts\\python.exe export_logs_csv.py`