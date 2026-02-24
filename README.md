# MCP Text2SQL PoC (Local + Cloud)

Read-only SQL Server MCP server with guardrails, logging, downloads, and LLM integration tests.

## MCP Tools
- `get_schema`
- `execute_readonly_sql`
- `explain_reasoning`
- `preview_table`
- `download_result`
  - `download_mode=link` (default): save CSV on server, return URL
  - `download_mode=base64`: return CSV content inline as base64

## Main Endpoints
- `POST /mcp`
- `GET /sse`
- `POST /messages?session_id=<id>`
- `POST /tools/get_schema`
- `POST /tools/execute_readonly_sql`
- `POST /tools/explain_reasoning`
- `POST /tools/preview_table`
- `POST /tools/download_result`
- `GET /downloads/<file_name>.csv`

## Setup
1. `python -m venv venv`
2. `venv\\Scripts\\python.exe -m pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill values.
4. Run server: `venv\\Scripts\\python.exe mcp_server.py`

## Cloud Access
Cloud models cannot call `127.0.0.1` directly.
- expose local server using HTTPS tunnel (example: `ngrok http 8000`)
- for SSE-based MCP connectors, use `https://<public-host>/sse` as MCP server URL
- set `MCP_PUBLIC_BASE_URL` if you want absolute download links
- configure `x-api-key` in your connector

## Project Files
- `mcp_server.py`
  - thin compatibility entrypoint
  - re-exports `app` and `SSE_SESSIONS`
  - runs server via `main()`
- `src/mcp_app.py`
  - FastAPI app + all HTTP/SSE routes
  - mounts `/downloads/*`
- `src/mcp_runtime.py`
  - MCP JSON-RPC handler (`initialize`, `tools/list`, `tools/call`, etc.)
  - API key auth, trace/log helpers, SSE session store
- `src/mcp_models.py`
  - Pydantic request models for `/tools/*`
- `src/tools.py`
  - tool implementations:
    - `get_schema`
    - `execute_readonly_sql`
    - `explain_reasoning`
    - `preview_table`
    - `download_result` (read-only SQL -> CSV)
- `src/guard.py`
  - SQL safety rules:
    - single statement only
    - SELECT-only
    - denylist for dangerous tokens
    - blocks `SELECT INTO`
    - enforces `TOP (SQL_MAX_ROWS)`
    - limits complexity with `SQL_MAX_TABLES`
- `src/db_connection.py`
  - SQLAlchemy + SQL Server connection builder from `.env`
- `src/logger.py`
  - JSONL event logging to `LOG_PATH`
  - truncates/sanitizes fields (including `user_prompt`)
- `mcp_tools.json`
  - MCP tool definitions returned by `tools/list`
- `logs/export_logs_csv.py`
  - exports `logs/events.jsonl` to CSV
  - also exports a column guide (CSV + XLSX)
  - backfills missing `user_prompt` by `trace_id`
- `logs/events.jsonl`
  - runtime event log (original source)
- `logs/downloads/`
  - CSV files created by `download_result` in `link` mode

## Logs and Export
- JSONL runtime log (original): `logs/events.jsonl`
- Export logs with:
  - `venv\\Scripts\\python.exe logs/export_logs_csv.py`
- Optional custom output paths:
  - `--input logs/events.jsonl`
  - `--output logs/events.csv`
  - `--columns-guide-output logs/events_columns_guide.csv`
  - `--columns-guide-xlsx-output logs/events_columns_guide.xlsx`
- Export behavior:
  - does **not** modify the original `logs/events.jsonl`
  - writes new export files only
  - if a target file is locked, creates a new file name like `*_new1.csv`

## Notes
- To capture original activating prompt across tool events, pass `user_prompt` (or `x-user-prompt` header for `/mcp`).
