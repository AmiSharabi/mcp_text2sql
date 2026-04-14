# MCP Text2SQL PoC (Local + Cloud)

Read-only SQL Server MCP server with guardrails, logging, downloads, and LLM integration tests.

## MCP Tools
- `list_databases`
- `get_schema`
- `execute_readonly_sql`
- `explain_reasoning`
- `preview_table`
- `download_result`
  - `download_mode=link` (default): save CSV on server, return URL
  - `download_mode=base64`: return CSV content inline as base64
- `build_chart`
- `build_dashboard`

Most SQL tools accept optional `database` to select DB profile from `list_databases`.

## Main Endpoints
- `POST /mcp`
- `GET /sse`
- `POST /messages?session_id=<id>`
- `POST /tools/get_schema`
- `POST /tools/list_databases`
- `POST /tools/execute_readonly_sql`
- `POST /tools/explain_reasoning`
- `POST /tools/preview_table`
- `POST /tools/download_result`
- `POST /tools/build_chart`
- `POST /tools/build_dashboard`
- `GET /downloads/<file_name>.csv`

## Setup
1. `python -m venv venv`
2. `venv\\Scripts\\python.exe -m pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill values.
   - single DB: set `DB_*`
   - multi DB: set `DB_CATALOG_PATH` to an existing JSON file (see `db_catalog.example.json`)
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
    - `list_databases`
    - `get_schema`
    - `execute_readonly_sql`
    - `explain_reasoning`
    - `preview_table`
    - `download_result` (read-only SQL -> CSV)
    - `build_chart`
    - `build_dashboard`
- `src/guard.py`
  - SQL safety rules:
    - single statement only
    - SELECT-only
    - denylist for dangerous tokens
    - blocks `SELECT INTO`
    - enforces `TOP (SQL_MAX_ROWS)`
    - limits complexity with `SQL_MAX_TABLES`
- `src/db_connection.py`
  - SQLAlchemy + SQL Server connection layer
  - supports legacy single DB from `.env` and modular multi-DB via `DB_CATALOG_PATH`
- `db_catalog.example.json`
  - sample multi-DB catalog with shared credentials and per-database names
- `src/logger.py`
  - event logging sink (`file` / `sql` / `both`)
  - SQL sink writes to SQL Server table (default target: `mcp_logging.events`)
  - truncates/sanitizes fields (including `user_prompt`)
- `logs/sql/001_create_mcp_logging.sql`
  - SQL Server script to create logging schema/table/indexes/views in observability DB
- `logs/sql/002_mcp_logging_analysis_queries.sql`
  - ready-to-run SQL queries for traces/sessions/errors/performance analysis
- `logs/sql/000_create_mcp_observability_db.sql`
  - creates dedicated observability database (`McpObservability` by default)
- `logs/sql/003_mcp_logging_security_and_retention.sql`
  - creates least-privilege roles/users and retention procedure
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
- SQL log sink:
  - recommended setup order:
    1. run `logs/sql/000_create_mcp_observability_db.sql`
    2. run `logs/sql/001_create_mcp_logging.sql`
    3. run `logs/sql/003_mcp_logging_security_and_retention.sql`
  - scripts default to DB name `McpObservability` (edit `USE [...]` if you choose a different name)
  - if you do not have `CREATE DATABASE` permission, ask DBA to pre-create `McpObservability` then start from step 2
  - use dedicated DB + dedicated login for logging writes (least privilege)
  - set `.env`:
    - `LOG_SINK=sql` (SQL only) or `LOG_SINK=both` (SQL + JSONL)
    - set dedicated SQL log connection via `LOG_DB_*` (recommended)
      - `LOG_DB_NAME=McpObservability`
      - `LOG_DB_USER=<logging_writer_user>`
      - `LOG_DB_PASSWORD=<logging_writer_password>`
    - default table target: `LOG_DB_SCHEMA=mcp_logging`, `LOG_DB_TABLE=events`
  - retention:
    - schedule SQL Agent job to execute:
      - `EXEC mcp_logging.usp_purge_events_older_than_days @retention_days = 90, @batch_size = 5000;`
  - if SQL insert fails, logger writes fallback JSONL to `LOG_SQL_FALLBACK_PATH`
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
