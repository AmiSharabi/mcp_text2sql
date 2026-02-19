# MCP Text2SQL PoC (Local + Cloud)

Read-only SQL Server MCP server with guardrails, logging, downloads, and LLM integration tests.

## Project Files
- `mcp_server.py`
  - FastAPI entrypoint
  - HTTP tool routes and `/mcp` JSON-RPC endpoint
  - API key auth (`MCP_API_KEY`)
  - Serves download files at `/downloads/*`
- `src/tools.py`
  - Tool implementations:
    - `get_schema`
    - `execute_readonly_sql`
    - `explain_reasoning`
    - `preview_table`
    - `download_result` (read-only SQL -> CSV file)
- `src/guard.py`
  - SQL safety rules:
    - single statement
    - SELECT-only
    - denylist for dangerous tokens
    - blocks `SELECT INTO`
    - enforces `TOP (SQL_MAX_ROWS)`
    - limits complexity with `SQL_MAX_TABLES`
- `src/db_connection.py`
  - SQLAlchemy + SQL Server connection builder from `.env`
- `src/logger.py`
  - JSONL event logging to `LOG_PATH`
  - truncates/sanitizes log fields (including `user_prompt`)
- `mcp_tools.json`
  - MCP tool definitions returned by `tools/list`
- `logs/export_logs_csv.py`
  - Converts `logs/events.jsonl` -> CSV
  - includes fixed `user_prompt` column and backfills per `trace_id`
- `requirements.txt`
  - Python dependencies
- `.env.example`
  - configuration template
- `test/test.ipynb`
  - regular project tests (guard, logger, db/tools, api, direct tool calls)
- `test/llm_test.ipynb`
  - LLM-focused tests
  - LangChain conversation with tool/function-calling
  - OpenAI SDK MCP test
- `logs/events.jsonl`
  - runtime event log
- `logs/downloads/`
  - CSV files created by `download_result`

## MCP Tools
- `get_schema`
- `execute_readonly_sql`
- `explain_reasoning`
- `preview_table`
- `download_result`
  - supports `download_mode`:
    - `link` (default): save CSV on MCP server and return URL
    - `base64`: return CSV content inline as base64 (no file save required by client)

## Main Endpoints
- `POST /mcp`
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
- set `MCP_PUBLIC_BASE_URL` if you want absolute download links
- configure `x-api-key` in your connector

## Logs and Export
- JSONL runtime log: `logs/events.jsonl`
- Export to CSV:
  - `venv\\Scripts\\python.exe logs/export_logs_csv.py`
  - optional: `--output logs/events_with_prompt.csv`

## Notes
- `download_result` with `download_mode=link` saves CSV on MCP server machine.
- `download_result` with `download_mode=base64` returns CSV content inline so your app can upload it to OpenAI Files if needed.
- To capture original activating prompt across tool events, pass `user_prompt` (or `x-user-prompt` header for `/mcp`).
