# MCP Text2SQL PoC (Local + Cloud Tunnel)

Minimal read-only SQL Server MCP-style tool server with guardrails.

## Implemented
- `mcp_server.py` (FastAPI entrypoint)
- `tools.py` with exactly 3 tools:
  - `get_schema`
  - `execute_readonly_sql`
  - `explain_reasoning`
- `guard.py` strict SELECT-only validation/sanitization
- `db_connection.py` SQL Server connection helper
- `logger.py` JSONL logging to `LOG_PATH`

## Endpoints
- `POST /tools/get_schema`
- `POST /tools/execute_readonly_sql`
- `POST /tools/explain_reasoning`

Each request uses `x-trace-id` if provided, otherwise server generates a UUID.

## Security check
- Tool endpoints support API-key auth using env `MCP_API_KEY`.
- Send one of:
  - `x-api-key: <MCP_API_KEY>`
  - `Authorization: Bearer <MCP_API_KEY>`
- If `MCP_API_KEY` is empty, auth is disabled (local dev mode).

## Setup
1. Install dependencies in venv:
   - `python -m venv venv`
   - `venv\\Scripts\\python.exe -m pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and set values.
3. Run server:
   - `venv\\Scripts\\python.exe mcp_server.py`

## Cloud GPT access (public URL)
Cloud-hosted GPT cannot reach `127.0.0.1`. Expose your local server with HTTPS tunnel.

Example with ngrok:
1. Install ngrok and authenticate it.
2. Run tunnel to local port:
   - `ngrok http 8000`
3. Use the generated `https://<id>.ngrok-free.app` URL in your GPT/MCP configuration.
4. Configure auth header in your GPT/MCP client:
   - `x-api-key: <MCP_API_KEY>`

## Logging
- JSONL file at `logs/events.jsonl` by default.
- Events include request/tool start/end, timings, and row counts.
- Secrets are not logged.