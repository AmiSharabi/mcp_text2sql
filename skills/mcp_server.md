Implement `mcp_server.py` as the minimal server entrypoint that exposes the tools.

Requirements:
- Load env using python-dotenv.
- Start FastAPI server on MCP_HOST:MCP_PORT.
- Create endpoints that map to tools:
  - POST /tools/get_schema
  - POST /tools/execute_readonly_sql
  - POST /tools/explain_reasoning
- Ensure each request has a `trace_id`:
  - Read from header `x-trace-id` if provided; else generate UUID.
- Log:
  - request_start at entry
  - request_end with total_ms at exit
- Do not implement any UI.

Return JSON responses from tools unchanged.
