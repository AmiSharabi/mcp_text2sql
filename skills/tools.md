Create a single `tools.py` file containing exactly 3 tools:

1) `get_schema(trace_id: str) -> dict`
   - Introspect schema using INFORMATION_SCHEMA:
     - Tables (BASE TABLE): schema.table -> columns(name, type)
     - Views: schema.view -> columns(name, type)
   - Implement in-memory cache with TTL = SCHEMA_CACHE_SECONDS.
   - Measure `agent_think_ms` for this tool and log events via logger.py.

2) `execute_readonly_sql(trace_id: str, sql: str) -> dict`
   - Split timing:
     - `agent_think_ms`: time spent validating/sanitizing via guard.py
     - `sql_exec_ms`: DB execution + fetching
   - Use SQL_TIMEOUT_SECONDS for DB timeout.
   - Ensure returned rows are limited (TOP already enforced).
   - Return:
     `{ "sql": sql_safe, "rows": [...], "row_count": N, "agent_think_ms": X, "sql_exec_ms": Y }`
   - Log tool_start/tool_ok/tool_error with row_count and timings.

3) `explain_reasoning(trace_id: str, question: str, chosen_tables: list[str], sql: str) -> dict`
   - Return a concise, structured explanation:
     - interpretation, tables_used, join_logic, filters, aggregations, limit_policy
   - No chain-of-thought; keep it short and factual.
   - Measure `agent_think_ms` for this tool and log events.

All tools must call logger.py and must never log secrets.
