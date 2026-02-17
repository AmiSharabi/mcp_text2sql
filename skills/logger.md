Implement JSONL logging utilities in `logger.py`:

Functions:
- `log_event(event: dict) -> None`
  - Append one JSON object per line to `LOG_PATH` (from env, default logs/events.jsonl).
  - Ensure logs directory exists.
  - Add `ts_iso` automatically if missing.

Fields conventions:
- Always: `ts_iso`, `trace_id`, `event_type`
- When relevant: `tool_name`, `agent_think_ms`, `sql_exec_ms`, `total_ms`, `row_count`, `sql_preview` (truncate to ~200 chars), `error`

Event types:
- request_start, request_end
- tool_start, tool_ok, tool_error

Do not log secrets (connection strings, passwords).
