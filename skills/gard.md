Implement strict read-only SQL guardrails in `guard.py`.

Expose:
- `sanitize_and_validate_sql(sql: str) -> str`

Rules:
1) Only allow a single SQL statement.
   - Reject multi-statement (e.g., extra semicolons or parser-based).
2) Denylist dangerous tokens (case-insensitive):
   INSERT, UPDATE, DELETE, MERGE, DROP, ALTER, CREATE, TRUNCATE,
   EXEC, EXECUTE, GRANT, REVOKE, sp_, xp_,
   OPENROWSET, OPENDATASOURCE
3) Parse SQL using `sqlglot.parse_one(..., read="tsql")` and ensure the root is SELECT.
4) Reject `SELECT INTO`.
5) Enforce TOP if missing:
   - Insert `TOP (SQL_MAX_ROWS)` right after SELECT.
6) Keep error messages short and safe (no secrets).

Return the sanitized SQL if valid; otherwise raise ValueError.
