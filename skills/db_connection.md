Implement SQL Server connection helper in `db_connection.py`:

- `get_connection() -> pyodbc.Connection`
  - Build an ODBC connection string using env vars:
    DB_DRIVER, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    DB_ENCRYPT, DB_TRUST_SERVER_CERTIFICATE
  - Use `autocommit=True`.
  - Do not print/log the connection string or secrets.

Optional:
- Provide `execute_query(sql: str, timeout_s: int) -> tuple[list[str], list[tuple]]` if helpful, but keep minimal.
