import os
import re

import sqlglot
from sqlglot import exp

_DENYLIST_PATTERN = re.compile(
    r'(?i)\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE|OPENROWSET|OPENDATASOURCE)\b|\b(sp_|xp_)',
)


def _sql_max_rows() -> int:
    # Read and validate maximum rows limit for SELECT TOP enforcement.
    raw = os.getenv('SQL_MAX_ROWS', '200')
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError('Invalid SQL_MAX_ROWS configuration.') from exc
    if value <= 0:
        raise ValueError('Invalid SQL_MAX_ROWS configuration.')
    return value


def _sql_max_tables() -> int:
    # Read and validate maximum table/complexity limit for query safety.
    raw = os.getenv('SQL_MAX_TABLES', '3')
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError('Invalid SQL_MAX_TABLES configuration.') from exc
    if value <= 0:
        raise ValueError('Invalid SQL_MAX_TABLES configuration.')
    return value


def _contains_multi_statement(sql: str) -> bool:
    # Detect whether input contains more than one SQL statement.
    stripped = sql.strip()
    if ';' in stripped[:-1]:
        return True

    try:
        statements = sqlglot.parse(stripped, read='tsql')
    except Exception as exc:
        raise ValueError('Invalid SQL syntax.') from exc
    return len(statements) != 1


def _is_select_root(node: exp.Expression) -> bool:
    # Check that parsed SQL root expression is a SELECT statement.
    return isinstance(node, exp.Select)


def _has_top_clause(sql: str) -> bool:
    # Check whether the SQL already includes a TOP clause.
    return re.search(r'(?i)\bSELECT\s+TOP\s*(\(|\d)', sql) is not None


def _inject_top_clause(sql: str, max_rows: int) -> str:
    # Inject TOP(max_rows) into the first SELECT when missing.
    return re.sub(r'(?i)\bSELECT\b', f'SELECT TOP ({max_rows})', sql, count=1)


def _table_key(table: exp.Table) -> str:
    # Build normalized table key for counting unique referenced tables.
    db = table.args.get('db')
    table_name = table.args.get('this')
    if table_name is None:
        return table.sql(dialect='tsql').lower()
    if db is not None:
        return f'{db.sql(dialect="tsql")}.{table_name.sql(dialect="tsql")}'.lower()
    return table_name.sql(dialect='tsql').lower()


def _enforce_query_limits(parsed: exp.Expression) -> None:
    # Enforce configured limits on table count and query complexity.
    max_tables = _sql_max_tables()
    tables = {_table_key(t) for t in parsed.find_all(exp.Table)}
    if len(tables) > max_tables:
        raise ValueError(f'Query exceeds max table limit ({max_tables}).')

    joins = len(list(parsed.find_all(exp.Join)))
    subqueries = len(list(parsed.find_all(exp.Subquery)))
    ctes = len(list(parsed.find_all(exp.CTE)))
    complexity = joins + subqueries + ctes
    if complexity > max_tables:
        raise ValueError(f'Query exceeds max complexity limit ({max_tables}).')


def sanitize_and_validate_sql(sql: str) -> str:
    # Validate read-only SQL safety rules and return sanitized executable SQL.
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError('SQL must be a non-empty string.')

    candidate = sql.strip()

    if _contains_multi_statement(candidate):
        raise ValueError('Only a single SQL statement is allowed.')

    if _DENYLIST_PATTERN.search(candidate):
        raise ValueError('Only read-only SELECT queries are allowed.')

    try:
        parsed = sqlglot.parse_one(candidate, read='tsql')
    except Exception as exc:
        raise ValueError('Invalid SQL syntax.') from exc

    if not _is_select_root(parsed):
        raise ValueError('Root statement must be SELECT.')

    if parsed.args.get('into') is not None:
        raise ValueError('SELECT INTO is not allowed.')

    _enforce_query_limits(parsed)

    safe_sql = candidate.rstrip(';').strip()
    if not _has_top_clause(safe_sql):
        safe_sql = _inject_top_clause(safe_sql, _sql_max_rows())

    return safe_sql
