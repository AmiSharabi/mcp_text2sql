import os
import re

import sqlglot
from sqlglot import exp

_DENYLIST_PATTERN = re.compile(
    r'(?i)\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE|OPENROWSET|OPENDATASOURCE)\b|\b(sp_|xp_)',
)


def _sql_max_rows() -> int:
    raw = os.getenv('SQL_MAX_ROWS', '200')
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError('Invalid SQL_MAX_ROWS configuration.') from exc
    if value <= 0:
        raise ValueError('Invalid SQL_MAX_ROWS configuration.')
    return value


def _contains_multi_statement(sql: str) -> bool:
    stripped = sql.strip()
    if ';' in stripped[:-1]:
        return True

    try:
        statements = sqlglot.parse(stripped, read='tsql')
    except Exception as exc:
        raise ValueError('Invalid SQL syntax.') from exc
    return len(statements) != 1


def _is_select_root(node: exp.Expression) -> bool:
    return isinstance(node, exp.Select)


def _has_top_clause(sql: str) -> bool:
    return re.search(r'(?i)\bSELECT\s+TOP\s*(\(|\d)', sql) is not None


def _inject_top_clause(sql: str, max_rows: int) -> str:
    return re.sub(r'(?i)\bSELECT\b', f'SELECT TOP ({max_rows})', sql, count=1)


def sanitize_and_validate_sql(sql: str) -> str:
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

    safe_sql = candidate.rstrip(';').strip()
    if not _has_top_clause(safe_sql):
        safe_sql = _inject_top_clause(safe_sql, _sql_max_rows())

    return safe_sql
