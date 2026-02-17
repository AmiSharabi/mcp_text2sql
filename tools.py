import os
import time
from typing import Any

from db_connection import get_connection
from guard import sanitize_and_validate_sql
from logger import log_event

_SCHEMA_CACHE: dict[str, Any] = {
    'expires_at': 0.0,
    'payload': None,
}


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f'Invalid integer environment variable: {name}') from exc
    if value <= 0:
        raise ValueError(f'Environment variable must be > 0: {name}')
    return value


def _rows_to_dicts(columns: list[str], rows: list[tuple]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row)) for row in rows]


def get_schema(trace_id: str) -> dict[str, Any]:
    started = time.perf_counter()
    log_event({'trace_id': trace_id, 'event_type': 'tool_start', 'tool_name': 'get_schema'})

    try:
        now = time.time()
        if _SCHEMA_CACHE['payload'] is not None and now < float(_SCHEMA_CACHE['expires_at']):
            payload = dict(_SCHEMA_CACHE['payload'])
            payload['agent_think_ms'] = int((time.perf_counter() - started) * 1000)
            log_event(
                {
                    'trace_id': trace_id,
                    'event_type': 'tool_ok',
                    'tool_name': 'get_schema',
                    'agent_think_ms': payload['agent_think_ms'],
                }
            )
            return payload

        sql = (
            'SELECT t.TABLE_TYPE, t.TABLE_SCHEMA, t.TABLE_NAME, '
            'c.COLUMN_NAME, c.DATA_TYPE '
            'FROM INFORMATION_SCHEMA.TABLES t '
            'INNER JOIN INFORMATION_SCHEMA.COLUMNS c '
            'ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME '
            "WHERE t.TABLE_TYPE IN ('BASE TABLE', 'VIEW') "
            'ORDER BY t.TABLE_TYPE, t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION'
        )

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()

        tables: dict[str, list[dict[str, str]]] = {}
        views: dict[str, list[dict[str, str]]] = {}

        for row in rows:
            table_type, schema_name, table_name, column_name, data_type = row
            key = f'{schema_name}.{table_name}'
            col = {'name': str(column_name), 'type': str(data_type)}
            if str(table_type).upper() == 'BASE TABLE':
                tables.setdefault(key, []).append(col)
            else:
                views.setdefault(key, []).append(col)

        payload = {
            'tables': tables,
            'views': views,
        }

        ttl = _int_env('SCHEMA_CACHE_SECONDS', 300)
        _SCHEMA_CACHE['payload'] = payload
        _SCHEMA_CACHE['expires_at'] = now + ttl

        payload_with_timing = dict(payload)
        payload_with_timing['agent_think_ms'] = int((time.perf_counter() - started) * 1000)

        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_ok',
                'tool_name': 'get_schema',
                'agent_think_ms': payload_with_timing['agent_think_ms'],
            }
        )
        return payload_with_timing
    except Exception as exc:
        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_error',
                'tool_name': 'get_schema',
                'error': str(exc),
            }
        )
        raise


def execute_readonly_sql(trace_id: str, sql: str) -> dict[str, Any]:
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'execute_readonly_sql',
            'sql_preview': sql,
        }
    )

    try:
        think_started = time.perf_counter()
        sql_safe = sanitize_and_validate_sql(sql)
        agent_think_ms = int((time.perf_counter() - think_started) * 1000)

        exec_started = time.perf_counter()
        timeout_s = _int_env('SQL_TIMEOUT_SECONDS', 10)

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.timeout = timeout_s
            cursor.execute(sql_safe)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description] if cursor.description else []

        row_dicts = _rows_to_dicts(columns, rows)
        sql_exec_ms = int((time.perf_counter() - exec_started) * 1000)

        result = {
            'sql': sql_safe,
            'rows': row_dicts,
            'row_count': len(row_dicts),
            'agent_think_ms': agent_think_ms,
            'sql_exec_ms': sql_exec_ms,
        }

        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_ok',
                'tool_name': 'execute_readonly_sql',
                'agent_think_ms': agent_think_ms,
                'sql_exec_ms': sql_exec_ms,
                'row_count': len(row_dicts),
                'sql_preview': sql_safe,
            }
        )
        return result
    except Exception as exc:
        current_time = time.perf_counter()
        agent_think_ms = int((current_time - think_started) * 1000) if 'think_started' in locals() else 0
        sql_exec_ms = int((current_time - exec_started) * 1000) if 'exec_started' in locals() else 0
        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_error',
                'tool_name': 'execute_readonly_sql',
                'agent_think_ms': agent_think_ms,
                'sql_exec_ms': sql_exec_ms,
                'error': str(exc),
                'sql_preview': locals().get('sql_safe', sql),
            }
        )
        raise


def explain_reasoning(trace_id: str, question: str, chosen_tables: list[str], sql: str) -> dict[str, Any]:
    started = time.perf_counter()
    log_event({'trace_id': trace_id, 'event_type': 'tool_start', 'tool_name': 'explain_reasoning'})

    try:
        sql_upper = sql.upper()
        aggregations: list[str] = []
        for fn_name in ('COUNT(', 'SUM(', 'AVG(', 'MIN(', 'MAX('):
            if fn_name in sql_upper:
                aggregations.append(fn_name[:-1])

        filters = 'Uses WHERE/HAVING filters.' if (' WHERE ' in sql_upper or ' HAVING ' in sql_upper) else 'No explicit filters.'
        joins = 'Uses JOIN relationships between selected tables.' if ' JOIN ' in sql_upper else 'No JOINs; single-source query.'
        limit_policy = 'Result size is bounded with TOP per policy.'

        response = {
            'interpretation': f'Question interpreted as: {question.strip()}',
            'tables_used': chosen_tables,
            'join_logic': joins,
            'filters': filters,
            'aggregations': aggregations if aggregations else ['none'],
            'limit_policy': limit_policy,
            'agent_think_ms': int((time.perf_counter() - started) * 1000),
        }

        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_ok',
                'tool_name': 'explain_reasoning',
                'agent_think_ms': response['agent_think_ms'],
            }
        )
        return response
    except Exception as exc:
        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_error',
                'tool_name': 'explain_reasoning',
                'error': str(exc),
            }
        )
        raise
