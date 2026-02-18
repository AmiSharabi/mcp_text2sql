import os
import re
import time
from typing import Any

from sqlalchemy import text

from db_connection import get_connection
from guard import sanitize_and_validate_sql
from logger import log_event

_SCHEMA_CACHE: dict[str, Any] = {
    'expires_at': 0.0,
    'payload': None,
}

_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f'Invalid integer environment variable: {name}') from exc
    if value <= 0:
        raise ValueError(f'Environment variable must be > 0: {name}')
    return value


def _validate_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field_name} is required.')
    candidate = value.strip()
    if not _IDENTIFIER_PATTERN.fullmatch(candidate):
        raise ValueError(f'Invalid {field_name}.')
    return candidate


def _rows_to_dicts(rows: list[Any]) -> list[dict[str, Any]]:
    return [dict(row._mapping) for row in rows]


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

        schema_sql = text(
            """
            SELECT
                t.TABLE_TYPE,
                t.TABLE_SCHEMA,
                t.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE
            FROM INFORMATION_SCHEMA.TABLES t
            INNER JOIN INFORMATION_SCHEMA.COLUMNS c
                ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND t.TABLE_NAME = c.TABLE_NAME
            WHERE t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            ORDER BY t.TABLE_TYPE, t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
            """
        )

        relationships_sql = text(
            """
            SELECT
                fk.name AS constraint_name,
                ps.name AS from_schema,
                pt.name AS from_table,
                pc.name AS from_column,
                rt.name AS from_data_type,
                rs.name AS to_schema,
                tt.name AS to_table,
                rc.name AS to_column,
                rt2.name AS to_data_type
            FROM sys.foreign_key_columns fkc
            INNER JOIN sys.foreign_keys fk ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables pt ON pt.object_id = fkc.parent_object_id
            INNER JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
            INNER JOIN sys.columns pc ON pc.object_id = pt.object_id AND pc.column_id = fkc.parent_column_id
            INNER JOIN sys.types rt ON rt.user_type_id = pc.user_type_id
            INNER JOIN sys.tables tt ON tt.object_id = fkc.referenced_object_id
            INNER JOIN sys.schemas rs ON rs.schema_id = tt.schema_id
            INNER JOIN sys.columns rc ON rc.object_id = tt.object_id AND rc.column_id = fkc.referenced_column_id
            INNER JOIN sys.types rt2 ON rt2.user_type_id = rc.user_type_id
            ORDER BY ps.name, pt.name, fk.name
            """
        )

        with get_connection() as conn:
            schema_rows = conn.execute(schema_sql).all()
            rel_rows = conn.execute(relationships_sql).all()

        tables: dict[str, list[dict[str, str]]] = {}
        views: dict[str, list[dict[str, str]]] = {}

        for row in schema_rows:
            record = row._mapping
            obj_key = f"{record['TABLE_SCHEMA']}.{record['TABLE_NAME']}"
            col = {'name': str(record['COLUMN_NAME']), 'type': str(record['DATA_TYPE'])}
            if str(record['TABLE_TYPE']).upper() == 'BASE TABLE':
                tables.setdefault(obj_key, []).append(col)
            else:
                views.setdefault(obj_key, []).append(col)

        relationships: list[dict[str, Any]] = []
        for row in rel_rows:
            record = row._mapping
            from_type = str(record['from_data_type'])
            to_type = str(record['to_data_type'])
            relationships.append(
                {
                    'constraint_name': str(record['constraint_name']),
                    'from_table': f"{record['from_schema']}.{record['from_table']}",
                    'from_column': str(record['from_column']),
                    'from_data_type': from_type,
                    'to_table': f"{record['to_schema']}.{record['to_table']}",
                    'to_column': str(record['to_column']),
                    'to_data_type': to_type,
                    'data_type_compatible': from_type.lower() == to_type.lower(),
                }
            )

        payload = {
            'tables': tables,
            'views': views,
            'relationships': relationships,
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
            result = conn.execution_options(query_timeout=timeout_s).execute(text(sql_safe))
            rows = result.fetchall()

        row_dicts = _rows_to_dicts(rows)
        sql_exec_ms = int((time.perf_counter() - exec_started) * 1000)

        response = {
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
        return response
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


def preview_table(trace_id: str, table_name: str, schema_name: str = 'dbo') -> dict[str, Any]:
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'preview_table',
            'sql_preview': f'{schema_name}.{table_name}',
        }
    )

    started = time.perf_counter()
    try:
        safe_schema = _validate_identifier(schema_name, 'schema_name')
        safe_table = _validate_identifier(table_name, 'table_name')
        sql = f'SELECT TOP (5) * FROM [{safe_schema}].[{safe_table}]'

        exec_started = time.perf_counter()
        timeout_s = _int_env('SQL_TIMEOUT_SECONDS', 10)
        with get_connection() as conn:
            result = conn.execution_options(query_timeout=timeout_s).execute(text(sql))
            rows = result.fetchall()

        row_dicts = _rows_to_dicts(rows)
        agent_think_ms = int((time.perf_counter() - started) * 1000)
        sql_exec_ms = int((time.perf_counter() - exec_started) * 1000)

        response = {
            'table': f'{safe_schema}.{safe_table}',
            'rows': row_dicts,
            'row_count': len(row_dicts),
            'agent_think_ms': agent_think_ms,
            'sql_exec_ms': sql_exec_ms,
        }

        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_ok',
                'tool_name': 'preview_table',
                'agent_think_ms': agent_think_ms,
                'sql_exec_ms': sql_exec_ms,
                'row_count': len(row_dicts),
                'sql_preview': sql,
            }
        )
        return response
    except Exception as exc:
        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_error',
                'tool_name': 'preview_table',
                'error': str(exc),
                'sql_preview': f'{schema_name}.{table_name}',
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
        limit_policy = 'Result size is bounded with TOP per policy and max table complexity.'

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