import os
import re
import time
import csv
import uuid
import io
import base64
from pathlib import Path
from typing import Any

from sqlalchemy import text

from src.db_connection import get_connection
from src.guard import sanitize_and_validate_sql
from src.logger import log_event

_SCHEMA_CACHE: dict[str, Any] = {
    'expires_at': 0.0,
    'payload': None,
}

_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _int_env(name: str, default: int) -> int:
    # Read a positive integer from environment variables with validation.
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f'Invalid integer environment variable: {name}') from exc
    if value <= 0:
        raise ValueError(f'Environment variable must be > 0: {name}')
    return value


def _validate_identifier(value: str, field_name: str) -> str:
    # Validate SQL identifier input (schema/table) against a safe pattern.
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field_name} is required.')
    candidate = value.strip()
    if not _IDENTIFIER_PATTERN.fullmatch(candidate):
        raise ValueError(f'Invalid {field_name}.')
    return candidate


def _rows_to_dicts(rows: list[Any]) -> list[dict[str, Any]]:
    # Convert SQLAlchemy row objects into plain dictionaries.
    return [dict(row._mapping) for row in rows]


def get_schema(trace_id: str, user_prompt: str | None = None) -> dict[str, Any]:
    # Return tables/views/relationships metadata, using short-lived cache.
    started = time.perf_counter()
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'get_schema',
            'user_prompt': user_prompt,
        }
    )

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
                    'user_prompt': user_prompt,
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
                'user_prompt': user_prompt,
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
                'user_prompt': user_prompt,
            }
        )
        raise


def execute_readonly_sql(trace_id: str, sql: str, user_prompt: str | None = None) -> dict[str, Any]:
    # Validate and execute a read-only SQL query and return rows plus timings.
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'execute_readonly_sql',
            'sql_preview': sql,
            'user_prompt': user_prompt or sql,
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
                'user_prompt': user_prompt or sql,
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
                'user_prompt': user_prompt or sql,
            }
        )
        raise


def preview_table(
    trace_id: str,
    table_name: str,
    schema_name: str = 'dbo',
    user_prompt: str | None = None,
) -> dict[str, Any]:
    # Return up to 5 rows from a validated schema.table for quick inspection.
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'preview_table',
            'sql_preview': f'{schema_name}.{table_name}',
            'user_prompt': user_prompt or f'preview {schema_name}.{table_name}',
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
                'user_prompt': user_prompt or f'preview {safe_schema}.{safe_table}',
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
                'user_prompt': user_prompt or f'preview {schema_name}.{table_name}',
            }
        )
        raise


def explain_reasoning(
    trace_id: str,
    question: str,
    chosen_tables: list[str],
    sql: str,
    user_prompt: str | None = None,
) -> dict[str, Any]:
    # Build a lightweight explanation of SQL intent from question/tables/query.
    started = time.perf_counter()
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'explain_reasoning',
            'user_prompt': user_prompt or question,
        }
    )

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
                'user_prompt': user_prompt or question,
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
                'user_prompt': user_prompt or question,
            }
        )
        raise


def _downloads_dir() -> Path:
    # Ensure and return the download directory path.
    path = Path(os.getenv('DOWNLOADS_DIR', 'logs/downloads'))
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_csv_filename(file_name: str | None) -> str:
    # Generate a safe CSV filename or fallback random result filename.
    if isinstance(file_name, str) and file_name.strip():
        base = re.sub(r'[^A-Za-z0-9._-]+', '_', file_name.strip())
        if not base.lower().endswith('.csv'):
            base += '.csv'
        return base
    return f'result_{uuid.uuid4().hex[:10]}.csv'


def _csv_text(columns: list[str], row_dicts: list[dict[str, Any]]) -> str:
    # Serialize query rows to CSV text with header row.
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns)
    writer.writeheader()
    for row in row_dicts:
        writer.writerow(row)
    return buffer.getvalue()


def download_readonly_sql_result(
    trace_id: str,
    sql: str,
    file_name: str | None = None,
    download_mode: str = 'link',
    user_prompt: str | None = None,
) -> dict[str, Any]:
    # Execute read-only SQL and return downloadable CSV as link or base64 content.
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'download_result',
            'sql_preview': sql,
            'download_mode': download_mode,
            'user_prompt': user_prompt or sql,
        }
    )

    try:
        mode = download_mode.strip().lower() if isinstance(download_mode, str) else ''
        if mode not in {'link', 'base64'}:
            raise ValueError('download_mode must be "link" or "base64".')

        think_started = time.perf_counter()
        sql_safe = sanitize_and_validate_sql(sql)
        agent_think_ms = int((time.perf_counter() - think_started) * 1000)

        exec_started = time.perf_counter()
        timeout_s = _int_env('SQL_TIMEOUT_SECONDS', 10)
        with get_connection() as conn:
            result = conn.execution_options(query_timeout=timeout_s).execute(text(sql_safe))
            columns = list(result.keys())
            rows = result.fetchall()
        sql_exec_ms = int((time.perf_counter() - exec_started) * 1000)

        row_dicts = _rows_to_dicts(rows)
        out_name = _safe_csv_filename(file_name)
        write_started = time.perf_counter()
        csv_text = _csv_text(columns, row_dicts)
        file_write_ms = 0

        relative_url = None
        download_url = None
        content_base64 = None
        if mode == 'link':
            out_file = _downloads_dir() / out_name
            out_file.write_text(csv_text, encoding='utf-8', newline='')
            file_write_ms = int((time.perf_counter() - write_started) * 1000)
            relative_url = f'/downloads/{out_name}'
            base = os.getenv('MCP_PUBLIC_BASE_URL', '').strip().rstrip('/')
            download_url = f'{base}{relative_url}' if base else relative_url
        else:
            content_base64 = base64.b64encode(csv_text.encode('utf-8')).decode('ascii')
            file_write_ms = int((time.perf_counter() - write_started) * 1000)

        response = {
            'sql': sql_safe,
            'row_count': len(row_dicts),
            'download_mode': mode,
            'file_name': out_name,
            'download_url': download_url,
            'download_path': relative_url,
            'content_base64': content_base64,
            'content_type': 'text/csv; charset=utf-8',
            'agent_think_ms': agent_think_ms,
            'sql_exec_ms': sql_exec_ms,
            'file_write_ms': file_write_ms,
        }

        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_ok',
                'tool_name': 'download_result',
                'agent_think_ms': agent_think_ms,
                'sql_exec_ms': sql_exec_ms,
                'row_count': len(row_dicts),
                'sql_preview': sql_safe,
                'download_mode': mode,
                'user_prompt': user_prompt or sql,
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
                'tool_name': 'download_result',
                'agent_think_ms': agent_think_ms,
                'sql_exec_ms': sql_exec_ms,
                'error': str(exc),
                'sql_preview': locals().get('sql_safe', sql),
                'download_mode': locals().get('mode', download_mode),
                'user_prompt': user_prompt or sql,
            }
        )
        raise
