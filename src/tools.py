import os
import re
import time
import csv
import uuid
import io
import base64
from datetime import date, datetime
from numbers import Number
from pathlib import Path
from typing import Any

from sqlalchemy import text

from src.db_connection import get_connection, list_database_profiles, resolve_database_name
from src.guard import sanitize_and_validate_sql
from src.logger import log_event

_SCHEMA_CACHE: dict[str, dict[str, Any]] = {}

_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_WIDGET_ID_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_-]*$')


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


def _max_chart_points() -> int:
    # Return upper bound for chart points returned to clients.
    return _int_env('MAX_CHART_POINTS', 200)


def _max_dashboard_widgets() -> int:
    # Return upper bound for dashboard widgets in a single request.
    return _int_env('MAX_DASHBOARD_WIDGETS', 6)


def _max_table_rows() -> int:
    # Return upper bound for rows returned by dashboard table widgets.
    return _int_env('MAX_TABLE_ROWS', 50)


def _is_numeric(value: Any) -> bool:
    # Check whether value should be treated as a numeric measure.
    return isinstance(value, Number) and not isinstance(value, bool)


def _vega_type_for_field(rows: list[dict[str, Any]], field: str) -> str:
    # Infer Vega-Lite data type from observed values in a field.
    for row in rows:
        value = row.get(field)
        if value is None:
            continue
        if isinstance(value, (datetime, date)):
            return 'temporal'
        if _is_numeric(value):
            return 'quantitative'
        return 'nominal'
    return 'nominal'


def _first_numeric_field(
    columns: list[str],
    rows: list[dict[str, Any]],
    excluded: set[str] | None = None,
) -> str | None:
    # Pick first column that appears numeric in result rows.
    skip = excluded or set()
    for column in columns:
        if column in skip:
            continue
        for row in rows:
            value = row.get(column)
            if value is None:
                continue
            if _is_numeric(value):
                return column
            break
    return None


def _require_field_name(field: Any, field_name: str) -> str | None:
    # Normalize optional field names from input.
    if field is None:
        return None
    if not isinstance(field, str) or not field.strip():
        raise ValueError(f'{field_name} must be a non-empty string when provided.')
    return field.strip()


def _require_field_in_columns(field: str, columns: list[str], field_name: str) -> str:
    # Ensure selected field exists in query result columns.
    if field not in columns:
        raise ValueError(f'{field_name} "{field}" was not found in query result columns.')
    return field


def _require_numeric_field(field: str, rows: list[dict[str, Any]], field_name: str) -> str:
    # Ensure a field behaves as a quantitative column.
    if _vega_type_for_field(rows, field) != 'quantitative':
        raise ValueError(f'{field_name} "{field}" must be numeric.')
    return field


def _normalize_chart_type(chart_type: str) -> str:
    # Validate and normalize supported chart types.
    normalized = chart_type.strip().lower()
    allowed = {'bar', 'line', 'area', 'pie', 'scatter'}
    if normalized not in allowed:
        raise ValueError(f'Unsupported chart_type "{chart_type}". Use one of: {", ".join(sorted(allowed))}.')
    return normalized


def _trim_rows(rows: list[dict[str, Any]], max_rows: int) -> tuple[list[dict[str, Any]], bool]:
    # Trim row list to max_rows and report whether truncation occurred.
    if len(rows) <= max_rows:
        return rows, False
    return rows[:max_rows], True


def _schema_cache_entry(database_name: str) -> dict[str, Any]:
    # Return per-database schema cache entry.
    entry = _SCHEMA_CACHE.get(database_name)
    if entry is None:
        entry = {'expires_at': 0.0, 'payload': None}
        _SCHEMA_CACHE[database_name] = entry
    return entry


def _run_safe_select_query(sql: str, database: str | None = None) -> tuple[str, list[str], list[dict[str, Any]], int, int, str]:
    # Validate SQL then execute it and return safe SQL, columns, rows, timings, and selected DB profile.
    think_started = time.perf_counter()
    sql_safe = sanitize_and_validate_sql(sql)
    agent_think_ms = int((time.perf_counter() - think_started) * 1000)

    exec_started = time.perf_counter()
    timeout_s = _int_env('SQL_TIMEOUT_SECONDS', 10)
    selected_database = resolve_database_name(database)
    with get_connection(selected_database) as conn:
        result = conn.execution_options(query_timeout=timeout_s).execute(text(sql_safe))
        columns = list(result.keys())
        rows = result.fetchall()
    sql_exec_ms = int((time.perf_counter() - exec_started) * 1000)
    return sql_safe, columns, _rows_to_dicts(rows), agent_think_ms, sql_exec_ms, selected_database


def _chart_spec(
    chart_type: str,
    rows: list[dict[str, Any]],
    x_field: str,
    y_field: str,
    title: str | None = None,
    series_field: str | None = None,
) -> dict[str, Any]:
    # Build a Vega-Lite spec for common analytics chart types.
    x_type = _vega_type_for_field(rows, x_field)
    y_type = _vega_type_for_field(rows, y_field)
    mark_type = 'point' if chart_type == 'scatter' else chart_type
    base: dict[str, Any] = {
        '$schema': 'https://vega.github.io/schema/vega-lite/v5.json',
        'mark': {'type': mark_type},
        'encoding': {},
    }
    if title:
        base['title'] = title

    if chart_type == 'pie':
        base['mark'] = {'type': 'arc', 'innerRadius': 40}
        base['encoding'] = {
            'theta': {'field': y_field, 'type': 'quantitative'},
            'color': {'field': x_field, 'type': 'nominal'},
        }
        return base

    encoding: dict[str, Any] = {
        'x': {'field': x_field, 'type': x_type},
        'y': {'field': y_field, 'type': y_type},
    }
    if series_field:
        encoding['color'] = {'field': series_field, 'type': _vega_type_for_field(rows, series_field)}
    base['encoding'] = encoding
    return base


def _dashboard_widget_id(widget: dict[str, Any], index: int, widget_type: str) -> str:
    # Build a safe widget id or fallback generated id.
    raw = widget.get('id')
    if isinstance(raw, str):
        candidate = raw.strip()
        if candidate and _WIDGET_ID_PATTERN.fullmatch(candidate):
            return candidate
    return f'{widget_type}_{index}'


def _dashboard_chart_widget(
    index: int,
    widget: dict[str, Any],
    database: str | None = None,
) -> tuple[dict[str, Any], int, int]:
    # Build a dashboard chart widget payload.
    sql = widget.get('sql')
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError(f'Widget at index {index} missing non-empty "sql".')

    requested_chart_type = widget.get('chart_type', 'bar')
    if not isinstance(requested_chart_type, str):
        raise ValueError(f'Widget at index {index} has invalid "chart_type".')
    chart_type = _normalize_chart_type(requested_chart_type)
    title = widget.get('title')
    if title is not None and (not isinstance(title, str) or not title.strip()):
        raise ValueError(f'Widget at index {index} has invalid "title".')
    normalized_title = title.strip() if isinstance(title, str) else None

    sql_safe, columns, rows, think_ms, exec_ms, selected_database = _run_safe_select_query(sql, database=database)
    if not rows:
        raise ValueError(f'Widget at index {index} returned no rows for chart.')

    x_field = _require_field_name(widget.get('x_field'), 'x_field')
    y_field = _require_field_name(widget.get('y_field'), 'y_field')
    series_field = _require_field_name(widget.get('series_field'), 'series_field')
    if x_field is None:
        x_field = columns[0]
    x_field = _require_field_in_columns(x_field, columns, 'x_field')
    if y_field is None:
        y_field = _first_numeric_field(columns, rows, excluded={x_field})
    if y_field is None:
        raise ValueError(f'Widget at index {index} requires a numeric measure column for y_field.')
    y_field = _require_field_in_columns(y_field, columns, 'y_field')
    y_field = _require_numeric_field(y_field, rows, 'y_field')
    if series_field is not None:
        series_field = _require_field_in_columns(series_field, columns, 'series_field')

    points, truncated = _trim_rows(rows, _max_chart_points())
    spec = _chart_spec(
        chart_type=chart_type,
        rows=points,
        x_field=x_field,
        y_field=y_field,
        title=normalized_title,
        series_field=series_field,
    )

    widget_payload = {
        'id': _dashboard_widget_id(widget, index, 'chart'),
        'title': normalized_title or f'Chart {index}',
        'type': 'chart',
        'database': selected_database,
        'sql': sql_safe,
        'chart_type': chart_type,
        'x_field': x_field,
        'y_field': y_field,
        'series_field': series_field,
        'engine': 'vega-lite',
        'spec': spec,
        'dataset': {
            'columns': columns,
            'rows': points,
            'row_count': len(points),
            'row_count_total': len(rows),
            'truncated': truncated,
        },
        'agent_think_ms': think_ms,
        'sql_exec_ms': exec_ms,
    }
    return widget_payload, think_ms, exec_ms


def _dashboard_table_widget(
    index: int,
    widget: dict[str, Any],
    database: str | None = None,
) -> tuple[dict[str, Any], int, int]:
    # Build a dashboard table widget payload.
    sql = widget.get('sql')
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError(f'Widget at index {index} missing non-empty "sql".')

    title = widget.get('title')
    if title is not None and (not isinstance(title, str) or not title.strip()):
        raise ValueError(f'Widget at index {index} has invalid "title".')
    normalized_title = title.strip() if isinstance(title, str) else None

    requested_limit = widget.get('table_limit')
    if requested_limit is None:
        table_limit = _max_table_rows()
    elif isinstance(requested_limit, int) and requested_limit > 0:
        table_limit = min(requested_limit, _max_table_rows())
    else:
        raise ValueError(f'Widget at index {index} has invalid "table_limit".')

    sql_safe, columns, rows, think_ms, exec_ms, selected_database = _run_safe_select_query(sql, database=database)
    table_rows, truncated = _trim_rows(rows, table_limit)
    widget_payload = {
        'id': _dashboard_widget_id(widget, index, 'table'),
        'title': normalized_title or f'Table {index}',
        'type': 'table',
        'database': selected_database,
        'sql': sql_safe,
        'columns': columns,
        'rows': table_rows,
        'row_count': len(table_rows),
        'row_count_total': len(rows),
        'truncated': truncated,
        'agent_think_ms': think_ms,
        'sql_exec_ms': exec_ms,
    }
    return widget_payload, think_ms, exec_ms


def _dashboard_kpi_widget(
    index: int,
    widget: dict[str, Any],
    database: str | None = None,
) -> tuple[dict[str, Any], int, int]:
    # Build a dashboard KPI widget payload from query first row.
    sql = widget.get('sql')
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError(f'Widget at index {index} missing non-empty "sql".')

    title = widget.get('title')
    if title is not None and (not isinstance(title, str) or not title.strip()):
        raise ValueError(f'Widget at index {index} has invalid "title".')
    normalized_title = title.strip() if isinstance(title, str) else None

    sql_safe, columns, rows, think_ms, exec_ms, selected_database = _run_safe_select_query(sql, database=database)
    if not rows:
        raise ValueError(f'Widget at index {index} returned no rows for KPI.')

    value_field = _require_field_name(widget.get('value_field'), 'value_field')
    if value_field is not None:
        value_field = _require_field_in_columns(value_field, columns, 'value_field')
    else:
        value_field = _first_numeric_field(columns, rows)
    if value_field is None:
        raise ValueError(f'Widget at index {index} needs numeric "value_field" or numeric result column.')

    value = rows[0].get(value_field)
    widget_payload = {
        'id': _dashboard_widget_id(widget, index, 'kpi'),
        'title': normalized_title or f'KPI {index}',
        'type': 'kpi',
        'database': selected_database,
        'sql': sql_safe,
        'value_field': value_field,
        'value': value,
        'row_count_total': len(rows),
        'agent_think_ms': think_ms,
        'sql_exec_ms': exec_ms,
    }
    return widget_payload, think_ms, exec_ms


def get_schema(
    trace_id: str,
    user_prompt: str | None = None,
    database: str | None = None,
) -> dict[str, Any]:
    # Return tables/views/relationships metadata, using short-lived cache.
    started = time.perf_counter()
    selected_database = resolve_database_name(database)
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'get_schema',
            'user_prompt': user_prompt,
            'database': selected_database,
        }
    )

    try:
        now = time.time()
        cache_entry = _schema_cache_entry(selected_database)
        if cache_entry['payload'] is not None and now < float(cache_entry['expires_at']):
            payload = dict(cache_entry['payload'])
            payload['agent_think_ms'] = int((time.perf_counter() - started) * 1000)
            log_event(
                {
                    'trace_id': trace_id,
                    'event_type': 'tool_ok',
                    'tool_name': 'get_schema',
                    'agent_think_ms': payload['agent_think_ms'],
                    'user_prompt': user_prompt,
                    'database': selected_database,
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

        with get_connection(selected_database) as conn:
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
            'database': selected_database,
            'tables': tables,
            'views': views,
            'relationships': relationships,
        }

        ttl = _int_env('SCHEMA_CACHE_SECONDS', 300)
        cache_entry['payload'] = payload
        cache_entry['expires_at'] = now + ttl

        payload_with_timing = dict(payload)
        payload_with_timing['agent_think_ms'] = int((time.perf_counter() - started) * 1000)

        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_ok',
                'tool_name': 'get_schema',
                'agent_think_ms': payload_with_timing['agent_think_ms'],
                'user_prompt': user_prompt,
                'database': selected_database,
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
                'database': selected_database,
            }
        )
        raise


def execute_readonly_sql(
    trace_id: str,
    sql: str,
    user_prompt: str | None = None,
    database: str | None = None,
) -> dict[str, Any]:
    # Validate and execute a read-only SQL query and return rows plus timings.
    selected_database = resolve_database_name(database)
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'execute_readonly_sql',
            'sql_preview': sql,
            'user_prompt': user_prompt or sql,
            'database': selected_database,
        }
    )

    try:
        think_started = time.perf_counter()
        sql_safe = sanitize_and_validate_sql(sql)
        agent_think_ms = int((time.perf_counter() - think_started) * 1000)

        exec_started = time.perf_counter()
        timeout_s = _int_env('SQL_TIMEOUT_SECONDS', 10)

        with get_connection(selected_database) as conn:
            result = conn.execution_options(query_timeout=timeout_s).execute(text(sql_safe))
            rows = result.fetchall()

        row_dicts = _rows_to_dicts(rows)
        sql_exec_ms = int((time.perf_counter() - exec_started) * 1000)

        response = {
            'sql': sql_safe,
            'database': selected_database,
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
                'database': selected_database,
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
                'database': selected_database,
            }
        )
        raise


def preview_table(
    trace_id: str,
    table_name: str,
    schema_name: str = 'dbo',
    user_prompt: str | None = None,
    database: str | None = None,
) -> dict[str, Any]:
    # Return up to 5 rows from a validated schema.table for quick inspection.
    selected_database = resolve_database_name(database)
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'preview_table',
            'sql_preview': f'{schema_name}.{table_name}',
            'user_prompt': user_prompt or f'preview {schema_name}.{table_name}',
            'database': selected_database,
        }
    )

    started = time.perf_counter()
    try:
        safe_schema = _validate_identifier(schema_name, 'schema_name')
        safe_table = _validate_identifier(table_name, 'table_name')
        sql = f'SELECT TOP (5) * FROM [{safe_schema}].[{safe_table}]'

        exec_started = time.perf_counter()
        timeout_s = _int_env('SQL_TIMEOUT_SECONDS', 10)
        with get_connection(selected_database) as conn:
            result = conn.execution_options(query_timeout=timeout_s).execute(text(sql))
            rows = result.fetchall()

        row_dicts = _rows_to_dicts(rows)
        agent_think_ms = int((time.perf_counter() - started) * 1000)
        sql_exec_ms = int((time.perf_counter() - exec_started) * 1000)

        response = {
            'table': f'{safe_schema}.{safe_table}',
            'database': selected_database,
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
                'database': selected_database,
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
                'database': selected_database,
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


def build_chart(
    trace_id: str,
    sql: str,
    chart_type: str = 'bar',
    x_field: str | None = None,
    y_field: str | None = None,
    series_field: str | None = None,
    title: str | None = None,
    user_prompt: str | None = None,
    database: str | None = None,
) -> dict[str, Any]:
    # Execute SQL and return chart-ready structured content for UI rendering.
    selected_database = resolve_database_name(database)
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'build_chart',
            'sql_preview': sql,
            'user_prompt': user_prompt or sql,
            'database': selected_database,
        }
    )

    try:
        normalized_chart_type = _normalize_chart_type(chart_type)
        normalized_title = None
        if title is not None:
            if not isinstance(title, str) or not title.strip():
                raise ValueError('title must be a non-empty string when provided.')
            normalized_title = title.strip()

        sql_safe, columns, rows, agent_think_ms, sql_exec_ms, selected_database = _run_safe_select_query(
            sql,
            database=selected_database,
        )
        if not rows:
            raise ValueError('Chart query returned no rows.')

        selected_x = _require_field_name(x_field, 'x_field')
        selected_y = _require_field_name(y_field, 'y_field')
        selected_series = _require_field_name(series_field, 'series_field')

        if selected_x is None:
            selected_x = columns[0]
        selected_x = _require_field_in_columns(selected_x, columns, 'x_field')

        if selected_y is None:
            selected_y = _first_numeric_field(columns, rows, excluded={selected_x})
        if selected_y is None:
            raise ValueError('Chart requires a numeric measure column for y_field.')
        selected_y = _require_field_in_columns(selected_y, columns, 'y_field')
        selected_y = _require_numeric_field(selected_y, rows, 'y_field')

        if selected_series is not None:
            selected_series = _require_field_in_columns(selected_series, columns, 'series_field')

        chart_rows, truncated = _trim_rows(rows, _max_chart_points())
        spec = _chart_spec(
            chart_type=normalized_chart_type,
            rows=chart_rows,
            x_field=selected_x,
            y_field=selected_y,
            title=normalized_title,
            series_field=selected_series,
        )

        response = {
            'schema_version': '1.0',
            'result_type': 'chart',
            'title': normalized_title or f'{normalized_chart_type.title()} chart',
            'database': selected_database,
            'sql': sql_safe,
            'chart_type': normalized_chart_type,
            'x_field': selected_x,
            'y_field': selected_y,
            'series_field': selected_series,
            'chart': {
                'engine': 'vega-lite',
                'spec': spec,
            },
            'dataset': {
                'columns': columns,
                'rows': chart_rows,
                'row_count': len(chart_rows),
                'row_count_total': len(rows),
                'truncated': truncated,
            },
            'row_count': len(chart_rows),
            'agent_think_ms': agent_think_ms,
            'sql_exec_ms': sql_exec_ms,
        }

        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_ok',
                'tool_name': 'build_chart',
                'agent_think_ms': agent_think_ms,
                'sql_exec_ms': sql_exec_ms,
                'row_count': len(chart_rows),
                'sql_preview': sql_safe,
                'user_prompt': user_prompt or sql,
                'database': selected_database,
            }
        )
        return response
    except Exception as exc:
        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_error',
                'tool_name': 'build_chart',
                'error': str(exc),
                'sql_preview': locals().get('sql_safe', sql),
                'user_prompt': user_prompt or sql,
                'database': selected_database,
            }
        )
        raise


def build_dashboard(
    trace_id: str,
    widgets: list[dict[str, Any]],
    title: str | None = None,
    user_prompt: str | None = None,
    database: str | None = None,
) -> dict[str, Any]:
    # Execute dashboard widgets and return normalized dashboard structured content.
    started = time.perf_counter()
    selected_database = resolve_database_name(database)
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'build_dashboard',
            'user_prompt': user_prompt or title,
            'database': selected_database,
        }
    )

    try:
        if not isinstance(widgets, list) or not widgets:
            raise ValueError('widgets must be a non-empty list.')
        max_widgets = _max_dashboard_widgets()
        if len(widgets) > max_widgets:
            raise ValueError(f'widgets count exceeds limit ({max_widgets}).')

        normalized_title = 'Dashboard'
        if title is not None:
            if not isinstance(title, str) or not title.strip():
                raise ValueError('title must be a non-empty string when provided.')
            normalized_title = title.strip()

        kpis: list[dict[str, Any]] = []
        charts: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []
        layout: list[dict[str, str]] = []
        total_sql_exec_ms = 0

        for idx, widget in enumerate(widgets, start=1):
            if not isinstance(widget, dict):
                raise ValueError(f'Widget at index {idx} must be an object.')

            raw_type = widget.get('type', 'chart')
            if not isinstance(raw_type, str) or not raw_type.strip():
                raise ValueError(f'Widget at index {idx} has invalid "type".')
            widget_type = raw_type.strip().lower()

            if widget_type == 'chart':
                chart_widget, _, exec_ms = _dashboard_chart_widget(idx, widget, database=selected_database)
                charts.append(chart_widget)
                layout.append({'id': chart_widget['id'], 'type': 'chart'})
                total_sql_exec_ms += exec_ms
                continue

            if widget_type == 'table':
                table_widget, _, exec_ms = _dashboard_table_widget(idx, widget, database=selected_database)
                tables.append(table_widget)
                layout.append({'id': table_widget['id'], 'type': 'table'})
                total_sql_exec_ms += exec_ms
                continue

            if widget_type == 'kpi':
                kpi_widget, _, exec_ms = _dashboard_kpi_widget(idx, widget, database=selected_database)
                kpis.append(kpi_widget)
                layout.append({'id': kpi_widget['id'], 'type': 'kpi'})
                total_sql_exec_ms += exec_ms
                continue

            raise ValueError(f'Widget at index {idx} has unsupported type "{widget_type}".')

        response = {
            'schema_version': '1.0',
            'result_type': 'dashboard',
            'title': normalized_title,
            'database': selected_database,
            'widget_count': len(layout),
            'kpi_count': len(kpis),
            'chart_count': len(charts),
            'table_count': len(tables),
            'kpis': kpis,
            'charts': charts,
            'tables': tables,
            'layout': layout,
            'agent_think_ms': int((time.perf_counter() - started) * 1000),
            'sql_exec_ms': total_sql_exec_ms,
        }

        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_ok',
                'tool_name': 'build_dashboard',
                'agent_think_ms': response['agent_think_ms'],
                'sql_exec_ms': total_sql_exec_ms,
                'row_count': len(layout),
                'user_prompt': user_prompt or title,
                'database': selected_database,
            }
        )
        return response
    except Exception as exc:
        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_error',
                'tool_name': 'build_dashboard',
                'error': str(exc),
                'user_prompt': user_prompt or title,
                'database': selected_database,
            }
        )
        raise


def list_databases(trace_id: str, user_prompt: str | None = None) -> dict[str, Any]:
    # Return available DB profiles so clients can choose a database explicitly.
    started = time.perf_counter()
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'list_databases',
            'user_prompt': user_prompt or 'list databases',
        }
    )
    try:
        catalog = list_database_profiles()
        response = {
            'result_type': 'database_catalog',
            'default_database': catalog['default_database'],
            'databases': catalog['databases'],
            'count': len(catalog['databases']),
            'agent_think_ms': int((time.perf_counter() - started) * 1000),
        }
        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_ok',
                'tool_name': 'list_databases',
                'row_count': response['count'],
                'agent_think_ms': response['agent_think_ms'],
                'user_prompt': user_prompt or 'list databases',
            }
        )
        return response
    except Exception as exc:
        log_event(
            {
                'trace_id': trace_id,
                'event_type': 'tool_error',
                'tool_name': 'list_databases',
                'error': str(exc),
                'user_prompt': user_prompt or 'list databases',
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
    database: str | None = None,
) -> dict[str, Any]:
    # Execute read-only SQL and return downloadable CSV as link or base64 content.
    selected_database = resolve_database_name(database)
    log_event(
        {
            'trace_id': trace_id,
            'event_type': 'tool_start',
            'tool_name': 'download_result',
            'sql_preview': sql,
            'download_mode': download_mode,
            'user_prompt': user_prompt or sql,
            'database': selected_database,
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
        with get_connection(selected_database) as conn:
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
            'database': selected_database,
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
                'database': selected_database,
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
                'database': selected_database,
            }
        )
        raise
