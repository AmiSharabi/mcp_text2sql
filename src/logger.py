import json
import math
import os
import re
import sys
import threading
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_LOG_ENGINE: Engine | None = None
_LOG_ENGINE_LOCK = threading.Lock()
_WARNED_KEYS: set[str] = set()

_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_SENSITIVE_KEYWORDS = ('password', 'secret', 'token', 'api_key', 'apikey', 'authorization')


def _now_utc() -> datetime:
    # Return current UTC timestamp as timezone-aware datetime.
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    # Return current UTC time in ISO-8601 format with Z suffix.
    return _now_utc().isoformat().replace('+00:00', 'Z')


def _truncate_sql_preview(value: str, max_len: int = 400) -> str:
    # Normalize whitespace and truncate SQL preview to safe log length.
    text = ' '.join(value.split())
    if len(text) <= max_len:
        return text
    return text[:max_len] + '...'


def _truncate_error(value: str, max_len: int = 2000) -> str:
    # Normalize whitespace and cap long error messages before persistence.
    text = ' '.join(value.split())
    if len(text) <= max_len:
        return text
    return text[:max_len] + '...'


def _is_sensitive_key(key: Any) -> bool:
    # Return True when a key name looks like secret-bearing data.
    key_str = str(key).strip().lower()
    if not key_str:
        return False
    return any(keyword in key_str for keyword in _SENSITIVE_KEYWORDS)


def _normalize_datetime(value: datetime) -> str:
    # Convert datetime to stable ISO-8601 UTC representation.
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def _json_compatible(value: Any) -> Any:
    # Convert nested values to JSON-safe primitives and redact sensitive keys.
    if value is None or isinstance(value, (str, bool, int)):
        return value

    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)

    if isinstance(value, Decimal):
        if not value.is_finite():
            return str(value)
        return int(value) if value == value.to_integral_value() else float(value)

    if isinstance(value, datetime):
        return _normalize_datetime(value)

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')

    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if _is_sensitive_key(key):
                continue
            result[str(key)] = _json_compatible(item)
        return result

    if isinstance(value, (list, tuple, set)):
        return [_json_compatible(item) for item in value]

    return str(value)


def _sanitize_event(event: dict[str, Any]) -> dict[str, Any]:
    # Remove sensitive fields and apply truncation before logging.
    clean_obj = _json_compatible(event)
    if not isinstance(clean_obj, dict):
        clean = {'value': clean_obj}
    else:
        clean = clean_obj

    if 'sql_preview' in clean and isinstance(clean['sql_preview'], str):
        clean['sql_preview'] = _truncate_sql_preview(clean['sql_preview'])
    if 'user_prompt' in clean and isinstance(clean['user_prompt'], str):
        clean['user_prompt'] = clean['user_prompt'].strip()
    if 'error' in clean and isinstance(clean['error'], str):
        clean['error'] = _truncate_error(clean['error'])

    return clean


def _env_non_empty(name: str) -> str | None:
    # Read environment variable and treat empty/blank values as missing.
    value = os.getenv(name)
    if value is None:
        return None
    if not value.strip():
        return None
    return value


def _env_prefer(primary: str, secondary: str | None = None, default: str | None = None) -> str:
    # Resolve env value by primary name, then secondary fallback, then default.
    primary_value = _env_non_empty(primary)
    if primary_value is not None:
        return primary_value

    if secondary is not None:
        secondary_value = _env_non_empty(secondary)
        if secondary_value is not None:
            return secondary_value

    if default is not None:
        return default

    if secondary is not None:
        raise ValueError(f'Missing required environment variable: {primary} (or fallback {secondary})')
    raise ValueError(f'Missing required environment variable: {primary}')


def _coerce_int(value: Any) -> int | None:
    # Convert arbitrary numeric-looking values to int when possible.
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _normalized_datetime(ts_iso: Any) -> datetime:
    # Parse event timestamp string and normalize to UTC datetime.
    if isinstance(ts_iso, str) and ts_iso.strip():
        raw = ts_iso.strip().replace('Z', '+00:00')
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return _now_utc()
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return _now_utc()


def _validate_identifier(name: str, field_name: str) -> str:
    # Validate SQL identifier used in dynamic object names.
    candidate = name.strip()
    if not _IDENTIFIER_PATTERN.fullmatch(candidate):
        raise ValueError(f'Invalid SQL identifier for {field_name}: {name!r}')
    return candidate


def _log_sink_mode() -> str:
    # Resolve logging sink mode from environment.
    mode = os.getenv('LOG_SINK', 'file').strip().lower()
    if mode in {'file', 'sql', 'both'}:
        return mode
    return 'file'


def _build_log_db_connection_url() -> str:
    # Build SQLAlchemy MSSQL URL for the logging database connection.
    driver = _env_prefer('LOG_DB_DRIVER', 'DB_DRIVER')
    host = _env_prefer('LOG_DB_HOST', 'DB_HOST')
    db_name = _env_prefer('LOG_DB_NAME', 'DB_NAME')
    db_user = _env_prefer('LOG_DB_USER', 'DB_USER')
    db_password = _env_prefer('LOG_DB_PASSWORD', 'DB_PASSWORD')
    encrypt = _env_prefer('LOG_DB_ENCRYPT', 'DB_ENCRYPT', 'yes')
    trust_cert = _env_prefer('LOG_DB_TRUST_SERVER_CERTIFICATE', 'DB_TRUST_SERVER_CERTIFICATE', 'yes')

    server = host
    port = _env_prefer('LOG_DB_PORT', 'DB_PORT', '').strip()
    if port and '\\' not in host:
        server = f'{host},{port}'

    odbc_conn_str = (
        f'DRIVER={{{driver}}};'
        f'SERVER={server};'
        f'DATABASE={db_name};'
        f'UID={db_user};'
        f'PWD={db_password};'
        f'Encrypt={encrypt};'
        f'TrustServerCertificate={trust_cert};'
    )
    return f'mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}'


def _get_log_engine() -> Engine:
    # Create and cache SQLAlchemy engine dedicated to logging sink.
    global _LOG_ENGINE
    with _LOG_ENGINE_LOCK:
        if _LOG_ENGINE is None:
            _LOG_ENGINE = create_engine(_build_log_db_connection_url(), pool_pre_ping=True, future=True)
        return _LOG_ENGINE


def _append_jsonl(log_path: str, safe_event: dict[str, Any]) -> None:
    # Append a single JSON event into JSONL file sink.
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(safe_event, ensure_ascii=True) + '\n')


def _sql_fallback_path() -> str:
    # Return fallback JSONL path for SQL logging failures.
    return os.getenv('LOG_SQL_FALLBACK_PATH', 'logs/events_sql_fallback.jsonl')


def _sql_table_target() -> tuple[str, str]:
    # Resolve target schema/table names for SQL event inserts.
    schema = _validate_identifier(os.getenv('LOG_DB_SCHEMA', 'mcp_logging'), 'LOG_DB_SCHEMA')
    table = _validate_identifier(os.getenv('LOG_DB_TABLE', 'events'), 'LOG_DB_TABLE')
    return schema, table


def _to_short_text(value: Any, max_len: int) -> str | None:
    # Normalize arbitrary values into bounded short text columns.
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    if len(text_value) <= max_len:
        return text_value
    return text_value[:max_len]


def _insert_sql_event(safe_event: dict[str, Any]) -> None:
    # Persist one log event row into SQL Server logging table.
    schema, table = _sql_table_target()
    payload_json = json.dumps(safe_event, ensure_ascii=True)
    ts_utc = _normalized_datetime(safe_event.get('ts_iso'))

    row = {
        'ts_utc': ts_utc,
        'trace_id': _to_short_text(safe_event.get('trace_id'), 64),
        'session_id': _to_short_text(safe_event.get('session_id'), 128),
        'request_id': _to_short_text(safe_event.get('request_id'), 128),
        'event_type': _to_short_text(safe_event.get('event_type'), 100) or 'unknown',
        'transport': _to_short_text(safe_event.get('transport'), 50),
        'method': _to_short_text(safe_event.get('method'), 120),
        'tool_name': _to_short_text(safe_event.get('tool_name'), 120),
        'download_mode': _to_short_text(safe_event.get('download_mode'), 20),
        'row_count': _coerce_int(safe_event.get('row_count')),
        'agent_think_ms': _coerce_int(safe_event.get('agent_think_ms')),
        'sql_exec_ms': _coerce_int(safe_event.get('sql_exec_ms')),
        'total_ms': _coerce_int(safe_event.get('total_ms')),
        'sql_preview': _to_short_text(safe_event.get('sql_preview'), 400),
        'user_prompt': _to_short_text(safe_event.get('user_prompt'), 1000),
        'error': _to_short_text(safe_event.get('error'), 2000),
        'payload_json': payload_json,
    }

    stmt = text(
        f"""
        INSERT INTO [{schema}].[{table}] (
            ts_utc,
            trace_id,
            session_id,
            request_id,
            event_type,
            transport,
            method,
            tool_name,
            download_mode,
            row_count,
            agent_think_ms,
            sql_exec_ms,
            total_ms,
            sql_preview,
            user_prompt,
            error,
            payload_json
        ) VALUES (
            :ts_utc,
            :trace_id,
            :session_id,
            :request_id,
            :event_type,
            :transport,
            :method,
            :tool_name,
            :download_mode,
            :row_count,
            :agent_think_ms,
            :sql_exec_ms,
            :total_ms,
            :sql_preview,
            :user_prompt,
            :error,
            :payload_json
        )
        """
    )
    with _get_log_engine().begin() as conn:
        conn.execute(stmt, row)


def _warn_once(key: str, message: str) -> None:
    # Print a warning once per key to avoid noisy repeated failures.
    with _LOG_ENGINE_LOCK:
        if key in _WARNED_KEYS:
            return
        _WARNED_KEYS.add(key)
    print(message, file=sys.stderr)


def log_event(event: dict[str, Any]) -> None:
    # Persist a sanitized log event to configured sink(s) without breaking requests.
    safe_event = _sanitize_event(event)
    if 'ts_iso' not in safe_event:
        safe_event['ts_iso'] = _now_iso()

    mode = _log_sink_mode()

    if mode in {'file', 'both'}:
        try:
            _append_jsonl(os.getenv('LOG_PATH', 'logs/events.jsonl'), safe_event)
        except Exception as exc:
            _warn_once('file_sink', f'Log file sink failed: {exc}')

    if mode in {'sql', 'both'}:
        try:
            _insert_sql_event(safe_event)
        except Exception as exc:
            _warn_once('sql_sink', f'SQL log sink failed, writing fallback JSONL: {exc}')
            try:
                fallback_event = dict(safe_event)
                fallback_event['logger_error'] = str(exc)
                _append_jsonl(_sql_fallback_path(), fallback_event)
            except Exception:
                pass
