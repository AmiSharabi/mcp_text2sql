import json
import os
import threading
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine


_ENGINE_CACHE: dict[str, Engine] = {}
_ENGINE_LOCK = threading.Lock()
_PROFILE_CACHE: tuple[dict[str, dict[str, str]], str] | None = None
_PROFILE_LOCK = threading.Lock()


def _env(name: str, default: str | None = None) -> str:
    # Read an environment variable and fail when required value is missing.
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f'Missing required environment variable: {name}')
    return value


def _optional_text(value: Any) -> str | None:
    # Normalize an optional scalar into trimmed string.
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    normalized = value.strip()
    return normalized or None


def _require_text(value: Any, field_name: str) -> str:
    # Require a non-empty text field in configuration.
    normalized = _optional_text(value)
    if normalized is None:
        raise ValueError(f'Missing required database config field: {field_name}')
    return normalized


def _catalog_path() -> Path:
    # Return configured DB catalog path (relative to project root when not absolute).
    project_root = Path(__file__).resolve().parent.parent
    raw = os.getenv('DB_CATALOG_PATH', 'db_catalog.json')
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path


def _env_default_profile() -> tuple[dict[str, dict[str, str]], str]:
    # Build fallback single-database profile from legacy DB_* environment variables.
    profile = {
        'driver': _env('DB_DRIVER'),
        'host': _env('DB_HOST'),
        'database': _env('DB_NAME'),
        'user': _env('DB_USER'),
        'password': _env('DB_PASSWORD'),
        'encrypt': _env('DB_ENCRYPT', 'yes'),
        'trust_server_certificate': _env('DB_TRUST_SERVER_CERTIFICATE', 'yes'),
        'port': _env('DB_PORT', '').strip(),
    }
    return {'default': profile}, 'default'


def _normalize_profile(name_hint: str | None, shared: dict[str, Any], raw: dict[str, Any]) -> tuple[str, dict[str, str]]:
    # Merge shared and per-database config and normalize required fields.
    merged: dict[str, Any] = dict(shared)
    merged.update(raw)
    if name_hint and 'name' not in merged:
        merged['name'] = name_hint

    name = _require_text(merged.get('name'), 'name')
    database_name = _optional_text(merged.get('database')) or _optional_text(merged.get('db_name'))
    host = _optional_text(merged.get('host')) or _optional_text(merged.get('server'))
    user = _optional_text(merged.get('user')) or _optional_text(merged.get('db_user')) or os.getenv('DB_USER')
    password = _optional_text(merged.get('password')) or _optional_text(merged.get('db_password')) or os.getenv(
        'DB_PASSWORD'
    )
    driver = _optional_text(merged.get('driver')) or os.getenv('DB_DRIVER')
    encrypt = _optional_text(merged.get('encrypt')) or os.getenv('DB_ENCRYPT', 'yes')
    trust = (
        _optional_text(merged.get('trust_server_certificate'))
        or _optional_text(merged.get('trustServerCertificate'))
        or os.getenv('DB_TRUST_SERVER_CERTIFICATE', 'yes')
    )
    port = _optional_text(merged.get('port')) or _optional_text(merged.get('db_port')) or os.getenv('DB_PORT', '')

    profile = {
        'driver': _require_text(driver, 'driver'),
        'host': _require_text(host, 'host'),
        'database': _require_text(database_name, 'database'),
        'user': _require_text(user, 'user'),
        'password': _require_text(password, 'password'),
        'encrypt': _require_text(encrypt, 'encrypt'),
        'trust_server_certificate': _require_text(trust, 'trust_server_certificate'),
        'port': (port or '').strip(),
    }
    return name, profile


def _resolve_default_name(
    requested: str | None,
    profiles: dict[str, dict[str, str]],
) -> str:
    # Resolve configured default profile name by profile key or database name.
    if not profiles:
        raise ValueError('No database profiles configured.')

    if requested is None:
        return next(iter(profiles.keys()))

    candidate = requested.strip().lower()
    for name in profiles:
        if name.lower() == candidate:
            return name
    for name, profile in profiles.items():
        if profile['database'].lower() == candidate:
            return name
    raise ValueError(f'Unknown default_database "{requested}".')


def _catalog_profiles(path: Path) -> tuple[dict[str, dict[str, str]], str]:
    # Load database profiles from catalog JSON.
    try:
        parsed = json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise ValueError(f'Invalid JSON in database catalog: {path}') from exc

    if not isinstance(parsed, dict):
        raise ValueError(f'Database catalog must be a JSON object: {path}')

    shared = parsed.get('shared', {})
    if shared is None:
        shared = {}
    if not isinstance(shared, dict):
        raise ValueError('"shared" in database catalog must be an object.')

    databases = parsed.get('databases')
    if not isinstance(databases, (list, dict)):
        raise ValueError('Database catalog must include "databases" as array or object.')

    profiles: dict[str, dict[str, str]] = {}
    if isinstance(databases, list):
        for idx, entry in enumerate(databases):
            if not isinstance(entry, dict):
                raise ValueError(f'Database catalog item at index {idx} must be an object.')
            name, profile = _normalize_profile(None, shared, entry)
            profiles[name] = profile
    else:
        for name_hint, entry in databases.items():
            if not isinstance(entry, dict):
                raise ValueError(f'Database catalog item "{name_hint}" must be an object.')
            name, profile = _normalize_profile(str(name_hint), shared, entry)
            profiles[name] = profile

    default_requested = _optional_text(parsed.get('default_database')) or _optional_text(parsed.get('default'))
    default_name = _resolve_default_name(default_requested, profiles)
    return profiles, default_name


def _load_profiles() -> tuple[dict[str, dict[str, str]], str]:
    # Load and cache active database profiles from catalog or legacy ENV.
    global _PROFILE_CACHE
    with _PROFILE_LOCK:
        if _PROFILE_CACHE is not None:
            return _PROFILE_CACHE

        path = _catalog_path()
        configured_catalog = _optional_text(os.getenv('DB_CATALOG_PATH'))
        if path.exists():
            profiles, default_name = _catalog_profiles(path)
        elif configured_catalog is not None:
            raise ValueError(f'Database catalog file not found: {path}')
        else:
            profiles, default_name = _env_default_profile()

        _PROFILE_CACHE = (profiles, default_name)
        return _PROFILE_CACHE


def reset_connection_caches() -> None:
    # Clear cached profiles and engines, mainly for tests or config reloads.
    global _PROFILE_CACHE
    with _ENGINE_LOCK:
        for engine in _ENGINE_CACHE.values():
            try:
                engine.dispose()
            except Exception:
                pass
        _ENGINE_CACHE.clear()
    with _PROFILE_LOCK:
        _PROFILE_CACHE = None


def resolve_database_name(database: str | None = None) -> str:
    # Resolve an optional database selector to a configured profile name.
    profiles, default_name = _load_profiles()
    if database is None:
        return default_name
    candidate = database.strip()
    if not candidate:
        return default_name

    candidate_l = candidate.lower()
    for name in profiles:
        if name.lower() == candidate_l:
            return name
    for name, profile in profiles.items():
        if profile['database'].lower() == candidate_l:
            return name
    available = ', '.join(sorted(profiles.keys()))
    raise ValueError(f'Unknown database "{database}". Available databases: {available}')


def list_database_profiles() -> dict[str, Any]:
    # Return safe metadata describing configured databases and default profile.
    profiles, default_name = _load_profiles()
    items = []
    for name, profile in profiles.items():
        items.append(
            {
                'name': name,
                'database': profile['database'],
                'host': profile['host'],
                'default': name == default_name,
            }
        )
    return {
        'default_database': default_name,
        'databases': items,
    }


def _build_connection_url(profile: dict[str, str]) -> str:
    # Build SQLAlchemy MSSQL/pyodbc connection URL from a profile.
    host = profile['host']
    server = host
    port = profile.get('port', '').strip()
    if port and '\\' not in host:
        server = f'{host},{port}'

    odbc_conn_str = (
        f"DRIVER={{{profile['driver']}}};"
        f'SERVER={server};'
        f"DATABASE={profile['database']};"
        f"UID={profile['user']};"
        f"PWD={profile['password']};"
        f"Encrypt={profile['encrypt']};"
        f"TrustServerCertificate={profile['trust_server_certificate']};"
    )
    return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}"


def create_sql_engine(database: str | None = None) -> Engine:
    # Create and cache one SQLAlchemy engine per configured database profile.
    profile_name = resolve_database_name(database)
    profiles, _ = _load_profiles()
    with _ENGINE_LOCK:
        engine = _ENGINE_CACHE.get(profile_name)
        if engine is None:
            engine = create_engine(_build_connection_url(profiles[profile_name]), pool_pre_ping=True, future=True)
            _ENGINE_CACHE[profile_name] = engine
        return engine


def connect(database: str | None = None) -> Connection:
    # Open and return a new DB connection from the selected shared engine.
    return create_sql_engine(database=database).connect()


# Backward compatibility for existing imports.
def get_engine(database: str | None = None) -> Engine:
    # Return engine for selected profile (compatibility alias).
    return create_sql_engine(database=database)


def get_connection(database: str | None = None) -> Connection:
    # Return DB connection for selected profile (compatibility alias).
    return connect(database=database)
