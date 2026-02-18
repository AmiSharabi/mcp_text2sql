from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine
from urllib.parse import quote_plus


_ENGINE: Engine | None = None


def _env(name: str, default: str | None = None) -> str:
    import os

    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f'Missing required environment variable: {name}')
    return value


def _build_connection_url() -> str:
    driver = _env('DB_DRIVER')
    host = _env('DB_HOST')
    db_name = _env('DB_NAME')
    db_user = _env('DB_USER')
    db_password = _env('DB_PASSWORD')
    encrypt = _env('DB_ENCRYPT')
    trust_cert = _env('DB_TRUST_SERVER_CERTIFICATE')

    # Support instance-based host (e.g., AMI02\AMI) and optional DB_PORT.
    server = host
    port = _env('DB_PORT', '').strip()
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
    return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}"


def create_sql_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(_build_connection_url(), pool_pre_ping=True, future=True)
    return _ENGINE


def connect() -> Connection:
    return create_sql_engine().connect()


# Backward compatibility for existing imports.
def get_engine() -> Engine:
    return create_sql_engine()


def get_connection() -> Connection:
    return connect()
