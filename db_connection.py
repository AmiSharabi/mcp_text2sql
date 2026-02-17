import pyodbc


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f'Missing required environment variable: {name}')
    return value


def get_connection() -> pyodbc.Connection:
    driver = _env('DB_DRIVER')
    host = _env('DB_HOST')
    port = _env('DB_PORT')
    db_name = _env('DB_NAME')
    db_user = _env('DB_USER')
    db_password = _env('DB_PASSWORD')
    encrypt = _env('DB_ENCRYPT')
    trust_cert = _env('DB_TRUST_SERVER_CERTIFICATE')

    conn_str = (
        f'DRIVER={{{driver}}};'
        f'SERVER={host},{port};'
        f'DATABASE={db_name};'
        f'UID={db_user};'
        f'PWD={db_password};'
        f'Encrypt={encrypt};'
        f'TrustServerCertificate={trust_cert};'
    )

    return pyodbc.connect(conn_str, autocommit=True)
