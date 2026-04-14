import os
import unittest
from urllib.parse import unquote_plus

from src import logger


class TestLoggerEnvResolution(unittest.TestCase):
    _ENV_KEYS = [
        'DB_DRIVER',
        'DB_HOST',
        'DB_NAME',
        'DB_USER',
        'DB_PASSWORD',
        'DB_PORT',
        'DB_ENCRYPT',
        'DB_TRUST_SERVER_CERTIFICATE',
        'LOG_DB_DRIVER',
        'LOG_DB_HOST',
        'LOG_DB_NAME',
        'LOG_DB_USER',
        'LOG_DB_PASSWORD',
        'LOG_DB_PORT',
        'LOG_DB_ENCRYPT',
        'LOG_DB_TRUST_SERVER_CERTIFICATE',
    ]

    def setUp(self) -> None:
        self._saved_env = {key: os.getenv(key) for key in self._ENV_KEYS}
        for key in self._ENV_KEYS:
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _decode_odbc(self, url: str) -> str:
        self.assertIn('odbc_connect=', url)
        return unquote_plus(url.split('odbc_connect=', 1)[1])

    def _set_db_defaults(self) -> None:
        os.environ['DB_DRIVER'] = 'ODBC Driver 18 for SQL Server'
        os.environ['DB_HOST'] = 'db-host'
        os.environ['DB_NAME'] = 'MainDb'
        os.environ['DB_USER'] = 'main_user'
        os.environ['DB_PASSWORD'] = 'main_pass'
        os.environ['DB_PORT'] = '1433'
        os.environ['DB_ENCRYPT'] = 'yes'
        os.environ['DB_TRUST_SERVER_CERTIFICATE'] = 'yes'

    def test_log_vars_fallback_to_db_when_log_vars_empty(self) -> None:
        self._set_db_defaults()
        os.environ['LOG_DB_DRIVER'] = '   '
        os.environ['LOG_DB_HOST'] = ''
        os.environ['LOG_DB_NAME'] = ''
        os.environ['LOG_DB_USER'] = ''
        os.environ['LOG_DB_PASSWORD'] = ''
        os.environ['LOG_DB_PORT'] = ''

        decoded = self._decode_odbc(logger._build_log_db_connection_url())
        self.assertIn('DRIVER={ODBC Driver 18 for SQL Server};', decoded)
        self.assertIn('SERVER=db-host,1433;', decoded)
        self.assertIn('DATABASE=MainDb;', decoded)
        self.assertIn('UID=main_user;', decoded)
        self.assertIn('PWD=main_pass;', decoded)

    def test_only_log_vars_do_not_require_db_vars(self) -> None:
        os.environ['LOG_DB_DRIVER'] = 'ODBC Driver 18 for SQL Server'
        os.environ['LOG_DB_HOST'] = 'log-host'
        os.environ['LOG_DB_NAME'] = 'McpObservability'
        os.environ['LOG_DB_USER'] = 'mcp_loger'
        os.environ['LOG_DB_PASSWORD'] = 'log_pass'
        os.environ['LOG_DB_PORT'] = '1433'
        os.environ['LOG_DB_ENCRYPT'] = 'yes'
        os.environ['LOG_DB_TRUST_SERVER_CERTIFICATE'] = 'yes'

        decoded = self._decode_odbc(logger._build_log_db_connection_url())
        self.assertIn('SERVER=log-host,1433;', decoded)
        self.assertIn('DATABASE=McpObservability;', decoded)
        self.assertIn('UID=mcp_loger;', decoded)


if __name__ == '__main__':
    unittest.main()
