import json
import os
import tempfile
import unittest
from pathlib import Path

from src import db_connection


class TestDbCatalog(unittest.TestCase):
    _ENV_KEYS = [
        'DB_CATALOG_PATH',
        'DB_HOST',
        'DB_PORT',
        'DB_NAME',
        'DB_USER',
        'DB_PASSWORD',
        'DB_DRIVER',
        'DB_ENCRYPT',
        'DB_TRUST_SERVER_CERTIFICATE',
    ]

    def setUp(self) -> None:
        self._saved_env = {key: os.getenv(key) for key in self._ENV_KEYS}
        self.tmp_dir = tempfile.TemporaryDirectory()
        db_connection.reset_connection_caches()

    def tearDown(self) -> None:
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        db_connection.reset_connection_caches()
        self.tmp_dir.cleanup()

    def _set_legacy_env(self) -> None:
        os.environ['DB_HOST'] = 'localhost'
        os.environ['DB_PORT'] = '1433'
        os.environ['DB_NAME'] = 'Northwind_DW'
        os.environ['DB_USER'] = 'readonly_user'
        os.environ['DB_PASSWORD'] = 'secret'
        os.environ['DB_DRIVER'] = 'ODBC Driver 18 for SQL Server'
        os.environ['DB_ENCRYPT'] = 'yes'
        os.environ['DB_TRUST_SERVER_CERTIFICATE'] = 'yes'

    def test_env_fallback_profile(self) -> None:
        self._set_legacy_env()
        os.environ.pop('DB_CATALOG_PATH', None)
        db_connection.reset_connection_caches()

        catalog = db_connection.list_database_profiles()
        self.assertEqual(catalog['default_database'], 'default')
        self.assertEqual(len(catalog['databases']), 1)
        self.assertEqual(catalog['databases'][0]['database'], 'Northwind_DW')

        self.assertEqual(db_connection.resolve_database_name(None), 'default')
        self.assertEqual(db_connection.resolve_database_name('default'), 'default')
        self.assertEqual(db_connection.resolve_database_name('Northwind_DW'), 'default')

    def test_catalog_profiles(self) -> None:
        self._set_legacy_env()
        catalog_path = Path(self.tmp_dir.name) / 'db_catalog.json'
        catalog_path.write_text(
            json.dumps(
                {
                    'default_database': 'analytics',
                    'shared': {
                        'host': 'localhost',
                        'port': '1433',
                        'user': 'readonly_user',
                        'password': 'secret',
                        'driver': 'ODBC Driver 18 for SQL Server',
                        'encrypt': 'yes',
                        'trust_server_certificate': 'yes',
                    },
                    'databases': [
                        {'name': 'northwind', 'database': 'Northwind_DW'},
                        {'name': 'analytics', 'database': 'SalesMart'},
                    ],
                }
            ),
            encoding='utf-8',
        )
        os.environ['DB_CATALOG_PATH'] = str(catalog_path)
        db_connection.reset_connection_caches()

        catalog = db_connection.list_database_profiles()
        self.assertEqual(catalog['default_database'], 'analytics')
        self.assertEqual(len(catalog['databases']), 2)
        names = {item['name'] for item in catalog['databases']}
        self.assertEqual(names, {'northwind', 'analytics'})

        self.assertEqual(db_connection.resolve_database_name(None), 'analytics')
        self.assertEqual(db_connection.resolve_database_name('northwind'), 'northwind')
        self.assertEqual(db_connection.resolve_database_name('SalesMart'), 'analytics')

    def test_unknown_database_raises(self) -> None:
        self._set_legacy_env()
        os.environ.pop('DB_CATALOG_PATH', None)
        db_connection.reset_connection_caches()

        with self.assertRaises(ValueError):
            db_connection.resolve_database_name('does_not_exist')

    def test_explicit_missing_catalog_path_raises(self) -> None:
        self._set_legacy_env()
        os.environ['DB_CATALOG_PATH'] = str(Path(self.tmp_dir.name) / 'missing_catalog.json')
        db_connection.reset_connection_caches()

        with self.assertRaises(ValueError):
            db_connection.list_database_profiles()


if __name__ == '__main__':
    unittest.main()
