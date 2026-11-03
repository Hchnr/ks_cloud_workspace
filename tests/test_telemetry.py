import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.telemetry import configure_connection


class ConnectionTests(unittest.TestCase):
    def test_explicit_files_override_environment_without_exposing_key_in_config(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {}, clear=True):
            key = Path(tmp) / 'api'
            proxy = Path(tmp) / 'proxy'
            key.write_text('test-key\n')
            proxy.write_text('http://localhost:8080\n')
            config = dict(swanlab_key_file=str(key), swanlab_proxy_file=str(proxy))
            self.assertEqual(configure_connection(config), 'test-key')
            self.assertEqual(os.environ['HTTPS_PROXY'], 'http://localhost:8080')
            self.assertNotIn('test-key', config.values())

    def test_environment_key_and_explicit_direct_connection(self):
        with patch.dict(os.environ, {'SWANLAB_API_KEY': 'test-env-key', 'ALL_PROXY': 'old'}, clear=True):
            self.assertEqual(configure_connection(dict(swanlab_proxy_file='')), 'test-env-key')
            self.assertNotIn('ALL_PROXY', os.environ)
            self.assertNotIn('HTTP_PROXY', os.environ)

    def test_empty_explicit_key_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {}, clear=True):
            key = Path(tmp) / 'empty'
            key.write_text(' \n')
            with self.assertRaisesRegex(ValueError, 'API key is empty'):
                configure_connection(dict(swanlab_key_file=str(key)))


if __name__ == '__main__':
    unittest.main()
