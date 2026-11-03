import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.telemetry import configure_connection, metrics_for_snapshot


class SnapshotFreshnessTests(unittest.TestCase):
    def test_stale_running_snapshot_does_not_publish_old_health_progress_or_rates(self):
        metrics = {'cluster/healthy_nodes': 16, 'nodes/n/healthy': 1,
                   'progress/completed_files': 42, 'throughput/completed_files_s': 0,
                   'overview/completed_TiB': 1, 'overview/dashboard': 'last-known HTML'}
        stale = metrics_for_snapshot(dict(updated=100, state='running'), {}, metrics, now=220)
        self.assertEqual(set(stale), {'overview/dashboard', 'overview/snapshot_age_seconds',
                                      'overview/snapshot_stale'})
        self.assertEqual(stale['overview/snapshot_stale'], 1)
        self.assertEqual(stale['overview/snapshot_age_seconds'], 120)
        fresh = metrics_for_snapshot(dict(updated=210, state='running'), {}, metrics, now=220)
        self.assertEqual(fresh['cluster/healthy_nodes'], 16)
        self.assertEqual(fresh['progress/completed_files'], 42)
        self.assertEqual(fresh['overview/snapshot_stale'], 0)

    def test_expiry_is_configurable_and_terminal_results_remain_reportable(self):
        snapshot = dict(updated=100, state='running')
        self.assertEqual(metrics_for_snapshot(snapshot, {'snapshot_stale_after_seconds': 150},
                                             {}, now=220)['overview/snapshot_stale'], 0)
        snapshot['state'] = 'finished_with_issues'
        metrics = metrics_for_snapshot(snapshot, {}, {'progress/completed_files': 42}, now=220)
        self.assertEqual(metrics['progress/completed_files'], 42)
        self.assertEqual(metrics['overview/snapshot_stale'], 1)


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
