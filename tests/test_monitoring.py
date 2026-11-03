import json
from pathlib import Path
import subprocess
import tempfile
import unittest


class MonitoringTests(unittest.TestCase):
    def test_snapshot_monitor_accepts_directory_and_active_pointer(self):
        tool = Path(__file__).resolve().parents[1] / 'bin' / 'ks-cloud'
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / 'DASHBOARD.md').write_text('running: test snapshot\n')
            (base / 'status.json').write_text(json.dumps(dict(state='running')))
            pointer = base / 'active.json'
            pointer.write_text(json.dumps(dict(kind='dynamic', run_dir=str(base))))
            for target in [['--run-dir', str(base)], ['--active', str(pointer)]]:
                p = subprocess.run([str(tool), 'monitor-dynamic', *target, '--once'],
                                   capture_output=True, text=True, cwd=base, timeout=10)
                self.assertEqual(p.returncode, 0, p.stderr)
                self.assertIn('test snapshot', p.stdout)

    def test_invalid_pointer_is_rejected_before_monitoring(self):
        tool = Path(__file__).resolve().parents[1] / 'bin' / 'ks-cloud'
        with tempfile.TemporaryDirectory() as tmp:
            pointer = Path(tmp) / 'active.json'
            pointer.write_text('{}')
            p = subprocess.run([str(tool), 'monitor-dynamic', '--active', str(pointer), '--once'],
                               capture_output=True, text=True, cwd=tmp, timeout=10)
            self.assertEqual(p.returncode, 2)
            self.assertIn('Invalid active-run pointer', p.stderr)


if __name__ == '__main__':
    unittest.main()
