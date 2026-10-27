import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.common import parse_progress, successful
from ks_cloud.cli import distribute


class WorkflowTests(unittest.TestCase):
    def test_terminal_summary_variants(self):
        p = parse_progress('完成数:12/20 完成大小:1.00GB/2.00GB\n'
                           'Succeed count: 4970 (4944 files, 26 dirs) Failed count: 0\nSucceed bytes: 246.95MB')
        self.assertEqual(p['handled'], 4970)
        self.assertEqual(p['total_bytes'], 2 * 1024**3)
        self.assertTrue(successful(dict(p, exit_code=0)))
        self.assertFalse(successful({'exit_code': 0}))

    def test_unique_static_assignment(self):
        tasks = [dict(id=str(i), weight=i+1) for i in range(100)]
        plan = distribute({'hosts': ['a', 'b', 'c']}, tasks)
        ids = [t['id'] for n in plan['nodes'] for t in n['tasks']]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(set(ids), {t['id'] for t in tasks})

    def test_worker_retries_failure_and_skips_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            b = Path(tmp)
            fake = b / 'fake-ks3util'
            fake.write_text('#!/usr/bin/env python3\nfrom pathlib import Path\n'
                            f'p=Path({str(b / "calls")!r})\n'
                            'n=int(p.read_text())+1 if p.exists() else 1\np.write_text(str(n))\n'
                            'print("Succeed count: 0 Failed count: 1" if n==1 else "Succeed count: 1 Failed count: 0")\n'
                            'raise SystemExit(1 if n==1 else 0)\n')
            fake.chmod(0o755)
            config = dict(tool=str(fake), source='/source', destination='ks3://bucket/prefix/', config_file='/private/config')
            (b / 'plan.json').write_text(json.dumps(dict(host='test', config=config, tasks=[dict(id='one', manifest='/manifest', weight=1)])))
            command = [sys.executable, '-m', 'ks_cloud.worker', '--run-dir', str(b)]
            import os
            env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
            for _ in range(3):
                subprocess.run(command, env=env, check=True)
            self.assertEqual((b / 'calls').read_text(), '2')
            self.assertEqual(json.loads((b / 'status.json').read_text())['state'], 'finished')
            self.assertEqual(json.loads((b / 'status.json').read_text())['failed_tasks'], 0)


if __name__ == '__main__':
    unittest.main()
