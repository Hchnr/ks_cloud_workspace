import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.delete_telemetry import metrics


class DeleteTelemetryTests(unittest.TestCase):
    def test_deletion_counts_do_not_claim_absent_files_were_removed(self):
        config = {'verified_total_files': 10}
        snap = dict(updated=20, completed_files=6, completed_bytes=1024**4, scan_complete=True,
                    state='running', counters={'succeeded': 4, 'skipped': 2},
                    groups=[dict(state='done', chunks=1)], active=[], nodes={'node': {'healthy': True}})
        previous = dict(updated=10, completed_files=2)
        m = metrics(snap, config, previous)
        self.assertEqual(m['deletion/progress_pct'], 60)
        self.assertEqual(m['deletion/removed_confirmed_files'], 4)
        self.assertEqual(m['deletion/already_absent_files'], 2)
        self.assertEqual(m['deletion/processed_files_per_second'], 0.4)
        self.assertNotIn('progress/uploaded_files_confirmed', m)


if __name__ == '__main__':
    unittest.main()
