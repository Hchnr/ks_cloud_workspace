import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.global_followup import completed_round, prepare_full_round, tick


class GlobalFollowupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / 'primary'
        self.root.mkdir()
        self.tasks = [dict(id='one', source='/one', destination='ks3://bucket/one/'),
                      dict(id='two', source='/two', destination='ks3://bucket/two/')]
        self.config = dict(run_dir=str(self.root), tasks=self.tasks, scope_allowed_roots=['/one', '/two'],
                           update_rule=1, endpoint='http://host:18767',
                           coordinator_endpoints={'a': 'http://host:18767'}, hosts=['a'])

    def tearDown(self):
        self.tmp.cleanup()

    def status(self, run, tasks, succeeded, skipped, error=''):
        run.mkdir(exist_ok=True)
        data = dict(state='finished_with_issues' if error else 'finished', scan_complete=True,
                    scans=[dict(source=t['id'], state='done', error=error) for t in tasks],
                    groups=[dict(source=t['id'], state='done') for t in tasks],
                    completed_files=succeeded + skipped, counters=dict(succeeded=succeeded, skipped=skipped))
        (run / 'status.json').write_text(json.dumps(data))
        return data

    def test_positive_full_and_link_rounds_trigger_another_global_round(self):
        self.status(self.root, self.tasks, 3, 5)
        (self.root / 'followup-state.json').write_text(json.dumps(dict(phase='finished', round=2)))
        for n, succeeded in [(1, 2), (2, 0)]:
            link = self.root.parent / (self.root.name + '_links_r' + str(n))
            link.mkdir()
            (link / 'config.json').write_text(json.dumps(dict(tasks=[self.tasks[0]], update_rule=1,
                                                             scope_allowed_roots=self.config['scope_allowed_roots'])))
            self.status(link, [self.tasks[0]], succeeded, 5)
        with patch('ks_cloud.global_followup.find_free_port', return_value=18801), \
             patch('ks_cloud.global_followup.bootstrap_round', side_effect=lambda p: ((p/'START').touch() or [])):
            state = tick(self.config)
        self.assertEqual(state['phase'], 'checking')
        self.assertEqual(state['round'], 2)
        self.assertEqual(state['completed_rounds'][0]['uploaded'], 5)
        second = self.root.parent / (self.root.name + '_full_r2')
        child = json.loads((second / 'config.json').read_text())
        self.assertEqual(child['tasks'], self.tasks)
        self.assertEqual(child['scope_allowed_roots'], self.config['scope_allowed_roots'])
        self.assertTrue(child['presence_followup_enabled'])
        self.assertFalse(child['global_followup_enabled'])
        self.status(second, self.tasks, 0, 10)
        (second / 'followup-state.json').write_text(json.dumps(dict(phase='finished', rounds=0)))
        state = tick(self.config)
        self.assertEqual(state['phase'], 'finished')
        self.assertEqual(state['round'], 2)
        self.assertFalse((self.root.parent / (self.root.name + '_full_r3')).exists())

    def test_zero_round_preserves_exclusions(self):
        self.status(self.root, self.tasks, 0, 5, error='1 excluded entries')
        (self.root / 'followup-state.json').write_text(json.dumps(dict(phase='finished', rounds=0)))
        state = tick(self.config)
        self.assertEqual(state['phase'], 'finished_with_exclusions')
        self.assertEqual(len(state['completed_rounds'][0]['exclusions']), 2)

    def test_fails_closed_on_inconsistent_counts_or_changed_mapping(self):
        data = self.status(self.root, self.tasks, 1, 5)
        data['completed_files'] = 99
        (self.root / 'status.json').write_text(json.dumps(data))
        (self.root / 'followup-state.json').write_text(json.dumps(dict(phase='finished', rounds=0)))
        with self.assertRaises(ValueError):
            tick(self.config)
        self.status(self.root, self.tasks, 1, 5)
        child = prepare_full_round(self.config, 2, 18801)
        path = child / 'config.json'
        altered = json.loads(path.read_text())
        altered['tasks'][0]['destination'] = 'ks3://other/'
        path.write_text(json.dumps(altered))
        with self.assertRaises(ValueError):
            tick(self.config)

    def test_waits_for_complete_link_round_and_stop_marker(self):
        self.status(self.root, self.tasks, 1, 0)
        (self.root / 'followup-state.json').write_text(json.dumps(dict(phase='checking_links')))
        result, reason = completed_round(self.root, self.tasks, self.config['scope_allowed_roots'])
        self.assertIsNone(result)
        self.assertIn('checking_links', reason)
        (self.root / 'STOP').touch()
        self.assertEqual(tick(self.config)['phase'], 'paused')


if __name__ == '__main__':
    unittest.main()
