import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.presence_followup import bootstrap_round, drive, prepare_round, snapshot_link_inputs, terminal_status


class PresenceFollowupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name) / 'primary'
        self.base.mkdir()
        self.config = dict(run_dir=str(self.base), tasks=[dict(id='one', source='/one', destination='ks3://b/one/'),
                                                         dict(id='two', source='/two', destination='ks3://b/two/')],
                           endpoint='http://host:18767', coordinator_endpoints={'a': 'http://host:18767'},
                           hosts=['a', 'b'], update_rule=1)

    def tearDown(self):
        self.tmp.cleanup()

    def test_only_directory_links_become_immutable_followup_inputs(self):
        (self.base / 'excluded-one.jsonl').write_text(
            json.dumps(dict(path='/one/link', reason='directory symlink (not followed)')) + '\n' +
            json.dumps(dict(path='/one/missing', reason='No such file')) + '\n')
        (self.base / 'excluded-two.jsonl').write_text('')
        self.assertEqual(snapshot_link_inputs(self.config), {'one': 1, 'two': 0})
        ledger = self.base / 'followup-inputs' / 'one.jsonl'
        self.assertEqual(json.loads(ledger.read_text())['path'], '/one/link')
        (self.base / 'excluded-one.jsonl').write_text('')
        self.assertEqual(snapshot_link_inputs(self.config), {'one': 1, 'two': 0})

    def test_new_round_keeps_mapping_and_presence_only_rule(self):
        run = prepare_round(self.config, {'one': 1, 'two': 0}, 1, 18807)
        child = json.loads((run / 'config.json').read_text())
        self.assertEqual(child['update_rule'], 1)
        self.assertEqual(child['port'], 18807)
        self.assertEqual(child['tasks'][0]['destination'], 'ks3://b/one/')
        self.assertEqual(len(child['tasks']), 1)
        self.assertFalse((run / 'START').exists())
        self.assertEqual(prepare_round(self.config, {'one': 1, 'two': 0}, 1, 18808), run)
        self.assertEqual(json.loads((run / 'config.json').read_text())['port'], 18807)

    def test_round_does_not_start_until_all_nodes_bootstrap(self):
        run = prepare_round(self.config, {'one': 1, 'two': 0}, 1, 18807)
        def command(config, host):
            return [host]
        def run_command(cmd, **kwargs):
            class Result:
                returncode = 1 if cmd[0] == 'b' else 0
            return Result()
        with patch('ks_cloud.presence_followup.peer_command', command), patch('ks_cloud.presence_followup.subprocess.run', run_command):
            self.assertEqual(bootstrap_round(run), ['b'])
        self.assertFalse((run / 'START').exists())

    def test_terminal_requires_all_scans_and_chunks_done(self):
        path = self.base / 'status.json'
        state = dict(state='finished_with_issues', scan_complete=True,
                     scans=[dict(source='one', state='done', error='1 excluded entries')],
                     groups=[dict(source='one', state='done')])
        path.write_text(json.dumps(state))
        self.assertIsNotNone(terminal_status(path)[0])
        state['groups'][0]['state'] = 'failed'
        path.write_text(json.dumps(state))
        self.assertIsNone(terminal_status(path)[0])

    def test_coordinator_runs_link_round_until_no_new_uploads(self):
        (self.base / 'excluded-one.jsonl').write_text(
            json.dumps(dict(path='/one/link', reason='directory symlink (not followed)')) + '\n')
        (self.base / 'excluded-two.jsonl').write_text('')
        parent = dict(state='finished_with_issues', scan_complete=True,
                      scans=[dict(source='one', state='done'), dict(source='two', state='done')],
                      groups=[dict(source='one', state='done')])
        (self.base / 'status.json').write_text(json.dumps(parent))
        def bootstrap(run):
            (run / 'START').touch()
            number = int(run.name.rsplit('r', 1)[1])
            (run / 'status.json').write_text(json.dumps(dict(
                state='finished', scan_complete=True, scans=[dict(source='one', state='done', error='')],
                groups=[dict(source='one', state='done')], counters=dict(succeeded=1 if number == 1 else 0))))
            return []
        clock = iter(range(0, 10000, 100))
        state = self.base / 'followup-state.json'
        with patch('ks_cloud.presence_followup.time.sleep'), patch('ks_cloud.presence_followup.time.monotonic', side_effect=lambda: next(clock)), \
             patch('ks_cloud.presence_followup.find_free_port', return_value=18807), \
             patch('ks_cloud.presence_followup.bootstrap_round', bootstrap):
            drive(self.config, state, 0)
        result = json.loads(state.read_text())
        self.assertEqual(result['phase'], 'finished')
        self.assertEqual(result['round'], 2)


if __name__ == '__main__':
    unittest.main()
