import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from ks_cloud.delete_plan import prepare


class DeletePlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.primary = self.base / 'primary'
        self.primary.mkdir()
        self.source = self.base / 'source'
        self.source.mkdir()
        self.release = self.base / 'release'
        self.release.mkdir()
        for name in ('dynamic.py', 'verified_delete.py', 'delete_telemetry.py', 'supervisor.py', 'ha.py', 'bootstrap.py'):
            (self.release / name).write_text('')
        self.task = dict(id='data', source=str(self.source), destination='ks3://bucket/data/')
        config = dict(tasks=[self.task], hosts=['a', 'b'], update_rule=1, tool=str(self.base / 'ks3util'),
                      config_file=str(self.base / 'credentials'), endpoint='http://a:18767',
                      coordinator_endpoints={'a': 'http://a:18767', 'b': 'http://b:18767'})
        (self.primary / 'config.json').write_text(json.dumps(config))
        (self.primary / 'mapping-baseline.json').write_text(json.dumps(dict(tasks=[
            {key: self.task.get(key) for key in ('id', 'source', 'destination', 'exclude_child')}])))
        (self.primary / 'root-baseline.json').write_text(json.dumps(dict(hosts=['a', 'b'],
            roots={'data': dict(ino=os.lstat(self.source).st_ino, real=str(self.source))})))
        inputs = self.primary / 'followup-inputs'
        inputs.mkdir()
        (inputs / 'counts.json').write_text(json.dumps({'data': 0}))
        (self.primary / 'followup-state.json').write_text(json.dumps(dict(phase='finished', link_counts={'data': 0}, rounds=0)))
        self.result = dict(exit_code=0, failed=0, succeeded=0, skipped=1, handled=1)
        self.status = dict(state='finished', scan_complete=True, completed_files=1, discovered_files=1, completed_bytes=3,
            counters=dict(succeeded=0, skipped=1), scans=[dict(source='data', state='done', error='')],
            groups=[dict(source='data', state='done', chunks=1, files=1, bytes=3)])
        (self.primary / 'status.json').write_text(json.dumps(self.status))
        manifest = self.primary / 'manifests' / 'data' / 'one.txt'
        manifest.parent.mkdir(parents=True)
        manifest.write_text(str(self.source / 'file') + '\n')
        with sqlite3.connect(self.primary / 'queue.sqlite') as db:
            db.execute('CREATE TABLE tasks(id TEXT,source TEXT,manifest TEXT,files INTEGER,bytes INTEGER,state TEXT,result TEXT)')
            db.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?,?)', ('data-one', 'data', str(manifest), 1, 3, 'done', json.dumps(self.result)))
            db.execute('CREATE TABLE scans(source TEXT,state TEXT,error TEXT)')
            db.execute('INSERT INTO scans VALUES (?,?,?)', ('data', 'done', ''))

    def tearDown(self):
        self.tmp.cleanup()

    def test_prepares_unarmed_run_from_matching_completed_snapshot(self):
        run = prepare(self.primary, self.release, 18800)
        config = json.loads((run / 'config.json').read_text())
        self.assertEqual(config['verified_total_files'], 1)
        self.assertEqual(config['operation'], 'delete_verified')
        self.assertEqual(config['tasks'][0]['root_ino'], os.lstat(self.source).st_ino)
        self.assertFalse((run / 'DELETE_ENABLED').exists())
        self.assertFalse((run / 'START').exists())
        self.assertTrue((run / 'verified.sqlite').exists())

    def test_refuses_lagging_queue_snapshot(self):
        with sqlite3.connect(self.primary / 'queue.sqlite') as db:
            db.execute("UPDATE tasks SET state='running'")
        with self.assertRaisesRegex(ValueError, 'snapshot does not match'):
            prepare(self.primary, self.release, 18800)
        self.assertFalse((self.base / 'primary_delete').exists())

    def test_refuses_unfinished_or_missing_followup(self):
        (self.primary / 'followup-state.json').write_text(json.dumps(dict(phase='checking_links')))
        with self.assertRaisesRegex(ValueError, 'follow-up is incomplete'):
            prepare(self.primary, self.release, 18800)

    def test_link_followup_requires_same_destination_as_primary(self):
        inputs = self.primary / 'followup-inputs'
        (inputs / 'counts.json').write_text(json.dumps({'data': 1}))
        run = self.base / 'primary_links_r1'
        run.mkdir()
        (self.primary / 'followup-state.json').write_text(json.dumps(dict(
            phase='finished', link_counts={'data': 1}, round=1, run_dir=str(run))))
        followup_task = dict(self.task, destination='ks3://bucket/other/',
                             link_exclusions_file=str(inputs / 'data.jsonl'))
        (run / 'config.json').write_text(json.dumps(dict(update_rule=1, tasks=[followup_task])))
        with self.assertRaisesRegex(ValueError, 'mapping differs from primary'):
            prepare(self.primary, self.release, 18800)
        self.assertFalse((self.base / 'primary_delete').exists())

        followup_task['destination'] = self.task['destination']
        (run / 'config.json').write_text(json.dumps(dict(update_rule=1, tasks=[followup_task])))
        (run / 'status.json').write_text(json.dumps(self.status))
        self.assertTrue(prepare(self.primary, self.release, 18800).exists())

    def test_refuses_changed_root_or_runtime_inside_source(self):
        baseline = json.loads((self.primary / 'root-baseline.json').read_text())
        baseline['roots']['data']['ino'] += 1
        (self.primary / 'root-baseline.json').write_text(json.dumps(baseline))
        with self.assertRaisesRegex(ValueError, 'identity changed'):
            prepare(self.primary, self.release, 18800)
        baseline['roots']['data']['ino'] -= 1
        (self.primary / 'root-baseline.json').write_text(json.dumps(baseline))
        config = json.loads((self.primary / 'config.json').read_text())
        config['config_file'] = str(self.source / 'credentials')
        (self.primary / 'config.json').write_text(json.dumps(config))
        with self.assertRaisesRegex(ValueError, 'credential path'):
            prepare(self.primary, self.release, 18800)


if __name__ == '__main__':
    unittest.main()
