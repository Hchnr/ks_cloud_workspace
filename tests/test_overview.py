from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from ks_cloud.overview import build_overview, render_guide, render_overview


class OverviewTests(unittest.TestCase):
    def setUp(self):
        self.config = dict(tasks=[dict(id='large',source='/large'),dict(id='small',source='/small')])
        self.s = dict(updated=100,state='running',scan_complete=False,
                      completed_files=99,completed_bytes=99,discovered_files=100,discovered_bytes=100,
                      counters=dict(succeeded_bytes=20,skipped_bytes=79),
                      scans=[dict(source='large',state='scanning',error=''),dict(source='small',state='done',error='')],
                      groups=[dict(source='large',state='done',files=89,bytes=89,chunks=1),
                              dict(source='large',state='pending',files=1,bytes=1,chunks=1),
                              dict(source='small',state='done',files=10,bytes=10,chunks=1)],
                      nodes={'n':dict(healthy=True,active_slots=1,oom_kill_events=0)},active=[{}])
        self.ref = dict(sources={'large':dict(source='/large',complete=True,files=990,bytes=990)})

    def test_nearly_complete_discovered_range_is_not_nearly_complete_total(self):
        r = build_overview(self.s,self.config,self.ref,now=110)
        self.assertEqual(r['metrics']['overview/coverage_bytes_pct'],9.9)
        self.assertEqual(r['metrics']['overview/coverage_files_pct'],9.9)
        self.assertEqual(r['metrics']['overview/total_is_exact'],0)
        self.assertEqual(r['metrics']['overview/snapshot_age_seconds'],10)
        self.assertIn('估算',render_overview(r))

    def test_without_matching_reference_total_stays_unknown(self):
        self.ref['sources']['large']['source'] = '/another-dataset'
        r = build_overview(self.s,self.config,self.ref)
        self.assertIsNone(r['total'])
        self.assertNotIn('overview/coverage_bytes_pct',r['metrics'])
        self.assertIn('总量待确定',render_overview(r))

    def test_completed_scan_replaces_old_reference_and_exclusions_prevent_clean_success(self):
        self.s['scans'][0].update(state='done',error='1 excluded entry')
        self.s['state'] = 'finished_with_issues'
        r = build_overview(self.s,self.config,self.ref)
        self.assertEqual(r['total']['bytes'],100)
        self.assertEqual(r['metrics']['overview/total_is_exact'],1)
        self.assertEqual(r['metrics']['overview/coverage_bytes_pct'],99)
        self.assertEqual(r['metrics']['overview/all_sources_successful'],0)

    def test_html_escapes_source_names_and_guide_covers_all_metric_families(self):
        self.config['tasks'][0]['id']='<script>alert(1)</script>'
        r = build_overview(self.s,self.config,self.ref)
        html = render_overview(r)
        self.assertNotIn('<script>',html)
        self.assertIn('&lt;script&gt;',html)
        guide = render_guide()
        for key in ['progress/completed_files','throughput/uploaded_MiB_s_confirmed','cluster/leased_chunks',
                    'memory_file_cache_GiB','overview/coverage_bytes_pct','overview/metric_guide']:
            self.assertIn(key,guide)


if __name__=='__main__':
    unittest.main()
