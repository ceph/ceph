#!/usr/bin/env python3
#
# Unit tests for ceph_log_budget.py.  Run: python3 test_ceph_log_budget.py

import gzip
import importlib.util
import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    'ceph_log_budget', os.path.join(HERE, 'ceph_log_budget.py'))
clb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(clb)

PG = ('osd.3 pg_epoch: 844 pg[6.6s0( v 844\'13693 (822\'3600,844\'13693] '
      'local-lis/les=817/818 n=13693 ec=817/817 lis/c=817/817 '
      'les/c/f=818/818/0 sis=817) [3,6,15]p3(0) r=0 lpr=817 '
      'crt=844\'13693 active+clean] ')
LINES = [
    # start of a client op (level 10)
    '2026-07-10T07:29:46.450+0000 7f8adb893640 10 osd.3 844 dequeue_op '
    'osd_op(client.13496.0:216140 6.6s0 6.79858b36 (undecoded) '
    'ondisk+write e844) v9 prio 63 cost 65536 latency 0.000031',
    # PG line at 20
    '2026-07-10T07:29:46.451+0000 7f8adb893640 20 ' + PG +
    'start_rmw op=Op(6:6cd1a19e:::obj1:head v=844\'13694)',
    # PG line at 10, followed by a continuation line
    '2026-07-10T07:29:46.452+0000 7f8adb893640 10 ' + PG +
    'append_log approx pg log length = 13693 [dump',
    'continued dump]',
    # bluestore / bluefs / ms lines; single digit levels use "%2d"
    '2026-07-10T07:29:46.453+0000 7f8adb893640 20 '
    'bluestore(/var/lib/ceph/osd/ceph-3) _txc_state_proc txc 0x55 prepare',
    '2026-07-10T07:29:46.454+0000 7f8adb893640 10 '
    'bluefs _flush_F 0x55 ignoring, length 4096 < min_flush_size 524288',
    '2026-07-10T07:29:46.455+0000 7f8adb893640  1 -- '
    '[v2:10.0.0.1:6800/1,v1:10.0.0.1:6801/1] <== osd.6 v2:10.0.0.2:6800/2 '
    '17 ==== MOSDECSubOpWriteReply(6.6s1 844/817) v1 ==== 67+0+0',
    # finish line of the same op
    '2026-07-10T07:29:46.456+0000 7f8adb893640 10 osd.3 844 dequeue_op '
    'osd_op(client.13496.0:216140 6.6s0 6:6cd1a19e:::obj1:head [write]) v9 '
    'finish latency 0.003357',
    # a second op, start only
    '2026-07-10T07:29:47.450+0000 7f8adb893640 10 osd.3 844 dequeue_op '
    'osd_op(client.13496.0:216141 6.6s0 6.8f8cd4ac (undecoded) '
    'ondisk+write e844) v9 prio 63 cost 65536 latency 0.000031',
]


def nbytes(lines):
    return sum(len(x) + 1 for x in lines)


class TestAnalysis(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.plain = os.path.join(self.dir, 'ceph-osd.3.log')
        with open(self.plain, 'w') as f:
            f.write('\n'.join(LINES) + '\n')
        self.gz = self.plain + '.1.gz'
        with gzip.open(self.gz, 'wt') as f:
            f.write('\n'.join(LINES) + '\n')

    def tearDown(self):
        shutil.rmtree(self.dir)

    def analyse(self, path, **kw):
        an = clb.Analysis(**kw)
        an.add_file(path)
        return an.report(top=50)

    def test_totals(self):
        for path in (self.plain, self.gz):
            r = self.analyse(path, level=10)
            self.assertEqual(r['lines'], len(LINES))
            self.assertEqual(r['entries'], len(LINES) - 1)
            self.assertEqual(r['bytes'], nbytes(LINES))
            self.assertEqual(r['max_level_seen'], 20)
            self.assertEqual(r['multiline_entries'], 1)
            self.assertEqual(r['kept']['multiline_entries'], 1)
            kept = [x for i, x in enumerate(LINES) if i not in (1, 4)]
            self.assertEqual(r['kept']['bytes'], nbytes(kept))
            self.assertEqual(r['levels']['1']['lines'], 1)
            self.assertEqual(r['levels']['20']['lines'], 2)
            self.assertEqual(r['levels']['10']['lines'], 6)  # incl. cont.
            self.assertEqual(r['client_ops'], 2)
            self.assertAlmostEqual(r['kept_bytes_per_op'],
                                   nbytes(kept) / 2.0)
            self.assertAlmostEqual(r['duration_s'], 1.0)
            comps = r['components']
            self.assertEqual(comps['bluestore']['kept_bytes'], 0)
            self.assertEqual(comps['bluefs']['lines'], 1)
            self.assertEqual(comps['ms']['lines'], 1)

    def test_level_20_keeps_everything(self):
        r = self.analyse(self.plain, level=20)
        self.assertEqual(r['kept']['bytes'], r['bytes'])
        self.assertAlmostEqual(r['kept']['fraction'], 1.0)

    def test_since_until(self):
        r = self.analyse(self.plain, level=10,
                         since='2026-07-10T07:29:46.452',
                         until='2026-07-10T07:29:46.456+9999')
        # lines 2 (+continuation) .. 7
        self.assertEqual(r['entries'], 5)
        self.assertEqual(r['client_ops'], 1)     # finish line only

    def test_templates(self):
        r = self.analyse(self.plain, level=10)
        by = dict((t['template'], t) for t in r['top_templates'])
        ap = [t for k, t in by.items() if k.startswith('append_log')]
        self.assertEqual(len(ap), 1)
        self.assertEqual(ap[0]['component'], 'osd')
        self.assertEqual(ap[0]['level'], 10)
        self.assertEqual(ap[0]['token'], 'append_log')
        # continuation bytes are attributed to the entry
        self.assertEqual(ap[0]['bytes'], nbytes(LINES[2:4]))
        for t in r['top_kept_templates']:
            self.assertLessEqual(t['level'], 10)

    def test_sampling_keeps_totals_exact(self):
        r1 = self.analyse(self.plain, level=10)
        r3 = self.analyse(self.plain, level=10, sample=3)
        for k in ('lines', 'bytes', 'client_ops'):
            self.assertEqual(r1[k], r3[k])
        self.assertEqual(r1['kept'], r3['kept'])
        self.assertEqual(r1['components'], r3['components'])

    def test_budgets_and_merge(self):
        r = self.analyse(self.plain, level=10)
        v = clb.check_budgets(r, max_kept_bytes_per_op=10,
                              max_kept_fraction=0.99, min_client_ops=1)
        self.assertEqual([x['budget'] for x in v], ['max_kept_bytes_per_op'])
        self.assertTrue(v[0]['offenders'])
        v = clb.check_budgets(r, max_kept_bytes_per_op=10,
                              min_client_ops=1000)
        self.assertEqual(v, [])
        self.assertTrue(any('not evaluated' in n for n in r['budget_notes']))
        m = clb.merge_reports([r, self.analyse(self.gz, level=10)])
        self.assertEqual(m['bytes'], 2 * r['bytes'])
        self.assertEqual(m['client_ops'], 4)
        self.assertEqual(m['daemons'], 2)
        self.assertAlmostEqual(m['kept_bytes_per_op'], r['kept_bytes_per_op'])
        t0 = r['top_templates'][0]
        mt = [t for t in m['top_templates']
              if t['template'] == t0['template'] and
              t['level'] == t0['level']]
        self.assertEqual(mt[0]['bytes'], 2 * t0['bytes'])

    def test_main_json_and_exit_code(self):
        out = io.StringIO()
        with redirect_stdout(out):
            rc = clb.main(['--json', '--level', '10', self.plain])
        self.assertEqual(rc, 0)
        r = json.loads(out.getvalue())
        self.assertEqual(r['version'], clb.REPORT_VERSION)
        self.assertEqual(r['violations'], [])
        out = io.StringIO()
        with redirect_stdout(out):
            rc = clb.main(['--level', '10', '--min-client-ops', '1',
                           '--max-kept-bytes-per-op', '1', self.plain])
        self.assertEqual(rc, 1)
        self.assertIn('BUDGET EXCEEDED', out.getvalue())

    def test_client_ops_excludes_requeues_and_drops(self):
        # A requeue (e.g. waiting_for_readable) is dequeued, and logged,
        # more than once for the same client op; a "pg ... is deleting,
        # dropping" line also matches CLIENT_OP_MARK.  Neither carries the
        # start line's ' prio ' field, so neither must inflate client_ops.
        lines = [
            '2026-07-10T07:29:46.450+0000 7f8adb893640 10 osd.3 844 '
            'dequeue_op osd_op(client.13496.0:1 6.6s0 6.79858b36 '
            '(undecoded) ondisk+write e844) v9 prio 63 cost 65536 '
            'latency 0.000031',
            # requeued: dequeued again, no ' prio ' this time in this
            # synthetic example other than the embedded op description
            '2026-07-10T07:29:46.460+0000 7f8adb893640 10 osd.3 844 '
            'dequeue_op osd_op(client.13496.0:1 6.6s0 6:6cd1a19e:::obj1:'
            'head [read]) requeued: waiting_for_readable',
            '2026-07-10T07:29:46.470+0000 7f8adb893640 10 osd.3 844 '
            'dequeue_op osd_op(client.13496.0:1 6.6s0 6:6cd1a19e:::obj1:'
            'head [read]) v9 pg 6.6s0 is deleting, dropping',
            '2026-07-10T07:29:46.480+0000 7f8adb893640 10 osd.3 844 '
            'dequeue_op osd_op(client.13496.0:1 6.6s0 6:6cd1a19e:::obj1:'
            'head [read]) v9 finish latency 0.003357',
        ]
        path = os.path.join(self.dir, 'requeue.log')
        with open(path, 'w') as f:
            f.write('\n'.join(lines) + '\n')
        r = self.analyse(path, level=10)
        self.assertEqual(r['client_ops'], 1)

    def test_max_kept_multiline_entries(self):
        r = self.analyse(self.plain, level=10)
        self.assertEqual(r['kept']['multiline_entries'], 1)
        self.assertIn('append_log', r['kept']['first_multiline_example'])
        v = clb.check_budgets(r, max_kept_multiline_entries=0)
        self.assertEqual([x['budget'] for x in v],
                         ['max_kept_multiline_entries'])
        self.assertEqual(v[0]['actual'], 1)
        self.assertIn('append_log', v[0]['example'])
        # off by default, and satisfied when the budget is high enough
        self.assertEqual(clb.check_budgets(r), [])
        self.assertEqual(
            clb.check_budgets(r, max_kept_multiline_entries=1), [])
        m = clb.merge_reports([r, self.analyse(self.gz, level=10)])
        self.assertEqual(m['kept']['multiline_entries'], 2)
        self.assertIn('append_log', m['kept']['first_multiline_example'])

    def test_source_attribution(self):
        src = os.path.join(self.dir, 'src', 'osd')
        os.makedirs(src)
        with open(os.path.join(src, 'PeeringState.cc'), 'w') as f:
            f.write('void PeeringState::append_log(int x)\n{\n'
                    '  psdout(10) << "approx pg log length = " << x\n'
                    '             << dendl;\n}\n')
        with open(os.path.join(src, 'OSD.cc'), 'w') as f:
            f.write('void OSD::dequeue_op(int op)\n{\n'
                    '  dout(10) << "dequeue_op " << op << dendl;\n}\n')
        r = self.analyse(self.plain, level=10)
        clb.attribute_sources(r['top_templates'], self.dir)
        by = dict((t['token'], t) for t in r['top_templates'])
        self.assertEqual(by['append_log']['source'],
                         ['src/osd/PeeringState.cc:3'])
        self.assertEqual(by['dequeue_op']['source'],
                         ['src/osd/OSD.cc:1 (dequeue_op)'])


if __name__ == '__main__':
    unittest.main()
