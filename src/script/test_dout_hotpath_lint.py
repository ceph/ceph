#!/usr/bin/env python3
#
# Unit tests for dout_hotpath_lint.py.  Run: python3 test_dout_hotpath_lint.py

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
    'dout_hotpath_lint', os.path.join(HERE, 'dout_hotpath_lint.py'))
lint = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lint)

SOURCE = r'''
#define dout_subsys ceph_subsys_osd
#define psdout(x) ldout(cct, x)

// Foo::hot(int) in a comment must not be found
void Foo::hot(int a) const
{
  dout(10) << "hot start " << a << dendl;
  const char *s = "dout(1) inside a string {";
  if (a) {
    dout(20) << "detail" << dendl;
    ldpp_dout(dpp, 5) << "five" << dendl;
    psdout(15) << "fifteen" << dendl;
    lgeneric_subdout(cct, osd, 1) << "one" << dendl;
  }
#ifdef SOMETHING
  if (a > 1) {
#else
  if (a > 2) {
#endif
    derr << "error" << dendl;
    // dout-lint: error-path
    dout(0) << "error path" << dendl;
    ldout(cct, ceph::dout::need_dynamic(a)) << "dynamic" << dendl;
  }
  int x = 1'000;
  char c = '}';
}

void Foo::cold()
{
  if (Foo::hot(1)) {
    dout(1) << "cold" << dendl;
  }
}
'''


class TestLint(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.dir, 'src'))
        self.src = os.path.join(self.dir, 'src', 'foo.cc')
        with open(self.src, 'w') as f:
            f.write(SOURCE)
        self.baseline = os.path.join(self.dir, 'baseline.json')
        with open(self.baseline, 'w') as f:
            json.dump({'version': 1, 'max_level': 10, 'functions': [
                {'file': 'src/foo.cc', 'function': 'Foo::hot',
                 'le10': None, 'dynamic': None}]}, f)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def run_lint(self, *extra):
        out = io.StringIO()
        with redirect_stdout(out):
            rc = lint.main(['--source-root', self.dir,
                            '--baseline', self.baseline] + list(extra))
        return rc, out.getvalue()

    def test_mask_preserves_offsets(self):
        masked = lint.mask_source(SOURCE)
        self.assertEqual(len(masked), len(SOURCE))
        self.assertEqual(masked.count('\n'), SOURCE.count('\n'))
        self.assertEqual(masked.count('{'), masked.count('}'))

    def test_counts(self):
        masked = lint.mask_source(SOURCE)
        bodies = lint.find_function_bodies(masked, 'Foo::hot')
        self.assertEqual(len(bodies), 1)
        c = lint.count_statements(SOURCE, masked, bodies[0][0],
                                  bodies[0][1], 10)
        # dout(10), ldpp_dout(5), lgeneric_subdout(1); dout(0) is exempt
        self.assertEqual(c['le10'], 3)
        self.assertEqual(c['exempt'], 1)
        self.assertEqual(c['le15'], 1)
        self.assertEqual(c['le20'], 1)
        self.assertEqual(c['dynamic'], 1)
        self.assertEqual(c['err'], 1)

    def test_ratchet(self):
        rc, out = self.run_lint()
        self.assertEqual(rc, 1)                      # no baseline yet
        rc, out = self.run_lint('--update')
        self.assertEqual(rc, 0)
        rc, out = self.run_lint()
        self.assertEqual(rc, 0, out)
        # add a good-path level-10 line: must fail
        with open(self.src, 'w') as f:
            f.write(SOURCE.replace(
                '  int x = 1\'000;',
                '  dout(10) << "new line" << dendl;\n  int x = 1\'000;'))
        rc, out = self.run_lint()
        self.assertEqual(rc, 1)
        self.assertIn('le10 4 > baseline 3', out)
        # moving a line to 20 lowers the count: passes, strict fails
        with open(self.src, 'w') as f:
            f.write(SOURCE.replace('dout(10) << "hot start',
                                   'dout(20) << "hot start'))
        rc, out = self.run_lint()
        self.assertEqual(rc, 0, out)
        rc, out = self.run_lint('--strict')
        self.assertEqual(rc, 1)

    def test_missing_function(self):
        with open(self.baseline, 'w') as f:
            json.dump({'version': 1, 'max_level': 10, 'functions': [
                {'file': 'src/foo.cc', 'function': 'Foo::gone',
                 'le10': 0, 'dynamic': 0}]}, f)
        rc, out = self.run_lint()
        self.assertEqual(rc, 1)
        self.assertIn('definition not found', out)


if __name__ == '__main__':
    unittest.main()
