#!/usr/bin/env python3
# -*- mode:python; tab-width:4; indent-tabs-mode:nil -*-
# vim: ts=4 sw=4 expandtab

import os
import shutil
import tempfile
import unittest
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader

CEPH_CRASH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          '..', 'ceph-crash.in')


def load_ceph_crash():
    loader = SourceFileLoader('ceph_crash', CEPH_CRASH)
    module = module_from_spec(spec_from_loader('ceph_crash', loader))
    loader.exec_module(module)
    return module


class FakePopen:
    def __init__(self, results, names):
        self.results = results
        self.names = names

    def __call__(self, args, **kwargs):
        name = args[args.index('-n') + 1]
        self.names.append(name)
        self.rc, self.stderr = self.results[name]
        self.returncode = self.rc
        return self

    def communicate(self, input=None):
        return None, self.stderr.encode()

    def wait(self):
        return self.rc


class TestPostCrash(unittest.TestCase):
    PERM = (1, '[errno 1] RADOS permission error '
               '(error connecting to the cluster)\n')

    def setUp(self):
        self.crash = load_ceph_crash()
        self.names = []
        self.dir = tempfile.mkdtemp()
        os.mkdir(os.path.join(self.dir, 'posted'))
        self.crashdir = os.path.join(self.dir, 'crash1')
        os.mkdir(self.crashdir)
        for f in ('meta', 'done'):
            with open(os.path.join(self.crashdir, f), 'w') as fh:
                fh.write('{}')

    def tearDown(self):
        shutil.rmtree(self.dir)

    def run_post(self, results):
        names = self.crash.auth_names
        self.crash.subprocess.Popen = FakePopen(dict(zip(names, results)),
                                                self.names)
        return self.crash.post_crash(self.crashdir)

    def test_fallback_on_connect_error(self):
        rc = self.run_post([self.PERM, self.PERM, (0, '')])
        self.assertEqual(rc, 0)
        self.assertEqual(self.names, self.crash.auth_names)

    def test_fallback_on_eacces(self):
        rc = self.run_post([(13, 'denied\n'), (0, ''), (0, '')])
        self.assertEqual(rc, 0)
        self.assertEqual(self.names, self.crash.auth_names[:2])

    def test_all_names_fail(self):
        rc = self.run_post([self.PERM, self.PERM, self.PERM])
        self.assertNotEqual(rc, 0)
        self.assertEqual(self.names, self.crash.auth_names)

    def test_no_fallback_when_command_ran(self):
        rc = self.run_post([(0, 'malformed crash metadata\n'), (0, ''),
                            (0, '')])
        self.assertEqual(rc, 0)
        self.assertEqual(self.names, self.crash.auth_names[:1])

    def test_scrape_path_posts_with_fallback(self):
        names = self.crash.auth_names
        self.crash.subprocess.Popen = FakePopen(
            dict(zip(names, [self.PERM, self.PERM, (0, '')])), self.names)
        self.crash.scrape_path(self.dir)
        self.assertTrue(os.path.isdir(os.path.join(self.dir, 'posted',
                                                   'crash1')))


if __name__ == '__main__':
    unittest.main()
