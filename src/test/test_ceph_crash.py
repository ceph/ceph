#!/usr/bin/env python3
# -*- mode:python; tab-width:4; indent-tabs-mode:nil -*-
# vim: ts=4 sw=4 expandtab

import logging
import os
import shutil
import tempfile
import unittest
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from unittest import mock

CEPH_CRASH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          '..', 'ceph-crash.in')


def load_ceph_crash():
    loader = SourceFileLoader('ceph_crash', CEPH_CRASH)
    module = module_from_spec(spec_from_loader('ceph_crash', loader))
    loader.exec_module(module)
    return module


class FakeProc:
    def __init__(self, name, args, rc, stderr):
        self.name = name
        self.args = args
        self.returncode = rc
        self.stderr = stderr
        self.input = None

    def communicate(self, input=None):
        self.input = input
        return None, self.stderr.encode()


class FakeCeph:
    """stands in for subprocess.Popen, with one canned result per auth name"""

    def __init__(self, results):
        self.results = results
        self.calls = []

    def __call__(self, args, **kwargs):
        name = args[args.index('-n') + 1]
        rc, stderr = self.results[name]
        proc = FakeProc(name, args, rc, stderr)
        self.calls.append(proc)
        return proc

    @property
    def names(self):
        return [proc.name for proc in self.calls]


class CephCrashTestCase(unittest.TestCase):
    # ceph exits 1 when it has no key for the name it runs as
    PERM = (1, '[errno 1] RADOS permission error '
               '(error connecting to the cluster)\n')
    # and 13 when the monitor rejects the key it does have
    EACCES = (13, '[errno 13] RADOS permission denied\n')
    # timeout(1) kills a client that keeps hunting for monitors
    TIMEOUT = (124, '2026-10-15T20:59:34.123+0000 7f4e1a7fc6c0 -1 '
                    'monclient(hunting): handle_auth_bad_method\n')
    OK = (0, '')
    # ceph can exit 0 and report the failure on stderr (a77b47eeeb57)
    SILENT_FAILURE = (0, 'malformed crash metadata: '
                         "time data '...' does not match format\n")
    # the cluster read the dump and rejected it
    EINVAL = (22, 'Error EINVAL: malformed crash metadata: '
                  "time data '...' does not match format\n")
    # a command that worked, with a log line ceph wrote to stderr as well
    NOISY_OK = (0, '2026-10-15T20:59:34.123+0000 7f4e1a7fc6c0 -1 WARNING: '
                   'all dangerous and experimental features are enabled.\n')

    def setUp(self):
        self.crash = load_ceph_crash()
        self.crash.log.setLevel(logging.CRITICAL)
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)
        os.mkdir(os.path.join(self.dir, 'posted'))
        self.crashdir = os.path.join(self.dir, 'crash1')
        os.mkdir(self.crashdir)
        with open(os.path.join(self.crashdir, 'meta'), 'w') as f:
            f.write('{"crash_id":"crash1"}')
        open(os.path.join(self.crashdir, 'done'), 'w').close()

    def fake_ceph(self, *results):
        fake = FakeCeph(dict(zip(self.crash.auth_names, results)))
        patcher = mock.patch.object(self.crash.subprocess, 'Popen', fake)
        patcher.start()
        self.addCleanup(patcher.stop)
        return fake

    def posted(self):
        return os.path.isdir(os.path.join(self.dir, 'posted', 'crash1'))


class TestPostCrash(CephCrashTestCase):
    def test_posts_with_the_first_name_that_works(self):
        fake = self.fake_ceph(self.OK, self.PERM, self.PERM)
        self.assertEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names[:1])
        post = fake.calls[0]
        self.assertEqual(post.args[-4:], ['crash', 'post', '-i', '-'])
        self.assertEqual(post.input, b'{"crash_id":"crash1"}')

    def test_falls_back_when_a_name_has_no_key(self):
        fake = self.fake_ceph(self.PERM, self.PERM, self.OK)
        self.assertEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names)

    def test_falls_back_when_the_monitor_rejects_a_key(self):
        fake = self.fake_ceph(self.EACCES, self.OK, self.PERM)
        self.assertEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names[:2])

    def test_falls_back_when_a_name_times_out(self):
        fake = self.fake_ceph(self.TIMEOUT, self.PERM, self.OK)
        self.assertEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names)

    def test_fails_when_no_name_works(self):
        fake = self.fake_ceph(self.PERM, self.PERM, self.PERM)
        self.assertNotEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names)

    def test_bounds_the_time_spent_reaching_the_cluster(self):
        fake = self.fake_ceph(self.OK, self.PERM, self.PERM)
        self.crash.post_crash(self.crashdir)
        args = fake.calls[0].args
        self.assertIn('--connect-timeout', args)
        seconds = int(args[args.index('--connect-timeout') + 1])
        self.assertLess(seconds, 30)

    def test_does_not_retry_a_command_that_ran_and_failed(self):
        fake = self.fake_ceph(self.SILENT_FAILURE, self.OK, self.OK)
        self.assertNotEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names[:1])

    def test_does_not_retry_a_dump_the_cluster_rejected(self):
        fake = self.fake_ceph(self.EINVAL, self.OK, self.OK)
        self.assertNotEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names[:1])

    def test_takes_a_post_that_only_logged_to_stderr(self):
        fake = self.fake_ceph(self.NOISY_OK, self.PERM, self.PERM)
        self.assertEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names[:1])

    def test_overrides_log_stderr_prefix(self):
        # a prefix would hide the timestamp of the log lines
        fake = self.fake_ceph(self.NOISY_OK, self.PERM, self.PERM)
        self.crash.post_crash(self.crashdir)
        self.assertIn('--log-stderr-prefix=', fake.calls[0].args)


class TestLastGoodName(CephCrashTestCase):
    def test_remembers_the_name_that_worked(self):
        fake = self.fake_ceph(self.PERM, self.PERM, self.OK)
        self.assertEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names)
        fake.calls.clear()
        self.assertEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, self.crash.auth_names[-1:])

    def test_falls_back_when_the_remembered_name_stops_working(self):
        names = self.crash.auth_names
        fake = self.fake_ceph(self.PERM, self.PERM, self.OK)
        self.assertEqual(self.crash.post_crash(self.crashdir), 0)
        fake.results[names[2]] = self.PERM
        fake.results[names[1]] = self.OK
        fake.calls.clear()
        self.assertEqual(self.crash.post_crash(self.crashdir), 0)
        self.assertEqual(fake.names, [names[2], names[0], names[1]])


class TestScrapePath(CephCrashTestCase):
    def test_moves_a_posted_dump_to_posted(self):
        self.fake_ceph(self.PERM, self.PERM, self.OK)
        self.crash.scrape_path(self.dir)
        self.assertTrue(self.posted())

    def test_keeps_a_dump_the_cluster_refused(self):
        self.fake_ceph(self.PERM, self.PERM, self.PERM)
        self.crash.scrape_path(self.dir)
        self.assertFalse(self.posted())

    def test_keeps_a_dump_the_command_failed_on(self):
        self.fake_ceph(self.SILENT_FAILURE, self.OK, self.OK)
        self.crash.scrape_path(self.dir)
        self.assertFalse(self.posted())

    def test_keeps_a_dump_the_cluster_rejected(self):
        self.fake_ceph(self.EINVAL, self.OK, self.OK)
        self.crash.scrape_path(self.dir)
        self.assertFalse(self.posted())

    def test_moves_a_dump_posted_with_log_noise(self):
        self.fake_ceph(self.NOISY_OK, self.PERM, self.PERM)
        self.crash.scrape_path(self.dir)
        self.assertTrue(self.posted())


if __name__ == '__main__':
    unittest.main()
