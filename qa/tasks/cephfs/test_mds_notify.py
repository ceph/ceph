"""
CephFS change notification (MDS-side producer) - teuthology integration test.

The endpoint is configured at daemon start, so the suite sets it up
(qa/suites/fs/functional/tasks/mds_notify.yaml):

    mds_notify_enable = true
    mds_notify_file   = /tmp/cephfs-notify.jsonl
    mds_notify_root   = /mds_notify

The tests drive the filesystem through a client mount and read the emitted
records back from the MDS node. What they check is the wire contract that the
consumer (reva's posixfs `cephfswatcher`) relies on: inotify-style mask bits,
paths relative to the watch root, and one action per message.
"""

import json
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase


class TestMdsNotify(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    SINK = "/tmp/cephfs-notify.jsonl"
    ROOT = "/mds_notify"          # mds_notify_root

    # ----------------------------------------------------------------- helpers

    def _active_mds(self):
        return self.fs.get_active_names()[0]

    def _sh(self, *args, **kwargs):
        """Run a command on the client mount (as root, like the other fs tests)."""
        return self.mount_a.run_shell(['sudo'] + list(args), **kwargs).stdout.getvalue()

    def _status(self):
        # the asok output shape differs between `ceph daemon` and `ceph tell`
        out = self.fs.rank_asok(['notify', 'status'])
        return out.get('change_notifier', out) if isinstance(out, dict) else out

    def _records(self, mds_id=None):
        mds_id = mds_id or self._active_mds()
        raw = self.mds_cluster.mds_daemons[mds_id].remote.sh(['cat', self.SINK])
        return [json.loads(line) for line in raw.splitlines() if line.strip()]

    def _wait_for(self, predicate, what, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(1)
        self.fail("timed out waiting for %s" % what)

    def _rel(self, path):
        """Client path -> path as it appears on the wire (relative to the root)."""
        return path[len(self.ROOT):].lstrip('/')

    # ------------------------------------------------------------------- tests

    def setUp(self):
        super(TestMdsNotify, self).setUp()
        status = self._status()
        if status['endpoint'] != 'file':
            self.skipTest("the file endpoint is not configured (see the suite's "
                          "ceph.conf overrides)")
        self._sh('mkdir', '-p', self.ROOT)
        self._wait_for(lambda: self._records(), "the first record after the root mkdir",
                       timeout=30)

    def test_admin_socket_surface(self):
        """notify status reports the rank and the endpoint; enable/disable works."""
        mds_id = self._active_mds()
        status = self._status()
        self.assertEqual(status['rank'], 0)
        self.assertTrue(status['enabled'])
        for key in ('queued', 'sent', 'dropped_queue', 'dropped_endpoint', 'dropped'):
            self.assertIn(key, status)
        self.assertEqual(status['last_error'], '')
        # the secret-free endpoint view: no password, just the configuration
        self.assertIn('endpoint_config', status)

        self.fs.rank_asok(['notify', 'disable'])
        try:
            self.assertFalse(self._status()['enabled'])
        finally:
            self.fs.rank_asok(['notify', 'enable'])
        self.assertTrue(self._status()['enabled'])
        self.assertEqual(self.fs.get_active_names()[0], mds_id,
                         "the admin socket commands must not move the rank")

    def test_namespace_and_close_write_events(self):
        """The six consumed operations produce the expected masks and paths."""
        base = '%s/projects' % self.ROOT
        before = len(self._records())

        self._sh('mkdir', '-p', '%s/alpha' % base)
        self._sh('touch', '%s/alpha/report.txt' % base)
        self._sh('sh', '-c', 'echo hello > %s/alpha/report.txt' % base)
        self._sh('sync', '%s/alpha/report.txt' % base)
        self._sh('mv', '%s/alpha/report.txt' % base, '%s/alpha/final.txt' % base)
        self._sh('rm', '%s/alpha/final.txt' % base)
        self._sh('rmdir', '%s/alpha' % base)

        rel = self._rel(base)
        recs = self._wait_for(
            lambda: [r for r in self._records()[before:]
                     if (r.get('path') or r.get('dest_path', '')).startswith(rel + '/')
                     or r.get('src_path', '').startswith(rel + '/')],
            "the workload records")
        # give the drain thread a moment for the tail of the sequence
        time.sleep(2)
        recs = [r for r in self._records()[before:]
                if (r.get('path') or r.get('dest_path', '')).startswith(rel + '/')
                or r.get('src_path', '').startswith(rel + '/')]

        by_path = {}
        for r in recs:
            by_path.setdefault(r.get('path') or r.get('dest_path'), []).append(r)

        p_alpha = '%s/alpha' % rel
        p_file = '%s/alpha/report.txt' % rel
        p_final = '%s/alpha/final.txt' % rel

        # directory create carries ONLYDIR
        self.assertTrue(any(r['mask'] == 16 | 65536 for r in by_path.get(p_alpha, [])),
                        "mkdir must be CREATE|ONLYDIR: %s" % by_path.get(p_alpha))
        # file create
        self.assertTrue(any(r['mask'] & 16 for r in by_path.get(p_file, [])),
                        "create must set CREATE: %s" % by_path.get(p_file))
        # close_write after the flush
        self.assertTrue(any(r['mask'] & 4 for r in by_path.get(p_file, [])),
                        "write+flush must set CLOSE_WRITE: %s" % by_path.get(p_file))
        # the rename travels as one message with both halves
        moves = [r for r in recs if r.get('src_path') == p_file and
                 r.get('dest_path') == p_final]
        self.assertTrue(moves, "rename must be one message with src and dest")
        self.assertTrue(moves[0]['src_mask'] & 512 and moves[0]['dest_mask'] & 1024)
        # delete of the renamed file, and rmdir of the directory
        self.assertTrue(any(r['mask'] == 32 for r in by_path.get(p_final, [])),
                        "unlink must be DELETE: %s" % by_path.get(p_final))
        self.assertTrue(any(r['mask'] == 32 | 65536 for r in by_path.get(p_alpha, [])),
                        "rmdir must be DELETE|ONLYDIR: %s" % by_path.get(p_alpha))

    def test_disabled_emits_nothing(self):
        """With the notifier disabled the same operations emit no records."""
        before = len(self._records())
        self.fs.rank_asok(['notify', 'disable'])
        try:
            self._sh('mkdir', '-p', '%s/quiet' % self.ROOT)
            self._sh('touch', '%s/quiet/file' % self.ROOT)
            self._sh('sync')
            time.sleep(5)
            self.assertEqual(len(self._records()), before,
                             "records were emitted while disabled")
        finally:
            self.fs.rank_asok(['notify', 'enable'])

        # ... and they start again once it is on
        self._sh('touch', '%s/quiet/loud' % self.ROOT)
        self._wait_for(lambda: len(self._records()) > before,
                       "records after re-enabling")
