"""
CephFS change notification (MDS-side producer) - teuthology integration test.

The endpoint is configured at daemon start, so the suite sets it up
(qa/suites/fs/functional/tasks/mds_notify.yaml):

    mds_notify_enable = true
    mds_notify_file   = /tmp/cephfs-notify.$name.jsonl
    mds_notify_root   = /mds_notify

The tests drive the filesystem through a client mount and read the emitted
records back from the MDS node. What they check is the wire contract that the
consumer (reva's posixfs `cephfswatcher`) relies on: mask bits in the
consumer's CEPH_MDS_NOTIFY_* numbering (inotify names, not inotify values),
paths relative to the watch root, and one action per message.
"""

import json
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase


class TestMdsNotify(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    # one sink per daemon (the suite sets /tmp/cephfs-notify.$name.jsonl):
    # co-located daemons must not append to the same file
    SINK = "/tmp/cephfs-notify.mds.%s.jsonl"
    ROOT = "/mds_notify"          # mds_notify_root (a file system path)
    # The same directory as the client sees it: relative to the mount point.
    # Mount.run_shell() only sets cwd to the mount point, so commands must
    # use relative paths; an absolute path would act on the client's local
    # root file system instead of CephFS.
    DIR = "mds_notify"

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
        raw = self.mds_cluster.mds_daemons[mds_id].remote.sh(
            ['cat', self.SINK % mds_id])
        return [json.loads(line) for line in raw.splitlines() if line.strip()]

    def _set_enabled(self, on):
        # the runtime option; set on the daemon so that it overrides the
        # suite's ceph.conf value
        self.fs.rank_asok(['config', 'set', 'mds_notify_enable',
                           'true' if on else 'false'])

    def _wait_for(self, predicate, what, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(1)
        self.fail("timed out waiting for %s" % what)

    def _rel(self, path):
        """Mount-relative path -> path as it appears on the wire (relative to
        the watch root)."""
        return path[len(self.DIR):].lstrip('/')

    # ------------------------------------------------------------------- tests

    def setUp(self):
        super(TestMdsNotify, self).setUp()
        # skip first: on an MDS that predates change notification the
        # notify commands do not exist and "notify status" would fail
        if 'notify status' not in self.fs.rank_asok(['help']):
            self.skipTest("this MDS has no CephFS change notification")
        status = self._status()
        if status['endpoint'] != 'file':
            self.skipTest("the file endpoint is not configured (see the suite's "
                          "ceph.conf overrides)")
        self._sh('mkdir', '-p', self.DIR)
        self._wait_for(lambda: self._records(), "the first record after the root mkdir",
                       timeout=30)

    def test_admin_socket_surface(self):
        """notify status reports the rank and the endpoint; the option switches it."""
        mds_id = self._active_mds()
        status = self._status()
        self.assertEqual(status['rank'], 0)
        self.assertTrue(status['enabled'])
        for key in ('queued', 'sent', 'dropped_queue', 'dropped_endpoint',
                    'dropped_delivery', 'dropped', 'epoch', 'next_seq'):
            self.assertIn(key, status)
        self.assertGreater(status['epoch'], 0)
        self.assertEqual(status['last_error'], '')
        # the secret-free endpoint view: no password, just the configuration
        self.assertIn('endpoint_config', status)

        # mds_notify_enable is the only switch (no asok enable/disable)
        self.assertNotIn('notify enable', self.fs.rank_asok(['help']))
        self._set_enabled(False)
        try:
            self._wait_for(lambda: not self._status()['enabled'],
                           "the notifier to turn off", timeout=30)
        finally:
            self._set_enabled(True)
        self._wait_for(lambda: self._status()['enabled'],
                       "the notifier to turn on", timeout=30)
        self.assertEqual(self.fs.get_active_names()[0], mds_id,
                         "switching notification must not move the rank")

    def test_namespace_and_close_write_events(self):
        """The six consumed operations produce the expected masks and paths."""
        base = '%s/projects' % self.DIR
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

    def test_stream_position(self):
        """Every record ends with rank, epoch and seq; seq has no holes."""
        before = len(self._records())
        for i in range(5):
            self._sh('touch', '%s/seq-%d' % (self.DIR, i))
        self._sh('sync')
        self._wait_for(lambda: len(self._records()) >= before + 5,
                       "the records of the five creates")
        # every event numbered so far must reach the sink
        status = self._status()
        epoch, last = status['epoch'], status['next_seq'] - 1

        def numbered():
            recs = self._records()
            cur = [r['seq'] for r in recs
                   if r['rank'] == 0 and r['epoch'] == epoch]
            return (recs, cur) if cur and cur[-1] >= last else None
        recs, cur = self._wait_for(numbered, "event %d of epoch %d" % (last, epoch))

        for r in recs:
            # appended after the existing members, in this order
            self.assertEqual(list(r.keys())[-3:], ['rank', 'epoch', 'seq'], r)
        # numbered from 1 in the current epoch, with no gaps (nothing dropped)
        self.assertEqual(cur, list(range(1, len(cur) + 1)))
        self.assertEqual(status['dropped'], 0)

    def test_runtime_options(self):
        """mds_notify_enable and mds_notify_root take effect without a restart."""
        try:
            self.fs.rank_asok(['config', 'set', 'mds_notify_root', '/elsewhere'])
            self._wait_for(lambda: self._status()['root'] == '/elsewhere',
                           "the new watch root in notify status", timeout=30)
            self.fs.rank_asok(['config', 'set', 'mds_notify_enable', 'false'])
            self._wait_for(lambda: not self._status()['enabled'],
                           "the notifier to turn off", timeout=30)
        finally:
            self.fs.rank_asok(['config', 'set', 'mds_notify_root', self.ROOT])
            self.fs.rank_asok(['config', 'set', 'mds_notify_enable', 'true'])
        self._wait_for(lambda: self._status()['enabled'] and
                       self._status()['root'] == self.ROOT,
                       "the original settings", timeout=30)

    def test_disabled_emits_nothing(self):
        """With the notifier disabled the same operations emit no records."""
        before = len(self._records())
        self._set_enabled(False)
        self._wait_for(lambda: not self._status()['enabled'],
                       "the notifier to turn off", timeout=30)
        try:
            self._sh('mkdir', '-p', '%s/quiet' % self.DIR)
            self._sh('touch', '%s/quiet/file' % self.DIR)
            self._sh('sync')
            time.sleep(5)
            self.assertEqual(len(self._records()), before,
                             "records were emitted while disabled")
        finally:
            self._set_enabled(True)

        # ... and they start again once it is on
        self._sh('touch', '%s/quiet/loud' % self.DIR)
        self._wait_for(lambda: len(self._records()) > before,
                       "records after re-enabling")
