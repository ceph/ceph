import json
import signal
from io import BytesIO, StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase


class TestSnapPurge(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def setUp(self):
        super().setUp()
        for key, value in {
            'mds_bal_fragment_dirs': 'false',
            'mds_dir_prefetch': 'false',
            'mds_dir_prefetch_backend': 'false',
            'mds_dir_keys_per_op': '4',
            'mds_early_reply': 'false',
            # Keep stale keys on disk until the crash.
            'mds_log_max_segments': '100000',
            'mds_log_max_events': '-1',
        }.items():
            self.addCleanup(self.config_rm, 'mds', key)
            self.config_set('mds', key, value)
        self.fs.set_allow_new_snaps(True)

    def _keys(self, obj):
        return set(self.fs.radosmo(
            ['listomapkeys', obj], stdout=StringIO()).splitlines())

    def _header(self, obj):
        raw = self.fs.radosmo(['getomapheader', obj, '-'], stdout=BytesIO())
        return json.loads(self.fs.dencoder('fnode_t', raw))

    def _dump_dir(self):
        dirs = self.fs.rank_asok(['dump', 'dir', '/tree/dir', 'true'])
        self.assertEqual(len(dirs), 1)
        return dirs[0]

    def _prepare_stale_keys(self):
        self.mount_a.run_shell(['mkdir', '-p', 'tree/dir'])
        victims = ['victim_{:02d}'.format(i) for i in range(16)]
        self.mount_a.run_shell(
            ['touch', 'tree/dir/keep'] + ['tree/dir/' + v for v in victims])
        obj = '{:x}.00000000'.format(self.mount_a.path_to_ino('tree/dir'))
        self.mount_a.run_shell(['mkdir', 'tree/.snap/s1'])
        self.mount_a.run_shell(['rm'] + ['tree/dir/' + v for v in victims])
        self.fs.flush()
        stale = {k for k in self._keys(obj)
                 if not k.endswith('_head') and k.rsplit('_', 1)[0] in victims}
        self.assertEqual(len(stale), len(victims))

        self.mount_a.umount_wait()
        self.fs.flush()
        self.fs.mds_fail_restart(self.fs.get_rank()['name'])
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

        destroyed_before = int(self.fs.rank_asok(
            ['dump', 'snaps', '--server'])['last_destroyed'])
        self.mount_a.run_shell(['rmdir', 'tree/.snap/s1'])

        def snap_destroyed():
            snaps = self.fs.rank_asok(['dump', 'snaps', '--server'])
            return (not snaps['pending_destroy'] and
                    int(snaps['last_destroyed']) > destroyed_before)

        self.wait_until_true(snap_destroyed, timeout=60)
        target = int(self.fs.rank_asok(
            ['dump', 'snaps', '--server'])['last_destroyed'])
        header = self._header(obj)
        self.assertLess(int(header['snap_purged_thru']), target)
        self.assertTrue(stale <= self._keys(obj),
                        'stale keys must survive until the target is fetched')

        self.mount_a.run_shell(['stat', 'tree/dir'])
        cold = self._dump_dir()
        if cold.get('status') != 'dirfrag not in cache':
            self.assertNotIn('complete', cold['states'])
            self.assertEqual(cold['dentries'], [])
        return obj, stale, header, target

    def _list_dir(self):
        return sorted(self.mount_a.run_shell(
            ['ls', '-1', 'tree/dir']).stdout.getvalue().splitlines())

    def test_purge_watermark_survives_journal_replay(self):
        obj, stale, header, target = self._prepare_stale_keys()
        self.assertEqual(self._list_dir(), ['keep'])
        self.assertIn('complete', self._dump_dir()['states'])

        # A journal flush would commit the removals before the crash.
        self.mount_a.run_shell(['touch', 'tree/dir/journal_marker'])
        self.mount_a.umount_wait()
        before_crash = self._keys(obj)
        self.assertTrue(stale <= before_crash)
        self.assertNotIn('journal_marker_head', before_crash)
        self.assertEqual(self._header(obj), header)

        mds_id = self.fs.get_rank()['name']
        self.fs.mds_signal(mds_id, signal.SIGKILL)
        self.fs.rank_fail()
        self.fs.mds_restart(mds_id)
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

        self.assertEqual(self._list_dir(), ['journal_marker', 'keep'])
        self.fs.flush()
        self.assertFalse(stale & self._keys(obj),
                         'replay advanced the watermark past uncommitted deletes')
        self.assertGreaterEqual(int(self._header(obj)['snap_purged_thru']), target)

    def test_partial_fetch_does_not_certify_purge(self):
        obj, stale, header, target = self._prepare_stale_keys()
        self.mount_a.run_shell(['stat', 'tree/dir/keep'])
        self.assertNotIn('complete', self._dump_dir()['states'])
        self.fs.flush()
        self.assertTrue(stale <= self._keys(obj))
        self.assertEqual(self._header(obj)['snap_purged_thru'],
                         header['snap_purged_thru'])

        self.assertEqual(self._list_dir(), ['keep'])
        self.fs.flush()
        self.assertFalse(stale & self._keys(obj))
        self.assertGreaterEqual(int(self._header(obj)['snap_purged_thru']), target)
