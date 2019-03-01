
from unittest import SkipTest
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.orchestra.run import CommandFailedError, ConnectionLostError
import errno
import time
import json
import logging
import time

log = logging.getLogger(__name__)

class TestMisc(CephFSTestCase):
    CLIENTS_REQUIRED = 2

    def test_getattr_caps(self):
        """
        Check if MDS recognizes the 'mask' parameter of open request.
        The parameter allows client to request caps when opening file
        """

        if not isinstance(self.mount_a, FuseMount):
            raise SkipTest("Require FUSE client")

        # Enable debug. Client will requests CEPH_CAP_XATTR_SHARED
        # on lookup/open
        self.mount_b.umount_wait()
        self.set_conf('client', 'client debug getattr caps', 'true')
        self.mount_b.mount()
        self.mount_b.wait_until_mounted()

        # create a file and hold it open. MDS will issue CEPH_CAP_EXCL_*
        # to mount_a
        p = self.mount_a.open_background("testfile")
        self.mount_b.wait_for_visible("testfile")

        # this triggers a lookup request and an open request. The debug
        # code will check if lookup/open reply contains xattrs
        self.mount_b.run_shell(["cat", "testfile"])

        self.mount_a.kill_background(p)

    def test_root_rctime(self):
        """
        Check that the root inode has a non-default rctime on startup.
        """

        t = time.time()
        rctime = self.mount_a.getfattr(".", "ceph.dir.rctime")
        log.info("rctime = {}".format(rctime))
        self.assertGreaterEqual(rctime, t-10)

    def test_fs_new(self):
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        data_pool_name = self.fs.get_data_pool_name()

        self.fs.mds_stop()
        self.fs.mds_fail()

        self.fs.mon_manager.raw_cluster_cmd('fs', 'rm', self.fs.name,
                                            '--yes-i-really-mean-it')

        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                            self.fs.metadata_pool_name,
                                            self.fs.metadata_pool_name,
                                            '--yes-i-really-really-mean-it')
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                            self.fs.metadata_pool_name,
                                            self.fs.get_pgs_per_fs_pool().__str__())

        dummyfile = '/etc/fstab'

        self.fs.put_metadata_object_raw("key", dummyfile)

        def get_pool_df(fs, name):
            try:
                return fs.get_pool_df(name)['objects'] > 0
            except RuntimeError as e:
                return False

        self.wait_until_true(lambda: get_pool_df(self.fs, self.fs.metadata_pool_name), timeout=30)

        try:
            self.fs.mon_manager.raw_cluster_cmd('fs', 'new', self.fs.name,
                                                self.fs.metadata_pool_name,
                                                data_pool_name)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EINVAL)
        else:
            raise AssertionError("Expected EINVAL")

        self.fs.mon_manager.raw_cluster_cmd('fs', 'new', self.fs.name,
                                            self.fs.metadata_pool_name,
                                            data_pool_name, "--force")

        self.fs.mon_manager.raw_cluster_cmd('fs', 'rm', self.fs.name,
                                            '--yes-i-really-mean-it')


        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                            self.fs.metadata_pool_name,
                                            self.fs.metadata_pool_name,
                                            '--yes-i-really-really-mean-it')
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                            self.fs.metadata_pool_name,
                                            self.fs.get_pgs_per_fs_pool().__str__())
        self.fs.mon_manager.raw_cluster_cmd('fs', 'new', self.fs.name,
                                            self.fs.metadata_pool_name,
                                            data_pool_name)

    def test_evict_client(self):
        """
        Check that a slow client session won't get evicted if it's the
        only session
        """

        session_autoclose = self.fs.get_var("session_autoclose")

        self.mount_b.umount_wait()
        ls_data = self.fs.mds_asok(['session', 'ls'])
        self.assert_session_count(1, ls_data)

        self.mount_a.kill()
        self.mount_a.kill_cleanup()

        time.sleep(session_autoclose * 1.5)
        ls_data = self.fs.mds_asok(['session', 'ls'])
        self.assert_session_count(1, ls_data)

        self.mount_a.mount()
        self.mount_a.wait_until_mounted()
        self.mount_b.mount()
        self.mount_b.wait_until_mounted()

        ls_data = self._session_list()
        self.assert_session_count(2, ls_data)

        self.mount_a.kill()
        self.mount_a.kill_cleanup()

        time.sleep(session_autoclose * 1.5)
        ls_data = self.fs.mds_asok(['session', 'ls'])
        self.assert_session_count(1, ls_data)

    def test_cap_revoke_nonresponder(self):
        """
        Check that a client is evicted if it has not responded to cap revoke
        request for configured number of seconds.
        """
        session_timeout = self.fs.get_var("session_timeout")
        eviction_timeout = session_timeout / 2.0

        self.fs.mds_asok(['config', 'set', 'mds_cap_revoke_eviction_timeout',
                          str(eviction_timeout)])

        cap_holder = self.mount_a.open_background()

        # Wait for the file to be visible from another client, indicating
        # that mount_a has completed its network ops
        self.mount_b.wait_for_visible()

        # Simulate client death
        self.mount_a.kill()

        try:
            # The waiter should get stuck waiting for the capability
            # held on the MDS by the now-dead client A
            cap_waiter = self.mount_b.write_background()

            a = time.time()
            time.sleep(eviction_timeout)
            cap_waiter.wait()
            b = time.time()
            cap_waited = b - a
            log.info("cap_waiter waited {0}s".format(cap_waited))

            # check if the cap is transferred before session timeout kicked in.
            # this is a good enough check to ensure that the client got evicted
            # by the cap auto evicter rather than transitioning to stale state
            # and then getting evicted.
            self.assertLess(cap_waited, session_timeout,
                            "Capability handover took {0}, expected less than {1}".format(
                                cap_waited, session_timeout
                            ))

            self.assertTrue(self.mount_a.is_blacklisted())
            cap_holder.stdin.close()
            try:
                cap_holder.wait()
            except (CommandFailedError, ConnectionLostError):
                # We killed it (and possibly its node), so it raises an error
                pass
        finally:
            self.mount_a.kill_cleanup()

        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

    def test_filtered_df(self):
        pool_name = self.fs.get_data_pool_name()
        raw_df = self.fs.get_pool_df(pool_name)
        raw_avail = float(raw_df["max_avail"])
        out = self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'get',
                                                  pool_name, 'size',
                                                  '-f', 'json-pretty')
        j = json.loads(out)
        pool_size = int(j['size'])

        proc = self.mount_a.run_shell(['df', '.'])
        output = proc.stdout.getvalue()
        fs_avail = output.split('\n')[1].split()[3]
        fs_avail = float(fs_avail) * 1024

        ratio = raw_avail / fs_avail
        assert 0.9 < ratio < 1.1

    def test_dump_inode(self):
        info = self.fs.mds_asok(['dump', 'inode', '1'])
        assert(info['path'] == "/")

    def test_dump_inode_hexademical(self):
        self.mount_a.run_shell(["mkdir", "-p", "foo"])
        ino = self.mount_a.path_to_ino("foo")
        assert type(ino) is int
        info = self.fs.mds_asok(['dump', 'inode', hex(ino)])
        assert info['path'] == "/foo"


class TestCacheDrop(CephFSTestCase):
    CLIENTS_REQUIRED = 1

    def _run_drop_cache_cmd(self, timeout=None):
        result = None
        mds_id = self.fs.get_lone_mds_id()
        if timeout is not None:
            result = self.fs.mon_manager.raw_cluster_cmd("tell", "mds.{0}".format(mds_id),
                                                    "cache", "drop", str(timeout))
        else:
            result = self.fs.mon_manager.raw_cluster_cmd("tell", "mds.{0}".format(mds_id),
                                                    "cache", "drop")
        return json.loads(result)

    def _setup(self, max_caps=20, threshold=400):
        # create some files
        self.mount_a.create_n_files("dc-dir/dc-file", 1000, sync=True)

        # Reduce this so the MDS doesn't rkcall the maximum for simple tests
        self.fs.rank_asok(['config', 'set', 'mds_recall_max_caps', str(max_caps)])
        self.fs.rank_asok(['config', 'set', 'mds_recall_max_decay_threshold', str(threshold)])

    def test_drop_cache_command(self):
        """
        Basic test for checking drop cache command.
        Confirm it halts without a timeout.
        Note that the cache size post trimming is not checked here.
        """
        mds_min_caps_per_client = int(self.fs.get_config("mds_min_caps_per_client"))
        self._setup()
        result = self._run_drop_cache_cmd()
        self.assertTrue(result['client_recall']['return_code'] == 0)
        self.assertTrue(result['flush_journal']['return_code'] == 0)
        # It should take at least 1 second
        self.assertTrue(result['duration'] > 1)
        self.assertGreaterEqual(result['trim_cache']['trimmed'], 1000-2*mds_min_caps_per_client)

    def test_drop_cache_command_timeout(self):
        """
        Basic test for checking drop cache command.
        Confirm recall halts early via a timeout.
        Note that the cache size post trimming is not checked here.
        """
        self._setup()
        result = self._run_drop_cache_cmd(timeout=10)
        self.assertTrue(result['client_recall']['return_code'] == -errno.ETIMEDOUT)
        self.assertTrue(result['flush_journal']['return_code'] == 0)
        self.assertTrue(result['duration'] > 10)
        self.assertGreaterEqual(result['trim_cache']['trimmed'], 100) # we did something, right?

    def test_drop_cache_command_dead_timeout(self):
        """
        Check drop cache command with non-responding client using tell
        interface. Note that the cache size post trimming is not checked
        here.
        """
        self._setup()
        self.mount_a.kill()
        # Note: recall is subject to the timeout. The journal flush will
        # be delayed due to the client being dead.
        result = self._run_drop_cache_cmd(timeout=5)
        self.assertTrue(result['client_recall']['return_code'] == -errno.ETIMEDOUT)
        self.assertTrue(result['flush_journal']['return_code'] == 0)
        self.assertTrue(result['duration'] > 5)
        self.assertTrue(result['duration'] < 120)
        self.assertEqual(0, result['trim_cache']['trimmed'])
        self.mount_a.kill_cleanup()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

    def test_drop_cache_command_dead(self):
        """
        Check drop cache command with non-responding client using tell
        interface. Note that the cache size post trimming is not checked
        here.
        """
        self._setup()
        self.mount_a.kill()
        result = self._run_drop_cache_cmd()
        self.assertTrue(result['client_recall']['return_code'] == 0)
        self.assertTrue(result['flush_journal']['return_code'] == 0)
        self.assertTrue(result['duration'] > 5)
        self.assertTrue(result['duration'] < 120)
        self.assertEqual(0, result['trim_cache']['trimmed'])
        self.mount_a.kill_cleanup()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()
