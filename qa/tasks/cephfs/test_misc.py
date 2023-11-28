from io import StringIO

from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from textwrap import dedent
from threading import Thread
import errno
import platform
import time
import json
import logging
import os

log = logging.getLogger(__name__)

class TestMisc(CephFSTestCase):
    CLIENTS_REQUIRED = 2

    def test_statfs_on_deleted_fs(self):
        """
        That statfs does not cause monitors to SIGSEGV after fs deletion.
        """

        self.mount_b.umount_wait()
        self.mount_a.run_shell_payload("stat -f .")
        self.fs.delete_all_filesystems()
        # This will hang either way, run in background.
        p = self.mount_a.run_shell_payload("stat -f .", wait=False, timeout=60, check_status=False)
        time.sleep(30)
        self.assertFalse(p.finished)
        # the process is stuck in uninterruptible sleep, just kill the mount
        self.mount_a.umount_wait(force=True)
        p.wait()

    def test_fuse_mount_on_already_mounted_path(self):
        if platform.system() != "Linux":
            self.skipTest("Require Linux platform")

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Require FUSE client")

        # Try to mount already mounted path
        # expecting EBUSY error
        try:
            mount_cmd = ['sudo'] + self.mount_a._mount_bin + [self.mount_a.hostfs_mntpt]
            self.mount_a.client_remote.run(args=mount_cmd, stderr=StringIO(),
                    stdout=StringIO(), timeout=60, omit_sudo=False)
        except CommandFailedError as e:
            self.assertEqual(e.exitstatus, errno.EBUSY)
        else:
            self.fail("Expected EBUSY")

    def test_getattr_caps(self):
        """
        Check if MDS recognizes the 'mask' parameter of open request.
        The parameter allows client to request caps when opening file
        """

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Require FUSE client")

        # Enable debug. Client will requests CEPH_CAP_XATTR_SHARED
        # on lookup/open
        self.mount_b.umount_wait()
        self.set_conf('client', 'client debug getattr caps', 'true')
        self.mount_b.mount_wait()

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
        self.assertGreaterEqual(float(rctime), t - 10)

    def test_fs_new(self):
        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        data_pool_name = self.fs.get_data_pool_name()

        self.fs.fail()

        self.fs.mon_manager.raw_cluster_cmd('fs', 'rm', self.fs.name,
                                            '--yes-i-really-mean-it')

        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                            self.fs.metadata_pool_name,
                                            self.fs.metadata_pool_name,
                                            '--yes-i-really-really-mean-it')
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                            self.fs.metadata_pool_name,
                                            '--pg_num_min', str(self.fs.pg_num_min))

        # insert a garbage object
        self.fs.radosm(["put", "foo", "-"], stdin=StringIO("bar"))

        def get_pool_df(fs, name):
            try:
                return fs.get_pool_df(name)['objects'] > 0
            except RuntimeError:
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

        self.fs.mon_manager.raw_cluster_cmd('fs', 'fail', self.fs.name)

        self.fs.mon_manager.raw_cluster_cmd('fs', 'rm', self.fs.name,
                                            '--yes-i-really-mean-it')

        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'delete',
                                            self.fs.metadata_pool_name,
                                            self.fs.metadata_pool_name,
                                            '--yes-i-really-really-mean-it')
        self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'create',
                                            self.fs.metadata_pool_name,
                                            '--pg_num_min', str(self.fs.pg_num_min))
        self.fs.mon_manager.raw_cluster_cmd('fs', 'new', self.fs.name,
                                            self.fs.metadata_pool_name,
                                            data_pool_name)

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
        self.mount_a.suspend_netns()

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

            self.assertTrue(self.mds_cluster.is_addr_blocklisted(
                self.mount_a.get_global_addr()))
            self.mount_a._kill_background(cap_holder)
        finally:
            self.mount_a.resume_netns()

    def test_filtered_df(self):
        pool_name = self.fs.get_data_pool_name()
        raw_df = self.fs.get_pool_df(pool_name)
        raw_avail = float(raw_df["max_avail"])
        out = self.fs.mon_manager.raw_cluster_cmd('osd', 'pool', 'get',
                                                  pool_name, 'size',
                                                  '-f', 'json-pretty')
        _ = json.loads(out)

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

    def test_fs_lsflags(self):
        """
        Check that the lsflags displays the default state and the new state of flags
        """
        # Set some flags
        self.fs.set_joinable(False)
        self.fs.set_allow_new_snaps(False)
        self.fs.set_allow_standby_replay(True)

        lsflags = json.loads(self.fs.mon_manager.raw_cluster_cmd('fs', 'lsflags',
                                                                 self.fs.name,
                                                                 "--format=json-pretty"))
        self.assertEqual(lsflags["joinable"], False)
        self.assertEqual(lsflags["allow_snaps"], False)
        self.assertEqual(lsflags["allow_multimds_snaps"], True)
        self.assertEqual(lsflags["allow_standby_replay"], True)

    def _test_sync_stuck_for_around_5s(self, dir_path, file_sync=False):
        self.mount_a.run_shell(["mkdir", dir_path])

        sync_dir_pyscript = dedent("""
                import os

                path = "{path}"
                dfd = os.open(path, os.O_DIRECTORY)
                os.fsync(dfd)
                os.close(dfd)
            """.format(path=dir_path))

        # run create/delete directories and test the sync time duration
        for i in range(300):
            for j in range(5):
                self.mount_a.run_shell(["mkdir", os.path.join(dir_path, f"{i}_{j}")])
            start = time.time()
            if file_sync:
                self.mount_a.run_shell(['python3', '-c', sync_dir_pyscript], timeout=4)
            else:
                self.mount_a.run_shell(["sync"], timeout=4)
            # the real duration should be less than the rough one
            duration = time.time() - start
            log.info(f"sync mkdir i = {i}, rough duration = {duration}")

            for j in range(5):
                self.mount_a.run_shell(["rm", "-rf", os.path.join(dir_path, f"{i}_{j}")])
            start = time.time()
            if file_sync:
                self.mount_a.run_shell(['python3', '-c', sync_dir_pyscript], timeout=4)
            else:
                self.mount_a.run_shell(["sync"], timeout=4)
            # the real duration should be less than the rough one
            duration = time.time() - start
            log.info(f"sync rmdir i = {i}, rough duration = {duration}")

        self.mount_a.run_shell(["rm", "-rf", dir_path])

    def test_filesystem_sync_stuck_for_around_5s(self):
        """
        To check whether the filesystem sync will be stuck to wait for the
        mdlog to be flushed for at most 5 seconds.
        """

        dir_path = "filesystem_sync_do_not_wait_mdlog_testdir"
        self._test_sync_stuck_for_around_5s(dir_path)

    def test_file_sync_stuck_for_around_5s(self):
        """
        To check whether the fsync will be stuck to wait for the mdlog to
        be flushed for at most 5 seconds.
        """

        dir_path = "file_sync_do_not_wait_mdlog_testdir"
        self._test_sync_stuck_for_around_5s(dir_path, True)

    def test_file_filesystem_sync_crash(self):
        """
        To check whether the kernel crashes when doing the file/filesystem sync.
        """

        stop_thread = False
        dir_path = "file_filesystem_sync_crash_testdir"
        self.mount_a.run_shell(["mkdir", dir_path])

        def mkdir_rmdir_thread(mount, path):
            #global stop_thread

            log.info(" mkdir_rmdir_thread starting...")
            num = 0
            while not stop_thread:
                n = num
                m = num
                for __ in range(10):
                    mount.run_shell(["mkdir", os.path.join(path, f"{n}")])
                    n += 1
                for __ in range(10):
                    mount.run_shell(["rm", "-rf", os.path.join(path, f"{m}")])
                    m += 1
                num += 10
            log.info(" mkdir_rmdir_thread stopped")

        def filesystem_sync_thread(mount, path):
            #global stop_thread

            log.info(" filesystem_sync_thread starting...")
            while not stop_thread:
                mount.run_shell(["sync"])
            log.info(" filesystem_sync_thread stopped")

        def file_sync_thread(mount, path):
            #global stop_thread

            log.info(" file_sync_thread starting...")
            pyscript = dedent("""
                    import os

                    path = "{path}"
                    dfd = os.open(path, os.O_DIRECTORY)
                    os.fsync(dfd)
                    os.close(dfd)
                """.format(path=path))

            while not stop_thread:
                mount.run_shell(['python3', '-c', pyscript])
            log.info(" file_sync_thread stopped")

        td1 = Thread(target=mkdir_rmdir_thread, args=(self.mount_a, dir_path,))
        td2 = Thread(target=filesystem_sync_thread, args=(self.mount_a, dir_path,))
        td3 = Thread(target=file_sync_thread, args=(self.mount_a, dir_path,))

        td1.start()
        td2.start()
        td3.start()
        time.sleep(1200) # run 20 minutes
        stop_thread = True
        td1.join()
        td2.join()
        td3.join()
        self.mount_a.run_shell(["rm", "-rf", dir_path])


    def _session_client_ls(self, cmd):
        mount_a_client_id = self.mount_a.get_global_id()
        info = self.fs.rank_asok(cmd)
        mount_a_mountpoint = self.mount_a.mountpoint
        mount_b_mountpoint = self.mount_b.mountpoint
        self.assertIsNotNone(info)
        for i in range(0, len(info)):
            self.assertIn(info[i]["client_metadata"]["mount_point"], 
                             [mount_a_mountpoint, mount_b_mountpoint])        
        info = self.fs.rank_asok(cmd + [f"id={mount_a_client_id}"])
        self.assertEqual(len(info), 1)
        self.assertEqual(info[0]["id"], mount_a_client_id)
        self.assertEqual(info[0]["client_metadata"]["mount_point"], mount_a_mountpoint)
        info = self.fs.rank_asok(cmd + ['--cap_dump'])
        for i in range(0, len(info)):
            self.assertIn("caps", info[i])

    def test_session_ls(self):
        self._session_client_ls(['session', 'ls'])

    def test_client_ls(self):
        self._session_client_ls(['client', 'ls'])
        
class TestCacheDrop(CephFSTestCase):
    CLIENTS_REQUIRED = 1

    def _run_drop_cache_cmd(self, timeout=None):
        result = None
        args = ["cache", "drop"]
        if timeout is not None:
            args.append(str(timeout))
        result = self.fs.rank_tell(args)
        return result

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
        self.assertEqual(result['client_recall']['return_code'], 0)
        self.assertEqual(result['flush_journal']['return_code'], 0)
        # It should take at least 1 second
        self.assertGreater(result['duration'], 1)
        self.assertGreaterEqual(result['trim_cache']['trimmed'], 1000-2*mds_min_caps_per_client)

    def test_drop_cache_command_timeout(self):
        """
        Basic test for checking drop cache command.
        Confirm recall halts early via a timeout.
        Note that the cache size post trimming is not checked here.
        """
        self._setup()
        result = self._run_drop_cache_cmd(timeout=10)
        self.assertEqual(result['client_recall']['return_code'], -errno.ETIMEDOUT)
        self.assertEqual(result['flush_journal']['return_code'], 0)
        self.assertGreater(result['duration'], 10)
        self.assertGreaterEqual(result['trim_cache']['trimmed'], 100) # we did something, right?

    def test_drop_cache_command_dead_timeout(self):
        """
        Check drop cache command with non-responding client using tell
        interface. Note that the cache size post trimming is not checked
        here.
        """
        self._setup()
        self.mount_a.suspend_netns()
        # Note: recall is subject to the timeout. The journal flush will
        # be delayed due to the client being dead.
        result = self._run_drop_cache_cmd(timeout=5)
        self.assertEqual(result['client_recall']['return_code'], -errno.ETIMEDOUT)
        self.assertEqual(result['flush_journal']['return_code'], 0)
        self.assertGreater(result['duration'], 5)
        self.assertLess(result['duration'], 120)
        # Note: result['trim_cache']['trimmed'] may be >0 because dropping the
        # cache now causes the Locker to drive eviction of stale clients (a
        # stale session will be autoclosed at mdsmap['session_timeout']). The
        # particular operation causing this is journal flush which causes the
        # MDS to wait wait for cap revoke.
        #self.assertEqual(0, result['trim_cache']['trimmed'])
        self.mount_a.resume_netns()

    def test_drop_cache_command_dead(self):
        """
        Check drop cache command with non-responding client using tell
        interface. Note that the cache size post trimming is not checked
        here.
        """
        self._setup()
        self.mount_a.suspend_netns()
        result = self._run_drop_cache_cmd()
        self.assertEqual(result['client_recall']['return_code'], 0)
        self.assertEqual(result['flush_journal']['return_code'], 0)
        self.assertGreater(result['duration'], 5)
        self.assertLess(result['duration'], 120)
        # Note: result['trim_cache']['trimmed'] may be >0 because dropping the
        # cache now causes the Locker to drive eviction of stale clients (a
        # stale session will be autoclosed at mdsmap['session_timeout']). The
        # particular operation causing this is journal flush which causes the
        # MDS to wait wait for cap revoke.
        self.mount_a.resume_netns()

class TestSkipReplayInoTable(CephFSTestCase):
    MDSS_REQUIRED = 1
    CLIENTS_REQUIRED = 1

    def test_alloc_cinode_assert(self):
        """
        Test alloc CInode assert.

        See: https://tracker.ceph.com/issues/52280
        """

        # Create a directory and the mds will journal this and then crash
        self.mount_a.run_shell(["rm", "-rf", "test_alloc_ino"])
        self.mount_a.run_shell(["mkdir", "test_alloc_ino"])

        status = self.fs.status()
        rank0 = self.fs.get_rank(rank=0, status=status)

        self.fs.mds_asok(['config', 'set', 'mds_kill_skip_replaying_inotable', "true"])
        # This will make the MDS crash, since we only have one MDS in the
        # cluster and without the "wait=False" it will stuck here forever.
        self.mount_a.run_shell(["mkdir", "test_alloc_ino/dir1"], wait=False)

        # sleep 10 seconds to make sure the journal logs are flushed and
        # the mds crashes
        time.sleep(10)

        # Now set the mds config to skip replaying the inotable
        self.fs.set_ceph_conf('mds', 'mds_inject_skip_replaying_inotable', True)
        self.fs.set_ceph_conf('mds', 'mds_wipe_sessions', True)

        self.fs.mds_restart()
        # sleep 5 seconds to make sure the mds tell command won't stuck
        time.sleep(5)
        self.fs.wait_for_daemons()

        self.delete_mds_coredump(rank0['name']);

        self.mount_a.run_shell(["mkdir", "test_alloc_ino/dir2"])

        ls_out = set(self.mount_a.ls("test_alloc_ino/"))
        self.assertEqual(ls_out, set({"dir1", "dir2"}))
