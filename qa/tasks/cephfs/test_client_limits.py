
"""
Exercise the MDS's behaviour when clients and the MDCache reach or
exceed the limits of how many caps/inodes they should hold.
"""

import logging
from textwrap import dedent
from unittest import SkipTest
from teuthology.orchestra.run import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase, needs_trimming
from tasks.cephfs.fuse_mount import FuseMount
import os


log = logging.getLogger(__name__)


# Arbitrary timeouts for operations involving restarting
# an MDS or waiting for it to come up
MDS_RESTART_GRACE = 60

# Hardcoded values from Server::recall_client_state
CAP_RECALL_RATIO = 0.8
CAP_RECALL_MIN = 100


class TestClientLimits(CephFSTestCase):
    REQUIRE_KCLIENT_REMOTE = True
    CLIENTS_REQUIRED = 2

    def _test_client_pin(self, use_subdir):
        """
        When a client pins an inode in its cache, for example because the file is held open,
        it should reject requests from the MDS to trim these caps.  The MDS should complain
        to the user that it is unable to enforce its cache size limits because of this
        objectionable client.

        :param use_subdir: whether to put test files in a subdir or use root
        """

        cache_size = 200
        open_files = 250

        self.set_conf('mds', 'mds cache size', cache_size)
        self.fs.mds_fail_restart()
        self.fs.wait_for_daemons()

        mount_a_client_id = self.mount_a.get_global_id()
        path = "subdir/mount_a" if use_subdir else "mount_a"
        open_proc = self.mount_a.open_n_background(path, open_files)

        # Client should now hold:
        # `open_files` caps for the open files
        # 1 cap for root
        # 1 cap for subdir
        self.wait_until_equal(lambda: self.get_session(mount_a_client_id)['num_caps'],
                              open_files + (2 if use_subdir else 1),
                              timeout=600,
                              reject_fn=lambda x: x > open_files + 2)

        # MDS should not be happy about that, as the client is failing to comply
        # with the SESSION_RECALL messages it is being sent
        mds_recall_state_timeout = int(self.fs.get_config("mds_recall_state_timeout"))
        self.wait_for_health("failing to respond to cache pressure", mds_recall_state_timeout + 10)

        # When the client closes the files, it should retain only as many caps as allowed
        # under the SESSION_RECALL policy
        log.info("Terminating process holding files open")
        open_proc.stdin.close()
        try:
            open_proc.wait()
        except CommandFailedError:
            # We killed it, so it raises an error
            pass

        # The remaining caps should comply with the numbers sent from MDS in SESSION_RECALL message,
        # which depend on the cache size and overall ratio
        self.wait_until_equal(
            lambda: self.get_session(mount_a_client_id)['num_caps'],
            int(cache_size * 0.8),
            timeout=600,
            reject_fn=lambda x: x < int(cache_size*.8))

    @needs_trimming
    def test_client_pin_root(self):
        self._test_client_pin(False)

    @needs_trimming
    def test_client_pin(self):
        self._test_client_pin(True)

    def test_client_release_bug(self):
        """
        When a client has a bug (which we will simulate) preventing it from releasing caps,
        the MDS should notice that releases are not being sent promptly, and generate a health
        metric to that effect.
        """

        # The debug hook to inject the failure only exists in the fuse client
        if not isinstance(self.mount_a, FuseMount):
            raise SkipTest("Require FUSE client to inject client release failure")

        self.set_conf('client.{0}'.format(self.mount_a.client_id), 'client inject release failure', 'true')
        self.mount_a.teardown()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()
        mount_a_client_id = self.mount_a.get_global_id()

        # Client A creates a file.  He will hold the write caps on the file, and later (simulated bug) fail
        # to comply with the MDSs request to release that cap
        self.mount_a.run_shell(["touch", "file1"])

        # Client B tries to stat the file that client A created
        rproc = self.mount_b.write_background("file1")

        # After mds_revoke_cap_timeout, we should see a health warning (extra lag from
        # MDS beacon period)
        mds_revoke_cap_timeout = int(self.fs.get_config("mds_revoke_cap_timeout"))
        self.wait_for_health("failing to respond to capability release", mds_revoke_cap_timeout + 10)

        # Client B should still be stuck
        self.assertFalse(rproc.finished)

        # Kill client A
        self.mount_a.kill()
        self.mount_a.kill_cleanup()

        # Client B should complete
        self.fs.mds_asok(['session', 'evict', "%s" % mount_a_client_id])
        rproc.wait()

    def test_client_oldest_tid(self):
        """
        When a client does not advance its oldest tid, the MDS should notice that
        and generate health warnings.
        """

        # num of requests client issues
        max_requests = 1000

        # The debug hook to inject the failure only exists in the fuse client
        if not isinstance(self.mount_a, FuseMount):
            raise SkipTest("Require FUSE client to inject client release failure")

        self.set_conf('client', 'client inject fixed oldest tid', 'true')
        self.mount_a.teardown()
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        self.fs.mds_asok(['config', 'set', 'mds_max_completed_requests', '{0}'.format(max_requests)])

        # Create lots of files
        self.mount_a.create_n_files("testdir/file1", max_requests + 100)

        # Create a few files synchronously. This makes sure previous requests are completed
        self.mount_a.create_n_files("testdir/file2", 5, True)

        # Wait for the health warnings. Assume mds can handle 10 request per second at least
        self.wait_for_health("failing to advance its oldest client/flush tid", max_requests / 10)

    def _test_client_cache_size(self, mount_subdir):
        """
        check if client invalidate kernel dcache according to its cache size config
        """

        # The debug hook to inject the failure only exists in the fuse client
        if not isinstance(self.mount_a, FuseMount):
            raise SkipTest("Require FUSE client to inject client release failure")

        if mount_subdir:
            # fuse assigns a fix inode number (1) to root inode. But in mounting into
            # subdir case, the actual inode number of root is not 1. This mismatch
            # confuses fuse_lowlevel_notify_inval_entry() when invalidating dentries
            # in root directory.
            self.mount_a.run_shell(["mkdir", "subdir"])
            self.mount_a.umount_wait()
            self.set_conf('client', 'client mountpoint', '/subdir')
            self.mount_a.mount()
            self.mount_a.wait_until_mounted()
            root_ino = self.mount_a.path_to_ino(".")
            self.assertEqual(root_ino, 1);

        dir_path = os.path.join(self.mount_a.mountpoint, "testdir")

        mkdir_script = dedent("""
            import os
            os.mkdir("{path}")
            for n in range(0, {num_dirs}):
                os.mkdir("{path}/dir{{0}}".format(n))
            """)

        num_dirs = 1000
        self.mount_a.run_python(mkdir_script.format(path=dir_path, num_dirs=num_dirs))
        self.mount_a.run_shell(["sync"])

        dentry_count, dentry_pinned_count = self.mount_a.get_dentry_count()
        self.assertGreaterEqual(dentry_count, num_dirs)
        self.assertGreaterEqual(dentry_pinned_count, num_dirs)

        cache_size = num_dirs / 10
        self.mount_a.set_cache_size(cache_size)

        def trimmed():
            dentry_count, dentry_pinned_count = self.mount_a.get_dentry_count()
            log.info("waiting, dentry_count, dentry_pinned_count: {0}, {1}".format(
                dentry_count, dentry_pinned_count
            ))
            if dentry_count > cache_size or dentry_pinned_count > cache_size:
                return False

            return True

        self.wait_until_true(trimmed, 30)

    @needs_trimming
    def test_client_cache_size(self):
        self._test_client_cache_size(False)
        self._test_client_cache_size(True)
