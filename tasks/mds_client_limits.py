
"""
Exercise the MDS's behaviour when clients and the MDCache reach or
exceed the limits of how many caps/inodes they should hold.
"""

import contextlib
import logging
from unittest import SkipTest
from teuthology.orchestra.run import CommandFailedError

from tasks.cephfs.filesystem import Filesystem
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase, run_tests


log = logging.getLogger(__name__)


# Arbitrary timeouts for operations involving restarting
# an MDS or waiting for it to come up
MDS_RESTART_GRACE = 60

# Hardcoded values from Server::recall_client_state
CAP_RECALL_RATIO = 0.8
CAP_RECALL_MIN = 100


class TestClientLimits(CephFSTestCase):
    # Environment references
    mds_session_timeout = None
    mds_reconnect_timeout = None
    ms_max_backoff = None

    def wait_for_health(self, pattern, timeout):
        """
        Wait until 'ceph health' contains a single message matching the pattern
        """
        def seen_health_warning():
            health = self.fs.mon_manager.get_mon_health()
            summary_strings = [s['summary'] for s in health['summary']]
            if len(summary_strings) == 0:
                log.debug("Not expected number of summary strings ({0})".format(summary_strings))
                return False
            elif len(summary_strings) == 1 and pattern in summary_strings[0]:
                return True
            else:
                raise RuntimeError("Unexpected health messages: {0}".format(summary_strings))

        self.wait_until_true(seen_health_warning, timeout)

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
        self.fs.mds_restart()
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

    def test_client_pin_root(self):
        self._test_client_pin(False)

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


@contextlib.contextmanager
def task(ctx, config):
    fs = Filesystem(ctx)

    # Pick out the clients we will use from the configuration
    # =======================================================
    if len(ctx.mounts) < 2:
        raise RuntimeError("Need at least two clients")
    mount_a = ctx.mounts.values()[0]
    mount_b = ctx.mounts.values()[1]

    if not isinstance(mount_a, FuseMount) or not isinstance(mount_b, FuseMount):
        # kclient kill() power cycles nodes, so requires clients to each be on
        # their own node
        if mount_a.client_remote.hostname == mount_b.client_remote.hostname:
            raise RuntimeError("kclient clients must be on separate nodes")

    # Stash references on ctx so that we can easily debug in interactive mode
    # =======================================================================
    ctx.filesystem = fs
    ctx.mount_a = mount_a
    ctx.mount_b = mount_b

    run_tests(ctx, config, TestClientLimits, {
        'fs': fs,
        'mount_a': mount_a,
        'mount_b': mount_b
    })

    # Continue to any downstream tasks
    # ================================
    yield
