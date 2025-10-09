
"""
Exercise the MDS's behaviour when clients and the MDCache reach or
exceed the limits of how many caps/inodes they should hold.
"""

import logging
from textwrap import dedent
from tasks.ceph_test_case import TestTimeoutError
from tasks.cephfs.cephfs_test_case import CephFSTestCase, needs_trimming
from tasks.cephfs.fuse_mount import FuseMount
from teuthology.exceptions import CommandFailedError
import os
from io import StringIO


log = logging.getLogger(__name__)


# Arbitrary timeouts for operations involving restarting
# an MDS or waiting for it to come up
MDS_RESTART_GRACE = 60

# Hardcoded values from Server::recall_client_state
CAP_RECALL_RATIO = 0.8
CAP_RECALL_MIN = 100


class TestClientLimits(CephFSTestCase):
    CLIENTS_REQUIRED = 2

    def _test_client_pin(self, use_subdir, open_files):
        """
        When a client pins an inode in its cache, for example because the file is held open,
        it should reject requests from the MDS to trim these caps.  The MDS should complain
        to the user that it is unable to enforce its cache size limits because of this
        objectionable client.

        :param use_subdir: whether to put test files in a subdir or use root
        """

        # Set MDS cache memory limit to a low value that will make the MDS to
        # ask the client to trim the caps.
        cache_memory_limit = "1K"

        self.config_set('mds', 'mds_cache_memory_limit', cache_memory_limit)
        self.config_set('mds', 'mds_recall_max_caps', int(open_files/2))
        self.config_set('mds', 'mds_recall_warning_threshold', open_files)

        mds_min_caps_per_client = int(self.config_get('mds', "mds_min_caps_per_client"))
        self.config_set('mds', 'mds_min_caps_working_set', mds_min_caps_per_client)
        mds_max_caps_per_client = int(self.config_get('mds', "mds_max_caps_per_client"))
        mds_recall_warning_decay_rate = float(self.config_get('mds', "mds_recall_warning_decay_rate"))
        self.assertGreaterEqual(open_files, mds_min_caps_per_client)

        mount_a_client_id = self.mount_a.get_global_id()
        path = "subdir" if use_subdir else "."
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
        self.wait_for_health("MDS_CLIENT_RECALL", mds_recall_warning_decay_rate*2)

        # We can also test that the MDS health warning for oversized
        # cache is functioning as intended.
        self.wait_for_health("MDS_CACHE_OVERSIZED", mds_recall_warning_decay_rate*2)

        # When the client closes the files, it should retain only as many caps as allowed
        # under the SESSION_RECALL policy
        log.info("Terminating process holding files open")
        open_proc.stdin.close()
        open_proc.wait()

        # The remaining caps should comply with the numbers sent from MDS in SESSION_RECALL message,
        # which depend on the caps outstanding, cache size and overall ratio
        def expected_caps():
            num_caps = self.get_session(mount_a_client_id)['num_caps']
            if num_caps <= mds_min_caps_per_client:
                return True
            elif num_caps <= mds_max_caps_per_client:
                return True
            else:
                return False

        self.wait_until_true(expected_caps, timeout=60)

    @needs_trimming
    def test_client_pin_root(self):
        self._test_client_pin(False, 400)

    @needs_trimming
    def test_client_pin(self):
        self._test_client_pin(True, 800)

    @needs_trimming
    def test_client_pin_mincaps(self):
        self._test_client_pin(True, 200)

    def test_client_min_caps_working_set(self):
        """
        When a client has inodes pinned in its cache (open files), that the MDS
        will not warn about the client not responding to cache pressure when
        the number of caps is below mds_min_caps_working_set.
        """

        # Set MDS cache memory limit to a low value that will make the MDS to
        # ask the client to trim the caps.
        cache_memory_limit = "1K"
        open_files = 400

        self.config_set('mds', 'mds_cache_memory_limit', cache_memory_limit)
        self.config_set('mds', 'mds_recall_max_caps', int(open_files/2))
        self.config_set('mds', 'mds_recall_warning_threshold', open_files)
        self.config_set('mds', 'mds_min_caps_working_set', open_files*2)

        mds_min_caps_per_client = int(self.config_get('mds', "mds_min_caps_per_client"))
        mds_recall_warning_decay_rate = float(self.config_get('mds', "mds_recall_warning_decay_rate"))
        self.assertGreaterEqual(open_files, mds_min_caps_per_client)

        mount_a_client_id = self.mount_a.get_global_id()
        p = self.mount_a.open_n_background("subdir", open_files)

        # Client should now hold:
        # `open_files` caps for the open files
        # 1 cap for root
        # 1 cap for subdir
        self.wait_until_equal(lambda: self.get_session(mount_a_client_id)['num_caps'],
                              open_files + 2,
                              timeout=600,
                              reject_fn=lambda x: x > open_files + 2)

        # We can also test that the MDS health warning for oversized
        # cache is functioning as intended.
        self.wait_for_health("MDS_CACHE_OVERSIZED", mds_recall_warning_decay_rate*2)

        try:
            # MDS should not be happy about that but it's not sending
            # MDS_CLIENT_RECALL warnings because the client's caps are below
            # mds_min_caps_working_set.
            self.wait_for_health("MDS_CLIENT_RECALL", mds_recall_warning_decay_rate*2)
        except TestTimeoutError:
            pass
        else:
            raise RuntimeError("expected no client recall warning")
        p.stdin.close()
        p.wait()

    def test_cap_acquisition_throttle_readdir(self):
        """
        Mostly readdir acquires caps faster than the mds recalls, so the cap
        acquisition via readdir is throttled by retrying the readdir after
        a fraction of second (0.5) by default when throttling condition is met.
        """

        subdir_count = 4
        files_per_dir = 25

        # throttle in a way so that two dir reads are already hitting it.
        throttle_value = (files_per_dir * 3) // 2

        # activate throttling logic by setting max per client to a low value
        self.config_set('mds', 'mds_max_caps_per_client', 1)
        self.config_set('mds', 'mds_session_cap_acquisition_throttle', throttle_value)

        # Create files split across {subdir_count} directories, {per_dir_count} in each dir
        for i in range(1, subdir_count+1):
            self.mount_a.create_n_files("dir{0}/file".format(i), files_per_dir, sync=True)

        mount_a_client_id = self.mount_a.get_global_id()

        # recursive readdir. macOs wants an explicit directory for `find`.
        proc = self.mount_a.run_shell_payload("find . | wc", stderr=StringIO())
        # return code may be None if the command got interrupted
        self.assertTrue(proc.returncode is None or proc.returncode == 0, proc.stderr.getvalue())

        # validate the throttle condition to be hit atleast once
        cap_acquisition_throttle_hit_count = self.perf_dump()['mds_server']['cap_acquisition_throttle']
        self.assertGreaterEqual(cap_acquisition_throttle_hit_count, 1)

        # validate cap_acquisition decay counter after readdir to NOT exceed the throttle value
        # plus one batch that could have been taken immediately before querying
        # assuming the batch is equal to the per dir file count.
        cap_acquisition_value = self.get_session(mount_a_client_id)['cap_acquisition']['value']
        self.assertLessEqual(cap_acquisition_value, files_per_dir + throttle_value)

        # make sure that the throttle was reported in the events
        def historic_ops_have_event(expected_event):
            ops_dump = self.fs.rank_tell(['dump_historic_ops'])
            # reverse the events and the ops assuming that later ops would be throttled
            for op in reversed(ops_dump['ops']):
                for ev in reversed(op.get('type_data', {}).get('events', [])):
                    if ev['event'] == expected_event:
                        return True
            return False

        self.assertTrue(historic_ops_have_event('cap_acquisition_throttle'))

    def test_client_release_bug(self):
        """
        When a client has a bug (which we will simulate) preventing it from releasing caps,
        the MDS should notice that releases are not being sent promptly, and generate a health
        metric to that effect.
        """

        # The debug hook to inject the failure only exists in the fuse client
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Require FUSE client to inject client release failure")

        self.set_conf('client.{0}'.format(self.mount_a.client_id), 'client inject release failure', 'true')
        self.mount_a.teardown()
        self.mount_a.mount_wait()
        mount_a_client_id = self.mount_a.get_global_id()

        # Client A creates a file.  He will hold the write caps on the file, and later (simulated bug) fail
        # to comply with the MDSs request to release that cap
        self.mount_a.run_shell(["touch", "file1"])

        # Client B tries to stat the file that client A created
        rproc = self.mount_b.write_background("file1")

        # After session_timeout, we should see a health warning (extra lag from
        # MDS beacon period)
        session_timeout = self.fs.get_var("session_timeout")
        self.wait_for_health("MDS_CLIENT_LATE_RELEASE", session_timeout + 10)

        # Client B should still be stuck
        self.assertFalse(rproc.finished)

        # Kill client A
        self.mount_a.kill()
        self.mount_a.kill_cleanup()

        # Client B should complete
        self.fs.mds_asok(['session', 'evict', "%s" % mount_a_client_id])
        rproc.wait()

    def test_client_blocklisted_oldest_tid(self):
        """
        that a client is blocklisted when its encoded session metadata exceeds the
        configured threshold (due to ever growing `completed_requests` caused due
        to an unidentified bug (in the client or the MDS)).
        """

        # num of requests client issues
        max_requests = 10000

        # The debug hook to inject the failure only exists in the fuse client
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Require FUSE client to inject client release failure")

        self.config_set('client', 'client inject fixed oldest tid', 'true')
        self.mount_a.teardown()
        self.mount_a.mount_wait()

        self.config_set('mds', 'mds_max_completed_requests', max_requests);

        # Create lots of files
        self.mount_a.create_n_files("testdir/file1", max_requests + 100)

        # Create a few files synchronously. This makes sure previous requests are completed
        self.mount_a.create_n_files("testdir/file2", 5, True)

        # Wait for the health warnings. Assume mds can handle 10 request per second at least
        self.wait_for_health("MDS_CLIENT_OLDEST_TID", max_requests // 10, check_in_detail=str(self.mount_a.client_id))

        # set the threshold low so that it has a high probability of
        # hitting.
        self.config_set('mds', 'mds_session_metadata_threshold', 5000);

        # Create lot many files synchronously. This would hit the session metadata threshold
        # causing the client to get blocklisted.
        with self.assertRaises(CommandFailedError):
            self.mount_a.create_n_files("testdir/file2", 100000, True)

        self.mds_cluster.is_addr_blocklisted(self.mount_a.get_global_addr())
        # the mds should bump up the relevant perf counter
        pd = self.perf_dump()
        self.assertGreater(pd['mds_sessions']['mdthresh_evicted'], 0)

        # reset the config
        self.config_set('client', 'client inject fixed oldest tid', 'false')

        self.mount_a.kill_cleanup()
        self.mount_a.mount_wait()

    def test_client_oldest_tid(self):
        """
        When a client does not advance its oldest tid, the MDS should notice that
        and generate health warnings.
        """

        # num of requests client issues
        max_requests = 1000

        # The debug hook to inject the failure only exists in the fuse client
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Require FUSE client to inject client release failure")

        self.set_conf('client', 'client inject fixed oldest tid', 'true')
        self.mount_a.teardown()
        self.mount_a.mount_wait()

        self.fs.mds_asok(['config', 'set', 'mds_max_completed_requests', '{0}'.format(max_requests)])

        # Create lots of files
        self.mount_a.create_n_files("testdir/file1", max_requests + 100)

        # Create a few files synchronously. This makes sure previous requests are completed
        self.mount_a.create_n_files("testdir/file2", 5, True)

        # Wait for the health warnings. Assume mds can handle 10 request per second at least
        self.wait_for_health("MDS_CLIENT_OLDEST_TID", max_requests // 10, check_in_detail=str(self.mount_a.client_id))

        # reset the config val
        self.set_conf('client', 'client inject fixed oldest tid', 'false')
        self.mount_a.teardown()
        self.mount_a.mount_wait()

    def _test_client_cache_size(self, mount_subdir):
        """
        check if client invalidate kernel dcache according to its cache size config
        """

        # The debug hook to inject the failure only exists in the fuse client
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Require FUSE client to inject client release failure")

        if mount_subdir:
            # fuse assigns a fix inode number (1) to root inode. But in mounting into
            # subdir case, the actual inode number of root is not 1. This mismatch
            # confuses fuse_lowlevel_notify_inval_entry() when invalidating dentries
            # in root directory.
            self.mount_a.run_shell(["mkdir", "subdir"])
            self.mount_a.umount_wait()
            self.set_conf('client', 'client mountpoint', '/subdir')
            self.mount_a.mount_wait()
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

        cache_size = num_dirs // 10
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

    def test_client_max_caps(self):
        """
        That the MDS will not let a client sit above mds_max_caps_per_client caps.
        """

        mds_min_caps_per_client = int(self.config_get('mds', "mds_min_caps_per_client"))
        mds_max_caps_per_client = 2*mds_min_caps_per_client
        self.config_set('mds', 'mds_max_caps_per_client', mds_max_caps_per_client)

        self.mount_a.create_n_files("foo/", 3*mds_max_caps_per_client, sync=True)

        mount_a_client_id = self.mount_a.get_global_id()
        def expected_caps():
            num_caps = self.get_session(mount_a_client_id)['num_caps']
            if num_caps <= mds_max_caps_per_client:
                return True
            else:
                return False

        self.wait_until_true(expected_caps, timeout=60)
