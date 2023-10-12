
"""
Teuthology task for exercising CephFS client recovery
"""

import logging
import signal
from textwrap import dedent
import time
import distutils.version as version
import random
import re
import string
import os

from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.exceptions import CommandFailedError
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.packaging import get_package_version

log = logging.getLogger(__name__)


# Arbitrary timeouts for operations involving restarting
# an MDS or waiting for it to come up
MDS_RESTART_GRACE = 60


class TestClientNetworkRecovery(CephFSTestCase):
    REQUIRE_ONE_CLIENT_REMOTE = True
    CLIENTS_REQUIRED = 2

    LOAD_SETTINGS = ["mds_reconnect_timeout", "ms_max_backoff"]

    # Environment references
    mds_reconnect_timeout = None
    ms_max_backoff = None

    def test_network_death(self):
        """
        Simulate software freeze or temporary network failure.

        Check that the client blocks I/O during failure, and completes
        I/O after failure.
        """

        session_timeout = self.fs.get_var("session_timeout")
        self.fs.mds_asok(['config', 'set', 'mds_defer_session_stale', 'false'])

        # We only need one client
        self.mount_b.umount_wait()

        # Initially our one client session should be visible
        client_id = self.mount_a.get_global_id()
        ls_data = self._session_list()
        self.assert_session_count(1, ls_data)
        self.assertEqual(ls_data[0]['id'], client_id)
        self.assert_session_state(client_id, "open")

        # ...and capable of doing I/O without blocking
        self.mount_a.create_files()

        # ...but if we turn off the network
        self.fs.set_clients_block(True)

        # ...and try and start an I/O
        write_blocked = self.mount_a.write_background()

        # ...then it should block
        self.assertFalse(write_blocked.finished)
        self.assert_session_state(client_id, "open")
        time.sleep(session_timeout * 1.5)  # Long enough for MDS to consider session stale
        self.assertFalse(write_blocked.finished)
        self.assert_session_state(client_id, "stale")

        # ...until we re-enable I/O
        self.fs.set_clients_block(False)

        # ...when it should complete promptly
        a = time.time()
        self.wait_until_true(lambda: write_blocked.finished, self.ms_max_backoff * 2)
        write_blocked.wait()  # Already know we're finished, wait() to raise exception on errors
        recovery_time = time.time() - a
        log.info("recovery time: {0}".format(recovery_time))
        self.assert_session_state(client_id, "open")


class TestClientRecovery(CephFSTestCase):
    CLIENTS_REQUIRED = 2

    LOAD_SETTINGS = ["mds_reconnect_timeout", "ms_max_backoff"]

    # Environment references
    mds_reconnect_timeout = None
    ms_max_backoff = None

    def test_basic(self):
        # Check that two clients come up healthy and see each others' files
        # =====================================================
        self.mount_a.create_files()
        self.mount_a.check_files()
        self.mount_a.umount_wait()

        self.mount_b.check_files()

        self.mount_a.mount_wait()

        # Check that the admin socket interface is correctly reporting
        # two sessions
        # =====================================================
        ls_data = self._session_list()
        self.assert_session_count(2, ls_data)

        self.assertSetEqual(
            set([l['id'] for l in ls_data]),
            {self.mount_a.get_global_id(), self.mount_b.get_global_id()}
        )

    def test_restart(self):
        # Check that after an MDS restart both clients reconnect and continue
        # to handle I/O
        # =====================================================
        self.fs.mds_fail_restart()
        self.fs.wait_for_state('up:active', timeout=MDS_RESTART_GRACE)

        self.mount_a.create_destroy()
        self.mount_b.create_destroy()

    def _session_num_caps(self, client_id):
        ls_data = self.fs.mds_asok(['session', 'ls'])
        return int(self._session_by_id(ls_data).get(client_id, {'num_caps': None})['num_caps'])

    def test_reconnect_timeout(self):
        # Reconnect timeout
        # =================
        # Check that if I stop an MDS and a client goes away, the MDS waits
        # for the reconnect period

        mount_a_client_id = self.mount_a.get_global_id()

        self.fs.fail()

        self.mount_a.umount_wait(force=True)

        self.fs.set_joinable()

        self.fs.wait_for_state('up:reconnect', reject='up:active', timeout=MDS_RESTART_GRACE)
        # Check that the MDS locally reports its state correctly
        status = self.fs.mds_asok(['status'])
        self.assertIn("reconnect_status", status)

        ls_data = self._session_list()
        self.assert_session_count(2, ls_data)

        # The session for the dead client should have the 'reconnect' flag set
        self.assertTrue(self.get_session(mount_a_client_id)['reconnecting'])

        # Wait for the reconnect state to clear, this should take the
        # reconnect timeout period.
        in_reconnect_for = self.fs.wait_for_state('up:active', timeout=self.mds_reconnect_timeout * 2)
        # Check that the period we waited to enter active is within a factor
        # of two of the reconnect timeout.
        self.assertGreater(in_reconnect_for, self.mds_reconnect_timeout // 2,
                           "Should have been in reconnect phase for {0} but only took {1}".format(
                               self.mds_reconnect_timeout, in_reconnect_for
                           ))

        self.assert_session_count(1)

        # Check that the client that timed out during reconnect can
        # mount again and do I/O
        self.mount_a.mount_wait()
        self.mount_a.create_destroy()

        self.assert_session_count(2)

    def test_reconnect_eviction(self):
        # Eviction during reconnect
        # =========================
        mount_a_client_id = self.mount_a.get_global_id()

        self.fs.fail()

        # The mount goes away while the MDS is offline
        self.mount_a.kill()

        # wait for it to die
        time.sleep(5)

        self.fs.set_joinable()

        # Enter reconnect phase
        self.fs.wait_for_state('up:reconnect', reject='up:active', timeout=MDS_RESTART_GRACE)
        self.assert_session_count(2)

        # Evict the stuck client
        self.fs.mds_asok(['session', 'evict', "%s" % mount_a_client_id])
        self.assert_session_count(1)

        # Observe that we proceed to active phase without waiting full reconnect timeout
        evict_til_active = self.fs.wait_for_state('up:active', timeout=MDS_RESTART_GRACE)
        # Once we evict the troublemaker, the reconnect phase should complete
        # in well under the reconnect timeout.
        self.assertLess(evict_til_active, self.mds_reconnect_timeout * 0.5,
                        "reconnect did not complete soon enough after eviction, took {0}".format(
                            evict_til_active
                        ))

        # We killed earlier so must clean up before trying to use again
        self.mount_a.kill_cleanup()

        # Bring the client back
        self.mount_a.mount_wait()
        self.mount_a.create_destroy()

    def _test_stale_caps(self, write):
        session_timeout = self.fs.get_var("session_timeout")

        # Capability release from stale session
        # =====================================
        if write:
            content = ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
            cap_holder = self.mount_a.open_background(content=content)
        else:
            content = ''
            self.mount_a.run_shell(["touch", "background_file"])
            self.mount_a.umount_wait()
            self.mount_a.mount_wait()
            cap_holder = self.mount_a.open_background(write=False)

        self.assert_session_count(2)
        mount_a_gid = self.mount_a.get_global_id()

        # Wait for the file to be visible from another client, indicating
        # that mount_a has completed its network ops
        self.mount_b.wait_for_visible(size=len(content))

        # Simulate client death
        self.mount_a.suspend_netns()

        # wait for it to die so it doesn't voluntarily release buffer cap
        time.sleep(5)

        try:
            # Now, after session_timeout seconds, the waiter should
            # complete their operation when the MDS marks the holder's
            # session stale.
            cap_waiter = self.mount_b.write_background()
            a = time.time()
            cap_waiter.wait()
            b = time.time()

            # Should have succeeded
            self.assertEqual(cap_waiter.exitstatus, 0)

            if write:
                self.assert_session_count(1)
            else:
                self.assert_session_state(mount_a_gid, "stale")

            cap_waited = b - a
            log.info("cap_waiter waited {0}s".format(cap_waited))
            self.assertTrue(session_timeout / 2.0 <= cap_waited <= session_timeout * 2.0,
                            "Capability handover took {0}, expected approx {1}".format(
                                cap_waited, session_timeout
                            ))
        finally:
            self.mount_a.resume_netns() # allow the mount to recover otherwise background proc is unkillable
        self.mount_a._kill_background(cap_holder)

    def test_stale_read_caps(self):
        self._test_stale_caps(False)

    def test_stale_write_caps(self):
        self._test_stale_caps(True)

    def test_evicted_caps(self):
        # Eviction while holding a capability
        # ===================================

        session_timeout = self.fs.get_var("session_timeout")

        # Take out a write capability on a file on client A,
        # and then immediately kill it.
        cap_holder = self.mount_a.open_background()
        mount_a_client_id = self.mount_a.get_global_id()

        # Wait for the file to be visible from another client, indicating
        # that mount_a has completed its network ops
        self.mount_b.wait_for_visible()

        # Simulate client death
        self.mount_a.suspend_netns()

        # wait for it to die so it doesn't voluntarily release buffer cap
        time.sleep(5)

        try:
            # The waiter should get stuck waiting for the capability
            # held on the MDS by the now-dead client A
            cap_waiter = self.mount_b.write_background()
            time.sleep(5)
            self.assertFalse(cap_waiter.finished)

            self.fs.mds_asok(['session', 'evict', "%s" % mount_a_client_id])
            # Now, because I evicted the old holder of the capability, it should
            # immediately get handed over to the waiter
            a = time.time()
            cap_waiter.wait()
            b = time.time()
            cap_waited = b - a
            log.info("cap_waiter waited {0}s".format(cap_waited))
            # This is the check that it happened 'now' rather than waiting
            # for the session timeout
            self.assertLess(cap_waited, session_timeout / 2.0,
                            "Capability handover took {0}, expected less than {1}".format(
                                cap_waited, session_timeout / 2.0
                            ))

        finally:
            self.mount_a.resume_netns() # allow the mount to recover otherwise background proc is unkillable
        self.mount_a._kill_background(cap_holder)

    def test_trim_caps(self):
        # Trim capability when reconnecting MDS
        # ===================================

        count = 500
        # Create lots of files
        for i in range(count):
            self.mount_a.run_shell(["touch", "f{0}".format(i)])

        # Populate mount_b's cache
        self.mount_b.run_shell(["ls", "-l"])

        client_id = self.mount_b.get_global_id()
        num_caps = self._session_num_caps(client_id)
        self.assertGreaterEqual(num_caps, count)

        # Restart MDS. client should trim its cache when reconnecting to the MDS
        self.fs.mds_fail_restart()
        self.fs.wait_for_state('up:active', timeout=MDS_RESTART_GRACE)

        num_caps = self._session_num_caps(client_id)
        self.assertLess(num_caps, count,
                        "should have less than {0} capabilities, have {1}".format(
                            count, num_caps
                        ))

    def _is_flockable(self):
        a_version_str = get_package_version(self.mount_a.client_remote, "fuse")
        b_version_str = get_package_version(self.mount_b.client_remote, "fuse")
        flock_version_str = "2.9"

        version_regex = re.compile(r"[0-9\.]+")
        a_result = version_regex.match(a_version_str)
        self.assertTrue(a_result)
        b_result = version_regex.match(b_version_str)
        self.assertTrue(b_result)
        a_version = version.StrictVersion(a_result.group())
        b_version = version.StrictVersion(b_result.group())
        flock_version=version.StrictVersion(flock_version_str)

        if (a_version >= flock_version and b_version >= flock_version):
            log.info("flock locks are available")
            return True
        else:
            log.info("not testing flock locks, machines have versions {av} and {bv}".format(
                av=a_version_str,bv=b_version_str))
            return False

    def test_filelock(self):
        """
        Check that file lock doesn't get lost after an MDS restart
        """

        flockable = self._is_flockable()
        lock_holder = self.mount_a.lock_background(do_flock=flockable)

        self.mount_b.wait_for_visible("background_file-2")
        self.mount_b.check_filelock(do_flock=flockable)

        self.fs.mds_fail_restart()
        self.fs.wait_for_state('up:active', timeout=MDS_RESTART_GRACE)

        self.mount_b.check_filelock(do_flock=flockable)

        self.mount_a._kill_background(lock_holder)

    def test_filelock_eviction(self):
        """
        Check that file lock held by evicted client is given to
        waiting client.
        """
        if not self._is_flockable():
            self.skipTest("flock is not available")

        lock_holder = self.mount_a.lock_background()
        self.mount_b.wait_for_visible("background_file-2")
        self.mount_b.check_filelock()

        lock_taker = self.mount_b.lock_and_release()
        # Check the taker is waiting (doesn't get it immediately)
        time.sleep(2)
        self.assertFalse(lock_holder.finished)
        self.assertFalse(lock_taker.finished)

        try:
            mount_a_client_id = self.mount_a.get_global_id()
            self.fs.mds_asok(['session', 'evict', "%s" % mount_a_client_id])

            # Evicting mount_a should let mount_b's attempt to take the lock
            # succeed
            self.wait_until_true(lambda: lock_taker.finished, timeout=10)
        finally:
            self.mount_a._kill_background(lock_holder)

            # teardown() doesn't quite handle this case cleanly, so help it out
            self.mount_a.kill()
            self.mount_a.kill_cleanup()

        # Bring the client back
        self.mount_a.mount_wait()

    def test_dir_fsync(self):
        self._test_fsync(True);

    def test_create_fsync(self):
        self._test_fsync(False);

    def _test_fsync(self, dirfsync):
        """
        That calls to fsync guarantee visibility of metadata to another
        client immediately after the fsyncing client dies.
        """

        # Leave this guy out until he's needed
        self.mount_b.umount_wait()

        # Create dir + child dentry on client A, and fsync the dir
        path = os.path.join(self.mount_a.mountpoint, "subdir")
        self.mount_a.run_python(
            dedent("""
                import os
                import time

                path = "{path}"

                print("Starting creation...")
                start = time.time()

                os.mkdir(path)
                dfd = os.open(path, os.O_DIRECTORY)

                fd = open(os.path.join(path, "childfile"), "w")
                print("Finished creation in {{0}}s".format(time.time() - start))

                print("Starting fsync...")
                start = time.time()
                if {dirfsync}:
                    os.fsync(dfd)
                else:
                    os.fsync(fd)
                print("Finished fsync in {{0}}s".format(time.time() - start))
            """.format(path=path,dirfsync=str(dirfsync)))
        )

        # Immediately kill the MDS and then client A
        self.fs.fail()
        self.mount_a.kill()
        self.mount_a.kill_cleanup()

        # Restart the MDS.  Wait for it to come up, it'll have to time out in clientreplay
        self.fs.set_joinable()
        log.info("Waiting for reconnect...")
        self.fs.wait_for_state("up:reconnect")
        log.info("Waiting for active...")
        self.fs.wait_for_state("up:active", timeout=MDS_RESTART_GRACE + self.mds_reconnect_timeout)
        log.info("Reached active...")

        # Is the child dentry visible from mount B?
        self.mount_b.mount_wait()
        self.mount_b.run_shell(["ls", "subdir/childfile"])

    def test_unmount_for_evicted_client(self):
        """Test if client hangs on unmount after evicting the client."""
        mount_a_client_id = self.mount_a.get_global_id()
        self.fs.mds_asok(['session', 'evict', "%s" % mount_a_client_id])

        self.mount_a.umount_wait(require_clean=True, timeout=30)

    def test_mount_after_evicted_client(self):
        """Test if a new mount of same fs works after client eviction."""

        # trash this : we need it to use same remote as mount_a
        self.mount_b.umount_wait()

        cl = self.mount_a.__class__

        # create a new instance of mount_a's class with most of the
        # same settings, but mounted on mount_b's mountpoint.
        m = cl(ctx=self.mount_a.ctx,
               client_config=self.mount_a.client_config,
               test_dir=self.mount_a.test_dir,
               client_id=self.mount_a.client_id,
               client_remote=self.mount_a.client_remote,
               client_keyring_path=self.mount_a.client_keyring_path,
               cephfs_name=self.mount_a.cephfs_name,
               cephfs_mntpt= self.mount_a.cephfs_mntpt,
               hostfs_mntpt=self.mount_b.hostfs_mntpt,
               brxnet=self.mount_a.ceph_brx_net)

        # evict mount_a
        mount_a_client_id = self.mount_a.get_global_id()
        self.fs.mds_asok(['session', 'evict', "%s" % mount_a_client_id])

        m.mount_wait()
        m.create_files()
        m.check_files()
        m.umount_wait(require_clean=True)

    def test_stale_renew(self):
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Require FUSE client to handle signal STOP/CONT")

        session_timeout = self.fs.get_var("session_timeout")

        self.mount_a.run_shell(["mkdir", "testdir"])
        self.mount_a.run_shell(["touch", "testdir/file1"])
        # populate readdir cache
        self.mount_a.run_shell(["ls", "testdir"])
        self.mount_b.run_shell(["ls", "testdir"])

        # check if readdir cache is effective
        initial_readdirs = self.fs.mds_asok(['perf', 'dump', 'mds_server', 'req_readdir_latency'])
        self.mount_b.run_shell(["ls", "testdir"])
        current_readdirs = self.fs.mds_asok(['perf', 'dump', 'mds_server', 'req_readdir_latency'])
        self.assertEqual(current_readdirs, initial_readdirs);

        mount_b_gid = self.mount_b.get_global_id()
        # stop ceph-fuse process of mount_b
        self.mount_b.suspend_netns()

        self.assert_session_state(mount_b_gid, "open")
        time.sleep(session_timeout * 1.5)  # Long enough for MDS to consider session stale

        self.mount_a.run_shell(["touch", "testdir/file2"])
        self.assert_session_state(mount_b_gid, "stale")

        # resume ceph-fuse process of mount_b
        self.mount_b.resume_netns()
        # Is the new file visible from mount_b? (caps become invalid after session stale)
        self.mount_b.run_shell(["ls", "testdir/file2"])

    def test_abort_conn(self):
        """
        Check that abort_conn() skips closing mds sessions.
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Testing libcephfs function")

        self.fs.mds_asok(['config', 'set', 'mds_defer_session_stale', 'false'])
        session_timeout = self.fs.get_var("session_timeout")

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        gid_str = self.mount_a.run_python(dedent("""
            import cephfs as libcephfs
            cephfs = libcephfs.LibCephFS(conffile='')
            cephfs.mount()
            client_id = cephfs.get_instance_id()
            cephfs.abort_conn()
            print(client_id)
            """)
        )
        gid = int(gid_str);

        self.assert_session_state(gid, "open")
        time.sleep(session_timeout * 1.5)  # Long enough for MDS to consider session stale
        self.assert_session_state(gid, "stale")

    def test_dont_mark_unresponsive_client_stale(self):
        """
        Test that an unresponsive client holding caps is not marked stale or
        evicted unless another clients wants its caps.
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Require FUSE client to handle signal STOP/CONT")

        # XXX: To conduct this test we need at least two clients since a
        # single client is never evcited by MDS.
        SESSION_TIMEOUT = 30
        SESSION_AUTOCLOSE = 50
        time_at_beg = time.time()
        mount_a_gid = self.mount_a.get_global_id()
        _ = self.mount_a.client_pid
        self.fs.set_var('session_timeout', SESSION_TIMEOUT)
        self.fs.set_var('session_autoclose', SESSION_AUTOCLOSE)
        self.assert_session_count(2, self.fs.mds_asok(['session', 'ls']))

        # test that client holding cap not required by any other client is not
        # marked stale when it becomes unresponsive.
        self.mount_a.run_shell(['mkdir', 'dir'])
        self.mount_a.send_signal('sigstop')
        time.sleep(SESSION_TIMEOUT + 2)
        self.assert_session_state(mount_a_gid, "open")

        # test that other clients have to wait to get the caps from
        # unresponsive client until session_autoclose.
        self.mount_b.run_shell(['stat', 'dir'])
        self.assert_session_count(1, self.fs.mds_asok(['session', 'ls']))
        self.assertLess(time.time(), time_at_beg + SESSION_AUTOCLOSE)

        self.mount_a.send_signal('sigcont')

    def test_config_session_timeout(self):
        self.fs.mds_asok(['config', 'set', 'mds_defer_session_stale', 'false'])
        session_timeout = self.fs.get_var("session_timeout")
        mount_a_gid = self.mount_a.get_global_id()

        self.fs.mds_asok(['session', 'config', '%s' % mount_a_gid, 'timeout', '%s' % (session_timeout * 2)])

        self.mount_a.kill();

        self.assert_session_count(2)

        time.sleep(session_timeout * 1.5)
        self.assert_session_state(mount_a_gid, "open")

        time.sleep(session_timeout)
        self.assert_session_count(1)

        self.mount_a.kill_cleanup()

    def test_reconnect_after_blocklisted(self):
        """
        Test reconnect after blocklisted.
        - writing to a fd that was opened before blocklist should return -EBADF
        - reading/writing to a file with lost file locks should return -EIO
        - readonly fd should continue to work
        """

        self.mount_a.umount_wait()

        if isinstance(self.mount_a, FuseMount):
            self.mount_a.mount_wait(mntopts=['--client_reconnect_stale=1', '--fuse_disable_pagecache=1'])
        else:
            try:
                self.mount_a.mount_wait(mntopts=['recover_session=clean'])
            except CommandFailedError:
                self.mount_a.kill_cleanup()
                self.skipTest("Not implemented in current kernel")

        self.mount_a.wait_until_mounted()

        path = os.path.join(self.mount_a.mountpoint, 'testfile_reconnect_after_blocklisted')
        pyscript = dedent("""
            import os
            import sys
            import fcntl
            import errno
            import time

            fd1 = os.open("{path}.1", os.O_RDWR | os.O_CREAT, 0O666)
            fd2 = os.open("{path}.1", os.O_RDONLY)
            fd3 = os.open("{path}.2", os.O_RDWR | os.O_CREAT, 0O666)
            fd4 = os.open("{path}.2", os.O_RDONLY)

            os.write(fd1, b'content')
            os.read(fd2, 1);

            os.write(fd3, b'content')
            os.read(fd4, 1);
            fcntl.flock(fd4, fcntl.LOCK_SH | fcntl.LOCK_NB)

            print("blocklist")
            sys.stdout.flush()

            sys.stdin.readline()

            # wait for mds to close session
            time.sleep(10);

            # trigger 'open session' message. kclient relies on 'session reject' message
            # to detect if itself is blocklisted
            try:
                os.stat("{path}.1")
            except:
                pass

            # wait for auto reconnect
            time.sleep(10);

            try:
                os.write(fd1, b'content')
            except OSError as e:
                if e.errno != errno.EBADF:
                    raise
            else:
                raise RuntimeError("write() failed to raise error")

            os.read(fd2, 1);

            try:
                os.read(fd4, 1)
            except OSError as e:
                if e.errno != errno.EIO:
                    raise
            else:
                raise RuntimeError("read() failed to raise error")
            """).format(path=path)
        rproc = self.mount_a.client_remote.run(
                    args=['python3', '-c', pyscript],
                    wait=False, stdin=run.PIPE, stdout=run.PIPE)

        rproc.stdout.readline()

        mount_a_client_id = self.mount_a.get_global_id()
        self.fs.mds_asok(['session', 'evict', "%s" % mount_a_client_id])

        rproc.stdin.writelines(['done\n'])
        rproc.stdin.flush()

        rproc.wait()
        self.assertEqual(rproc.exitstatus, 0)


class TestClientOnLaggyOSD(CephFSTestCase):
    CLIENTS_REQUIRED = 2

    def make_osd_laggy(self, osd, sleep=120):
        self.mds_cluster.mon_manager.signal_osd(osd, signal.SIGSTOP)
        time.sleep(sleep)
        self.mds_cluster.mon_manager.signal_osd(osd, signal.SIGCONT)

    def clear_laggy_params(self, osd):
        default_laggy_weight = self.config_get('mon', 'mon_osd_laggy_weight')
        self.config_set('mon', 'mon_osd_laggy_weight', 1)
        self.mds_cluster.mon_manager.revive_osd(osd)
        self.config_set('mon', 'mon_osd_laggy_weight', default_laggy_weight)

    def get_a_random_osd(self):
        osds = self.mds_cluster.mon_manager.get_osd_status()
        return random.choice(osds['live'])

    def test_client_eviction_if_config_is_set(self):
        """
        If any client gets unresponsive/it's session get idle due to lagginess
        with any OSD and if config option defer_client_eviction_on_laggy_osds
        is set true(default true) then make sure clients are not evicted until
        OSD(s) return to normal.
        """

        self.fs.mds_asok(['config', 'set', 'mds_defer_session_stale', 'false'])
        self.config_set('mds', 'defer_client_eviction_on_laggy_osds', 'true')
        self.assertEqual(self.config_get(
            'mds', 'defer_client_eviction_on_laggy_osds'), 'true')

        # make an OSD laggy
        osd = self.get_a_random_osd()
        self.make_osd_laggy(osd)

        try:
            mount_a_gid = self.mount_a.get_global_id()

            self.mount_a.kill()

            # client session should be open, it gets stale
            # only after session_timeout time.
            self.assert_session_state(mount_a_gid, "open")

            # makes session stale
            time.sleep(self.fs.get_var("session_timeout") * 1.5)
            self.assert_session_state(mount_a_gid, "stale")

            # it takes time to have laggy clients entries in cluster log,
            # wait for 6 minutes to see if it is visible, finally restart
            # the client
            with contextutil.safe_while(sleep=5, tries=6) as proceed:
                while proceed():
                    try:
                        with self.assert_cluster_log("1 client(s) laggy due to"
                                                     " laggy OSDs",
                                                     timeout=55):
                            # make sure clients weren't evicted
                            self.assert_session_count(2)
                            break
                    except (AssertionError, CommandFailedError) as e:
                        log.debug(f'{e}, retrying')

            # clear lagginess, expect to get the warning cleared and make sure
            # client gets evicted
            self.clear_laggy_params(osd)
            self.wait_for_health_clear(60)
            self.assert_session_count(1)
        finally:
            self.mount_a.kill_cleanup()
            self.mount_a.mount_wait()
            self.mount_a.create_destroy()

    def test_client_eviction_if_config_is_unset(self):
        """
        If an OSD is laggy but config option defer_client_eviction_on_laggy_osds
        is unset then an unresponsive client does get evicted.
        """

        self.fs.mds_asok(['config', 'set', 'mds_defer_session_stale', 'false'])
        self.config_set('mds', 'defer_client_eviction_on_laggy_osds', 'false')
        self.assertEqual(self.config_get(
            'mds', 'defer_client_eviction_on_laggy_osds'), 'false')

        # make an OSD laggy
        osd = self.get_a_random_osd()
        self.make_osd_laggy(osd)

        try:
            session_timeout = self.fs.get_var("session_timeout")
            mount_a_gid = self.mount_a.get_global_id()

            self.fs.mds_asok(['session', 'config', '%s' % mount_a_gid, 'timeout', '%s' % (session_timeout * 2)])

            self.mount_a.kill()

            self.assert_session_count(2)

            time.sleep(session_timeout * 1.5)
            self.assert_session_state(mount_a_gid, "open")

            time.sleep(session_timeout)
            self.assert_session_count(1)

            # make sure warning wasn't seen in cluster log
            with self.assert_cluster_log("laggy due to laggy OSDs",
                                         timeout=120, present=False):
                pass
        finally:
            self.mount_a.kill_cleanup()
            self.mount_a.mount_wait()
            self.mount_a.create_destroy()
            self.clear_laggy_params(osd)
