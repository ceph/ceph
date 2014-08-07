
"""
Teuthology task for exercising CephFS client recovery
"""

import contextlib
import logging
import time
import unittest

from teuthology import misc
from teuthology.orchestra.run import CommandFailedError
from teuthology.task import interactive
from cephfs.filesystem import Filesystem
from tasks.ceph_fuse import get_client_configs, FuseMount


log = logging.getLogger(__name__)


# Arbitrary timeouts for operations involving restarting
# an MDS or waiting for it to come up
MDS_RESTART_GRACE = 60


class TestClientRecovery(unittest.TestCase):
    # Environment references
    fs = None
    mount_a = None
    mount_b = None
    mds_session_timeout = None
    mds_reconnect_timeout = None

    def setUp(self):
        self.fs.mds_restart()
        self.mount_a.mount()
        self.mount_b.mount()
        self.mount_a.wait_until_mounted()
        self.mount_a.wait_until_mounted()

    def tearDown(self):
        self.mount_a.teardown()
        self.mount_b.teardown()
        # mount_a.umount()
        # mount_b.umount()
        # run.wait([mount_a.fuse_daemon, mount_b.fuse_daemon], timeout=600)
        # mount_a.cleanup()
        # mount_b.cleanup()

    def test_basic(self):
        # Check that two clients come up healthy and see each others' files
        # =====================================================
        self.mount_a.create_files()
        self.mount_a.check_files()
        self.mount_a.umount_wait()

        self.mount_b.check_files()

        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

        # Check that the admin socket interface is correctly reporting
        # two sessions
        # =====================================================
        ls_data = self._session_list()
        self.assert_session_count(2, ls_data)

        self.assertSetEqual(
            set([l['id'] for l in ls_data]),
            {self.mount_a.get_client_id(), self.mount_b.get_client_id()}
        )

    def test_restart(self):
        # Check that after an MDS restart both clients reconnect and continue
        # to handle I/O
        # =====================================================
        self.fs.mds_stop()
        self.fs.mds_fail()
        self.fs.mds_restart()
        self.fs.wait_for_state('up:active', timeout=MDS_RESTART_GRACE)

        self.mount_a.create_destroy()
        self.mount_b.create_destroy()

    def assert_session_count(self, expected, ls_data=None):
        if ls_data is None:
            ls_data = self.fs.mds_asok(['session', 'ls'])

        self.assertEqual(expected, len(ls_data), "Expected {0} sessions, found {1}".format(
            expected, len(ls_data)
        ))

    def _session_list(self):
        ls_data = self.fs.mds_asok(['session', 'ls'])
        ls_data = [s for s in ls_data if s['state'] not in ['stale', 'closed']]
        return ls_data

    def _session_by_id(self, session_ls):
        return dict([(s['id'], s) for s in session_ls])

    def test_reconnect_timeout(self):
        # Reconnect timeout
        # =================
        # Check that if I stop an MDS and a client goes away, the MDS waits
        # for the reconnect period
        self.fs.mds_stop()
        self.fs.mds_fail()

        mount_a_client_id = self.mount_a.get_client_id()
        self.mount_a.umount_wait(force=True)

        self.fs.mds_restart()

        self.fs.wait_for_state('up:reconnect', reject='up:active', timeout=MDS_RESTART_GRACE)

        ls_data = self._session_list()
        self.assert_session_count(2, ls_data)

        # The session for the dead client should have the 'reconnect' flag set
        self.assertTrue(self._session_by_id(ls_data)[mount_a_client_id]['reconnecting'])

        # Wait for the reconnect state to clear, this should take the
        # reconnect timeout period.
        in_reconnect_for = self.fs.wait_for_state('up:active', timeout=self.mds_reconnect_timeout * 2)
        # Check that the period we waited to enter active is within a factor
        # of two of the reconnect timeout.
        self.assertGreater(in_reconnect_for, self.mds_reconnect_timeout / 2,
                           "Should have been in reconnect phase for {0} but only took {1}".format(
                               self.mds_reconnect_timeout, in_reconnect_for
                           ))

        self.assert_session_count(1)

        # Check that the client that timed out during reconnect can
        # mount again and do I/O
        self.mount_a.mount()
        self.mount_a.wait_until_mounted()
        self.mount_a.create_destroy()

        self.assert_session_count(2)

    def test_reconnect_eviction(self):
        # Eviction during reconnect
        # =========================
        self.fs.mds_stop()
        self.fs.mds_fail()

        mount_a_client_id = self.mount_a.get_client_id()
        self.mount_a.umount_wait(force=True)

        self.fs.mds_restart()

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

        # Bring the client back
        self.mount_a.mount()
        self.mount_a.create_destroy()

    def test_stale_caps(self):
        # Capability release from stale session
        # =====================================
        cap_holder = self.mount_a.open_background()
        self.mount_a.kill()

        # Now, after mds_session_timeout seconds, the waiter should
        # complete their operation when the MDS marks the holder's
        # session stale.
        cap_waiter = self.mount_b.write_background()
        a = time.time()
        cap_waiter.wait()
        b = time.time()
        cap_waited = b - a
        log.info("cap_waiter waited {0}s".format(cap_waited))
        self.assertTrue(self.mds_session_timeout / 2.0 <= cap_waited <= self.mds_session_timeout * 2.0,
                        "Capability handover took {0}, expected approx {1}".format(
                            cap_waited, self.mds_session_timeout
                        ))

        cap_holder.stdin.close()
        try:
            cap_holder.wait()
        except CommandFailedError:
            # We killed it, so it raises an error
            pass

        self.mount_a.kill_cleanup()

        self.mount_a.mount()
        self.mount_a.wait_until_mounted()

    def test_evicted_caps(self):
        # Eviction while holding a capability
        # ===================================

        # Take out a write capability on a file on client A,
        # and then immediately kill it.
        cap_holder = self.mount_a.open_background()
        mount_a_client_id = self.mount_a.get_client_id()
        self.mount_a.kill()

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
        self.assertLess(cap_waited, self.mds_session_timeout / 2.0,
                        "Capability handover took {0}, expected less than {1}".format(
                            cap_waited, self.mds_session_timeout / 2.0
                        ))

        cap_holder.stdin.close()
        try:
            cap_holder.wait()
        except CommandFailedError:
            # We killed it, so it raises an error
            pass

        self.mount_a.kill_cleanup()

        self.mount_a.mount()
        self.mount_a.wait_until_mounted()


class LogStream(object):
    def __init__(self):
        self.buffer = ""

    def write(self, data):
        self.buffer += data
        if "\n" in self.buffer:
            lines = self.buffer.split("\n")
            for line in lines[:-1]:
                log.info(line)
            self.buffer = lines[-1]

    def flush(self):
        pass


class InteractiveFailureResult(unittest.TextTestResult):
    """
    Specialization that implements interactive-on-error style
    behavior.
    """
    ctx = None

    def addFailure(self, test, err):
        log.error(self._exc_info_to_string(err, test))
        log.error("Failure in test '{0}', going interactive".format(
            self.getDescription(test)
        ))
        interactive.task(ctx=self.ctx, config=None)

    def addError(self, test, err):
        log.error(self._exc_info_to_string(err, test))
        log.error("Error in test '{0}', going interactive".format(
            self.getDescription(test)
        ))
        interactive.task(ctx=self.ctx, config=None)


@contextlib.contextmanager
def task(ctx, config):
    fs = Filesystem(ctx, config)

    # Pick out the clients we will use from the configuration
    # =======================================================
    client_list = list(misc.all_roles_of_type(ctx.cluster, 'client'))
    if len(client_list) < 2:
        raise RuntimeError("Need at least two clients")

    client_a_id = client_list[0]
    client_a_role = "client.{0}".format(client_a_id)
    client_a_remote = list(misc.get_clients(ctx=ctx, roles=["client.{0}".format(client_a_id)]))[0][1]

    client_b_id = client_list[1]
    client_b_role = "client.{0}".format(client_b_id)
    client_b_remote = list(misc.get_clients(ctx=ctx, roles=["client.{0}".format(client_a_id)]))[0][1]

    test_dir = misc.get_testdir(ctx)

    # TODO: enable switching FUSE to kclient here
    # or perhaps just use external client tasks and consume ctx.mounts here?
    client_configs = get_client_configs(ctx, config)
    mount_a = FuseMount(client_configs.get(client_a_role, {}), test_dir, client_a_id, client_a_remote)
    mount_b = FuseMount(client_configs.get(client_b_role, {}), test_dir, client_b_id, client_b_remote)

    # Attach environment references to test case
    # ==========================================
    TestClientRecovery.mds_reconnect_timeout = int(fs.mds_asok(
        ['config', 'get', 'mds_reconnect_timeout']
    )['mds_reconnect_timeout'])
    TestClientRecovery.mds_session_timeout = int(fs.mds_asok(
        ['config', 'get', 'mds_session_timeout']
    )['mds_session_timeout'])
    TestClientRecovery.fs = fs
    TestClientRecovery.mount_a = mount_a
    TestClientRecovery.mount_b = mount_b

    # Stash references on ctx so that we can easily debug in interactive mode
    # =======================================================================
    ctx.filesystem = fs
    ctx.mount_a = mount_a
    ctx.mount_b = mount_b

    # Execute test suite
    # ==================
    suite = unittest.TestLoader().loadTestsFromTestCase(TestClientRecovery)
    if ctx.config.get("interactive-on-error", False):
        InteractiveFailureResult.ctx = ctx
        result_class = InteractiveFailureResult
    else:
        result_class = unittest.TextTestResult
    result = unittest.TextTestRunner(
        stream=LogStream(),
        resultclass=result_class,
        verbosity=2,
        failfast=True).run(suite)

    if not result.wasSuccessful():
        result.printErrors()  # duplicate output at end for convenience
        raise RuntimeError("Test failure.")

    # Continue to any downstream tasks
    # ================================
    yield
