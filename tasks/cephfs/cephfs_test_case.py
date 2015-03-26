import logging
import unittest
from unittest import case
import time
import os
import re
from StringIO import StringIO

from tasks.cephfs.fuse_mount import FuseMount
from teuthology.orchestra import run
from teuthology.orchestra.run import CommandFailedError


log = logging.getLogger(__name__)


class CephFSTestCase(unittest.TestCase):
    """
    Test case for Ceph FS, requires caller to populate Filesystem and Mounts,
    into the fs, mount_a, mount_b class attributes (setting mount_b is optional)

    Handles resetting the cluster under test between tests.
    """
    # Environment references
    mounts = None
    fs = None
    ctx = None

    # FIXME weird explicit naming
    mount_a = None
    mount_b = None

    # Declarative test requirements: subclasses should override these to indicate
    # their special needs.  If not met, tests will be skipped.
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1
    REQUIRE_KCLIENT_REMOTE = False
    REQUIRE_ONE_CLIENT_REMOTE = False

    LOAD_SETTINGS = []

    def setUp(self):
        if len(self.fs.mds_ids) < self.MDSS_REQUIRED:
            raise case.SkipTest("Only have {0} MDSs, require {1}".format(
                len(self.fs.mds_ids), self.MDSS_REQUIRED
            ))

        if len(self.mounts) < self.CLIENTS_REQUIRED:
            raise case.SkipTest("Only have {0} clients, require {1}".format(
                len(self.mounts), self.CLIENTS_REQUIRED
            ))

        if self.REQUIRE_KCLIENT_REMOTE:
            if not isinstance(self.mounts[0], FuseMount) or not isinstance(self.mounts[1], FuseMount):
                # kclient kill() power cycles nodes, so requires clients to each be on
                # their own node
                if self.mounts[0].client_remote.hostname == self.mounts[1].client_remote.hostname:
                    raise case.SkipTest("kclient clients must be on separate nodes")

        if self.REQUIRE_ONE_CLIENT_REMOTE:
            if self.mounts[0].client_remote.hostname in self.fs.get_mds_hostnames():
                raise case.SkipTest("Require first client to be on separate server from MDSs")

        # Unmount all surplus clients
        for i in range(self.CLIENTS_REQUIRED, len(self.mounts)):
            mount = self.mounts[i]
            log.info("Unmounting unneeded client {0}".format(mount.client_id))
            mount.umount_wait()

        # Create friendly mount_a, mount_b attrs
        for i in range(0, self.CLIENTS_REQUIRED):
            setattr(self, "mount_{0}".format(chr(ord('a') + i)), self.mounts[i])

        self.fs.clear_firewall()

        # Unmount in order to start each test on a fresh mount, such
        # that test_barrier can have a firm expectation of what OSD
        # epoch the clients start with.
        if self.mount_a.is_mounted():
            self.mount_a.umount_wait()

        if self.mount_b:
            if self.mount_b.is_mounted():
                self.mount_b.umount_wait()

        # To avoid any issues with e.g. unlink bugs, we destroy and recreate
        # the filesystem rather than just doing a rm -rf of files
        self.fs.mds_stop()
        self.fs.mds_fail()
        self.fs.delete()
        self.fs.create()

        # In case the previous filesystem had filled up the RADOS cluster, wait for that
        # flag to pass.
        osd_mon_report_interval_max = int(self.fs.get_config("osd_mon_report_interval_max", service_type='osd'))
        self.wait_until_true(lambda: not self.fs.is_full(),
                             timeout=osd_mon_report_interval_max * 5)

        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        if not self.mount_a.is_mounted():
            self.mount_a.mount()
            self.mount_a.wait_until_mounted()

        if self.mount_b:
            if not self.mount_b.is_mounted():
                self.mount_b.mount()
                self.mount_b.wait_until_mounted()

        # Load an config settings of interest
        for setting in self.LOAD_SETTINGS:
            setattr(self, setting, int(self.fs.mds_asok(
                ['config', 'get', setting], self.fs.mds_ids[0]
            )[setting]))

        self.configs_set = set()

    def tearDown(self):
        self.fs.clear_firewall()
        self.mount_a.teardown()
        if self.mount_b:
            self.mount_b.teardown()

        for subsys, key in self.configs_set:
            self.fs.clear_ceph_conf(subsys, key)

    def set_conf(self, subsys, key, value):
        self.configs_set.add((subsys, key))
        self.fs.set_ceph_conf(subsys, key, value)

    def assert_session_count(self, expected, ls_data=None, mds_id=None):
        if ls_data is None:
            ls_data = self.fs.mds_asok(['session', 'ls'], mds_id=mds_id)

        self.assertEqual(expected, len(ls_data), "Expected {0} sessions, found {1}".format(
            expected, len(ls_data)
        ))

    def assert_session_state(self, client_id,  expected_state):
        self.assertEqual(
            self._session_by_id(
                self.fs.mds_asok(['session', 'ls'])).get(client_id, {'state': None})['state'],
            expected_state)

    def get_session_data(self, client_id):
        return self._session_by_id(client_id)

    def _session_list(self):
        ls_data = self.fs.mds_asok(['session', 'ls'])
        ls_data = [s for s in ls_data if s['state'] not in ['stale', 'closed']]
        return ls_data

    def get_session(self, client_id, session_ls=None):
        if session_ls is None:
            session_ls = self.fs.mds_asok(['session', 'ls'])

        return self._session_by_id(session_ls)[client_id]

    def _session_by_id(self, session_ls):
        return dict([(s['id'], s) for s in session_ls])

    def wait_until_equal(self, get_fn, expect_val, timeout, reject_fn=None):
        period = 5
        elapsed = 0
        while True:
            val = get_fn()
            if val == expect_val:
                return
            elif reject_fn and reject_fn(val):
                raise RuntimeError("wait_until_equal: forbidden value {0} seen".format(val))
            else:
                if elapsed >= timeout:
                    raise RuntimeError("Timed out after {0} seconds waiting for {1} (currently {2})".format(
                        elapsed, expect_val, val
                    ))
                else:
                    log.debug("wait_until_equal: {0} != {1}, waiting...".format(val, expect_val))
                time.sleep(period)
                elapsed += period

        log.debug("wait_until_equal: success")

    def wait_until_true(self, condition, timeout):
        period = 5
        elapsed = 0
        while True:
            if condition():
                return
            else:
                if elapsed >= timeout:
                    raise RuntimeError("Timed out after {0} seconds".format(elapsed))
                else:
                    log.debug("wait_until_true: waiting...")
                time.sleep(period)
                elapsed += period

        log.debug("wait_until_true: success")

    def assert_mds_crash(self, daemon_id):
        """
        Assert that the a particular MDS daemon crashes (block until
        it does)
        """
        try:
            self.fs.mds_daemons[daemon_id].proc.wait()
        except CommandFailedError as e:
            log.info("MDS '{0}' crashed with status {1} as expected".format(daemon_id, e.exitstatus))
            self.fs.mds_daemons[daemon_id].proc = None

            # Go remove the coredump from the crash, otherwise teuthology.internal.coredump will
            # catch it later and treat it as a failure.
            p = self.fs.mds_daemons[daemon_id].remote.run(args=[
                "sudo", "sysctl", "-n", "kernel.core_pattern"], stdout=StringIO())
            core_pattern = p.stdout.getvalue().strip()
            if os.path.dirname(core_pattern):  # Non-default core_pattern with a directory in it
                # We have seen a core_pattern that looks like it's from teuthology's coredump
                # task, so proceed to clear out the core file
                log.info("Clearing core from pattern: {0}".format(core_pattern))

                # Determine the PID of the crashed MDS by inspecting the MDSMap, it had
                # to talk to the mons to get assigned a rank to reach the point of crashing
                addr = self.fs.mon_manager.get_mds_status(daemon_id)['addr']
                pid_str = addr.split("/")[1]
                log.info("Determined crasher PID was {0}".format(pid_str))

                # Substitute PID into core_pattern to get a glob
                core_glob = core_pattern.replace("%p", pid_str)
                core_glob = re.sub("%[a-z]", "*", core_glob)  # Match all for all other % tokens

                # Verify that we see the expected single coredump matching the expected pattern
                ls_proc = self.fs.mds_daemons[daemon_id].remote.run(args=[
                    "sudo", "ls", run.Raw(core_glob)
                ], stdout=StringIO())
                cores = [f for f in ls_proc.stdout.getvalue().strip().split("\n") if f]
                log.info("Enumerated cores: {0}".format(cores))
                self.assertEqual(len(cores), 1)

                log.info("Found core file {0}, deleting it".format(cores[0]))

                self.fs.mds_daemons[daemon_id].remote.run(args=[
                    "sudo", "rm", "-f", cores[0]
                ])
            else:
                log.info("No core_pattern directory set, nothing to clear (internal.coredump not enabled?)")

        else:
            raise AssertionError("MDS daemon '{0}' did not crash as expected".format(daemon_id))
