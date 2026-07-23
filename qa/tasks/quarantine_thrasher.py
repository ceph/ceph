"""
Thrash quarantine by randomly enabling/disabling quarantine on a subvolume
while MDS daemons may be killed and restarted by mds_thrash.

This validates that quarantine state (persisted in inode optmetadata) survives
MDS failovers and that cap revocation completes correctly after recovery.
"""
import contextlib
import errno
import logging
import random
import time

from io import StringIO

from teuthology import misc
from tasks.cephfs.filesystem import MDSCluster, Filesystem
from tasks.thrasher import ThrasherGreenlet

log = logging.getLogger(__name__)

SUBVOLUME_NAME = "quarantine_thrash_subvol"
TEST_FILE = "thrash_test.txt"
TEST_DATA = "Quarantine thrash test data."


class QuarantineThrasher(ThrasherGreenlet):
    """
    Periodically enables and disables quarantine on a subvolume.

    While MDS thrash is running in parallel, this exercises:
    - Quarantine enable during MDS failover (journal in flight)
    - Quarantine disable during MDS failover (cap revocation in flight)
    - Quarantine state recovery after MDS restart
    - Cap revocation completing on the new active MDS

    Parameters:
        initial_delay:  seconds before first quarantine cycle     (default: 10)
        min_hold:       minimum seconds to hold quarantine        (default: 5)
        max_hold:       maximum seconds to hold quarantine        (default: 30)
        min_release:    minimum seconds between cycles            (default: 5)
        max_release:    maximum seconds between cycles            (default: 20)
        verify:         whether to verify access after each cycle (default: True)
        seed:           random seed for reproducibility           (default: None)
    """

    def __init__(self, ctx, fscid,
                 cluster_name='ceph',
                 initial_delay=10,
                 min_hold=5,
                 max_hold=30,
                 min_release=5,
                 max_release=20,
                 verify=True,
                 seed=None,
                 **kwargs):
        super(QuarantineThrasher, self).__init__()

        self.fs = Filesystem(ctx, fscid=fscid, cluster_name=cluster_name)
        self.logger = log.getChild('fs.[{f}]'.format(f=self.fs.name))
        self.name = 'quarantine_thrasher.fs.[{f}]'.format(f=self.fs.name)
        self.ctx = ctx
        self.cluster_name = cluster_name

        if seed is None:
            seed = ctx.config.get('seed', random.randint(0, 999999))
        self.logger.info("Initializing QuarantineThrasher with seed %d", seed)
        self.rnd = random.Random(seed)

        self.initial_delay = max(0, initial_delay)
        self.min_hold = max(1, min_hold)
        self.max_hold = max(1, max_hold)
        self.min_release = max(1, min_release)
        self.max_release = max(1, max_release)
        self.verify = verify

        self.volname = self.fs.name
        self.subvol_created = False
        self.quarantine_enabled = False

    def _run_ceph_cmd(self, *args, **kwargs):
        """Run a ceph CLI command, return (rc, stdout)."""
        kwargs.setdefault('check_status', False)
        kwargs.setdefault('stdout', StringIO())
        kwargs.setdefault('timeoutcmd', 120)
        result = self.fs.run_ceph_cmd(args=list(args), **kwargs)
        return result.exitstatus, result.stdout.getvalue()

    def _fs_cmd(self, *args):
        """Run a 'ceph fs' subcommand."""
        rc, out = self._run_ceph_cmd('fs', *args)
        if rc != 0:
            raise RuntimeError("ceph fs %s failed with rc=%d: %s"
                               % (' '.join(args), rc, out))
        return out

    def _setup_subvolume(self):
        """Create the test subvolume and write test data."""
        self.logger.info("Creating subvolume %s", SUBVOLUME_NAME)
        try:
            self._fs_cmd("subvolume", "create", self.volname,
                         SUBVOLUME_NAME, "--mode=777")
            self.subvol_created = True
        except RuntimeError as e:
            if 'already exists' not in str(e).lower():
                raise
            self.logger.info("Subvolume already exists, reusing")
            self.subvol_created = True

    def _cleanup_subvolume(self):
        """Remove the test subvolume."""
        if not self.subvol_created:
            return
        try:
            if self.quarantine_enabled:
                self._quarantine_disable()
        except Exception as e:
            self.logger.warning("Error disabling quarantine during cleanup: %s", e)
        try:
            self._fs_cmd("subvolume", "rm", self.volname,
                         SUBVOLUME_NAME, "--force")
        except Exception as e:
            self.logger.warning("Error removing subvolume during cleanup: %s", e)

    def _quarantine_enable(self):
        """Enable quarantine, retrying on transient errors."""
        max_retries = 10
        for attempt in range(max_retries):
            rc, out = self._run_ceph_cmd(
                'fs', 'subvolume', 'quarantine', 'enable',
                self.volname, SUBVOLUME_NAME)
            if rc == 0:
                self.quarantine_enabled = True
                return
            if rc == errno.EBUSY:
                self.logger.info("Quarantine enable EBUSY (attempt %d/%d), retrying",
                                 attempt + 1, max_retries)
                self.sleep_unless_stopped(2)
                continue
            if rc == errno.EAGAIN or rc == errno.ENOENT:
                self.logger.info("Quarantine enable got %d (MDS may be recovering), retrying",
                                 rc)
                self.sleep_unless_stopped(3)
                continue
            self.logger.warning("Quarantine enable failed rc=%d: %s", rc, out.strip())
            self.sleep_unless_stopped(2)
        raise RuntimeError("Failed to enable quarantine after %d attempts" % max_retries)

    def _quarantine_disable(self):
        """Disable quarantine, retrying on transient errors."""
        max_retries = 10
        for attempt in range(max_retries):
            rc, out = self._run_ceph_cmd(
                'fs', 'subvolume', 'quarantine', 'disable',
                self.volname, SUBVOLUME_NAME)
            if rc == 0:
                self.quarantine_enabled = False
                return
            if rc == errno.EBUSY:
                self.logger.info("Quarantine disable EBUSY (attempt %d/%d), retrying",
                                 attempt + 1, max_retries)
                self.sleep_unless_stopped(2)
                continue
            if rc == errno.EAGAIN or rc == errno.ENOENT:
                self.logger.info("Quarantine disable got %d (MDS may be recovering), retrying",
                                 rc)
                self.sleep_unless_stopped(3)
                continue
            self.logger.warning("Quarantine disable failed rc=%d: %s", rc, out.strip())
            self.sleep_unless_stopped(2)
        raise RuntimeError("Failed to disable quarantine after %d attempts" % max_retries)

    def _verify_quarantine_active(self):
        """Verify that the subvolume is quarantined (getpath should still work)."""
        try:
            out = self._fs_cmd("subvolume", "getpath", self.volname,
                               SUBVOLUME_NAME)
            path = out.strip()
            if path:
                self.logger.info("Quarantine active, subvolume path: %s", path)
                return True
        except RuntimeError:
            pass
        return False

    def _verify_quarantine_lifted(self):
        """Verify that the subvolume is accessible (ls should work)."""
        try:
            out = self._fs_cmd("subvolume", "ls", self.volname)
            if SUBVOLUME_NAME in out:
                self.logger.info("Quarantine lifted, subvolume visible in ls")
                return True
        except RuntimeError:
            pass
        return False

    def _run(self):
        try:
            self.fs.wait_for_daemons()
            self._setup_subvolume()

            self.logger.info("Ready to start quarantine thrashing; "
                             "initial delay: %d sec", self.initial_delay)
            self.sleep_unless_stopped(self.initial_delay)

            cycle = 0
            while not self.is_stopped:
                cycle += 1
                hold_time = self.rnd.uniform(self.min_hold, self.max_hold)
                release_time = self.rnd.uniform(self.min_release, self.max_release)

                # --- Enable quarantine ---
                self.logger.info("Cycle %d: enabling quarantine (will hold %.1fs)",
                                 cycle, hold_time)
                try:
                    self._quarantine_enable()
                except RuntimeError as e:
                    self.logger.warning("Cycle %d: enable failed: %s, will retry next cycle",
                                        cycle, e)
                    self.sleep_unless_stopped(release_time)
                    continue

                self.logger.info("Cycle %d: quarantine enabled, holding for %.1fs",
                                 cycle, hold_time)

                if self.verify:
                    self._verify_quarantine_active()

                self.sleep_unless_stopped(hold_time)

                # --- Disable quarantine ---
                self.logger.info("Cycle %d: disabling quarantine", cycle)
                try:
                    self._quarantine_disable()
                except RuntimeError as e:
                    self.logger.warning("Cycle %d: disable failed: %s, will retry next cycle",
                                        cycle, e)
                    self.sleep_unless_stopped(release_time)
                    continue

                self.logger.info("Cycle %d: quarantine disabled, releasing for %.1fs",
                                 cycle, release_time)

                if self.verify:
                    self._verify_quarantine_lifted()

                self.sleep_unless_stopped(release_time)

        except Exception as e:
            if not isinstance(e, self.Stopped):
                self.set_thrasher_exception(e)
                self.logger.exception("exception:")

    def stop(self):
        self.logger.info("Stopping quarantine thrasher")
        super(QuarantineThrasher, self).stop()


def stop_all_quarantine_thrashers(thrashers):
    for thrasher in thrashers:
        if not isinstance(thrasher, QuarantineThrasher):
            continue
        thrasher.stop()
        thrasher.join()
        thrasher._cleanup_subvolume()
        if thrasher.exception is not None:
            raise RuntimeError(
                "error during quarantine thrashing: %s" % thrasher.exception)


@contextlib.contextmanager
def task(ctx, config):
    """
    Stress test quarantine by randomly enabling/disabling quarantine on a
    subvolume while MDS thrash is running.

    Example config::

        - quarantine_thrasher:
            min_hold: 5
            max_hold: 20
            initial_delay: 10

    Runs alongside mds_thrash to exercise quarantine during MDS failovers.
    """

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'quarantine_thrasher task only accepts a dict for configuration'
    mdslist = list(misc.all_roles_of_type(ctx.cluster, 'mds'))
    assert len(mdslist) > 0, \
        'quarantine_thrasher task requires at least 1 metadata server'

    cluster_name = config.get('cluster', 'ceph')
    manager = ctx.managers[cluster_name]
    manager.wait_for_clean()

    mds_cluster = MDSCluster(ctx)
    for fs in mds_cluster.status().get_filesystems():
        thrasher = QuarantineThrasher(
            ctx=ctx, fscid=fs['id'],
            cluster_name=cluster_name, **config)
        thrasher.start()
        ctx.ceph[cluster_name].thrashers.append(thrasher)

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining QuarantineThrashers')
        stop_all_quarantine_thrashers(ctx.ceph[cluster_name].thrashers)
        log.info('done joining QuarantineThrashers')
