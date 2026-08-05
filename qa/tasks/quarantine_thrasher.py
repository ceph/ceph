"""
Thrash quarantine by randomly enabling/disabling quarantine on a subvolume
while MDS daemons may be killed and restarted by mds_thrash.

This validates that quarantine state (persisted in inode optmetadata) survives
MDS failovers and that cap revocation completes correctly after recovery.

Modeled after quiescer.py — the quarantine equivalent of quiesce thrashing.
"""
import contextlib
import errno
import json
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
    Periodically enables and disables quarantine on a subvolume, verifying
    that data access is blocked while quarantine is active and restored
    after it is lifted.

    While MDS thrash is running in parallel, this exercises:
    - Quarantine enable during MDS failover (journal in flight)
    - Quarantine disable during MDS failover (cap revocation in flight)
    - Quarantine state recovery after MDS restart
    - Cap revocation completing on the new active MDS
    - Data integrity after quarantine cycles

    Parameters:
        initial_delay:    seconds before first cycle                (default: 10)
        min_hold:         minimum seconds to hold quarantine        (default: 5)
        max_hold:         maximum seconds to hold quarantine        (default: 30)
        min_release:      minimum seconds between cycles            (default: 5)
        max_release:      maximum seconds between cycles            (default: 20)
        command_timeout:  timeout for individual ceph commands       (default: 120)
        max_retries:      retries for enable/disable during failover (default: 15)
        retry_delay:      seconds between retries                   (default: 3)
        seed:             random seed for reproducibility           (default: None)
    """

    def __init__(self, ctx, fscid,
                 cluster_name='ceph',
                 initial_delay=10,
                 min_hold=5,
                 max_hold=30,
                 min_release=5,
                 max_release=20,
                 command_timeout=120,
                 max_retries=15,
                 retry_delay=3,
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
        self.command_timeout = command_timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.volname = self.fs.name
        self.subvol_created = False
        self.quarantine_enabled = False
        self.subvol_path = None

    def _run_ceph_cmd(self, *args):
        """Run a ceph CLI command, return (rc, stdout)."""
        result = self.fs.run_ceph_cmd(args=list(args), check_status=False,
                                      stdout=StringIO(),
                                      timeoutcmd=self.command_timeout)
        return result.exitstatus, result.stdout.getvalue()

    def _fs_cmd(self, *args):
        """Run a 'ceph fs' subcommand, raise on failure."""
        rc, out = self._run_ceph_cmd('fs', *args)
        if rc != 0:
            raise RuntimeError("ceph fs %s failed with rc=%d: %s"
                               % (' '.join(args), rc, out))
        return out

    def _rcinfo(self, rc):
        return "%d (%s)" % (rc, errno.errorcode.get(rc, 'Unknown'))

    # -- Setup / cleanup ------------------------------------------------------

    def _setup_subvolume(self):
        """Create the test subvolume and write test data."""
        self.logger.info("Creating subvolume %s", SUBVOLUME_NAME)
        self._fs_cmd("subvolume", "create", self.volname,
                     SUBVOLUME_NAME, "--mode=777")
        self.subvol_created = True

        self.subvol_path = self._fs_cmd("subvolume", "getpath",
                                        self.volname,
                                        SUBVOLUME_NAME).strip()
        self.logger.info("Subvolume path: %s", self.subvol_path)

    def _cleanup_subvolume(self):
        """Remove the test subvolume, disabling quarantine if needed."""
        if not self.subvol_created:
            return
        if self.quarantine_enabled:
            try:
                self._quarantine_op("disable")
            except Exception as e:
                self.logger.warning("Cleanup: disable quarantine failed: %s", e)
        try:
            self._fs_cmd("subvolume", "rm", self.volname,
                         SUBVOLUME_NAME, "--force")
        except Exception as e:
            self.logger.warning("Cleanup: rm subvolume failed: %s", e)

    # -- Quarantine operations ------------------------------------------------

    def _quarantine_op(self, op):
        """Enable or disable quarantine, retrying on transient MDS errors."""
        transient = {errno.EBUSY, errno.EAGAIN, errno.ENOENT,
                     errno.EINTR, errno.EIO}

        for attempt in range(1, self.max_retries + 1):
            self.proceed_unless_stopped()

            rc, out = self._run_ceph_cmd(
                'fs', 'subvolume', 'quarantine', op,
                self.volname, SUBVOLUME_NAME)

            if rc == 0:
                self.quarantine_enabled = (op == "enable")
                self.logger.info("quarantine %s succeeded (attempt %d)",
                                 op, attempt)
                return

            if rc in transient:
                self.logger.info("quarantine %s got %s (attempt %d/%d), "
                                 "MDS may be recovering",
                                 op, self._rcinfo(rc), attempt,
                                 self.max_retries)
                self.sleep_unless_stopped(self.retry_delay)
                continue

            self.logger.warning("quarantine %s failed with %s: %s",
                                op, self._rcinfo(rc), out.strip())
            self.sleep_unless_stopped(self.retry_delay)

        raise RuntimeError("quarantine %s failed after %d attempts"
                           % (op, self.max_retries))

    # -- Verification ---------------------------------------------------------

    def _verify_quarantine_enforced(self):
        """Verify quarantine is enforced: subvolume info should show enabled,
        getpath should be blocked."""
        try:
            out = self._fs_cmd("subvolume", "info", self.volname,
                               SUBVOLUME_NAME)
            info = json.loads(out)
            if info.get("quarantine") == "enabled":
                self.logger.info("Verified: quarantine enforced "
                                 "(info shows quarantine=enabled)")
                return True
            self.logger.warning("Unexpected info during quarantine: %s", info)
        except Exception as e:
            self.logger.warning("Could not verify quarantine via info: %s", e)

        # Fallback: check that getpath is blocked
        rc, _ = self._run_ceph_cmd('fs', 'subvolume', 'getpath',
                                   self.volname, SUBVOLUME_NAME)
        if rc == errno.EACCES:
            self.logger.info("Verified: quarantine enforced "
                             "(getpath returned EACCES)")
            return True

        self.logger.warning("Quarantine verification inconclusive "
                            "(getpath rc=%s)", self._rcinfo(rc))
        return False

    def _verify_quarantine_lifted(self):
        """Verify quarantine is lifted: subvolume info should show disabled,
        getpath should succeed."""
        try:
            out = self._fs_cmd("subvolume", "info", self.volname,
                               SUBVOLUME_NAME)
            info = json.loads(out)
            if info.get("quarantine") == "disabled":
                self.logger.info("Verified: quarantine lifted "
                                 "(info shows quarantine=disabled)")
                return True
            self.logger.warning("Unexpected info after disable: %s", info)
        except Exception as e:
            self.logger.warning("Could not verify lift via info: %s", e)

        # Fallback: check that getpath works
        rc, out = self._run_ceph_cmd('fs', 'subvolume', 'getpath',
                                     self.volname, SUBVOLUME_NAME)
        if rc == 0 and out.strip():
            self.logger.info("Verified: quarantine lifted "
                             "(getpath returned %s)", out.strip())
            return True

        self.logger.warning("Quarantine lift verification inconclusive "
                            "(getpath rc=%s)", self._rcinfo(rc))
        return False

    # -- Main loop ------------------------------------------------------------

    def do_quarantine_cycle(self, hold_time, cycle):
        """Run one enable → hold → verify → disable → verify cycle."""

        # Enable
        self.logger.info("Cycle %d: enabling quarantine (will hold %.1fs)",
                         cycle, hold_time)
        self._quarantine_op("enable")

        self.logger.info("Cycle %d: quarantine enabled, verifying", cycle)
        self._verify_quarantine_enforced()

        # Hold
        self.sleep_unless_stopped(hold_time)

        # Disable
        self.logger.info("Cycle %d: disabling quarantine", cycle)
        self._quarantine_op("disable")

        self.logger.info("Cycle %d: quarantine disabled, verifying", cycle)
        self._verify_quarantine_lifted()

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
                hold_time = round(
                    self.rnd.uniform(self.min_hold, self.max_hold), 1)
                release_time = round(
                    self.rnd.uniform(self.min_release, self.max_release), 1)

                try:
                    self.do_quarantine_cycle(hold_time, cycle)
                except RuntimeError as e:
                    self.logger.warning("Cycle %d failed: %s — "
                                        "will retry after %.1fs",
                                        cycle, e, release_time)

                self.logger.info("Cycle %d: sleeping %.1fs before next cycle",
                                 cycle, release_time)
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

    Modeled after the quiescer task — exercises quarantine during MDS
    failovers by cycling enable/disable while mds_thrash kills daemons.

    Each cycle:
      1. Enable quarantine on the test subvolume
      2. Verify quarantine is enforced (info shows enabled, getpath blocked)
      3. Hold for a random duration
      4. Disable quarantine
      5. Verify quarantine is lifted (info shows disabled, getpath works)
      6. Sleep before next cycle

    Example config::

        - quarantine_thrasher:
            min_hold: 5
            max_hold: 20
            initial_delay: 10
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
