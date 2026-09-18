import json
import logging
import time

from tasks.cephfs.test_mirroring import TestMirroring, retry_assert

log = logging.getLogger(__name__)


class TestMirroringRebalance(TestMirroring):
    """Directory assignment rebalance across multiple cephfs-mirror daemons.

    Expects cephfs-mirror daemons client.mirror1/2/3 as in
    qa/suites/fs/mirror-ha. Uses the default 300s shuffle throttle; deferred
    rebalance after rejoin is asserted by waiting past that window.
    """

    MIRROR_CLIENTS = ('mirror1', 'mirror2', 'mirror3')
    NUM_MIRROR_INSTANCES = len(MIRROR_CLIENTS)
    # Policy.DIR_SHUFFLE_THROTTLE_INTERVAL — rejoin while throttled must still
    # converge via deferred rebalance once this elapses.
    SHUFFLE_THROTTLE_SECS = 300
    # Extra slack after SHUFFLE_THROTTLE_SECS so the last-mapped directory
    # (dirs are added sequentially) is shuffle-eligible and any deferred
    # rebalance timer has time to fire.
    SHUFFLE_THROTTLE_PAD_SECS = 60

    def setUp(self):
        super(TestMirroringRebalance, self).setUp()
        for mid in self.MIRROR_CLIENTS:
            self.config_set(f'client.{mid}', 'cephfs_mirror_directory_scan_interval', 1)
            self.config_set(f'client.{mid}', 'cephfs_mirror_tick_interval',
                            self.MIRROR_TICK_INTERVAL)

    def get_daemon_admin_socket(self, mirror_client='mirror1'):
        # Matches qa/suites/fs/mirror-ha/clients/mirror.yaml
        return f'/var/run/ceph/cephfs-mirror{mirror_client[len("mirror"):]}.asok'

    def get_mirror_daemon_pid(self, mirror_client='mirror1'):
        pid_path = f'/var/run/ceph/cephfs-mirror{mirror_client[len("mirror"):]}.pid'
        return self.mount_a.run_shell(['cat', pid_path]).stdout.getvalue().strip()

    def get_mirror_daemon(self, mirror_client):
        return self.ctx.daemons.get_daemon('cephfs-mirror', f'client.{mirror_client}')

    def stop_mirror_daemon(self, mirror_client):
        log.debug(f'stopping cephfs-mirror client.{mirror_client}')
        self.get_mirror_daemon(mirror_client).stop()

    def start_mirror_daemon(self, mirror_client):
        log.debug(f'starting cephfs-mirror client.{mirror_client}')
        self.get_mirror_daemon(mirror_client).restart()

    def mirror_show_distribution(self, fs_name):
        return json.loads(self.get_ceph_cmd_stdout(
            'fs', 'snapshot', 'mirror', 'show', 'distribution', fs_name))

    def distribution_counts(self, fs_name):
        """Return {instance_id: dir_count} from show distribution."""
        dist = self.mirror_show_distribution(fs_name)
        counts = {}
        for instance_id, label in dist.get('mapping', {}).items():
            # label is e.g. "2 directories"
            counts[str(instance_id)] = int(str(label).split()[0])
        return counts

    def max_min_dir_delta(self, counts):
        if len(counts) <= 1:
            return 0
        vals = list(counts.values())
        return max(vals) - min(vals)

    @retry_assert(timeout=120, interval=2)
    def wait_for_balanced_distribution(self, fs_name, expected_dirs,
                                      min_instances=None):
        """Wait until dirs are spread with max-min <= 1 across mirror instances.

        min_instances: at least this many mirror instances must appear in the
        distribution. Defaults to NUM_MIRROR_INSTANCES (all HA daemons).
        """
        if min_instances is None:
            min_instances = self.NUM_MIRROR_INSTANCES
        counts = self.distribution_counts(fs_name)
        self.assertGreaterEqual(len(counts), min_instances)
        self.assertEqual(sum(counts.values()), expected_dirs)
        self.assertLessEqual(self.max_min_dir_delta(counts), 1,
                             msg=f'uneven distribution: {counts}')

    @retry_assert(timeout=90, interval=2)
    def wait_for_failover_imbalance(self, fs_name, expected_dirs, max_instances):
        """After failover, dirs concentrate on surviving instance(s).

        max_instances: at most this many mirror instances may hold directories
        (e.g. 2 when one of three daemons is down).
        """
        counts = self.distribution_counts(fs_name)
        self.assertEqual(sum(counts.values()), expected_dirs)
        nonzero = {i: c for i, c in counts.items() if c > 0}
        self.assertLessEqual(len(nonzero), max_instances,
                             msg=f'expected failover concentration: {counts}')
        self.assertGreater(self.max_min_dir_delta(counts), 1,
                           msg=f'expected imbalance after failover: {counts}')

    @retry_assert(timeout=450, interval=5)
    def wait_for_balanced_distribution_after_throttle(self, fs_name, expected_dirs,
                                                      min_instances=None):
        """Wait through the default 300s shuffle throttle for deferred rebalance.

        min_instances: at least this many mirror instances must appear in the
        distribution. Defaults to NUM_MIRROR_INSTANCES (all HA daemons).
        """
        if min_instances is None:
            min_instances = self.NUM_MIRROR_INSTANCES
        counts = self.distribution_counts(fs_name)
        self.assertGreaterEqual(len(counts), min_instances)
        self.assertEqual(sum(counts.values()), expected_dirs)
        self.assertLessEqual(self.max_min_dir_delta(counts), 1,
                             msg=f'uneven distribution: {counts}')

    def wait_past_shuffle_throttle(self):
        delay = self.SHUFFLE_THROTTLE_SECS + self.SHUFFLE_THROTTLE_PAD_SECS
        log.info(f'waiting {delay}s for shuffle throttle to elapse')
        time.sleep(delay)

    def _setup_mirrored_dirs(self, n_dirs):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.peer_add(self.primary_fs_name, self.primary_fs_id,
                      'client.mirror_remote@ceph', self.secondary_fs_name,
                      check_perf_counter=False)
        dirs = []
        for i in range(n_dirs):
            name = f'rebal_dir_{i}'
            self.mount_a.run_shell(['mkdir', '-p', name])
            path = f'/{name}'
            self.add_directory(self.primary_fs_name, self.primary_fs_id, path)
            dirs.append(path)
        return dirs

    def test_initial_distribution_balanced(self):
        """New dirs on multiple daemons end with max-min <= 1 (e.g. 2+1+1)."""
        dirs = self._setup_mirrored_dirs(4)
        for d in dirs:
            self.wait_directory_mapped(self.primary_fs_name, d)
        self.wait_for_balanced_distribution(self.primary_fs_name, 4)
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_failover_then_rejoin_rebalances(self):
        """Stop one daemon (failover), restart it; deferred rebalance restores balance.

        Rejoin happens while dirs are still inside the default 300s throttle
        after failover remap. Immediate shuffle may select nothing; deferred
        rebalance should converge once the throttle window elapses.
        """
        dirs = self._setup_mirrored_dirs(4)
        for d in dirs:
            self.wait_directory_mapped(self.primary_fs_name, d)
        self.wait_for_balanced_distribution(self.primary_fs_name, 4)

        before = self.distribution_counts(self.primary_fs_name)
        log.info(f'distribution before failover: {before}')

        victim = 'mirror2'
        self.stop_mirror_daemon(victim)
        # InstanceWatcher.INSTANCE_TIMEOUT is 30s
        time.sleep(40)

        self.wait_for_failover_imbalance(
            self.primary_fs_name, 4, max_instances=2)
        mid = self.distribution_counts(self.primary_fs_name)
        log.info(f'distribution after failover: {mid}')
        self.assertGreater(self.max_min_dir_delta(mid), 1)

        self.start_mirror_daemon(victim)
        # Default DIR_SHUFFLE_THROTTLE_INTERVAL (300s) + shuffle transitions
        self.wait_for_balanced_distribution_after_throttle(
            self.primary_fs_name, 4)
        after = self.distribution_counts(self.primary_fs_name)
        log.info(f'distribution after rejoin rebalance: {after}')
        self.assertEqual(
            len([c for c in after.values() if c > 0]),
            self.NUM_MIRROR_INSTANCES,
            msg=f'expected all mirror instances to hold dirs: {after}')

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_odd_dir_count_stable_balance(self):
        """Odd dir counts (5) reach max-min <= 1 and the mapping stays fixed.

        needs_rebalance() is internal (max-min > 1); we infer it from
        ``show distribution``. Once balanced (e.g. 2+2+1), a wrongly true
        needs_rebalance would still not shuffle until each dir's throttle
        elapses, so we wait past DIR_SHUFFLE_THROTTLE_INTERVAL before
        asserting the per-instance mapping is unchanged.
        """
        dirs = self._setup_mirrored_dirs(5)
        for d in dirs:
            self.wait_directory_mapped(self.primary_fs_name, d)
        self.wait_for_balanced_distribution(self.primary_fs_name, 5)
        first = self.distribution_counts(self.primary_fs_name)
        log.info(f'distribution before throttle stability wait: {first}')
        self.wait_past_shuffle_throttle()
        second = self.distribution_counts(self.primary_fs_name)
        log.info(f'distribution after throttle stability wait: {second}')
        self.assertEqual(first, second,
                         msg=f'distribution changed (possible ping-pong): {first} -> {second}')
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
