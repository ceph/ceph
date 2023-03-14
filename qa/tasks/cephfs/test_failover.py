import time
import signal
import logging
import operator
from random import randint, choice

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)

class TestClusterAffinity(CephFSTestCase):
    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 4

    def _verify_join_fs(self, target, status=None, fs=None):
        fs_select = fs
        if fs_select is None:
            fs_select = self.fs
        if status is None:
            status = fs_select.wait_for_daemons(timeout=30)
            log.debug("%s", status)
        target = sorted(target, key=operator.itemgetter('name'))
        log.info("target = %s", target)
        current = list(status.get_all())
        current = sorted(current, key=operator.itemgetter('name'))
        log.info("current = %s", current)
        self.assertEqual(len(current), len(target))
        for i in range(len(current)):
            for attr in target[i]:
                self.assertIn(attr, current[i])
                self.assertEqual(target[i][attr], current[i][attr])

    def _change_target_state(self, state, name, changes):
        for entity in state:
            if entity['name'] == name:
                for k, v in changes.items():
                    entity[k] = v
                return
        self.fail("no entity")

    def _verify_init(self, fs=None):
        fs_select = fs
        if fs_select is None:
            fs_select = self.fs
        status = fs_select.status()
        log.info("status = {0}".format(status))
        target = [{'join_fscid': -1, 'name': info['name']} for info in status.get_all()]
        self._verify_join_fs(target, status=status, fs=fs_select)
        return (status, target)

    def _reach_target(self, target):
        def takeover():
            try:
                self._verify_join_fs(target)
                return True
            except AssertionError as e:
                log.debug("%s", e)
                return False
        self.wait_until_true(takeover, 30)

    def test_join_fs_runtime(self):
        """
        That setting mds_join_fs at runtime affects the cluster layout.
        """
        status, target = self._verify_init()
        standbys = list(status.get_standbys())
        self.config_set('mds.'+standbys[0]['name'], 'mds_join_fs', 'cephfs')
        self._change_target_state(target, standbys[0]['name'], {'join_fscid': self.fs.id, 'state': 'up:active'})
        self._reach_target(target)

    def test_join_fs_unset(self):
        """
        That unsetting mds_join_fs will cause failover if another high-affinity standby exists.
        """
        status, target = self._verify_init()
        standbys = list(status.get_standbys())
        names = (standbys[0]['name'], standbys[1]['name'])
        self.config_set('mds.'+names[0], 'mds_join_fs', 'cephfs')
        self.config_set('mds.'+names[1], 'mds_join_fs', 'cephfs')
        self._change_target_state(target, names[0], {'join_fscid': self.fs.id})
        self._change_target_state(target, names[1], {'join_fscid': self.fs.id})
        self._reach_target(target)
        time.sleep(5) # MDSMonitor tick
        status = self.fs.wait_for_daemons()
        active = self.fs.get_active_names(status=status)[0]
        self.assertIn(active, names)
        self.config_rm('mds.'+active, 'mds_join_fs')
        self._change_target_state(target, active, {'join_fscid': -1})
        new_active = (set(names) - set((active,))).pop()
        self._change_target_state(target, new_active, {'state': 'up:active'})
        self._reach_target(target)

    def test_join_fs_drop(self):
        """
        That unsetting mds_join_fs will not cause failover if no high-affinity standby exists.
        """
        status, target = self._verify_init()
        standbys = list(status.get_standbys())
        active = standbys[0]['name']
        self.config_set('mds.'+active, 'mds_join_fs', 'cephfs')
        self._change_target_state(target, active, {'join_fscid': self.fs.id, 'state': 'up:active'})
        self._reach_target(target)
        self.config_rm('mds.'+active, 'mds_join_fs')
        self._change_target_state(target, active, {'join_fscid': -1})
        self._reach_target(target)

    def test_join_fs_vanilla(self):
        """
        That a vanilla standby is preferred over others with mds_join_fs set to another fs.
        """
        fs2 = self.mds_cluster.newfs(name="cephfs2")
        status, target = self._verify_init()
        active = self.fs.get_active_names(status=status)[0]
        status2, _ = self._verify_init(fs=fs2)
        active2 = fs2.get_active_names(status=status2)[0]
        standbys = [info['name'] for info in status.get_standbys()]
        victim = standbys.pop()
        # Set a bogus fs on the others
        for mds in standbys:
            self.config_set('mds.'+mds, 'mds_join_fs', 'cephfs2')
            self._change_target_state(target, mds, {'join_fscid': fs2.id})
        # The active MDS for cephfs2 will be replaced by the MDS for which
        # file system affinity has been set. Also, set the affinity for
        # the earlier active MDS so that it is not chosen by the monitors
        # as an active MDS for the existing file system.
        log.info(f'assigning affinity to cephfs2 for active mds (mds.{active2})')
        self.config_set(f'mds.{active2}', 'mds_join_fs', 'cephfs2')
        self._change_target_state(target, active2, {'join_fscid': fs2.id})
        self.fs.rank_fail()
        self._change_target_state(target, victim, {'state': 'up:active'})
        self._reach_target(target)
        status = self.fs.status()
        active = self.fs.get_active_names(status=status)[0]
        self.assertEqual(active, victim)

    def test_join_fs_last_resort(self):
        """
        That a standby with mds_join_fs set to another fs is still used if necessary.
        """
        status, target = self._verify_init()
        standbys = [info['name'] for info in status.get_standbys()]
        for mds in standbys:
            self.config_set('mds.'+mds, 'mds_join_fs', 'cephfs2')
        fs2 = self.mds_cluster.newfs(name="cephfs2")
        for mds in standbys:
            self._change_target_state(target, mds, {'join_fscid': fs2.id})
        self.fs.rank_fail()
        status = self.fs.status()
        ranks = list(self.fs.get_ranks(status=status))
        self.assertEqual(len(ranks), 1)
        self.assertIn(ranks[0]['name'], standbys)

        # Wait for the former active to reclaim its spot
        def reclaimed():
            ranks = list(self.fs.get_ranks())
            return len(ranks) > 0 and ranks[0]['name'] not in standbys

        log.info("Waiting for former active to reclaim its spot")
        self.wait_until_true(reclaimed, timeout=self.fs.beacon_timeout)

    def test_join_fs_last_resort_refused(self):
        """
        That a standby with mds_join_fs set to another fs is not used if refuse_standby_for_another_fs is set.
        """
        status, target = self._verify_init()
        standbys = [info['name'] for info in status.get_standbys()]
        for mds in standbys:
            self.config_set('mds.'+mds, 'mds_join_fs', 'cephfs2')
        fs2 = self.mds_cluster.newfs(name="cephfs2")
        for mds in standbys:
            self._change_target_state(target, mds, {'join_fscid': fs2.id})
        self.fs.set_refuse_standby_for_another_fs(True)
        self.fs.rank_fail()
        status = self.fs.status()
        ranks = list(self.fs.get_ranks(status=status))
        self.assertTrue(len(ranks) == 0 or ranks[0]['name'] not in standbys)

        # Wait for the former active to reclaim its spot
        def reclaimed():
            ranks = list(self.fs.get_ranks())
            return len(ranks) > 0 and ranks[0]['name'] not in standbys

        log.info("Waiting for former active to reclaim its spot")
        self.wait_until_true(reclaimed, timeout=self.fs.beacon_timeout)

    def test_join_fs_steady(self):
        """
        That a sole MDS with mds_join_fs set will come back as active eventually even after failover.
        """
        status, target = self._verify_init()
        active = self.fs.get_active_names(status=status)[0]
        self.config_set('mds.'+active, 'mds_join_fs', 'cephfs')
        self._change_target_state(target, active, {'join_fscid': self.fs.id})
        self._reach_target(target)
        self.fs.rank_fail()
        self._reach_target(target)

    def test_join_fs_standby_replay(self):
        """
        That a standby-replay daemon with weak affinity is replaced by a stronger one.
        """
        status, target = self._verify_init()
        standbys = [info['name'] for info in status.get_standbys()]
        self.config_set('mds.'+standbys[0], 'mds_join_fs', 'cephfs')
        self._change_target_state(target, standbys[0], {'join_fscid': self.fs.id, 'state': 'up:active'})
        self._reach_target(target)
        self.fs.set_allow_standby_replay(True)
        status = self.fs.status()
        standbys = [info['name'] for info in status.get_standbys()]
        self.config_set('mds.'+standbys[0], 'mds_join_fs', 'cephfs')
        self._change_target_state(target, standbys[0], {'join_fscid': self.fs.id, 'state': 'up:standby-replay'})
        self._reach_target(target)

class TestClusterResize(CephFSTestCase):
    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 3

    def test_grow(self):
        """
        That the MDS cluster grows after increasing max_mds.
        """

        # Need all my standbys up as well as the active daemons
        # self.wait_for_daemon_start() necessary?

        self.fs.grow(2)
        self.fs.grow(3)


    def test_shrink(self):
        """
        That the MDS cluster shrinks automatically after decreasing max_mds.
        """

        self.fs.grow(3)
        self.fs.shrink(1)

    def test_up_less_than_max(self):
        """
        That a health warning is generated when max_mds is greater than active count.
        """

        status = self.fs.status()
        mdss = [info['gid'] for info in status.get_all()]
        self.fs.set_max_mds(len(mdss)+1)
        self.wait_for_health("MDS_UP_LESS_THAN_MAX", 30)
        self.fs.shrink(2)
        self.wait_for_health_clear(30)

    def test_down_health(self):
        """
        That marking a FS down does not generate a health warning
        """

        self.fs.set_down()
        try:
            self.wait_for_health("", 30)
            raise RuntimeError("got health warning?")
        except RuntimeError as e:
            if "Timed out after" in str(e):
                pass
            else:
                raise

    def test_down_twice(self):
        """
        That marking a FS down twice does not wipe old_max_mds.
        """

        self.fs.grow(2)
        self.fs.set_down()
        self.fs.wait_for_daemons()
        self.fs.set_down(False)
        self.assertEqual(self.fs.get_var("max_mds"), 2)
        self.fs.wait_for_daemons(timeout=60)

    def test_down_grow(self):
        """
        That setting max_mds undoes down.
        """

        self.fs.set_down()
        self.fs.wait_for_daemons()
        self.fs.grow(2)
        self.fs.wait_for_daemons()

    def test_down(self):
        """
        That down setting toggles and sets max_mds appropriately.
        """

        self.fs.set_down()
        self.fs.wait_for_daemons()
        self.assertEqual(self.fs.get_var("max_mds"), 0)
        self.fs.set_down(False)
        self.assertEqual(self.fs.get_var("max_mds"), 1)
        self.fs.wait_for_daemons()
        self.assertEqual(self.fs.get_var("max_mds"), 1)

    def test_hole(self):
        """
        Test that a hole cannot be created in the FS ranks.
        """

        fscid = self.fs.id

        self.fs.grow(2)

        # Now add a delay which should slow down how quickly rank 1 stops
        self.config_set('mds', 'ms_inject_delay_max', '5.0')
        self.config_set('mds', 'ms_inject_delay_probability', '1.0')
        self.fs.set_max_mds(1)
        log.info("status = {0}".format(self.fs.status()))

        # Don't wait for rank 1 to stop
        self.fs.set_max_mds(3)
        log.info("status = {0}".format(self.fs.status()))

        # Now check that the mons didn't try to promote a standby to rank 2
        self.fs.set_max_mds(2)
        status = self.fs.status()
        try:
            status = self.fs.wait_for_daemons(timeout=90)
            ranks = set([info['rank'] for info in status.get_ranks(fscid)])
            self.assertEqual(ranks, set([0, 1]))
        finally:
            log.info("status = {0}".format(status))

    def test_thrash(self):
        """
        Test that thrashing max_mds does not fail.
        """

        max_mds = 2
        for i in range(0, 100):
            self.fs.set_max_mds(max_mds)
            max_mds = (max_mds+1)%3+1

        self.fs.wait_for_daemons(timeout=90)

class TestFailover(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 2

    def test_repeated_boot(self):
        """
        That multiple boot messages do not result in the MDS getting evicted.
        """

        interval = 10
        self.config_set("mon", "paxos_propose_interval", interval)

        mds = choice(list(self.fs.status().get_all()))

        with self.assert_cluster_log(f"daemon mds.{mds['name']} restarted", present=False):
            # Avoid a beacon to the monitors with down:dne by restarting:
            self.fs.mds_fail(mds_id=mds['name'])
            # `ceph mds fail` won't return until the FSMap is committed, double-check:
            self.assertIsNone(self.fs.status().get_mds_gid(mds['gid']))
            time.sleep(2) # for mds to restart and accept asok commands
            status1 = self.fs.mds_asok(['status'], mds_id=mds['name'])
            time.sleep(interval*1.5)
            status2 = self.fs.mds_asok(['status'], mds_id=mds['name'])
            self.assertEqual(status1['id'], status2['id'])

    def test_simple(self):
        """
        That when the active MDS is killed, a standby MDS is promoted into
        its rank after the grace period.

        This is just a simple unit test, the harder cases are covered
        in thrashing tests.
        """

        (original_active, ) = self.fs.get_active_names()
        original_standbys = self.mds_cluster.get_standby_daemons()

        # Kill the rank 0 daemon's physical process
        self.fs.mds_stop(original_active)

        # Wait until the monitor promotes his replacement
        def promoted():
            ranks = list(self.fs.get_ranks())
            return len(ranks) > 0 and ranks[0]['name'] in original_standbys

        log.info("Waiting for promotion of one of the original standbys {0}".format(
            original_standbys))
        self.wait_until_true(promoted, timeout=self.fs.beacon_timeout)

        # Start the original rank 0 daemon up again, see that he becomes a standby
        self.fs.mds_restart(original_active)
        self.wait_until_true(
            lambda: original_active in self.mds_cluster.get_standby_daemons(),
            timeout=60  # Approximately long enough for MDS to start and mon to notice
        )

    def test_client_abort(self):
        """
        That a client will respect fuse_require_active_mds and error out
        when the cluster appears to be unavailable.
        """

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Requires FUSE client to inject client metadata")

        require_active = self.fs.get_config("fuse_require_active_mds", service_type="mon").lower() == "true"
        if not require_active:
            self.skipTest("fuse_require_active_mds is not set")

        # Check it's not laggy to begin with
        (original_active, ) = self.fs.get_active_names()
        self.assertNotIn("laggy_since", self.fs.status().get_mds(original_active))

        self.mounts[0].umount_wait()

        # Control: that we can mount and unmount usually, while the cluster is healthy
        self.mounts[0].mount_wait()
        self.mounts[0].umount_wait()

        # Stop the daemon processes
        self.fs.mds_stop()

        # Wait for everyone to go laggy
        def laggy():
            mdsmap = self.fs.get_mds_map()
            for info in mdsmap['info'].values():
                if "laggy_since" not in info:
                    return False

            return True

        self.wait_until_true(laggy, self.fs.beacon_timeout)
        with self.assertRaises(CommandFailedError):
            self.mounts[0].mount_wait()

    def test_standby_count_wanted(self):
        """
        That cluster health warnings are generated by insufficient standbys available.
        """

        # Need all my standbys up as well as the active daemons
        self.wait_for_daemon_start()

        standbys = self.mds_cluster.get_standby_daemons()
        self.assertGreaterEqual(len(standbys), 1)
        self.fs.mon_manager.raw_cluster_cmd('fs', 'set', self.fs.name, 'standby_count_wanted', str(len(standbys)))

        # Kill a standby and check for warning
        victim = standbys.pop()
        self.fs.mds_stop(victim)
        self.wait_for_health("MDS_INSUFFICIENT_STANDBY", self.fs.beacon_timeout)

        # restart the standby, see that he becomes a standby, check health clears
        self.fs.mds_restart(victim)
        self.wait_until_true(
            lambda: victim in self.mds_cluster.get_standby_daemons(),
            timeout=60  # Approximately long enough for MDS to start and mon to notice
        )
        self.wait_for_health_clear(timeout=30)

        # Set it one greater than standbys ever seen
        standbys = self.mds_cluster.get_standby_daemons()
        self.assertGreaterEqual(len(standbys), 1)
        self.fs.mon_manager.raw_cluster_cmd('fs', 'set', self.fs.name, 'standby_count_wanted', str(len(standbys)+1))
        self.wait_for_health("MDS_INSUFFICIENT_STANDBY", self.fs.beacon_timeout)

        # Set it to 0
        self.fs.mon_manager.raw_cluster_cmd('fs', 'set', self.fs.name, 'standby_count_wanted', '0')
        self.wait_for_health_clear(timeout=30)

    def test_discontinuous_mdsmap(self):
        """
        That discontinuous mdsmap does not affect failover.
        See http://tracker.ceph.com/issues/24856.
        """
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        self.mount_a.umount_wait()

        monc_timeout = float(self.fs.get_config("mon_client_ping_timeout", service_type="mds"))

        mds_0 = self.fs.get_rank(rank=0, status=status)
        self.fs.rank_freeze(True, rank=0) # prevent failover
        self.fs.rank_signal(signal.SIGSTOP, rank=0, status=status)
        self.wait_until_true(
            lambda: "laggy_since" in self.fs.get_rank(),
            timeout=self.fs.beacon_timeout
        )

        self.fs.rank_fail(rank=1)
        self.fs.wait_for_state('up:resolve', rank=1, timeout=30)

        # Make sure of mds_0's monitor connection gets reset
        time.sleep(monc_timeout * 2)

        # Continue rank 0, it will get discontinuous mdsmap
        self.fs.rank_signal(signal.SIGCONT, rank=0)
        self.wait_until_true(
            lambda: "laggy_since" not in self.fs.get_rank(rank=0),
            timeout=self.fs.beacon_timeout
        )

        # mds.b will be stuck at 'reconnect' state if snapserver gets confused
        # by discontinuous mdsmap
        self.fs.wait_for_state('up:active', rank=1, timeout=30)
        self.assertEqual(mds_0['gid'], self.fs.get_rank(rank=0)['gid'])
        self.fs.rank_freeze(False, rank=0)

    def test_connect_bootstrapping(self):
        self.config_set("mds", "mds_sleep_rank_change", 10000000.0)
        self.config_set("mds", "mds_connect_bootstrapping", True)
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        self.fs.rank_fail(rank=0)
        # rank 0 will get stuck in up:resolve, see https://tracker.ceph.com/issues/53194
        self.fs.wait_for_daemons()


class TestStandbyReplay(CephFSTestCase):
    CLIENTS_REQUIRED = 0
    MDSS_REQUIRED = 4

    def _confirm_no_replay(self):
        status = self.fs.status()
        _ = len(list(status.get_standbys()))
        self.assertEqual(0, len(list(self.fs.get_replays(status=status))))
        return status

    def _confirm_single_replay(self, full=True, status=None, retries=3):
        status = self.fs.wait_for_daemons(status=status)
        ranks = sorted(self.fs.get_mds_map(status=status)['in'])
        replays = list(self.fs.get_replays(status=status))
        checked_replays = set()
        for rank in ranks:
            has_replay = False
            for replay in replays:
                if replay['rank'] == rank:
                    self.assertFalse(has_replay)
                    has_replay = True
                    checked_replays.add(replay['gid'])
            if full and not has_replay:
                if retries <= 0:
                    raise RuntimeError("rank "+str(rank)+" has no standby-replay follower")
                else:
                    retries = retries-1
                    time.sleep(2)
        self.assertEqual(checked_replays, set(info['gid'] for info in replays))
        return status

    def _check_replay_takeover(self, status, rank=0):
        replay = self.fs.get_replay(rank=rank, status=status)
        new_status = self.fs.wait_for_daemons()
        new_active = self.fs.get_rank(rank=rank, status=new_status)
        if replay:
            self.assertEqual(replay['gid'], new_active['gid'])
        else:
            # double check takeover came from a standby (or some new daemon via restart)
            found = False
            for info in status.get_standbys():
                if info['gid'] == new_active['gid']:
                    found = True
                    break
            if not found:
                for info in status.get_all():
                    self.assertNotEqual(info['gid'], new_active['gid'])
        return new_status

    def test_standby_replay_singleton(self):
        """
        That only one MDS becomes standby-replay.
        """

        self._confirm_no_replay()
        self.fs.set_allow_standby_replay(True)
        time.sleep(30)
        self._confirm_single_replay()

    def test_standby_replay_damaged(self):
        """
        That a standby-replay daemon can cause the rank to go damaged correctly.
        """

        self._confirm_no_replay()
        self.config_set("mds", "mds_standby_replay_damaged", True)
        self.fs.set_allow_standby_replay(True)
        self.wait_until_true(
            lambda: len(self.fs.get_damaged()) > 0,
            timeout=30
        )
        status = self.fs.status()
        self.assertListEqual([], list(self.fs.get_ranks(status=status)))
        self.assertListEqual([0], self.fs.get_damaged(status=status))

    def test_standby_replay_disable(self):
        """
        That turning off allow_standby_replay fails all standby-replay daemons.
        """

        self._confirm_no_replay()
        self.fs.set_allow_standby_replay(True)
        time.sleep(30)
        self._confirm_single_replay()
        self.fs.set_allow_standby_replay(False)
        self._confirm_no_replay()

    def test_standby_replay_singleton_fail(self):
        """
        That failures don't violate singleton constraint.
        """

        self._confirm_no_replay()
        self.fs.set_allow_standby_replay(True)
        status = self._confirm_single_replay()

        for i in range(10):
            time.sleep(randint(1, 5))
            self.fs.rank_restart(status=status)
            status = self._check_replay_takeover(status)
            status = self._confirm_single_replay(status=status)

        for i in range(10):
            time.sleep(randint(1, 5))
            self.fs.rank_fail()
            status = self._check_replay_takeover(status)
            status = self._confirm_single_replay(status=status)

    def test_standby_replay_singleton_fail_multimds(self):
        """
        That failures don't violate singleton constraint with multiple actives.
        """

        status = self._confirm_no_replay()
        new_max_mds = randint(2, len(list(status.get_standbys())))
        self.fs.set_max_mds(new_max_mds)
        self.fs.wait_for_daemons() # wait for actives to come online!
        self.fs.set_allow_standby_replay(True)
        status = self._confirm_single_replay(full=False)

        for i in range(10):
            time.sleep(randint(1, 5))
            victim = randint(0, new_max_mds-1)
            self.fs.rank_restart(rank=victim, status=status)
            status = self._check_replay_takeover(status, rank=victim)
            status = self._confirm_single_replay(status=status, full=False)

        for i in range(10):
            time.sleep(randint(1, 5))
            victim = randint(0, new_max_mds-1)
            self.fs.rank_fail(rank=victim)
            status = self._check_replay_takeover(status, rank=victim)
            status = self._confirm_single_replay(status=status, full=False)

    def test_standby_replay_failure(self):
        """
        That the failure of a standby-replay daemon happens cleanly
        and doesn't interrupt anything else.
        """

        status = self._confirm_no_replay()
        self.fs.set_max_mds(1)
        self.fs.set_allow_standby_replay(True)
        status = self._confirm_single_replay()

        for i in range(10):
            time.sleep(randint(1, 5))
            victim = self.fs.get_replay(status=status)
            self.fs.mds_restart(mds_id=victim['name'])
            status = self._confirm_single_replay(status=status)

    def test_standby_replay_prepare_beacon(self):
        """
        That a MDSMonitor::prepare_beacon handles standby-replay daemons
        correctly without removing the standby. (Note, usually a standby-replay
        beacon will just be replied to by MDSMonitor::preprocess_beacon.)
        """

        status = self._confirm_no_replay()
        self.fs.set_max_mds(1)
        self.fs.set_allow_standby_replay(True)
        status = self._confirm_single_replay()
        replays = list(status.get_replays(self.fs.id))
        self.assertEqual(len(replays), 1)
        self.config_set('mds.'+replays[0]['name'], 'mds_inject_health_dummy', True)
        time.sleep(10) # for something not to happen...
        status = self._confirm_single_replay()
        replays2 = list(status.get_replays(self.fs.id))
        self.assertEqual(replays[0]['gid'], replays2[0]['gid'])

    def test_rank_stopped(self):
        """
        That when a rank is STOPPED, standby replays for
        that rank get torn down
        """

        status = self._confirm_no_replay()
        standby_count = len(list(status.get_standbys()))
        self.fs.set_max_mds(2)
        self.fs.set_allow_standby_replay(True)
        status = self._confirm_single_replay()

        self.fs.set_max_mds(1) # stop rank 1

        status = self._confirm_single_replay()
        self.assertTrue(standby_count, len(list(status.get_standbys())))


class TestMultiFilesystems(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 4

    # We'll create our own filesystems and start our own daemons
    REQUIRE_FILESYSTEM = False

    def setUp(self):
        super(TestMultiFilesystems, self).setUp()
        self.mds_cluster.mon_manager.raw_cluster_cmd("fs", "flag", "set",
            "enable_multiple", "true",
            "--yes-i-really-mean-it")

    def _setup_two(self):
        fs_a = self.mds_cluster.newfs(name="alpha")
        fs_b = self.mds_cluster.newfs(name="bravo")

        self.mds_cluster.mds_restart()

        # Wait for both filesystems to go healthy
        fs_a.wait_for_daemons()
        fs_b.wait_for_daemons()

        # Reconfigure client auth caps
        for mount in self.mounts:
            self.get_ceph_cmd_result(
                'auth', 'caps', "client.{0}".format(mount.client_id),
                'mds', 'allow',
                'mon', 'allow r',
                'osd', 'allow rw pool={0}, allow rw pool={1}'.format(
                    fs_a.get_data_pool_name(), fs_b.get_data_pool_name()))

        return fs_a, fs_b

    def test_clients(self):
        fs_a, fs_b = self._setup_two()

        # Mount a client on fs_a
        self.mount_a.mount_wait(cephfs_name=fs_a.name)
        self.mount_a.write_n_mb("pad.bin", 1)
        self.mount_a.write_n_mb("test.bin", 2)
        a_created_ino = self.mount_a.path_to_ino("test.bin")
        self.mount_a.create_files()

        # Mount a client on fs_b
        self.mount_b.mount_wait(cephfs_name=fs_b.name)
        self.mount_b.write_n_mb("test.bin", 1)
        b_created_ino = self.mount_b.path_to_ino("test.bin")
        self.mount_b.create_files()

        # Check that a non-default filesystem mount survives an MDS
        # failover (i.e. that map subscription is continuous, not
        # just the first time), reproduces #16022
        old_fs_b_mds = fs_b.get_active_names()[0]
        self.mds_cluster.mds_stop(old_fs_b_mds)
        self.mds_cluster.mds_fail(old_fs_b_mds)
        fs_b.wait_for_daemons()
        background = self.mount_b.write_background()
        # Raise exception if the write doesn't finish (i.e. if client
        # has not kept up with MDS failure)
        try:
            self.wait_until_true(lambda: background.finished, timeout=30)
        except RuntimeError:
            # The mount is stuck, we'll have to force it to fail cleanly
            background.stdin.close()
            self.mount_b.umount_wait(force=True)
            raise

        self.mount_a.umount_wait()
        self.mount_b.umount_wait()

        # See that the client's files went into the correct pool
        self.assertTrue(fs_a.data_objects_present(a_created_ino, 1024 * 1024))
        self.assertTrue(fs_b.data_objects_present(b_created_ino, 1024 * 1024))

    def test_standby(self):
        fs_a, fs_b = self._setup_two()

        # Assert that the remaining two MDS daemons are now standbys
        a_daemons = fs_a.get_active_names()
        b_daemons = fs_b.get_active_names()
        self.assertEqual(len(a_daemons), 1)
        self.assertEqual(len(b_daemons), 1)
        original_a = a_daemons[0]
        original_b = b_daemons[0]
        expect_standby_daemons = set(self.mds_cluster.mds_ids) - (set(a_daemons) | set(b_daemons))

        # Need all my standbys up as well as the active daemons
        self.wait_for_daemon_start()
        self.assertEqual(expect_standby_daemons, self.mds_cluster.get_standby_daemons())

        # Kill fs_a's active MDS, see a standby take over
        self.mds_cluster.mds_stop(original_a)
        self.mds_cluster.mon_manager.raw_cluster_cmd("mds", "fail", original_a)
        self.wait_until_equal(lambda: len(fs_a.get_active_names()), 1, 30,
                              reject_fn=lambda v: v > 1)
        # Assert that it's a *different* daemon that has now appeared in the map for fs_a
        self.assertNotEqual(fs_a.get_active_names()[0], original_a)

        # Kill fs_b's active MDS, see a standby take over
        self.mds_cluster.mds_stop(original_b)
        self.mds_cluster.mon_manager.raw_cluster_cmd("mds", "fail", original_b)
        self.wait_until_equal(lambda: len(fs_b.get_active_names()), 1, 30,
                              reject_fn=lambda v: v > 1)
        # Assert that it's a *different* daemon that has now appeared in the map for fs_a
        self.assertNotEqual(fs_b.get_active_names()[0], original_b)

        # Both of the original active daemons should be gone, and all standbys used up
        self.assertEqual(self.mds_cluster.get_standby_daemons(), set())

        # Restart the ones I killed, see them reappear as standbys
        self.mds_cluster.mds_restart(original_a)
        self.mds_cluster.mds_restart(original_b)
        self.wait_until_true(
            lambda: {original_a, original_b} == self.mds_cluster.get_standby_daemons(),
            timeout=30
        )

    def test_grow_shrink(self):
        # Usual setup...
        fs_a, fs_b = self._setup_two()

        # Increase max_mds on fs_b, see a standby take up the role
        fs_b.set_max_mds(2)
        self.wait_until_equal(lambda: len(fs_b.get_active_names()), 2, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Increase max_mds on fs_a, see a standby take up the role
        fs_a.set_max_mds(2)
        self.wait_until_equal(lambda: len(fs_a.get_active_names()), 2, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Shrink fs_b back to 1, see a daemon go back to standby
        fs_b.set_max_mds(1)
        self.wait_until_equal(lambda: len(fs_b.get_active_names()), 1, 30,
                              reject_fn=lambda v: v > 2 or v < 1)

        # Grow fs_a up to 3, see the former fs_b daemon join it.
        fs_a.set_max_mds(3)
        self.wait_until_equal(lambda: len(fs_a.get_active_names()), 3, 60,
                              reject_fn=lambda v: v > 3 or v < 2)
