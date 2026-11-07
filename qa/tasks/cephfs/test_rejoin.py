"""
Regression for https://tracker.ceph.com/issues/80402.
"""
import logging

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.filesystem import NoSuchRank
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

# handle_mds_map() passes this to usleep(), so it is microseconds.
# It must stay under mds_heartbeat_grace: the sleep runs on the rank thread.
CREATE_SLEEP_US = 45 * 1000 * 1000
HEARTBEAT_GRACE_S = 120
# Hold the replacement in replay until the new rank has gone active.
# Milliseconds, one sleep at the start of MDLog::_replay_thread().
REPLAY_DELAY_MS = 180 * 1000


class TestRejoinGather(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 3

    def _ranks(self):
        status = self.fs.status()
        ranks = {}
        for rank in (0, 1):
            try:
                ranks[rank] = self.fs.get_rank(rank=rank, status=status)
            except NoSuchRank:
                ranks[rank] = None
        return ranks

    def _wait_conf(self, entity, key, expected, timeout=60):
        expected = float(expected)

        def ready():
            try:
                return float(self.config_show(entity, key)) == expected
            except (ValueError, TypeError):
                return False

        self.wait_until_true(ready, timeout=timeout, period=1)

    def test_creating_rank_misses_rejoin_gather(self):
        """
        A new rank that is still creating while a peer fails never runs
        handle_mds_failure() for that peer. It then becomes active with
        rejoin_gather empty. OP_WEAK from the recovering rank used to
        assert in handle_cache_rejoin_weak().

        https://tracker.ceph.com/issues/80402
        """
        plain = []
        try:
            self.config_set('mds', 'mds_heartbeat_grace', HEARTBEAT_GRACE_S)

            self.fs.set_allow_standby_replay(True)
            self.wait_until_true(
                lambda: self.fs.get_replay(rank=0) is not None,
                timeout=60, period=1)
            replay = self.fs.get_replay(rank=0)
            replay_name = replay['name']
            replay_gid = replay['gid']
            log.info("standby-replay for rank 0 is mds.%s gid %s",
                     replay_name, replay_gid)

            self.mount_a.write_file("rejoin_80402", "data")
            self.fs.flush(rank=0)

            def replay_caught_up():
                status = self.fs.mds_asok(['status'], mds_id=replay_name)
                if not status:
                    return False
                positions = status.get('replay_status')
                if not positions:
                    return False
                return positions['journal_read_pos'] == positions['journal_write_pos']

            self.wait_until_true(replay_caught_up, timeout=60, period=1)

            plain = [info['name'] for info in self.fs.status().get_standbys()]
            self.assertGreaterEqual(
                len(plain), 1, "need a standby that is not already replaying")
            log.info("plain standbys: %s", plain)
            for name in plain:
                self.config_set(f'mds.{name}', 'mds_sleep_rank_change',
                                CREATE_SLEEP_US)
                self._wait_conf(f'mds.{name}', 'mds_sleep_rank_change',
                                CREATE_SLEEP_US)
                self._wait_conf(f'mds.{name}', 'mds_heartbeat_grace',
                                HEARTBEAT_GRACE_S)

            # MDLog::replay() starts _replay_thread() only when the journal
            # has unread events. That thread applies
            # mds_delay_journal_replay_for_testing before it reads. Append
            # after the delay is visible, then wait until this standby has
            # reread the new head. It stays in that sleep across promotion.
            self.config_set('mds', 'mds_delay_journal_replay_for_testing',
                            REPLAY_DELAY_MS)
            self._wait_conf(f'mds.{replay_name}',
                            'mds_delay_journal_replay_for_testing',
                            REPLAY_DELAY_MS)

            self.mount_a.write_file("rejoin_80402_unread", "data")
            self.fs.flush(rank=0)

            def replay_armed():
                status = self.fs.mds_asok(['status'], mds_id=replay_name)
                if not status:
                    return False
                positions = status.get('replay_status')
                if not positions:
                    return False
                return int(positions['journal_read_pos']) < int(
                    positions['journal_write_pos'])

            self.wait_until_true(replay_armed, timeout=30, period=1)
            log.info("standby-replay is sleeping on unread journal")

            self.fs.set_max_mds(2)

            def rank1_creating():
                rank1 = self._ranks()[1]
                return rank1 is not None and rank1['state'] == 'up:creating'

            self.wait_until_true(rank1_creating, timeout=60, period=1)
            creating = self._ranks()[1]
            creating_gid = creating['gid']
            creating_name = creating['name']
            log.info("new rank is mds.%s gid %s, failing rank 0",
                     creating_name, creating_gid)
            self.assertIn(creating_name, plain)
            self.fs.rank_fail(rank=0)

            def creating_saw_replay():
                ranks = self._ranks()
                rank0, rank1 = ranks[0], ranks[1]
                log.info("overlap rank0=%s rank1=%s",
                         None if rank0 is None else rank0['state'],
                         None if rank1 is None else rank1['state'])
                return (rank0 is not None and rank0['gid'] == replay_gid
                        and rank0['state'] == 'up:replay'
                        and rank1 is not None and rank1['gid'] == creating_gid
                        and rank1['state'] == 'up:creating')

            # The new rank is inside mds_sleep_rank_change and has not
            # applied the failover map yet. That map is applied after the
            # sleep, still in creating, so the later active transition does
            # not call handle_mds_failure().
            self.wait_until_true(creating_saw_replay, timeout=70, period=1)

            # Do not sleep again on creating -> active.
            self.config_set(f'mds.{creating_name}', 'mds_sleep_rank_change', 0)

            def rank1_active_during_replay():
                ranks = self._ranks()
                rank0, rank1 = ranks[0], ranks[1]
                log.info("rank 1 becoming active: rank0=%s rank1=%s",
                         None if rank0 is None else rank0['state'],
                         None if rank1 is None else rank1['state'])
                if (rank0 is None or rank0['gid'] != replay_gid
                        or rank0['state'] != 'up:replay'):
                    raise RuntimeError(
                        "rank 0 left replay before rank 1 was active: {0}".format(rank0))
                return (rank1 is not None and rank1['gid'] == creating_gid
                        and rank1['state'] == 'up:active')

            self.wait_until_true(rank1_active_during_replay, timeout=120, period=1)
            log.info("rank 1 is active while rank 0 is still in replay")

            self.config_rm('mds', 'mds_delay_journal_replay_for_testing')
            status = self.fs.wait_for_daemons(timeout=REPLAY_DELAY_MS // 1000 + 120)
            rank1 = self.fs.get_rank(rank=1, status=status)
            self.assertEqual(rank1['gid'], creating_gid)
            self.assertEqual(rank1['state'], 'up:active')
            self.mount_a.create_destroy()
        finally:
            for section, key in [('mds', 'mds_heartbeat_grace'),
                                 ('mds', 'mds_delay_journal_replay_for_testing')]:
                try:
                    self.config_rm(section, key)
                except CommandFailedError:
                    pass
            for name in plain:
                try:
                    self.config_rm(f'mds.{name}', 'mds_sleep_rank_change')
                except CommandFailedError:
                    pass
