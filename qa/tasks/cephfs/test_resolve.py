"""
Regression for https://tracker.ceph.com/issues/82668.
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


class TestResolveCreatingPeer(CephFSTestCase):
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

    def test_resolve_while_peer_creating(self):
        """
        Fail rank 0 while rank 1, the only other in rank, is still
        up:creating. The recovering rank then has nobody in its recovery
        set: it sends no resolves and receives none, and used to stay in
        up:resolve forever.

        https://tracker.ceph.com/issues/82668
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

            self.mount_a.write_file("resolve_82668", "data")
            self.fs.flush(rank=0)

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

            def rank0_past_resolve():
                ranks = self._ranks()
                rank0, rank1 = ranks[0], ranks[1]
                log.info("rank0=%s rank1=%s",
                         None if rank0 is None else rank0['state'],
                         None if rank1 is None else rank1['state'])
                if (rank1 is None or rank1['gid'] != creating_gid
                        or rank1['state'] != 'up:creating'):
                    raise RuntimeError(
                        "rank 1 left creating first: {0}".format(rank1))
                return (rank0 is not None and rank0['gid'] == replay_gid
                        and rank0['state'] in ('up:reconnect', 'up:rejoin',
                                               'up:clientreplay',
                                               'up:active'))

            # The new rank sleeps in mds_sleep_rank_change, so it stays
            # up:creating while rank 0 recovers. Rank 0 must get out of
            # resolve without hearing from it.
            self.wait_until_true(rank0_past_resolve, timeout=30, period=1)
            log.info("rank 0 left resolve while rank 1 is still creating")

            self.config_set(f'mds.{creating_name}', 'mds_sleep_rank_change', 0)
            status = self.fs.wait_for_daemons(timeout=CREATE_SLEEP_US // 1000000 + 120)
            rank0 = self.fs.get_rank(rank=0, status=status)
            rank1 = self.fs.get_rank(rank=1, status=status)
            self.assertEqual(rank0['gid'], replay_gid)
            self.assertEqual(rank1['gid'], creating_gid)
            # an empty directory is not exported, so write to it first
            self.mount_a.run_shell(["mkdir", "d1"])
            self.mount_a.write_file("d1/f", "data")
            self.mount_a.setfattr("d1", "ceph.dir.pin", "1")
            self._wait_subtrees([('/d1', 1)], status=status, rank=1,
                                timeout=60)
            self.mount_a.run_shell(["ls", "-lR", "."])
            self.mount_a.create_destroy()
        finally:
            try:
                self.config_rm('mds', 'mds_heartbeat_grace')
            except CommandFailedError:
                pass
            for name in plain:
                try:
                    self.config_rm(f'mds.{name}', 'mds_sleep_rank_change')
                except CommandFailedError:
                    pass
