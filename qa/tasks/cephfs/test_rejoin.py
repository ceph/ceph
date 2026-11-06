"""
Regressions for MDS rejoin across rank recovery:
https://tracker.ceph.com/issues/80402 (TestRejoinGather) and
https://tracker.ceph.com/issues/80407 (TestCacheRejoin).
"""
import logging

from tasks.ceph_test_case import TestTimeoutError
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


class TestCacheRejoin(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 4

    def _cached_dir(self, path, ino, rank):
        """
        Return the cache dump of directory inode `ino` at `path` on `rank`,
        or None if the rank doesn't have it in cache.
        """
        try:
            dir_cache = self.fs.read_cache(path, 0, rank=rank)
        except (CommandFailedError, RuntimeError):
            return None
        for inode in dir_cache:
            if inode['ino'] == ino:
                return inode
        return None

    def _dirfrag_count(self, path, ino, rank):
        inode = self._cached_dir(path, ino, rank)
        return 0 if inode is None else len(inode['dirfrags'])

    def test_rejoin_ack_isolated_inode(self):
        """
        Regression test for https://tracker.ceph.com/issues/80407

        A surviving (active) MDS must not crash in
        handle_cache_rejoin_ack() on ceph_assert(isolated_inodes.empty())
        when a recovering rank acks dirfrags of an inode the recovering
        rank no longer records as replicated to the survivor.

        The scenario: a fragmented directory X authoritative on rank R is
        replicated on survivor S (inode plus one of its leaf frags). R is
        failed; on rejoin it has no X in cache, invents it from S's strong
        rejoin, then learns the real fragtree and splits the invented
        dirfrag -- CDir::split() copies the replica map onto every leaf.
        S then trims X and sends a cache expire for X and the one leaf it
        held before R sends its acks; R drops S from X and that leaf but
        keeps S on the other leaves, and used to ack those orphan leaves,
        crashing S.

        R only invents X if nothing it replays or reopens names X: no
        subtree root at or below X (every auth subtree is journaled with
        its full path) and no client caps on X or its files (reconnected
        caps are opened from disk before rejoins are sent).  The race is
        widened deterministically with mds_debug_rejoin_ack_delay.
        """

        # rank 0 is the survivor S, rank 2 is the recovering auth R of X.
        S_RANK = 0
        R_RANK = 2

        self.fs.set_max_mds(3)
        status = self.fs.wait_for_daemons()

        # deterministic one-shot split of X into 2**6 = 64 leaves, balancer
        # off so ownership only moves where we pin it.
        self._configure_mds({
            'mds_bal_split_size': 100,
            'mds_bal_split_bits': 6,
            'mds_bal_merge_size': 5,
            'mds_bal_interval': 0,
        })

        # P is the auth parent of X, pinned to R.  A placeholder subtree under
        # a sibling pinned to S keeps S in the recovery set.
        self.mount_a.run_shell_payload("mkdir -p P/X sibling/placeholder")
        self.mount_a.setfattr("P", "ceph.dir.pin", str(R_RANK))
        self.mount_a.setfattr("sibling", "ceph.dir.pin", str(S_RANK))
        self._wait_subtrees([('/P', R_RANK), ('/sibling', S_RANK)],
                            status=status, rank="all")
        x_ino = self.mount_a.path_to_ino("P/X")

        # Fragment X on R.
        self.mount_a.create_n_files("P/X/file", 200, sync=True)
        self.wait_until_true(
            lambda: self._dirfrag_count("/P/X", x_ino, R_RANK) >= 2,
            timeout=60)

        # Make S replicate X's inode and one of its leaf frags, as an ordinary
        # unpinned replica: pin a child of X to S, move it back to R, then
        # clear the pin so it merges back into R's /P subtree.  A pin left on
        # the child would keep it a subtree root on R, and R's subtree map
        # would journal X with it.  The child needs an entry: an empty
        # directory isn't exported for a pin.
        self.mount_a.run_shell_payload("mkdir P/X/child && touch P/X/child/f")
        self.mount_a.setfattr("P/X/child", "ceph.dir.pin", str(S_RANK))
        self._wait_subtrees([('/P', R_RANK), ('/P/X/child', S_RANK),
                             ('/sibling', S_RANK)], status=status, rank="all")
        self.assertIsNotNone(self._cached_dir("/P/X", x_ino, S_RANK))
        self.mount_a.setfattr("P/X/child", "ceph.dir.pin", str(R_RANK))
        self._wait_subtrees([('/P', R_RANK), ('/P/X/child', R_RANK),
                             ('/sibling', S_RANK)], status=status, rank="all")
        self.mount_a.setfattr("P/X/child", "ceph.dir.pin", "-1")
        self._wait_subtrees([('/P', R_RANK), ('/sibling', S_RANK)],
                            status=status, rank="all")

        # Drop the client's caps on X and its files, then flush R's journal,
        # so the new R neither replays X nor reopens it for a client.
        self.mount_a.umount_wait()
        self.fs.rank_asok(['flush', 'journal'], rank=R_RANK)
        self.assertIsNotNone(self._cached_dir("/P/X", x_ino, S_RANK),
                             "S no longer has X cached before the failover")
        s_gid = self.fs.get_rank(rank=S_RANK)['gid']

        # Widen the rejoin-ack window on every daemon (the standby that takes
        # over R will inherit it).
        self._configure_mds({'mds_debug_rejoin_ack_delay': 60})

        # Fail R; a standby takes over and begins recovery.
        self.fs.rank_fail(rank=R_RANK)

        # The new R has invented X from S's strong rejoin and split it once it
        # holds more than one of X's dirfrags.  Only then make S trim: an
        # expire that arrives before the split takes S off the single
        # invented dirfrag, and the split has nothing to copy.
        self.fs.wait_for_state('up:rejoin', rank=R_RANK, timeout=60)
        self.wait_until_true(
            lambda: self._dirfrag_count("/P/X", x_ino, R_RANK) >= 2,
            timeout=30)
        log.info("dropping survivor cache to trigger the expire")
        self.fs.rank_asok(['cache', 'drop', '0'], rank=S_RANK)
        self.assertIsNone(self._cached_dir("/P/X", x_ino, S_RANK),
                          "S did not trim X")

        # The fix: S survives and the cluster recovers.  Without it, S aborts
        # on isolated_inodes as soon as it handles R's acks, which R sends
        # before it goes active.  Watch S in the FSMap for a beacon timeout
        # after that: an MDS still writing its core never answers the tell
        # in wait_for_daemons.
        self.wait_until_true(
            lambda: self.fs.get_rank(rank=R_RANK)['state'] == 'up:active',
            timeout=180)

        def s_failed():
            try:
                info = self.fs.get_rank(rank=S_RANK)
            except NoSuchRank:
                return True
            return info['gid'] != s_gid or 'laggy_since' in info

        try:
            self.wait_until_true(s_failed, timeout=self.fs.beacon_timeout)
        except TestTimeoutError:
            pass
        else:
            self.fail("rank 0 failed during R's recovery")

        status = self.fs.wait_for_daemons(timeout=180)
        self.assertEqual(len(list(self.fs.get_ranks(status=status))), 3)
        self.assertEqual(len(self.fs.get_damaged(status=status)), 0)

    def _configure_mds(self, conf):
        for k, v in conf.items():
            self.config_set('mds', str(k), str(v))
