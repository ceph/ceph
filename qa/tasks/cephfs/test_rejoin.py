"""
Tests for MDS cache rejoin behaviour across rank recovery.
"""

import logging

from tasks.ceph_test_case import TestTimeoutError
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.filesystem import NoSuchRank
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


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
