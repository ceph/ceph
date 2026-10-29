"""
Tests for MDS cache rejoin behaviour across rank recovery.
"""

import logging
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestCacheRejoin(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 4

    def _dirfrag_count(self, path, rank):
        return len(self.get_dir_ino(path, rank=rank)['dirfrags'])

    def get_dir_ino(self, path, rank=0):
        dir_cache = self.fs.read_cache(path, 0, rank=rank)
        dir_inono = self.mount_a.path_to_ino(path.strip("/"))
        for ino in dir_cache:
            if ino['ino'] == dir_inono:
                return ino
        return None

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

        The race is widened deterministically with mds_debug_rejoin_ack_delay.
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
        self.mount_a.setfattr("P", "ceph.dir.pin", R_RANK)
        self.mount_a.setfattr("sibling", "ceph.dir.pin", S_RANK)
        self._wait_subtrees([('/P', R_RANK), ('/sibling', S_RANK)],
                            status=status, rank=R_RANK)

        # Fragment X on R.
        self.mount_a.create_n_files("P/X/file", 200, sync=True)
        self.wait_until_true(
            lambda: self._dirfrag_count("/P/X", rank=R_RANK) >= 2,
            timeout=60)

        # Make S replicate X's inode and one of its leaf frags, as an ordinary
        # unpinned replica: pin a child of X to S, then pin it back to R.
        self.mount_a.run_shell_payload("mkdir P/X/child")
        self.mount_a.setfattr("P/X/child", "ceph.dir.pin", S_RANK)
        self._wait_subtrees([('/P/X/child', S_RANK)], status=status, rank=S_RANK)
        # S now holds P, X and the frag of X containing 'child'.
        self.assertIsNotNone(self.get_dir_ino("/P/X", rank=S_RANK))
        self.mount_a.setfattr("P/X/child", "ceph.dir.pin", R_RANK)
        self._wait_subtrees([('/P/X/child', R_RANK)], status=status, rank=R_RANK)

        # Make X cold on R so a restarted R has to invent it from S's rejoin:
        # flush the journal so X is persisted but not replayed into cache.
        self.fs.rank_asok(['flush', 'journal'], rank=R_RANK)

        # Widen the rejoin-ack window on every daemon (the standby that takes
        # over R will inherit it).
        self._configure_mds({'mds_debug_rejoin_ack_delay': 30})

        # Fail R; a standby takes over and begins recovery.
        self.fs.rank_fail(rank=R_RANK)

        # Wait until the new R is in up:rejoin and (after a short settle) has
        # had time to fetch and split the invented inode, then make S trim so
        # its cache expire reaches R inside the ack-delay window.
        self.fs.wait_for_state('up:rejoin', rank=R_RANK, timeout=60)
        time.sleep(8)
        log.info("dropping survivor cache to trigger the expire")
        self.fs.rank_asok(['cache', 'drop', '0'], rank=S_RANK)

        # The fix: S survives and the cluster recovers.  Without it, S aborts
        # on isolated_inodes and the framework flags the coredump.
        status = self.fs.wait_for_daemons(timeout=90)
        self.assertTrue(self.fs.rank_is_running(rank=S_RANK, status=status))
        self.assertEqual(len(list(self.fs.get_ranks(status=status))), 3)
        self.assertEqual(len(self.fs.get_damaged(status=status)), 0)

    def _configure_mds(self, conf):
        for k, v in conf.items():
            self.config_set('mds', str(k), str(v))
