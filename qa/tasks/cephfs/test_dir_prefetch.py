from io import StringIO
from logging import getLogger

from tasks.ceph_test_case import TestTimeoutError
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = getLogger(__name__)


class TestDirPrefetchBackend(CephFSTestCase):
    """
    The backend dirfrag prefetch (mds_dir_prefetch_backend).

    With mds_dir_prefetch=false a lookup that misses the cache fetches only
    the key it wants. The backend prefetch additionally launches a full
    fetch of the dirfrag in the background, once the dirfrag has taken
    mds_dir_prefetch_backend_hit_threshold keyed-fetch hits.
    """

    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1

    FILES = 50
    HIT_THRESHOLD = 4

    def setUp(self):
        super().setUp()
        self.config_set('mds', 'mds_dir_prefetch', 'false')
        self.config_set('mds', 'mds_dir_prefetch_backend', 'true')
        self.config_set('mds', 'mds_dir_prefetch_backend_max', '1')
        self.config_set('mds', 'mds_dir_prefetch_backend_hit_threshold',
                        str(self.HIT_THRESHOLD))

    def tearDown(self):
        for key in ('mds_dir_prefetch',
                    'mds_dir_prefetch_backend',
                    'mds_dir_prefetch_backend_max',
                    'mds_dir_prefetch_backend_hit_threshold',
                    'mds_cache_memory_limit'):
            self.config_rm('mds', key)
        super().tearDown()

    def _background_fetches(self):
        perf = self.fs.rank_asok(['perf', 'dump', 'mds'])
        return perf['mds']['dir_fetch_background']

    def _is_complete(self, path):
        dirs = self.fs.rank_tell(['dump', 'dir', path])
        return 'complete' in dirs[0]['states']

    def _populate(self, path):
        self.mount_a.run_shell_payload(f"""
set -e
mkdir -p {path}
cd {path}
seq 1 {self.FILES} | sed 's/^/f/' | xargs -r touch
touch keep
""")
        # Commit the dirfrag so that a fetch has an object to read.
        self.fs.rank_asok(['flush', 'journal'])

    def _cold_dir(self, path):
        """
        Leave the dirfrag of path in the MDS cache but incomplete.

        mount_b holds path/keep open, which pins the dirfrag, and the cache
        drop trims every other dentry in it. mount_a is remounted first so
        it holds no caps on the files it created, and so that its lookups
        go to the MDS.
        """
        self.mount_a.umount_wait()
        self.mount_a.mount_wait()
        self.fs.rank_tell(['cache', 'drop', '30'])
        self.assertFalse(self._is_complete(path),
                         f"{path} is still complete after the cache drop")

    def _hold_open(self, path):
        self.mount_b.open_background(f"{path}/keep", write=False)

    def _lookup(self, path):
        # stat an entry that is not in the MDS cache: one keyed fetch
        self.mount_a.stat(path)

    def test_hit_count_reset_after_trim(self):
        """
        That a dirfrag that lost its complete state to trimming takes
        mds_dir_prefetch_backend_hit_threshold new hits before another
        background fetch, rather than refetching on the very next miss.
        """
        self._populate("d")
        self._hold_open("d")
        self._cold_dir("d")

        n = 0
        for rnd in range(2):
            before = self._background_fetches()
            for _ in range(self.HIT_THRESHOLD - 1):
                n += 1
                self._lookup(f"d/f{n}")
            self.assertEqual(self._background_fetches(), before,
                             f"round {rnd}: background fetch launched "
                             f"before {self.HIT_THRESHOLD} hits")

            n += 1
            self._lookup(f"d/f{n}")
            self.wait_until_equal(self._background_fetches, before + 1,
                                  timeout=30)
            self.wait_until_true(lambda: self._is_complete("d"), timeout=30)

            # Trim the dirfrag back to incomplete for the next round.
            self._cold_dir("d")

    def test_no_prefetch_when_cache_too_full(self):
        """
        That no background fetch is launched while the MDS cache is over
        its reservation, and that one is once the cache has room again.
        """
        self.config_set('mds', 'mds_dir_prefetch_backend_hit_threshold', '1')

        self._populate("d")
        self._hold_open("d")
        self._cold_dir("d")

        # Far below what the cache already holds, so cache_toofull().
        self.config_set('mds', 'mds_cache_memory_limit', '16384')
        before = self._background_fetches()
        for i in range(1, 5):
            self._lookup(f"d/f{i}")
        self.assertEqual(self._background_fetches(), before,
                         "background fetch launched while the cache is "
                         "too full")
        self.assertFalse(self._is_complete("d"))

        self.config_rm('mds', 'mds_cache_memory_limit')
        self._lookup("d/f5")
        self.wait_until_equal(self._background_fetches, before + 1,
                              timeout=30)
        self.wait_until_true(lambda: self._is_complete("d"), timeout=30)

    def test_fetch_failure(self):
        """
        That a failed dirfrag fetch fails the lookup waiting on it with EIO,
        rather than leaving it hung, and releases its background fetch
        slot so that other dirfrags can still be prefetched.
        """
        self.config_set('mds', 'mds_dir_prefetch_backend_hit_threshold', '1')

        self._populate("bad")
        self._populate("good")
        bad_ino = self.mount_a.path_to_ino("bad")
        # Keep both dirfrags, and their dentries in the root, in the cache,
        # so that no fetch of the root takes the single background slot.
        self._hold_open("bad")
        self._hold_open("good")
        self._cold_dir("bad")
        self.assertFalse(self._is_complete("good"))

        # With a threshold of 1 the first lookup issues both a keyed fetch
        # and a background full fetch of the missing object.
        self.fs.radosm(["rm", "{0:x}.00000000".format(bad_ino)])

        # A hung lookup cannot be killed from the client side: the request
        # is already with ceph-fuse and only the daemon going away ends it.
        p = self.mount_a.run_shell(["stat", "bad/f1"], wait=False,
                                   check_status=False, stderr=StringIO())
        try:
            self.wait_until_true(lambda: p.finished, timeout=60, period=1)
        except TestTimeoutError:
            self.mount_a.umount_wait(force=True)
            self.fail("lookup in the damaged dirfrag hung")
        self.assertNotEqual(p.exitstatus, 0,
                            "lookup in the damaged dirfrag succeeded")
        self.assertIn("Input/output error", p.stderr.getvalue())

        damage = self.fs.rank_tell(['damage', 'ls'])
        self.assertTrue(any(d['damage_type'] == 'dir_frag' and
                            d['ino'] == bad_ino for d in damage),
                        f"no dir_frag damage recorded for {bad_ino:x}: "
                        f"{damage}")

        # mds_dir_prefetch_backend_max is 1: a leaked slot would stop this.
        before = self._background_fetches()
        self._lookup("good/f1")
        self.wait_until_equal(self._background_fetches, before + 1,
                              timeout=30)
        self.wait_until_true(lambda: self._is_complete("good"), timeout=30)

        for d in damage:
            self.fs.rank_tell(['damage', 'rm', str(d['id'])])
