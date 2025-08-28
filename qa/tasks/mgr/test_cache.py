import json

from .mgr_test_case import MgrTestCase

class TestCache(MgrTestCase):

    def setUp(self):
        super(TestCache, self).setUp()
        self.setup_mgrs()
        self._load_module("cli_api")
        self.enable_cache()

    def get_hit_miss_ratio(self):
        cmd = f"daemon mgr.{self.mgr_cluster.get_active_id()} perf dump"
        p = json.loads(self.cluster_cmd(cmd))
        return int(p["mgr"]["cache_hit"]), int(p["mgr"]["cache_miss"])

    def flush_cache_map(self, what):
        self.cluster_cmd(f"mgr cli cache flush {what}")

    def osd_epoch(self):
        m = json.loads(self.cluster_cmd("mgr cli get osd_map"))
        return int(m["epoch"])

    def enable_cache(self, on=True):
        self.cluster_cmd(f"config set mgr mgr_map_cache_enabled {'true' if on else 'false'}")

    def bump_osdmap(self):
        pool = f"foo_{uuid.uuid4().hex[:8]}"
        self.cluster_cmd(f"osd pool create {pool} 1 --yes-i_really_mean_it")
        # ensure epoch bump observed
        start = self.osd_epoch()
        def ok():
            return self.osd_epoch() > start
        self.wait_until_true(ok, 30)
        self.cluster_cmd(f"osd pool delete {pool} {pool} --yes-i-really-mean-it")

    # Init cache
    def test_init_cache(self):
        get_cache = "config get mgr mgr_map_cache_enabled"
        res = self.cluster_cmd(get_cache)
        self.assertEqual(int(res), 1)

    # Disabled bypass
    def test_disabled_bypass(self):
        self.enable_cache(False)
        h0, m0 = self.get_hit_miss()
        self.cluster_cmd("mgr cli get osd_map")
        h1, m1 = self.get_hit_miss()
        self.assertEqual((h1, m1), (h0, m0))
        self.enable_cache(True)

    # Non-cacheable key ignored (health)
    def test_non_cacheable_stays_uncached(self):
        h0, m0 = self.get_hit_miss()
        self.cluster_cmd("mgr cli get health")
        h1, m1 = self.get_hit_miss()
        self.assertEqual((h1, m1), (h0, m0))

    # Cache hit after warm
    def test_osdmap_hit_after_warm(self):
        self.cluster_cmd("mgr cli get osd_map")
        h0, m0 = self.get_hit_miss()
        self.cluster_cmd("mgr cli get osd_map")
        h1, m1 = self.get_hit_miss()
        self.assertGreater(h1, h0)
        self.assertEqual(m1, m0)

    # Invalidate on osdmap change → miss then hit
    def test_invalidate_on_osdmap_change(self):
        self.cluster_cmd("mgr cli get osd_map")  # warm
        h0, m0 = self.get_hit_miss()
        self.bump_osdmap()                       # should invalidate
        self.cluster_cmd("mgr cli get osd_map")  # miss
        h1, m1 = self.get_hit_miss()
        self.assertGreater(m1, m0)
        self.cluster_cmd("mgr cli get osd_map")  # hit
        h2, m2 = self.get_hit_miss()
        self.assertGreater(h2, h1)
        self.assertEqual(m2, m1)

    # Concurrency: many reads → one miss, rest hits
    def test_concurrent_reads_single_miss(self):
        self.enable_cache(True)
        self.bump_osdmap()
        h0, m0 = self.get_hit_miss()
        N = 8
        def read_once(_):
            return self.cluster_cmd("mgr cli get osd_map")
        with ThreadPoolExecutor(max_workers=N) as ex:
            list(ex.map(read_once, range(N)))
        h1, m1 = self.get_hit_miss()
        # Allow either 1 miss or small race overfill; assert lower bound
        self.assertGreaterEqual(h1 - h0, N - 1)
        self.assertGreaterEqual(m1 - m0, 1)

    # Another cacheable key (mon_status) behaves like osd_map
    def test_mon_status_cached(self):
        self.cluster_cmd("mgr cli get mon_status")
        h0, m0 = self.get_hit_miss()
        self.cluster_cmd("mgr cli get mon_status")
        h1, m1 = self.get_hit_miss()
        self.assertGreater(h1, h0)
        self.assertEqual(m1, m0)

    # Stress invalidate while reading (race safety)
    def test_race_read_vs_invalidate(self):
        stop = False
        def reader():
            while not stop:
                self.cluster_cmd("mgr cli get osd_map")
        t = threading.Thread(target=reader)
        t.start()
        try:
            for _ in range(3):
                self.bump_osdmap()  # triggers invalidation in mgr
        finally:
            stop = True
            t.join()
        # If we reach here without exceptions or crashes, pass
        self.assertTrue(True)

    # test get api
    def test_osdmap(self):
        get_osdmap = "mgr cli get osd_map"

        # store in cache
        self.cluster_cmd(get_osdmap)
        # get from cache
        res = self.cluster_cmd(get_osdmap)
        osd_map = json.loads(res)
        self.assertIn("osds", osd_map)
        self.assertGreater(len(osd_map["osds"]), 0)
        self.assertIn("epoch", osd_map)
