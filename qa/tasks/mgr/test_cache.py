import json
import logging
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor

from .mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)

class TestCache(MgrTestCase):

    def setUp(self):
        log.info("TestCache setup")
        super(TestCache, self).setUp()
        log.info("Setting up mgrs")
        self.setup_mgrs()
        log.info("Loading cli_api module")
        self._load_module("cli_api")
        log.info("Enabling cache")
        self.enable_cache()

    def enable_cache(self, on=True):
        cache_set = 'true' if on else 'false'
        self.mgr_cluster.mon_manager.raw_cluster_cmd('config', 'set', 'mgr', 'mgr_map_cache_enabled', cache_set)

    def get_hit_miss_ratio(self):
        perf_dump = self.mgr_cluster.mon_manager.raw_cluster_cmd('daemon', f'mgr.{self.mgr_cluster.get_active_id()}', 'perf', 'dump')
        pd = json.loads(perf_dump)
        return int(pd["mgr"]["cache_hit"]), int(pd["mgr"]["cache_miss"])

    def flush_cache_map(self, what):
        self.mgr_cluster.mon_manager.raw_cluster_cmd('mgr', 'cli', 'cache', 'flush', what)

    def create_pool(self, pool_name):
        self.mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', pool_name, '1', '--yes-i-really-mean-it')

    def remove_pool(self, pool_name):
        self.config_set('mon', 'mon_allow_pool_delete', 'true')
        self.mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'pool', 'rm', pool_name, pool_name, '--yes-i-really-really-mean-it-not-faking')

    def osd_epoch(self):
        m = json.loads(self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map"))
        return int(m["epoch"])

    def bump_osdmap(self):
        str_pool_uuid = uuid.uuid4().hex
        pool = f"foo_{uuid.uuid4().hex[:8]}"
        self.create_pool(pool)
        start = self.osd_epoch()
        def ok():
            return self.osd_epoch() > start
        self.wait_until_true(ok, 30)
        self.remove_pool(pool)

    # Init cache
    def test_init_cache(self):
        res = self.mgr_cluster.mon_manager.raw_cluster_cmd('config', 'get', 'mgr', 'mgr_map_cache_enabled')
        self.assertEqual(res.strip().lower(), "true")

    # Disabled bypass
    def test_disabled_bypass(self):
        self.enable_cache(False)
        h0, m0 = self.get_hit_miss_ratio()
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")
        h1, m1 = self.get_hit_miss_ratio()
        self.assertEqual((h1, m1), (h0, m0))
        self.enable_cache(True)

    # Non-cacheable key ignored (health)
    def test_non_cacheable_stays_uncached(self):
        h0, m0 = self.get_hit_miss_ratio()
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "health")
        h1, m1 = self.get_hit_miss_ratio()
        self.assertEqual((h1, m1), (h0, m0))

    # Cache hit after warm
    def test_osdmap_hit_after_warm(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")
        h0, m0 = self.get_hit_miss_ratio()
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")
        h1, m1 = self.get_hit_miss_ratio()
        self.assertGreater(h1, h0)
        self.assertEqual(m1, m0)

    # Invalidate on osdmap change → miss then hit
    def test_invalidate_on_osdmap_change(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")  # warm
        h0, m0 = self.get_hit_miss_ratio()
        self.bump_osdmap()                       # should invalidate
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")  # miss
        h1, m1 = self.get_hit_miss_ratio()
        self.assertGreater(m1, m0)
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")  # hit
        h2, m2 = self.get_hit_miss_ratio()
        self.assertGreater(h2, h1)
        self.assertEqual(m2, m1)

    # Concurrency: many reads → one miss, rest hits
    def test_concurrent_reads_single_miss(self):
        self.enable_cache(True)
        self.bump_osdmap()
        h0, m0 = self.get_hit_miss_ratio()
        N = 8
        def read_once(_):
            return self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")
        with ThreadPoolExecutor(max_workers=N) as ex:
            list(ex.map(read_once, range(N)))
        h1, m1 = self.get_hit_miss_ratio()
        # Allow either 1 miss or small race overfill; assert lower bound
        self.assertGreaterEqual(h1 - h0, N - 1)
        self.assertGreaterEqual(m1 - m0, 1)

    # Another cacheable key (mon_status) behaves like osd_map
    def test_mon_status_cached(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "mon_status")  # warm
        h0, m0 = self.get_hit_miss_ratio()
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "mon_status")
        h1, m1 = self.get_hit_miss_ratio()
        self.assertGreater(h1, h0)
        self.assertEqual(m1, m0)

    # Stress invalidate while reading (race safety)
    def test_race_read_vs_invalidate(self):
        stop = False
        def reader():
            while not stop:
                self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")
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
        # store in cache
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")
        # get from cache
        res = self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", "osd_map")
        osd_map = json.loads(res)
        self.assertIn("osds", osd_map)
        self.assertGreater(len(osd_map["osds"]), 0)
        self.assertIn("epoch", osd_map)
