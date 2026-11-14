import json
import logging
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor

from .mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)

class TestCache(MgrTestCase):
    # cache_hit/cache_miss are mgr-wide and other mgr modules read cached
    # keys in the background, so checks that need an exact count retry
    # until they get a window nobody else touched.
    RETRY_TIMEOUT = 60

    def setUp(self):
        log.info("TestCache setup")
        super(TestCache, self).setUp()
        log.info("Setting up mgrs")
        self.setup_mgrs()
        log.info("Loading cli_api module")
        self._load_module("cli_api")
        self.config_set('mon', 'mon_allow_pool_delete', 'true')
        log.info("Enabling cache")
        self.enable_cache()

    def mgr_asok(self, *cmd):
        # run on the host of the active mgr, its admin socket is local
        mgr_id = self.mgr_cluster.get_active_id()
        proc = self.mgr_cluster.mon_manager.admin_socket('mgr', mgr_id, list(cmd))
        return json.loads(proc.stdout.getvalue())

    def cache_enabled(self):
        res = self.mgr_asok('config', 'get', 'mgr_map_cache_enabled')
        return res['mgr_map_cache_enabled'] == 'true'

    def enable_cache(self, on=True):
        cache_set = 'true' if on else 'false'
        self.mgr_cluster.mon_manager.raw_cluster_cmd('config', 'set', 'mgr', 'mgr_map_cache_enabled', cache_set)
        # config set returns before the mgr applies the change
        self.wait_until_true(lambda: self.cache_enabled() == on, 30, period=1)

    def get_hit_miss_ratio(self):
        pd = self.mgr_asok('perf', 'dump')
        return int(pd["mgr"]["cache_hit"]), int(pd["mgr"]["cache_miss"])

    def get_map(self, what):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "cli", "get", what)

    def flush_cache_map(self, what):
        # fails with EINVAL if the key is not cached, which is fine here
        self.mgr_cluster.mon_manager.raw_cluster_cmd_result('mgr', 'cli', 'cache', 'flush', what)

    def create_pool(self, pool_name):
        self.mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'pool', 'create', pool_name, '1', '--yes-i-really-mean-it')

    def remove_pool(self, pool_name):
        self.mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'pool', 'rm', pool_name, pool_name, '--yes-i-really-really-mean-it-not-faking')

    def osd_epoch(self):
        return int(json.loads(self.get_map("osd_map"))["epoch"])

    def bump_osdmap(self):
        pool = f"foo_{uuid.uuid4().hex[:8]}"
        self.create_pool(pool)
        self.remove_pool(pool)
        # wait until the mgr serves the epoch of the pool removal, so no
        # invalidation from this call is still in flight when we return
        target = int(json.loads(self.mgr_cluster.mon_manager.raw_cluster_cmd(
            'osd', 'dump', '--format=json'))['epoch'])
        self.wait_until_true(lambda: self.osd_epoch() >= target, 30, period=1)

    def assert_hit_after_warm(self, what):
        def check():
            self.get_map(what)  # warm
            h0, m0 = self.get_hit_miss_ratio()
            self.get_map(what)
            h1, m1 = self.get_hit_miss_ratio()
            log.debug(f"{what}: hit {h0}->{h1} miss {m0}->{m1}")
            return h1 > h0 and m1 == m0
        self.wait_until_true(check, self.RETRY_TIMEOUT, period=1)

    # Init cache
    def test_init_cache(self):
        self.assertTrue(self.cache_enabled())

    # Disabled bypass
    def test_disabled_bypass(self):
        self.enable_cache(False)
        h0, m0 = self.get_hit_miss_ratio()
        self.get_map("osd_map")
        h1, m1 = self.get_hit_miss_ratio()
        self.assertEqual((h1, m1), (h0, m0))

    # Non-cacheable key ignored (health)
    def test_non_cacheable_stays_uncached(self):
        def check():
            h0, m0 = self.get_hit_miss_ratio()
            self.get_map("health")
            return self.get_hit_miss_ratio() == (h0, m0)
        self.wait_until_true(check, self.RETRY_TIMEOUT, period=1)

    # Cache hit after warm
    def test_osdmap_hit_after_warm(self):
        self.assert_hit_after_warm("osd_map")

    # Invalidate on osdmap change → miss then hit
    def test_invalidate_on_osdmap_change(self):
        e0 = self.osd_epoch()  # warm
        h0, m0 = self.get_hit_miss_ratio()
        # times out if the cache keeps serving the old map
        self.bump_osdmap()
        h1, m1 = self.get_hit_miss_ratio()
        self.assertGreater(self.osd_epoch(), e0)
        self.assertGreater(m1, m0)
        self.assert_hit_after_warm("osd_map")

    # Concurrency: many reads after a flush, at least one miss and every
    # read is counted as a hit or a miss
    def test_concurrent_reads_single_miss(self):
        N = 8
        def read_once(_):
            return json.loads(self.get_map("osd_map"))
        def check():
            self.get_map("osd_map")  # make sure there is something to flush
            self.flush_cache_map("osd_map")
            h0, m0 = self.get_hit_miss_ratio()
            with ThreadPoolExecutor(max_workers=N) as ex:
                maps = list(ex.map(read_once, range(N)))
            h1, m1 = self.get_hit_miss_ratio()
            log.debug(f"osd_map: hit {h0}->{h1} miss {m0}->{m1}")
            for m in maps:
                self.assertIn("epoch", m)
            # concurrent misses are not merged, so several reads may miss
            return m1 - m0 >= 1 and (h1 - h0) + (m1 - m0) >= N
        self.wait_until_true(check, self.RETRY_TIMEOUT, period=1)

    # Another cacheable key (mon_status) behaves like osd_map, it is
    # invalidated on every mgr digest so this relies on the retry
    def test_mon_status_cached(self):
        self.assert_hit_after_warm("mon_status")

    # Stress invalidate while reading (race safety)
    def test_race_read_vs_invalidate(self):
        gid = self.mgr_cluster.get_active_gid()
        stop = threading.Event()
        errors = []
        def reader():
            while not stop.is_set():
                try:
                    self.assertIn("epoch", json.loads(self.get_map("osd_map")))
                except Exception as e:
                    errors.append(e)
                    return
        t = threading.Thread(target=reader)
        t.start()
        try:
            for _ in range(3):
                self.bump_osdmap()  # triggers invalidation in mgr
        finally:
            stop.set()
            t.join()
        self.assertEqual(errors, [])
        # a crash would have failed over to another mgr
        self.assertEqual(self.mgr_cluster.get_active_gid(), gid)

    # test get api
    def test_osdmap(self):
        res = self.get_map("osd_map")
        osd_map = json.loads(res)
        self.assertIn("osds", osd_map)
        self.assertGreater(len(osd_map["osds"]), 0)
        self.assertIn("epoch", osd_map)
