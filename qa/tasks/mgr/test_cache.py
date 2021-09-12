import json

from .mgr_test_case import MgrTestCase

class TestCache(MgrTestCase):

    def setUp(self):
        super(TestCache, self).setUp()
        self.setup_mgrs()
        self._load_module("cli_api")
        self.ttl = 10
        self.enable_cache(self.ttl)

    def tearDown(self):
        self.disable_cache()

    def get_hit_miss_ratio(self):
        perf_dump_command = f"daemon mgr.{self.mgr_cluster.get_active_id()} perf dump"
        perf_dump_res = self.cluster_cmd(perf_dump_command)
        perf_dump = json.loads(perf_dump_res)
        h = perf_dump["mgr"]["cache_hit"]
        m = perf_dump["mgr"]["cache_miss"]
        return int(h), int(m)

    def enable_cache(self, ttl):
        set_ttl = f"config set mgr mgr_ttl_cache_expire_seconds {ttl}"
        self.cluster_cmd(set_ttl)

    def disable_cache(self):
        set_ttl = "config set mgr mgr_ttl_cache_expire_seconds 0"
        self.cluster_cmd(set_ttl)


    def test_init_cache(self):
        get_ttl = "config get mgr mgr_ttl_cache_expire_seconds"
        res = self.cluster_cmd(get_ttl)
        self.assertEquals(int(res), 10)

    def test_health_not_cached(self):
        get_health = "mgr api get health"

        h_start, m_start = self.get_hit_miss_ratio()
        self.cluster_cmd(get_health)
        h, m = self.get_hit_miss_ratio()

        self.assertEquals(h, h_start)
        self.assertEquals(m, m_start)

    def test_osdmap(self):
        get_osdmap = "mgr api get osd_map"

        # store in cache
        self.cluster_cmd(get_osdmap)
        # get from cache
        res = self.cluster_cmd(get_osdmap)
        osd_map = json.loads(res)
        self.assertIn("osds", osd_map)
        self.assertGreater(len(osd_map["osds"]), 0)
        self.assertIn("epoch", osd_map)



    def test_hit_miss_ratio(self):
        get_osdmap = "mgr api get osd_map"

        hit_start, miss_start = self.get_hit_miss_ratio()

        def wait_miss():
            self.cluster_cmd(get_osdmap)
            _, m = self.get_hit_miss_ratio()
            return m == miss_start + 1

        # Miss, add osd_map to cache
        self.wait_until_true(wait_miss, self.ttl + 5)
        h, m = self.get_hit_miss_ratio()
        self.assertEquals(h, hit_start)
        self.assertEquals(m, miss_start+1)

        # Hit, get osd_map from cache
        self.cluster_cmd(get_osdmap)
        h, m = self.get_hit_miss_ratio()
        self.assertEquals(h, hit_start+1)
        self.assertEquals(m, miss_start+1)
