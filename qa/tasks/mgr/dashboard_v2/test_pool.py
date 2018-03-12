# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase, authenticate


class DashboardTest(DashboardTestCase):
    @authenticate
    def test_pool_list(self):
        data = self._get("/api/pool")
        self.assertStatus(200)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data))
        for pool in data:
            self.assertIn('pool_name', pool)
            self.assertIn('type', pool)
            self.assertIn('flags', pool)
            self.assertIn('flags_names', pool)
            self.assertNotIn('stats', pool)
            self.assertIn(pool['pool_name'], cluster_pools)

    @authenticate
    def test_pool_list_attrs(self):
        data = self._get("/api/pool?attrs=type,flags")
        self.assertStatus(200)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data))
        for pool in data:
            self.assertIn('pool_name', pool)
            self.assertIn('type', pool)
            self.assertIn('flags', pool)
            self.assertNotIn('flags_names', pool)
            self.assertNotIn('stats', pool)
            self.assertIn(pool['pool_name'], cluster_pools)

    @authenticate
    def test_pool_list_stats(self):
        data = self._get("/api/pool?stats=true")
        self.assertStatus(200)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data))
        for pool in data:
            self.assertIn('pool_name', pool)
            self.assertIn('type', pool)
            self.assertIn('flags', pool)
            self.assertIn('stats', pool)
            self.assertIn('flags_names', pool)
            self.assertIn(pool['pool_name'], cluster_pools)

    @authenticate
    def test_pool_get(self):
        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        pool = self._get("/api/pool/{}?stats=true&attrs=type,flags,stats"
                         .format(cluster_pools[0]))
        self.assertEqual(pool['pool_name'], cluster_pools[0])
        self.assertIn('type', pool)
        self.assertIn('flags', pool)
        self.assertIn('stats', pool)
        self.assertNotIn('flags_names', pool)
