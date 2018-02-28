# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase, authenticate


class DashboardTest(DashboardTestCase):
    CEPHFS = True

    @authenticate
    def test_health(self):
        data = self._get("/api/dashboard/health")
        self.assertStatus(200)

        self.assertIn('health', data)
        self.assertIn('mon_status', data)
        self.assertIn('fs_map', data)
        self.assertIn('osd_map', data)
        self.assertIn('clog', data)
        self.assertIn('audit_log', data)
        self.assertIn('pools', data)
        self.assertIn('mgr_map', data)
        self.assertIn('df', data)
        self.assertIsNotNone(data['health'])
        self.assertIsNotNone(data['mon_status'])
        self.assertIsNotNone(data['fs_map'])
        self.assertIsNotNone(data['osd_map'])
        self.assertIsNotNone(data['clog'])
        self.assertIsNotNone(data['audit_log'])
        self.assertIsNotNone(data['pools'])

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data['pools']))
        for pool in data['pools']:
            self.assertIn(pool['pool_name'], cluster_pools)

        self.assertIsNotNone(data['mgr_map'])
        self.assertIsNotNone(data['df'])
