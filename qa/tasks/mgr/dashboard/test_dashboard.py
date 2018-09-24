# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase


class DashboardTest(DashboardTestCase):
    CEPHFS = True

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
        self.assertIn('scrub_status', data)
        self.assertIn('pg_info', data)
        self.assertIn('client_perf', data)
        self.assertIn('hosts', data)
        self.assertIn('rgw', data)
        self.assertIn('iscsi_daemons', data)
        self.assertIsNotNone(data['health'])
        self.assertIsNotNone(data['mon_status'])
        self.assertIsNotNone(data['fs_map'])
        self.assertIsNotNone(data['osd_map'])
        self.assertIsNotNone(data['clog'])
        self.assertIsNotNone(data['audit_log'])
        self.assertIsNotNone(data['pools'])
        self.assertIsNotNone(data['scrub_status'])
        self.assertIsNotNone(data['pg_info'])
        self.assertIsNotNone(data['client_perf'])
        self.assertIsNotNone(data['hosts'])
        self.assertIsNotNone(data['rgw'])
        self.assertIsNotNone(data['iscsi_daemons'])

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data['pools']))
        for pool in data['pools']:
            self.assertIn(pool['pool_name'], cluster_pools)

        self.assertIsNotNone(data['mgr_map'])
        self.assertIsNotNone(data['df'])


    @DashboardTestCase.RunAs('test', 'test', ['pool-manager'])
    def test_health_permissions(self):
        data = self._get("/api/dashboard/health")
        self.assertStatus(200)

        self.assertIn('health', data)
        self.assertNotIn('mon_status', data)
        self.assertNotIn('fs_map', data)
        self.assertNotIn('osd_map', data)
        self.assertNotIn('clog', data)
        self.assertNotIn('audit_log', data)
        self.assertIn('pools', data)
        self.assertNotIn('mgr_map', data)
        self.assertIn('df', data)
        self.assertNotIn('scrub_status', data)
        self.assertNotIn('pg_info', data)
        self.assertIn('client_perf', data)
        self.assertNotIn('hosts', data)
        self.assertNotIn('rgw', data)
        self.assertNotIn('iscsi_daemons', data)
        self.assertIsNotNone(data['health'])
        self.assertIsNotNone(data['pools'])
        self.assertIsNotNone(data['client_perf'])

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data['pools']))
        for pool in data['pools']:
            self.assertIn(pool['pool_name'], cluster_pools)

        self.assertIsNotNone(data['df'])
