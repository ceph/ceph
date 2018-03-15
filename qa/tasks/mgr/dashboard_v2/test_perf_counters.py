# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase, authenticate


class PerfCountersControllerTest(DashboardTestCase):

    @authenticate
    def test_perf_counters_list(self):
        data = self._get('/api/perf_counters')
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        for mon in self.mons():
            self.assertIn('mon.{}'.format(mon), data)

        osds = self.ceph_cluster.mon_manager.get_osd_dump()
        for osd in osds:
            self.assertIn('osd.{}'.format(osd['osd']), data)

    @authenticate
    def test_perf_counters_mon_get(self):
        mon = self.mons()[0]
        data = self._get('/api/perf_counters/mon/{}'.format(mon))
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        self.assertEqual('mon', data['service']['type'])
        self.assertEqual(mon, data['service']['id'])
        self.assertIsInstance(data['counters'], list)
        self.assertGreater(len(data['counters']), 0)
        counter = data['counters'][0]
        self.assertIsInstance(counter, dict)
        self.assertIn('description', counter)
        self.assertIn('name', counter)
        self.assertIn('unit', counter)
        self.assertIn('value', counter)

    @authenticate
    def test_perf_counters_mgr_get(self):
        mgr = self.mgr_cluster.mgr_ids[0]
        data = self._get('/api/perf_counters/mgr/{}'.format(mgr))
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        self.assertEqual('mgr', data['service']['type'])
        self.assertEqual(mgr, data['service']['id'])
        self.assertIsInstance(data['counters'], list)
        self.assertGreater(len(data['counters']), 0)
        counter = data['counters'][0]
        self.assertIsInstance(counter, dict)
        self.assertIn('description', counter)
        self.assertIn('name', counter)
        self.assertIn('unit', counter)
        self.assertIn('value', counter)
