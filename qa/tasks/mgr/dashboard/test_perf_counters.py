# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase, JObj


class PerfCountersControllerTest(DashboardTestCase):

    def test_perf_counters_list(self):
        data = self._get('/api/perf_counters')
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        for mon in self.mons():
            self.assertIn('mon.{}'.format(mon), data)

        osds = self.ceph_cluster.mon_manager.get_osd_dump()
        for osd in osds:
            self.assertIn('osd.{}'.format(osd['osd']), data)

    def _validate_perf(self, srv_id, srv_type, data, allow_empty):
        self.assertIsInstance(data, dict)
        self.assertEqual(srv_type, data['service']['type'])
        self.assertEqual(str(srv_id), data['service']['id'])
        self.assertIsInstance(data['counters'], list)
        if not allow_empty:
            self.assertGreater(len(data['counters']), 0)
        for counter in data['counters'][0:1]:
            self.assertIsInstance(counter, dict)
            self.assertIn('description', counter)
            self.assertIn('name', counter)
            self.assertIn('unit', counter)
            self.assertIn('value', counter)

    def test_perf_counters_mon_get(self):
        mon = self.mons()[0]
        data = self._get('/api/perf_counters/mon/{}'.format(mon))
        self.assertStatus(200)
        self._validate_perf(mon, 'mon', data, allow_empty=False)

    def test_perf_counters_mgr_get(self):
        mgr = list(self.mgr_cluster.mgr_ids)[0]
        data = self._get('/api/perf_counters/mgr/{}'.format(mgr))
        self.assertStatus(200)
        self._validate_perf(mgr, 'mgr', data, allow_empty=False)

    def test_perf_counters_mds_get(self):
        for mds in self.mds_cluster.mds_ids:
            data = self._get('/api/perf_counters/mds/{}'.format(mds))
            self.assertStatus(200)
            self._validate_perf(mds, 'mds', data, allow_empty=True)

    def test_perf_counters_osd_get(self):
        for osd in self.ceph_cluster.mon_manager.get_osd_dump():
            osd = osd['osd']
            data = self._get('/api/perf_counters/osd/{}'.format(osd))
            self.assertStatus(200)
            self._validate_perf(osd, 'osd', data, allow_empty=False)

    def test_perf_counters_not_found(self):
        osds = self.ceph_cluster.mon_manager.get_osd_dump()
        unused_id = int(list(map(lambda o: o['osd'], osds)).pop()) + 1

        self._get('/api/perf_counters/osd/{}'.format(unused_id))
        self.assertStatus(404)
        schema = JObj(sub_elems={
            'status': str,
            'detail': str,
        }, allow_unknown=True)
        self.assertEqual(self._resp.json()['detail'], "'osd.{}' not found".format(unused_id))
        self.assertSchemaBody(schema)
