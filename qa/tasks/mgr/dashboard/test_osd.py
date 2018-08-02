# -*- coding: utf-8 -*-

from __future__ import absolute_import

import json
from time import sleep

from .helper import DashboardTestCase, JObj, JAny, JList, JLeaf, JTuple


class OsdTest(DashboardTestCase):

    AUTH_ROLES = ['cluster-manager']

    def tearDown(self):
        self._post('/api/osd/0/mark_in')

    @DashboardTestCase.RunAs('test', 'test', ['block-manager'])
    def test_access_permissions(self):
        self._get('/api/osd')
        self.assertStatus(403)
        self._get('/api/osd/0')
        self.assertStatus(403)

    def assert_in_and_not_none(self, data, properties):
        self.assertSchema(data, JObj({p: JAny(none=False) for p in properties}, allow_unknown=True))

    def test_list(self):
        data = self._get('/api/osd')
        self.assertStatus(200)

        self.assertGreaterEqual(len(data), 1)
        data = data[0]
        self.assert_in_and_not_none(data, ['host', 'tree', 'state', 'stats', 'stats_history'])
        self.assert_in_and_not_none(data['host'], ['name'])
        self.assert_in_and_not_none(data['tree'], ['id'])
        self.assert_in_and_not_none(data['stats'], ['numpg', 'stat_bytes_used', 'stat_bytes',
                                                    'op_r', 'op_w'])
        self.assert_in_and_not_none(data['stats_history'], ['op_out_bytes', 'op_in_bytes'])
        self.assertSchema(data['stats_history']['op_out_bytes'],
                          JList(JTuple([JLeaf(int), JLeaf(float)])))

    def test_details(self):
        data = self._get('/api/osd/0')
        self.assertStatus(200)
        self.assert_in_and_not_none(data, ['osd_metadata', 'histogram'])
        self.assert_in_and_not_none(data['histogram'], ['osd'])
        self.assert_in_and_not_none(data['histogram']['osd'], ['op_w_latency_in_bytes_histogram',
                                                               'op_r_latency_out_bytes_histogram'])

    def test_scrub(self):
        self._post('/api/osd/0/scrub?deep=False')
        self.assertStatus(200)

        self._post('/api/osd/0/scrub?deep=True')
        self.assertStatus(200)

    def test_mark_out_and_in(self):
        self._post('/api/osd/0/mark_out')
        self.assertStatus(200)

        self._post('/api/osd/0/mark_in')
        self.assertStatus(200)

    def test_mark_down(self):
        self._post('/api/osd/0/mark_down')
        self.assertStatus(200)

    def test_reweight(self):
        self._post('/api/osd/0/reweight', {'weight': 0.4})
        self.assertStatus(200)

        def get_reweight_value():
            self._get('/api/osd/0')
            response = self.jsonBody()
            if 'osd_map' in response and 'weight' in response['osd_map']:
                return round(response['osd_map']['weight'], 1)
        self.wait_until_equal(get_reweight_value, 0.4, 10)
        self.assertStatus(200)

        # Undo
        self._post('/api/osd/0/reweight', {'weight': 1})

    def test_create_lost_destroy_remove(self):
        # Create
        self._post('/api/osd', {
            'uuid': 'f860ca2e-757d-48ce-b74a-87052cad563f',
            'svc_id': 5
        })
        self.assertStatus(201)
        # Lost
        self._post('/api/osd/5/mark_lost')
        self.assertStatus(200)
        # Destroy
        self._post('/api/osd/5/destroy')
        self.assertStatus(200)
        # Remove
        self._post('/api/osd/5/remove')
        self.assertStatus(200)

    def test_safe_to_destroy(self):
        self._get('/api/osd/5/safe_to_destroy')
        self.assertStatus(200)
        self.assertJsonBody({'safe-to-destroy': True})

        def get_destroy_status():
            self._get('/api/osd/0/safe_to_destroy')
            return self.jsonBody()['safe-to-destroy']
        self.wait_until_equal(get_destroy_status, False, 10)
        self.assertStatus(200)


class OsdFlagsTest(DashboardTestCase):
    def __init__(self, *args, **kwargs):
        super(OsdFlagsTest, self).__init__(*args, **kwargs)
        self._initial_flags = sorted(  # These flags cannot be unset
            ['sortbitwise', 'recovery_deletes', 'purged_snapdirs'])

    @classmethod
    def _get_cluster_osd_flags(cls):
        return sorted(
            json.loads(cls._ceph_cmd(['osd', 'dump',
                                      '--format=json']))['flags']).split(',')

    @classmethod
    def _put_flags(cls, flags):
        cls._put('/api/osd/flags', data={'flags': flags})
        return sorted(cls._resp.json())

    def test_list_osd_flags(self):
        flags = self._get('/api/osd/flags')
        self.assertStatus(200)
        self.assertEqual(len(flags), 3)
        self.assertEqual(sorted(flags), self._initial_flags)

    def test_add_osd_flag(self):
        flags = self._put_flags([
            'sortbitwise', 'recovery_deletes', 'purged_snapdirs', 'noout',
            'pause'
        ])
        self.assertEqual(flags, sorted([
            'sortbitwise', 'recovery_deletes', 'purged_snapdirs', 'noout',
            'pause'
        ]))

        # Restore flags
        self._put_flags(self._initial_flags)
