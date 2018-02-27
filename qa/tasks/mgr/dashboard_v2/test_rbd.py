# -*- coding: utf-8 -*-

from __future__ import absolute_import

import unittest

from .helper import DashboardTestCase, authenticate


class RbdTest(DashboardTestCase):

    @classmethod
    def setUpClass(cls):
        super(RbdTest, cls).setUpClass()
        cls._ceph_cmd(['osd', 'pool', 'create', 'rbd', '100', '100'])
        cls._ceph_cmd(['osd', 'pool', 'application', 'enable', 'rbd', 'rbd'])
        cls._rbd_cmd(['create', '--size=1G', 'img1'])
        cls._rbd_cmd(['create', '--size=2G', 'img2'])

    @classmethod
    def tearDownClass(cls):
        super(RbdTest, cls).tearDownClass()
        cls._ceph_cmd(['osd', 'pool', 'delete', 'rbd', 'rbd', '--yes-i-really-really-mean-it'])

    @authenticate
    def test_list(self):
        data = self._get('/api/rbd/rbd')
        self.assertStatus(200)

        img1 = data['value'][0]
        self.assertEqual(img1['name'], 'img1')
        self.assertEqual(img1['size'], 1073741824)
        self.assertEqual(img1['num_objs'], 256)
        self.assertEqual(img1['obj_size'], 4194304)
        self.assertEqual(img1['features_name'],
                         'deep-flatten, exclusive-lock, fast-diff, layering, object-map')

        img2 = data['value'][1]
        self.assertEqual(img2['name'], 'img2')
        self.assertEqual(img2['size'], 2147483648)
        self.assertEqual(img2['num_objs'], 512)
        self.assertEqual(img2['obj_size'], 4194304)
        self.assertEqual(img2['features_name'],
                         'deep-flatten, exclusive-lock, fast-diff, layering, object-map')

    @authenticate
    def test_create(self):
        rbd_name = 'test_rbd'
        data = {'pool_name': 'rbd',
                'name': rbd_name,
                'size': 10240}
        self._post('/api/rbd', data)
        self.assertStatus(201)
        self.assertJsonBody({"success": True})

        # TODO: change to GET the specific RBD instead of the list as soon as it is available?
        get_res = self._get('/api/rbd/rbd')
        self.assertStatus(200)

        for rbd in get_res['value']:
            if rbd['name'] == rbd_name:
                self.assertEqual(rbd['size'], 10240)
                self.assertEqual(rbd['num_objs'], 1)
                self.assertEqual(rbd['obj_size'], 4194304)
                self.assertEqual(rbd['features_name'],
                                 'deep-flatten, exclusive-lock, fast-diff, layering, object-map')
                break

    # TODO: Re-enable this test for bluestore cluster by figuring out how to skip none-bluestore
    # ones automatically
    @unittest.skip("requires bluestore cluster")
    @authenticate
    def test_create_rbd_in_data_pool(self):
        self._ceph_cmd(['osd', 'pool', 'create', 'data_pool', '12', '12', 'erasure'])
        self._ceph_cmd(['osd', 'pool', 'application', 'enable', 'data_pool', 'rbd'])
        self._ceph_cmd(['osd', 'pool', 'set', 'data_pool', 'allow_ec_overwrites', 'true'])

        rbd_name = 'test_rbd_in_data_pool'
        data = {'pool_name': 'rbd',
                'name': rbd_name,
                'size': 10240,
                'data_pool': 'data_pool'}
        self._post('/api/rbd', data)
        self.assertStatus(201)
        self.assertJsonBody({"success": True})

        # TODO: possibly change to GET the specific RBD (see above)
        get_res = self._get('/api/rbd/rbd')
        self.assertStatus(200)

        for rbd in get_res['value']:
            if rbd['name'] == rbd_name:
                self.assertEqual(rbd['size'], 10240)
                self.assertEqual(rbd['num_objs'], 1)
                self.assertEqual(rbd['obj_size'], 4194304)
                self.assertEqual(rbd['features_name'], 'data-pool, deep-flatten, exclusive-lock, '
                                                       'fast-diff, layering, object-map')
                break

        self._ceph_cmd(['osd', 'pool', 'delete', 'data_pool', 'data_pool',
                       '--yes-i-really-really-mean-it'])

    @authenticate
    def test_create_rbd_twice(self):
        data = {'pool_name': 'rbd',
                'name': 'test_rbd_twice',
                'size': 10240}
        self._post('/api/rbd', data)

        self._post('/api/rbd', data)
        self.assertStatus(400)
        self.assertJsonBody({"success": False, "errno": 17,
                             "detail": "[errno 17] error creating image"})
