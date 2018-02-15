# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import ControllerTestCase, authenticate


class RbdTest(ControllerTestCase):

    @classmethod
    def setUpClass(cls):
        cls._ceph_cmd(['osd', 'pool', 'create', 'rbd', '100', '100'])
        cls._ceph_cmd(['osd', 'pool', 'application', 'enable', 'rbd', 'rbd'])
        cls._rbd_cmd(['create', '--size=1G', 'img1'])
        cls._rbd_cmd(['create', '--size=2G', 'img2'])

    @classmethod
    def tearDownClass(cls):
        cls._ceph_cmd(['osd', 'pool', 'delete', 'rbd', '--yes-i-really-really-mean-it'])

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
