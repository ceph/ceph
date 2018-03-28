# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import DashboardTestCase


class RbdTest(DashboardTestCase):

    @classmethod
    def authenticate(cls):
        cls._ceph_cmd(['dashboard', 'set-login-credentials', 'admin', 'admin'])
        cls._post('/api/auth', {'username': 'admin', 'password': 'admin'})

    @classmethod
    def create_pool(cls, name, pg_num, pool_type, application='rbd'):
        cls._post("/api/pool", {'pool': name, 'pg_num': pg_num,
                                'pool_type': pool_type,
                                'application_metadata': application})

    @classmethod
    def create_image(cls, name, pool, size, **kwargs):
        data = {'name': name, 'pool_name': pool, 'size': size}
        data.update(kwargs)
        return cls._task_post('/api/rbd', 'rbd/create',
                              {'pool_name': pool, 'image_name': name}, data)

    @classmethod
    def setUpClass(cls):
        super(RbdTest, cls).setUpClass()
        cls.authenticate()
        cls.create_pool('rbd', 10, 'replicated')
        cls.create_pool('rbd_iscsi', 10, 'replicated')

        cls.create_image('img1', 'rbd', 2**30)
        cls.create_image('img2', 'rbd', 2*2**30)
        cls.create_image('img1', 'rbd_iscsi', 2**30)
        cls.create_image('img2', 'rbd_iscsi', 2*2**30)

        osd_metadata = cls.ceph_cluster.mon_manager.get_osd_metadata()
        cls.bluestore_support = True
        for osd in osd_metadata:
            if osd['osd_objectstore'] != 'bluestore':
                cls.bluestore_support = False
                break

    @classmethod
    def tearDownClass(cls):
        super(RbdTest, cls).tearDownClass()
        cls._ceph_cmd(['osd', 'pool', 'delete', 'rbd', 'rbd', '--yes-i-really-really-mean-it'])
        cls._ceph_cmd(['osd', 'pool', 'delete', 'rbd_iscsi', 'rbd_iscsi',
                       '--yes-i-really-really-mean-it'])
        cls._ceph_cmd(['osd', 'pool', 'delete', 'rbd_data', 'rbd_data',
                       '--yes-i-really-really-mean-it'])

    def _validate_image(self, img, **kwargs):
        """
        Example of an RBD image json:

        {
            "size": 1073741824,
            "obj_size": 4194304,
            "num_objs": 256,
            "order": 22,
            "block_name_prefix": "rbd_data.10ae2ae8944a",
            "name": "img1",
            "pool_name": "rbd",
            "features": 61,
            "features_name": ["deep-flatten", "exclusive-lock", "fast-diff", "layering",
                              "object-map"]
        }
        """
        self.assertIn('size', img)
        self.assertIn('obj_size', img)
        self.assertIn('num_objs', img)
        self.assertIn('order', img)
        self.assertIn('block_name_prefix', img)
        self.assertIn('name', img)
        self.assertIn('id', img)
        self.assertIn('pool_name', img)
        self.assertIn('features', img)
        self.assertIn('features_name', img)
        self.assertIn('stripe_count', img)
        self.assertIn('stripe_unit', img)
        self.assertIn('parent', img)
        self.assertIn('data_pool', img)
        self.assertIn('snapshots', img)

        for k, v in kwargs.items():
            if isinstance(v, list):
                self.assertSetEqual(set(img[k]), set(v))
            else:
                self.assertEqual(img[k], v)

    def _validate_snapshot(self, snap, **kwargs):
        self.assertIn('id', snap)
        self.assertIn('name', snap)
        self.assertIn('is_protected', snap)
        self.assertIn('timestamp', snap)
        self.assertIn('size', snap)
        self.assertIn('children', snap)

        for k, v in kwargs.items():
            if isinstance(v, list):
                self.assertSetEqual(set(snap[k]), set(v))
            else:
                self.assertEqual(snap[k], v)

    def test_list(self):
        data = self._view_cache_get('/api/rbd')
        self.assertStatus(200)
        self.assertEqual(len(data), 2)

        for pool_view in data:
            self.assertEqual(pool_view['status'], 0)
            self.assertIsNotNone(pool_view['value'])
            self.assertIn('pool_name', pool_view)
            self.assertIn(pool_view['pool_name'], ['rbd', 'rbd_iscsi'])
            image_list = pool_view['value']
            self.assertEqual(len(image_list), 2)

            for img in image_list:
                self.assertIn('name', img)
                self.assertIn('pool_name', img)
                self.assertIn(img['pool_name'], ['rbd', 'rbd_iscsi'])
                if img['name'] == 'img1':
                    self._validate_image(img, size=1073741824,
                                         num_objs=256, obj_size=4194304,
                                         features_name=['deep-flatten',
                                                        'exclusive-lock',
                                                        'fast-diff',
                                                        'layering',
                                                        'object-map'])
                elif img['name'] == 'img2':
                    self._validate_image(img, size=2147483648,
                                         num_objs=512, obj_size=4194304,
                                         features_name=['deep-flatten',
                                                        'exclusive-lock',
                                                        'fast-diff',
                                                        'layering',
                                                        'object-map'])
                else:
                    assert False, "Unexcepted image '{}' in result list".format(img['name'])

    def test_create(self):
        rbd_name = 'test_rbd'
        res = self.create_image(rbd_name, 'rbd', 10240)
        self.assertEqual(res, {"success": True})

        img = self._get('/api/rbd/rbd/test_rbd')
        self.assertStatus(200)

        self._validate_image(img, name=rbd_name, size=10240,
                             num_objs=1, obj_size=4194304,
                             features_name=['deep-flatten',
                                            'exclusive-lock',
                                            'fast-diff', 'layering',
                                            'object-map'])

        self._rbd_cmd(['rm', 'rbd/{}'.format(rbd_name)])

    def test_create_rbd_in_data_pool(self):
        if not self.bluestore_support:
            self.skipTest('requires bluestore cluster')

        self._ceph_cmd(['osd', 'pool', 'create', 'data_pool', '12', '12', 'erasure'])
        self._ceph_cmd(['osd', 'pool', 'application', 'enable', 'data_pool', 'rbd'])
        self._ceph_cmd(['osd', 'pool', 'set', 'data_pool', 'allow_ec_overwrites', 'true'])

        rbd_name = 'test_rbd_in_data_pool'
        res = self.create_image(rbd_name, 'rbd', 10240, data_pool='data_pool')
        self.assertEqual(res, {"success": True})

        img = self._get('/api/rbd/rbd/test_rbd_in_data_pool')
        self.assertStatus(200)

        self._validate_image(img, name=rbd_name, size=10240,
                             num_objs=1, obj_size=4194304,
                             data_pool='data_pool',
                             features_name=['data-pool', 'deep-flatten',
                                            'exclusive-lock',
                                            'fast-diff', 'layering',
                                            'object-map'])

        self._rbd_cmd(['rm', 'rbd/{}'.format(rbd_name)])
        self._ceph_cmd(['osd', 'pool', 'delete', 'data_pool', 'data_pool',
                        '--yes-i-really-really-mean-it'])

    def test_create_rbd_twice(self):
        res = self.create_image('test_rbd_twice', 'rbd', 10240)

        res = self.create_image('test_rbd_twice', 'rbd', 10240)
        self.assertEqual(res, {"success": False, "errno": 17,
                               "detail": "[errno 17] error creating image"})
        self._rbd_cmd(['rm', 'rbd/test_rbd_twice'])

    def test_snapshots_and_clone_info(self):
        self._rbd_cmd(['snap', 'create', 'rbd/img1@snap1'])
        self._rbd_cmd(['snap', 'create', 'rbd/img1@snap2'])
        self._rbd_cmd(['snap', 'protect', 'rbd/img1@snap1'])
        self._rbd_cmd(['clone', 'rbd/img1@snap1', 'rbd_iscsi/img1_clone'])

        img = self._get('/api/rbd/rbd/img1')
        self.assertStatus(200)
        self._validate_image(img, name='img1', size=1073741824,
                             num_objs=256, obj_size=4194304, parent=None,
                             features_name=['deep-flatten', 'exclusive-lock',
                                            'fast-diff', 'layering',
                                            'object-map'])
        for snap in img['snapshots']:
            if snap['name'] == 'snap1':
                self._validate_snapshot(snap, is_protected=True)
                self.assertEqual(len(snap['children']), 1)
                self.assertDictEqual(snap['children'][0],
                                     {'pool_name': 'rbd_iscsi',
                                      'image_name': 'img1_clone'})
            elif snap['name'] == 'snap2':
                self._validate_snapshot(snap, is_protected=False)

        img = self._get('/api/rbd/rbd_iscsi/img1_clone')
        self.assertStatus(200)
        self._validate_image(img, name='img1_clone', size=1073741824,
                             num_objs=256, obj_size=4194304,
                             parent={'pool_name': 'rbd', 'image_name': 'img1',
                                     'snap_name': 'snap1'},
                             features_name=['deep-flatten', 'exclusive-lock',
                                            'fast-diff', 'layering',
                                            'object-map'])
        self._rbd_cmd(['rm', 'rbd_iscsi/img1_clone'])
