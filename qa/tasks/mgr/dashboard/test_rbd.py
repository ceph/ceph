# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

from __future__ import absolute_import

import time

from .helper import DashboardTestCase, JObj, JLeaf, JList


class RbdTest(DashboardTestCase):
    AUTH_ROLES = ['pool-manager', 'block-manager']

    @classmethod
    def create_pool(cls, name, pg_num, pool_type, application='rbd'):
        data = {
            'pool': name,
            'pg_num': pg_num,
            'pool_type': pool_type,
            'application_metadata': [application]
        }
        if pool_type == 'erasure':
            data['flags'] = ['ec_overwrites']
        cls._task_post("/api/pool", data)

    @DashboardTestCase.RunAs('test', 'test', [{'rbd-image': ['create', 'update', 'delete']}])
    def test_read_access_permissions(self):
        self._get('/api/block/image')
        self.assertStatus(403)
        self.get_image('pool', None, 'image')
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', [{'rbd-image': ['read', 'update', 'delete']}])
    def test_create_access_permissions(self):
        self.create_image('pool', None, 'name', 0)
        self.assertStatus(403)
        self.create_snapshot('pool', None, 'image', 'snapshot')
        self.assertStatus(403)
        self.copy_image('src_pool', None, 'src_image', 'dest_pool', None, 'dest_image')
        self.assertStatus(403)
        self.clone_image('parent_pool', None, 'parent_image', 'parent_snap', 'pool', None, 'name')
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', [{'rbd-image': ['read', 'create', 'delete']}])
    def test_update_access_permissions(self):
        self.edit_image('pool', None, 'image')
        self.assertStatus(403)
        self.update_snapshot('pool', None, 'image', 'snapshot', None, None)
        self.assertStatus(403)
        self.rollback_snapshot('rbd', None, 'rollback_img', 'snap1')
        self.assertStatus(403)
        self.flatten_image('pool', None, 'image')
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', [{'rbd-image': ['read', 'create', 'update']}])
    def test_delete_access_permissions(self):
        self.remove_image('pool', None, 'image')
        self.assertStatus(403)
        self.remove_snapshot('pool', None, 'image', 'snapshot')
        self.assertStatus(403)

    @classmethod
    def create_namespace(cls, pool, namespace):
        data = {'namespace': namespace}
        return cls._post('/api/block/pool/{}/namespace'.format(pool), data)

    @classmethod
    def remove_namespace(cls, pool, namespace):
        return cls._delete('/api/block/pool/{}/namespace/{}'.format(pool, namespace))

    @classmethod
    def create_image(cls, pool, namespace, name, size, **kwargs):
        data = {'name': name, 'pool_name': pool, 'namespace': namespace, 'size': size}
        data.update(kwargs)
        return cls._task_post('/api/block/image', data)

    @classmethod
    def get_image(cls, pool, namespace, name):
        namespace = '{}%2F'.format(namespace) if namespace else ''
        return cls._get('/api/block/image/{}%2F{}{}'.format(pool, namespace, name))

    @classmethod
    def clone_image(cls, parent_pool, parent_namespace, parent_image, parent_snap, pool, namespace,
                    name, **kwargs):
        # pylint: disable=too-many-arguments
        data = {'child_image_name': name, 'child_namespace': namespace, 'child_pool_name': pool}
        data.update(kwargs)
        parent_namespace = '{}%2F'.format(parent_namespace) if parent_namespace else ''
        return cls._task_post('/api/block/image/{}%2F{}{}/snap/{}/clone'
                              .format(parent_pool, parent_namespace, parent_image, parent_snap),
                              data)

    @classmethod
    def copy_image(cls, src_pool, src_namespace, src_image, dest_pool, dest_namespace, dest_image,
                   **kwargs):
        # pylint: disable=too-many-arguments
        data = {'dest_image_name': dest_image,
                'dest_pool_name': dest_pool,
                'dest_namespace': dest_namespace}
        data.update(kwargs)
        src_namespace = '{}%2F'.format(src_namespace) if src_namespace else ''
        return cls._task_post('/api/block/image/{}%2F{}{}/copy'
                              .format(src_pool, src_namespace, src_image), data)

    @classmethod
    def remove_image(cls, pool, namespace, image):
        namespace = '{}%2F'.format(namespace) if namespace else ''
        return cls._task_delete('/api/block/image/{}%2F{}{}'.format(pool, namespace, image))

    # pylint: disable=too-many-arguments
    @classmethod
    def edit_image(cls, pool, namespace, image, name=None, size=None, features=None, **kwargs):
        kwargs.update({'name': name, 'size': size, 'features': features})
        namespace = '{}%2F'.format(namespace) if namespace else ''
        return cls._task_put('/api/block/image/{}%2F{}{}'.format(pool, namespace, image), kwargs)

    @classmethod
    def flatten_image(cls, pool, namespace, image):
        namespace = '{}%2F'.format(namespace) if namespace else ''
        return cls._task_post('/api/block/image/{}%2F{}{}/flatten'.format(pool, namespace, image))

    @classmethod
    def create_snapshot(cls, pool, namespace, image, snapshot):
        namespace = '{}%2F'.format(namespace) if namespace else ''
        return cls._task_post('/api/block/image/{}%2F{}{}/snap'.format(pool, namespace, image),
                              {'snapshot_name': snapshot})

    @classmethod
    def remove_snapshot(cls, pool, namespace, image, snapshot):
        namespace = '{}%2F'.format(namespace) if namespace else ''
        return cls._task_delete('/api/block/image/{}%2F{}{}/snap/{}'.format(pool, namespace, image,
                                                                            snapshot))

    @classmethod
    def update_snapshot(cls, pool, namespace, image, snapshot, new_name, is_protected):
        namespace = '{}%2F'.format(namespace) if namespace else ''
        return cls._task_put('/api/block/image/{}%2F{}{}/snap/{}'.format(pool, namespace, image,
                                                                         snapshot),
                             {'new_snap_name': new_name, 'is_protected': is_protected})

    @classmethod
    def rollback_snapshot(cls, pool, namespace, image, snapshot):
        namespace = '{}%2F'.format(namespace) if namespace else ''
        return cls._task_post('/api/block/image/{}%2F{}{}/snap/{}/rollback'.format(pool,
                                                                                   namespace,
                                                                                   image,
                                                                                   snapshot))

    @classmethod
    def setUpClass(cls):
        super(RbdTest, cls).setUpClass()
        cls.create_pool('rbd', 2**3, 'replicated')
        cls.create_pool('rbd_iscsi', 2**3, 'replicated')

        cls.create_image('rbd', None, 'img1', 2**30)
        cls.create_image('rbd', None, 'img2', 2*2**30)
        cls.create_image('rbd_iscsi', None, 'img1', 2**30)
        cls.create_image('rbd_iscsi', None, 'img2', 2*2**30)

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

    @classmethod
    def create_image_in_trash(cls, pool, name, delay=0):
        cls.create_image(pool, None, name, 10240)
        img = cls._get('/api/block/image/{}%2F{}'.format(pool, name))

        cls._task_post("/api/block/image/{}%2F{}/move_trash".format(pool, name),
                       {'delay': delay})

        return img['id']

    @classmethod
    def remove_trash(cls, pool, image_id, force=False):
        return cls._task_delete('/api/block/image/trash/{}%2F{}/?force={}'.format(
            pool, image_id, force))

    @classmethod
    def restore_trash(cls, pool, namespace, image_id, new_image_name):
        data = {'new_image_name': new_image_name}
        namespace = '{}%2F'.format(namespace) if namespace else ''
        return cls._task_post('/api/block/image/trash/{}%2F{}{}/restore'.format(pool,
                                                                                namespace,
                                                                                image_id), data)

    @classmethod
    def purge_trash(cls, pool):
        return cls._task_post('/api/block/image/trash/purge?pool_name={}'.format(pool))

    @classmethod
    def get_trash(cls, pool, image_id):
        trash = cls._get('/api/block/image/trash/?pool_name={}'.format(pool))
        if isinstance(trash, list):
            for pool in trash:
                for image in pool['value']:
                    if image['id'] == image_id:
                        return image

        return None

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
        schema = JObj(sub_elems={
            'size': JLeaf(int),
            'obj_size': JLeaf(int),
            'num_objs': JLeaf(int),
            'order': JLeaf(int),
            'block_name_prefix': JLeaf(str),
            'name': JLeaf(str),
            'id': JLeaf(str),
            'unique_id': JLeaf(str),
            'image_format': JLeaf(int),
            'pool_name': JLeaf(str),
            'namespace': JLeaf(str, none=True),
            'features': JLeaf(int),
            'features_name': JList(JLeaf(str)),
            'stripe_count': JLeaf(int, none=True),
            'stripe_unit': JLeaf(int, none=True),
            'parent': JObj(sub_elems={'pool_name': JLeaf(str),
                                      'pool_namespace': JLeaf(str, none=True),
                                      'image_name': JLeaf(str),
                                      'snap_name': JLeaf(str)}, none=True),
            'data_pool': JLeaf(str, none=True),
            'snapshots': JList(JLeaf(dict)),
            'timestamp': JLeaf(str, none=True),
            'disk_usage': JLeaf(int, none=True),
            'total_disk_usage': JLeaf(int, none=True),
            'configuration': JList(JObj(sub_elems={
                'name': JLeaf(str),
                'source': JLeaf(int),
                'value': JLeaf(str),
            })),
        })
        self.assertSchema(img, schema)

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

    def _validate_snapshot_list(self, snap_list, snap_name=None, **kwargs):
        found = False
        for snap in snap_list:
            self.assertIn('name', snap)
            if snap_name and snap['name'] == snap_name:
                found = True
                self._validate_snapshot(snap, **kwargs)
                break
        if snap_name and not found:
            self.fail("Snapshot {} not found".format(snap_name))

    def test_list(self):
        data = self._view_cache_get('/api/block/image')
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
        self.create_image('rbd', None, rbd_name, 10240)
        self.assertStatus(201)

        img = self.get_image('rbd', None, 'test_rbd')
        self.assertStatus(200)

        self._validate_image(img, name=rbd_name, size=10240,
                             num_objs=1, obj_size=4194304,
                             features_name=['deep-flatten',
                                            'exclusive-lock',
                                            'fast-diff', 'layering',
                                            'object-map'])

        self.remove_image('rbd', None, rbd_name)

    def test_create_with_configuration(self):
        pool = 'rbd'
        image_name = 'image_with_config'
        size = 10240
        configuration = {
            'rbd_qos_bps_limit': 10240,
            'rbd_qos_bps_burst': 10240 * 2,
        }
        expected = [{
            'name': 'rbd_qos_bps_limit',
            'source': 2,
            'value': str(10240),
        }, {
            'name': 'rbd_qos_bps_burst',
            'source': 2,
            'value': str(10240 * 2),
        }]

        self.create_image(pool, None, image_name, size, configuration=configuration)
        self.assertStatus(201)
        img = self.get_image('rbd', None, image_name)
        self.assertStatus(200)
        for conf in expected:
            self.assertIn(conf, img['configuration'])

        self.remove_image(pool, None, image_name)

    def test_create_rbd_in_data_pool(self):
        if not self.bluestore_support:
            self.skipTest('requires bluestore cluster')

        self.create_pool('data_pool', 2**4, 'erasure')

        rbd_name = 'test_rbd_in_data_pool'
        self.create_image('rbd', None, rbd_name, 10240, data_pool='data_pool')
        self.assertStatus(201)

        img = self.get_image('rbd', None, 'test_rbd_in_data_pool')
        self.assertStatus(200)

        self._validate_image(img, name=rbd_name, size=10240,
                             num_objs=1, obj_size=4194304,
                             data_pool='data_pool',
                             features_name=['data-pool', 'deep-flatten',
                                            'exclusive-lock',
                                            'fast-diff', 'layering',
                                            'object-map'])

        self.remove_image('rbd', None, rbd_name)
        self.assertStatus(204)
        self._ceph_cmd(['osd', 'pool', 'delete', 'data_pool', 'data_pool',
                        '--yes-i-really-really-mean-it'])

    def test_create_rbd_twice(self):
        res = self.create_image('rbd', None, 'test_rbd_twice', 10240)

        res = self.create_image('rbd', None, 'test_rbd_twice', 10240)
        self.assertStatus(400)
        self.assertEqual(res, {"code": '17', 'status': 400, "component": "rbd",
                               "detail": "[errno 17] RBD image already exists (error creating image)",
                               'task': {'name': 'rbd/create',
                                        'metadata': {'pool_name': 'rbd', 'namespace': None,
                                                     'image_name': 'test_rbd_twice'}}})
        self.remove_image('rbd', None, 'test_rbd_twice')
        self.assertStatus(204)

    def test_snapshots_and_clone_info(self):
        self.create_snapshot('rbd', None, 'img1', 'snap1')
        self.create_snapshot('rbd', None, 'img1', 'snap2')
        self._rbd_cmd(['snap', 'protect', 'rbd/img1@snap1'])
        self._rbd_cmd(['clone', 'rbd/img1@snap1', 'rbd_iscsi/img1_clone'])

        img = self.get_image('rbd', None, 'img1')
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

        img = self.get_image('rbd_iscsi', None, 'img1_clone')
        self.assertStatus(200)
        self._validate_image(img, name='img1_clone', size=1073741824,
                             num_objs=256, obj_size=4194304,
                             parent={'pool_name': 'rbd', 'pool_namespace': '',
                                     'image_name': 'img1', 'snap_name': 'snap1'},
                             features_name=['deep-flatten', 'exclusive-lock',
                                            'fast-diff', 'layering',
                                            'object-map'])
        self.remove_image('rbd_iscsi', None, 'img1_clone')
        self.assertStatus(204)

    def test_disk_usage(self):
        self._rbd_cmd(['bench', '--io-type', 'write', '--io-total', '50M', 'rbd/img2'])
        self.create_snapshot('rbd', None, 'img2', 'snap1')
        self._rbd_cmd(['bench', '--io-type', 'write', '--io-total', '20M', 'rbd/img2'])
        self.create_snapshot('rbd', None, 'img2', 'snap2')
        self._rbd_cmd(['bench', '--io-type', 'write', '--io-total', '10M', 'rbd/img2'])
        self.create_snapshot('rbd', None, 'img2', 'snap3')
        self._rbd_cmd(['bench', '--io-type', 'write', '--io-total', '5M', 'rbd/img2'])
        img = self.get_image('rbd', None, 'img2')
        self.assertStatus(200)
        self._validate_image(img, name='img2', size=2147483648,
                             total_disk_usage=268435456, disk_usage=67108864)

    def test_delete_non_existent_image(self):
        res = self.remove_image('rbd', None, 'i_dont_exist')
        self.assertStatus(404)
        self.assertEqual(res, {u'code': 404, "status": 404, "component": None,
                               "detail": "(404, 'Image not found')",
                               'task': {'name': 'rbd/delete',
                                        'metadata': {'image_spec': 'rbd/i_dont_exist'}}})

    def test_image_delete(self):
        self.create_image('rbd', None, 'delete_me', 2**30)
        self.assertStatus(201)
        self.create_snapshot('rbd', None, 'delete_me', 'snap1')
        self.assertStatus(201)
        self.create_snapshot('rbd', None, 'delete_me', 'snap2')
        self.assertStatus(201)

        img = self.get_image('rbd', None, 'delete_me')
        self.assertStatus(200)
        self._validate_image(img, name='delete_me', size=2**30)
        self.assertEqual(len(img['snapshots']), 2)

        self.remove_snapshot('rbd', None, 'delete_me', 'snap1')
        self.assertStatus(204)
        self.remove_snapshot('rbd', None, 'delete_me', 'snap2')
        self.assertStatus(204)

        img = self.get_image('rbd', None, 'delete_me')
        self.assertStatus(200)
        self._validate_image(img, name='delete_me', size=2**30)
        self.assertEqual(len(img['snapshots']), 0)

        self.remove_image('rbd', None, 'delete_me')
        self.assertStatus(204)

    def test_image_delete_with_snapshot(self):
        self.create_image('rbd', None, 'delete_me', 2**30)
        self.assertStatus(201)
        self.create_snapshot('rbd', None, 'delete_me', 'snap1')
        self.assertStatus(201)
        self.create_snapshot('rbd', None, 'delete_me', 'snap2')
        self.assertStatus(201)

        img = self.get_image('rbd', None, 'delete_me')
        self.assertStatus(200)
        self._validate_image(img, name='delete_me', size=2**30)
        self.assertEqual(len(img['snapshots']), 2)

        self.remove_image('rbd', None, 'delete_me')
        self.assertStatus(204)

    def test_image_rename(self):
        self.create_image('rbd', None, 'edit_img', 2**30)
        self.assertStatus(201)
        self.get_image('rbd', None, 'edit_img')
        self.assertStatus(200)
        self.edit_image('rbd', None, 'edit_img', 'new_edit_img')
        self.assertStatus(200)
        self.get_image('rbd', None, 'edit_img')
        self.assertStatus(404)
        self.get_image('rbd', None, 'new_edit_img')
        self.assertStatus(200)
        self.remove_image('rbd', None, 'new_edit_img')
        self.assertStatus(204)

    def test_image_resize(self):
        self.create_image('rbd', None, 'edit_img', 2**30)
        self.assertStatus(201)
        img = self.get_image('rbd', None, 'edit_img')
        self.assertStatus(200)
        self._validate_image(img, size=2**30)
        self.edit_image('rbd', None, 'edit_img', size=2*2**30)
        self.assertStatus(200)
        img = self.get_image('rbd', None, 'edit_img')
        self.assertStatus(200)
        self._validate_image(img, size=2*2**30)
        self.remove_image('rbd', None, 'edit_img')
        self.assertStatus(204)

    def test_image_change_features(self):
        self.create_image('rbd', None, 'edit_img', 2**30, features=["layering"])
        self.assertStatus(201)
        img = self.get_image('rbd', None, 'edit_img')
        self.assertStatus(200)
        self._validate_image(img, features_name=["layering"])
        self.edit_image('rbd', None, 'edit_img',
                        features=["fast-diff", "object-map", "exclusive-lock"])
        self.assertStatus(200)
        img = self.get_image('rbd', None, 'edit_img')
        self.assertStatus(200)
        self._validate_image(img, features_name=['exclusive-lock',
                                                 'fast-diff', 'layering',
                                                 'object-map'])
        self.edit_image('rbd', None, 'edit_img',
                        features=["journaling", "exclusive-lock"])
        self.assertStatus(200)
        img = self.get_image('rbd', None, 'edit_img')
        self.assertStatus(200)
        self._validate_image(img, features_name=['exclusive-lock',
                                                 'journaling', 'layering'])
        self.remove_image('rbd', None, 'edit_img')
        self.assertStatus(204)

    def test_image_change_config(self):
        pool = 'rbd'
        image = 'image_with_config'
        initial_conf = {
            'rbd_qos_bps_limit': 10240,
            'rbd_qos_write_iops_limit': None
        }
        initial_expect = [{
            'name': 'rbd_qos_bps_limit',
            'source': 2,
            'value': '10240',
        }, {
            'name': 'rbd_qos_write_iops_limit',
            'source': 0,
            'value': '0',
        }]
        new_conf = {
            'rbd_qos_bps_limit': 0,
            'rbd_qos_bps_burst': 20480,
            'rbd_qos_write_iops_limit': None
        }
        new_expect = [{
            'name': 'rbd_qos_bps_limit',
            'source': 2,
            'value': '0',
        }, {
            'name': 'rbd_qos_bps_burst',
            'source': 2,
            'value': '20480',
        }, {
            'name': 'rbd_qos_write_iops_limit',
            'source': 0,
            'value': '0',
        }]

        self.create_image(pool, None, image, 2**30, configuration=initial_conf)
        self.assertStatus(201)
        img = self.get_image(pool, None, image)
        self.assertStatus(200)
        for conf in initial_expect:
            self.assertIn(conf, img['configuration'])

        self.edit_image(pool, None, image, configuration=new_conf)
        img = self.get_image(pool, None, image)
        self.assertStatus(200)
        for conf in new_expect:
            self.assertIn(conf, img['configuration'])

        self.remove_image(pool, None, image)
        self.assertStatus(204)

    def test_update_snapshot(self):
        self.create_snapshot('rbd', None, 'img1', 'snap5')
        self.assertStatus(201)
        img = self.get_image('rbd', None, 'img1')
        self._validate_snapshot_list(img['snapshots'], 'snap5', is_protected=False)

        self.update_snapshot('rbd', None, 'img1', 'snap5', 'snap6', None)
        self.assertStatus(200)
        img = self.get_image('rbd', None, 'img1')
        self._validate_snapshot_list(img['snapshots'], 'snap6', is_protected=False)

        self.update_snapshot('rbd', None, 'img1', 'snap6', None, True)
        self.assertStatus(200)
        img = self.get_image('rbd', None, 'img1')
        self._validate_snapshot_list(img['snapshots'], 'snap6', is_protected=True)

        self.update_snapshot('rbd', None, 'img1', 'snap6', 'snap5', False)
        self.assertStatus(200)
        img = self.get_image('rbd', None, 'img1')
        self._validate_snapshot_list(img['snapshots'], 'snap5', is_protected=False)

        self.remove_snapshot('rbd', None, 'img1', 'snap5')
        self.assertStatus(204)

    def test_snapshot_rollback(self):
        self.create_image('rbd', None, 'rollback_img', 2**30,
                          features=["layering", "exclusive-lock", "fast-diff",
                                    "object-map"])
        self.assertStatus(201)
        self.create_snapshot('rbd', None, 'rollback_img', 'snap1')
        self.assertStatus(201)

        img = self.get_image('rbd', None, 'rollback_img')
        self.assertStatus(200)
        self.assertEqual(img['disk_usage'], 0)

        self._rbd_cmd(['bench', '--io-type', 'write', '--io-total', '5M',
                       'rbd/rollback_img'])

        img = self.get_image('rbd', None, 'rollback_img')
        self.assertStatus(200)
        self.assertGreater(img['disk_usage'], 0)

        self.rollback_snapshot('rbd', None, 'rollback_img', 'snap1')
        self.assertStatus([201, 200])

        img = self.get_image('rbd', None, 'rollback_img')
        self.assertStatus(200)
        self.assertEqual(img['disk_usage'], 0)

        self.remove_snapshot('rbd', None, 'rollback_img', 'snap1')
        self.assertStatus(204)
        self.remove_image('rbd', None, 'rollback_img')
        self.assertStatus(204)

    def test_clone(self):
        self.create_image('rbd', None, 'cimg', 2**30, features=["layering"])
        self.assertStatus(201)
        self.create_snapshot('rbd', None, 'cimg', 'snap1')
        self.assertStatus(201)
        self.update_snapshot('rbd', None, 'cimg', 'snap1', None, True)
        self.assertStatus(200)
        self.clone_image('rbd', None, 'cimg', 'snap1', 'rbd', None, 'cimg-clone',
                         features=["layering", "exclusive-lock", "fast-diff",
                                   "object-map"])
        self.assertStatus([200, 201])

        img = self.get_image('rbd', None, 'cimg-clone')
        self.assertStatus(200)
        self._validate_image(img, features_name=['exclusive-lock',
                                                 'fast-diff', 'layering',
                                                 'object-map'],
                             parent={'pool_name': 'rbd', 'pool_namespace': '',
                                     'image_name': 'cimg', 'snap_name': 'snap1'})

        res = self.remove_image('rbd', None, 'cimg')
        self.assertStatus(400)
        self.assertIn('code', res)
        self.assertEqual(res['code'], '16')

        self.remove_image('rbd', None, 'cimg-clone')
        self.assertStatus(204)
        self.remove_image('rbd', None, 'cimg')
        self.assertStatus(204)

    def test_copy(self):
        self.create_image('rbd', None, 'coimg', 2**30,
                          features=["layering", "exclusive-lock", "fast-diff",
                                    "object-map"])
        self.assertStatus(201)

        self._rbd_cmd(['bench', '--io-type', 'write', '--io-total', '5M',
                       'rbd/coimg'])

        self.copy_image('rbd', None, 'coimg', 'rbd_iscsi', None, 'coimg-copy',
                        features=["layering", "fast-diff", "exclusive-lock",
                                  "object-map"])
        self.assertStatus([200, 201])

        img = self.get_image('rbd', None, 'coimg')
        self.assertStatus(200)
        self._validate_image(img, features_name=['layering', 'exclusive-lock',
                                                 'fast-diff', 'object-map'])

        img_copy = self.get_image('rbd_iscsi', None, 'coimg-copy')
        self._validate_image(img_copy, features_name=['exclusive-lock',
                                                      'fast-diff', 'layering',
                                                      'object-map'],
                             disk_usage=img['disk_usage'])

        self.remove_image('rbd', None, 'coimg')
        self.assertStatus(204)
        self.remove_image('rbd_iscsi', None, 'coimg-copy')
        self.assertStatus(204)

    def test_flatten(self):
        self.create_snapshot('rbd', None, 'img1', 'snapf')
        self.update_snapshot('rbd', None, 'img1', 'snapf', None, True)
        self.clone_image('rbd', None, 'img1', 'snapf', 'rbd_iscsi', None, 'img1_snapf_clone')

        img = self.get_image('rbd_iscsi', None, 'img1_snapf_clone')
        self.assertStatus(200)
        self.assertIsNotNone(img['parent'])

        self.flatten_image('rbd_iscsi', None, 'img1_snapf_clone')
        self.assertStatus([200, 201])

        img = self.get_image('rbd_iscsi', None, 'img1_snapf_clone')
        self.assertStatus(200)
        self.assertIsNone(img['parent'])

        self.update_snapshot('rbd', None, 'img1', 'snapf', None, False)
        self.remove_snapshot('rbd', None, 'img1', 'snapf')
        self.assertStatus(204)

        self.remove_image('rbd_iscsi', None, 'img1_snapf_clone')
        self.assertStatus(204)

    def test_default_features(self):
        default_features = self._get('/api/block/image/default_features')
        self.assertEqual(default_features, [
            'deep-flatten', 'exclusive-lock', 'fast-diff', 'layering', 'object-map'])

    def test_image_with_namespace(self):
        self.create_namespace('rbd', 'ns')
        self.create_image('rbd', 'ns', 'test', 10240)
        self.assertStatus(201)

        img = self.get_image('rbd', 'ns', 'test')
        self.assertStatus(200)

        self._validate_image(img, name='test', size=10240,
                             pool_name='rbd', namespace='ns',
                             num_objs=1, obj_size=4194304,
                             features_name=['deep-flatten',
                                            'exclusive-lock',
                                            'fast-diff', 'layering',
                                            'object-map'])

        self.remove_image('rbd', 'ns', 'test')
        self.remove_namespace('rbd', 'ns')

    def test_move_image_to_trash(self):
        id = self.create_image_in_trash('rbd', 'test_rbd')
        self.assertStatus(200)

        self.get_image('rbd', None, 'test_rbd')
        self.assertStatus(404)

        time.sleep(1)

        image = self.get_trash('rbd', id)
        self.assertIsNotNone(image)

        self.remove_trash('rbd', id)

    def test_list_trash(self):
        id = self.create_image_in_trash('rbd', 'test_rbd', 0)
        data = self._get('/api/block/image/trash/?pool_name={}'.format('rbd'))
        self.assertStatus(200)
        self.assertIsInstance(data, list)
        self.assertIsNotNone(data)

        self.remove_trash('rbd', id)
        self.assertStatus(204)

    def test_restore_trash(self):
        id = self.create_image_in_trash('rbd', 'test_rbd')

        self.restore_trash('rbd', None, id, 'test_rbd')

        self.get_image('rbd', None, 'test_rbd')
        self.assertStatus(200)

        image = self.get_trash('rbd', id)
        self.assertIsNone(image)

        self.remove_image('rbd', None, 'test_rbd')

    def test_remove_expired_trash(self):
        id = self.create_image_in_trash('rbd', 'test_rbd', 0)
        self.remove_trash('rbd', id, False)
        self.assertStatus(204)

        image = self.get_trash('rbd', id)
        self.assertIsNone(image)

    def test_remove_not_expired_trash(self):
        id = self.create_image_in_trash('rbd', 'test_rbd', 9999)
        self.remove_trash('rbd', id, False)
        self.assertStatus(400)

        time.sleep(1)

        image = self.get_trash('rbd', id)
        self.assertIsNotNone(image)

        self.remove_trash('rbd', id, True)

    def test_remove_not_expired_trash_with_force(self):
        id = self.create_image_in_trash('rbd', 'test_rbd', 9999)
        self.remove_trash('rbd', id, True)
        self.assertStatus(204)

        image = self.get_trash('rbd', id)
        self.assertIsNone(image)

    def test_purge_trash(self):
        id_expired = self.create_image_in_trash('rbd', 'test_rbd_expired', 0)
        id_not_expired = self.create_image_in_trash('rbd', 'test_rbd', 9999)

        time.sleep(1)

        self.purge_trash('rbd')
        self.assertStatus([200, 201])

        time.sleep(1)

        trash_not_expired = self.get_trash('rbd', id_not_expired)
        self.assertIsNotNone(trash_not_expired)

        self.wait_until_equal(lambda: self.get_trash('rbd', id_expired), None, 60)

    def test_list_namespaces(self):
        self.create_namespace('rbd', 'ns')

        namespaces = self._get('/api/block/pool/rbd/namespace')
        self.assertStatus(200)
        self.assertEqual(len(namespaces), 1)

        self.remove_namespace('rbd', 'ns')
