# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

from __future__ import absolute_import

import time

from .helper import DashboardTestCase, JObj, JLeaf, JList


class RbdMirroringTest(DashboardTestCase):
    AUTH_ROLES = ['pool-manager', 'block-manager']

    @classmethod
    def create_pool(cls, name, application='rbd'):
        data = {
            'pool': name,
            'pg_num': 10,
            'pool_type': 'replicated',
            'application_metadata': [application]
        }
        cls._task_post("/api/pool", data)

    @classmethod
    def get_pool(cls, pool):
        data = cls._get('/api/block/mirroring/pool/{}'.format(pool))
        if isinstance(data, dict):
            return data
        return {}

    @classmethod
    def update_pool(cls, pool, mirror_mode):
        data = {'mirror_mode': mirror_mode}
        return cls._task_put('/api/block/mirroring/pool/{}'.format(pool),
            data)

    @classmethod
    def list_peers(cls, pool):
        data = cls._get('/api/block/mirroring/pool/{}/peer'.format(pool))
        if isinstance(data, list):
            return data
        return []

    @classmethod
    def get_peer(cls, pool, peer_uuid):
        data = cls._get('/api/block/mirroring/pool/{}/peer/{}'.format(pool, peer_uuid))
        if isinstance(data, dict):
            return data
        return {}

    @classmethod
    def create_peer(cls, pool, cluster_name, client_id, **kwargs):
        data = {'cluster_name': cluster_name, 'client_id': client_id}
        data.update(kwargs)
        return cls._task_post('/api/block/mirroring/pool/{}/peer'.format(pool),
            data)

    @classmethod
    def update_peer(cls, pool, peer_uuid, **kwargs):
        return cls._task_put('/api/block/mirroring/pool/{}/peer/{}'.format(pool, peer_uuid),
            kwargs)

    @classmethod
    def delete_peer(cls, pool, peer_uuid):
        return cls._task_delete('/api/block/mirroring/pool/{}/peer/{}'.format(pool, peer_uuid))

    @classmethod
    def setUpClass(cls):
        super(RbdMirroringTest, cls).setUpClass()
        cls.create_pool('rbd')

    @classmethod
    def tearDownClass(cls):
        super(RbdMirroringTest, cls).tearDownClass()
        cls._ceph_cmd(['osd', 'pool', 'delete', 'rbd', 'rbd', '--yes-i-really-really-mean-it'])

    @DashboardTestCase.RunAs('test', 'test', [{'rbd-mirroring': ['create', 'update', 'delete']}])
    def test_read_access_permissions(self):
        self.get_pool('rbd')
        self.assertStatus(403)
        self.list_peers('rbd')
        self.assertStatus(403)
        self.get_peer('rbd', '123')
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', [{'rbd-mirroring': ['read', 'update', 'delete']}])
    def test_create_access_permissions(self):
        self.create_peer('rbd', 'remote', 'id')
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', [{'rbd-mirroring': ['read', 'create', 'delete']}])
    def test_update_access_permissions(self):
        self.update_peer('rbd', '123')
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', [{'rbd-mirroring': ['read', 'create', 'update']}])
    def test_delete_access_permissions(self):
        self.delete_peer('rbd', '123')
        self.assertStatus(403)

    def test_mirror_mode(self):
        self.update_pool('rbd', 'disabled')
        mode = self.get_pool('rbd').get('mirror_mode')
        self.assertEqual(mode, 'disabled')

        self.update_pool('rbd', 'image')
        mode = self.get_pool('rbd').get('mirror_mode')
        self.assertEqual(mode, 'image')

        self.update_pool('rbd', 'pool')
        mode = self.get_pool('rbd').get('mirror_mode')
        self.assertEqual(mode, 'pool')

        self.update_pool('rbd', 'disabled')
        mode = self.get_pool('rbd').get('mirror_mode')
        self.assertEqual(mode, 'disabled')

    def test_set_invalid_mirror_mode(self):
        self.update_pool('rbd', 'invalid')
        self.assertStatus(400)

    def test_set_same_mirror_mode(self):
        self.update_pool('rbd', 'disabled')
        self.update_pool('rbd', 'disabled')
        self.assertStatus(200)

    def test_peer(self):
        self.update_pool('rbd', 'image')
        self.assertStatus(200)

        peers = self.list_peers('rbd')
        self.assertStatus(200)
        self.assertEqual([], peers)

        uuid = self.create_peer('rbd', 'remote', 'admin')['uuid']
        self.assertStatus(201)

        peers = self.list_peers('rbd')
        self.assertStatus(200)
        self.assertEqual([uuid], peers)

        expected_peer = {
            'uuid': uuid,
            'cluster_name': 'remote',
            'client_id': 'admin',
            'mon_host': '',
            'key': ''
        }
        peer = self.get_peer('rbd', uuid)
        self.assertEqual(expected_peer, peer)

        self.update_peer('rbd', uuid, mon_host='1.2.3.4')
        self.assertStatus(200)

        expected_peer['mon_host'] = '1.2.3.4'
        peer = self.get_peer('rbd', uuid)
        self.assertEqual(expected_peer, peer)

        self.delete_peer('rbd', uuid)
        self.assertStatus(204)

        self.update_pool('rbd', 'disabled')
        self.assertStatus(200)

    def test_disable_mirror_with_peers(self):
        self.update_pool('rbd', 'image')
        self.assertStatus(200)

        uuid = self.create_peer('rbd', 'remote', 'admin')['uuid']
        self.assertStatus(201)

        self.update_pool('rbd', 'disabled')
        self.assertStatus(400)

        self.delete_peer('rbd', uuid)
        self.assertStatus(204)

        self.update_pool('rbd', 'disabled')
        self.assertStatus(200)
