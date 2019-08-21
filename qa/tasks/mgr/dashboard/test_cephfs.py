# -*- coding: utf-8 -*-
from __future__ import absolute_import

import six

from .helper import DashboardTestCase


class CephfsTest(DashboardTestCase):
    CEPHFS = True

    AUTH_ROLES = ['cephfs-manager']

    def assertToHave(self, data, key):
        self.assertIn(key, data)
        self.assertIsNotNone(data[key])

    @DashboardTestCase.RunAs('test', 'test', ['block-manager'])
    def test_access_permissions(self):
        fs_id = self.fs.get_namespace_id()
        self._get("/api/cephfs/{}/clients".format(fs_id))
        self.assertStatus(403)
        self._get("/api/cephfs/{}".format(fs_id))
        self.assertStatus(403)
        self._get("/api/cephfs/{}/mds_counters".format(fs_id))
        self.assertStatus(403)
        self._get("/ui-api/cephfs/{}/tabs".format(fs_id))
        self.assertStatus(403)

    def test_cephfs_clients(self):
        fs_id = self.fs.get_namespace_id()
        data = self._get("/api/cephfs/{}/clients".format(fs_id))
        self.assertStatus(200)

        self.assertIn('status', data)
        self.assertIn('data', data)

    def test_cephfs_evict_client_does_not_exist(self):
        fs_id = self.fs.get_namespace_id()
        data = self._delete("/api/cephfs/{}/client/1234".format(fs_id))
        self.assertStatus(404)

    def test_cephfs_get(self):
        fs_id = self.fs.get_namespace_id()
        data = self._get("/api/cephfs/{}/".format(fs_id))
        self.assertStatus(200)

        self.assertToHave(data, 'cephfs')
        self.assertToHave(data, 'standbys')
        self.assertToHave(data, 'versions')

    def test_cephfs_mds_counters(self):
        fs_id = self.fs.get_namespace_id()
        data = self._get("/api/cephfs/{}/mds_counters".format(fs_id))
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        self.assertIsNotNone(data)

    def test_cephfs_mds_counters_wrong(self):
        self._get("/api/cephfs/baadbaad/mds_counters")
        self.assertStatus(400)
        self.assertJsonBody({
            "component": 'cephfs',
            "code": "invalid_cephfs_id",
            "detail": "Invalid cephfs ID baadbaad"
        })

    def test_cephfs_list(self):
        data = self._get("/api/cephfs/")
        self.assertStatus(200)

        self.assertIsInstance(data, list)
        cephfs = data[0]
        self.assertToHave(cephfs, 'id')
        self.assertToHave(cephfs, 'mdsmap')

    def test_cephfs_tabs(self):
        fs_id = self.fs.get_namespace_id()
        data = self._get("/ui-api/cephfs/{}/tabs".format(fs_id))
        self.assertStatus(200)
        self.assertIsInstance(data, dict)

        # Pools
        pools = data['pools']
        self.assertIsInstance(pools, list)
        self.assertGreater(len(pools), 0)
        for pool in pools:
            self.assertEqual(pool['size'], pool['used'] + pool['avail'])

        # Ranks
        self.assertToHave(data, 'ranks')
        self.assertIsInstance(data['ranks'], list)

        # Name
        self.assertToHave(data, 'name')
        self.assertIsInstance(data['name'], six.string_types)

        # Standbys
        self.assertToHave(data, 'standbys')
        self.assertIsInstance(data['standbys'], six.string_types)

        # MDS counters
        counters = data['mds_counters']
        self.assertIsInstance(counters, dict)
        self.assertGreater(len(counters.keys()), 0)
        for k, v in counters.items():
            self.assertEqual(v['name'], k)

        # Clients
        self.assertToHave(data, 'clients')
        clients = data['clients']
        self.assertToHave(clients, 'data')
        self.assertIsInstance(clients['data'], list)
        self.assertToHave(clients, 'status')
        self.assertIsInstance(clients['status'], int)
