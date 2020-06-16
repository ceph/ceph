# -*- coding: utf-8 -*-
from __future__ import absolute_import

from contextlib import contextmanager

from .helper import DashboardTestCase


class CephfsTest(DashboardTestCase):
    CEPHFS = True

    AUTH_ROLES = ['cephfs-manager']

    @DashboardTestCase.RunAs('test', 'test', ['block-manager'])
    def test_access_permissions(self):
        fs_id = self.fs.get_namespace_id()
        self._get("/api/cephfs/{}/clients".format(fs_id))
        self.assertStatus(403)
        self._get("/api/cephfs/{}".format(fs_id))
        self.assertStatus(403)
        self._get("/api/cephfs/{}/mds_counters".format(fs_id))
        self.assertStatus(403)

    def test_cephfs_clients(self):
        fs_id = self.fs.get_namespace_id()
        data = self._get("/api/cephfs/{}/clients".format(fs_id))
        self.assertStatus(200)

        self.assertIn('status', data)
        self.assertIn('data', data)

    def test_cephfs_get(self):
        fs_id = self.fs.get_namespace_id()
        data = self._get("/api/cephfs/{}/".format(fs_id))
        self.assertStatus(200)

        self.assertIn('cephfs', data)
        self.assertIn('standbys', data)
        self.assertIn('versions', data)
        self.assertIsNotNone(data['cephfs'])
        self.assertIsNotNone(data['standbys'])
        self.assertIsNotNone(data['versions'])

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
        self.assertIn('id', cephfs)
        self.assertIn('mdsmap', cephfs)
        self.assertIsNotNone(cephfs['id'])
        self.assertIsNotNone(cephfs['mdsmap'])
