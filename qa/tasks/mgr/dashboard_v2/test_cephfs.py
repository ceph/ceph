# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase, authenticate


class CephfsTest(DashboardTestCase):
    CEPHFS = True

    @authenticate
    def test_cephfs_clients(self):
        fs_id = self.fs.get_namespace_id()
        data = self._get("/api/cephfs/clients/{}".format(fs_id))
        self.assertStatus(200)

        self.assertIn('status', data)
        self.assertIn('data', data)

    @authenticate
    def test_cephfs_data(self):
        fs_id = self.fs.get_namespace_id()
        data = self._get("/api/cephfs/data/{}/".format(fs_id))
        self.assertStatus(200)

        self.assertIn('cephfs', data)
        self.assertIn('standbys', data)
        self.assertIn('versions', data)
        self.assertIsNotNone(data['cephfs'])
        self.assertIsNotNone(data['standbys'])
        self.assertIsNotNone(data['versions'])

    @authenticate
    def test_cephfs_mds_counters(self):
        fs_id = self.fs.get_namespace_id()
        data = self._get("/api/cephfs/mds_counters/{}".format(fs_id))
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        self.assertIsNotNone(data)
