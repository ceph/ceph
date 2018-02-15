# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import ControllerTestCase, authenticate


class CephfsTest(ControllerTestCase):
    @authenticate
    def test_cephfs_clients(self):
        data = self._get("/api/cephfs/clients/1")
        self.assertStatus(200)

        self.assertIn('status', data)
        self.assertIn('data', data)

    @authenticate
    def test_cephfs_data(self):
        data = self._get("/api/cephfs/data/1/")
        self.assertStatus(200)

        self.assertIn('cephfs', data)
        self.assertIn('standbys', data)
        self.assertIn('versions', data)
        self.assertIsNotNone(data['cephfs'])
        self.assertIsNotNone(data['standbys'])
        self.assertIsNotNone(data['versions'])

    @authenticate
    def test_cephfs_mds_counters(self):
        data = self._get("/api/cephfs/mds_counters/1")
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        self.assertIsNotNone(data)
