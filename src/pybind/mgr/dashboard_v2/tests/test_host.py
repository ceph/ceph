# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import ControllerTestCase, authenticate


class HostControllerTest(ControllerTestCase):

    @authenticate
    def test_host_list(self):
        data = self._get('/api/host')
        self.assertStatus(200)

        self.assertEqual(len(data), 1)
        data = data[0]
        self.assertIn('services', data)
        self.assertIn('hostname', data)
        self.assertIn('ceph_version', data)
        self.assertIsNotNone(data['hostname'])
        self.assertIsNotNone(data['ceph_version'])
        self.assertGreaterEqual(len(data['services']), 1)
        for service in data['services']:
            self.assertIn('type', service)
            self.assertIn('id', service)
            self.assertIsNotNone(service['type'])
            self.assertIsNotNone(service['id'])
