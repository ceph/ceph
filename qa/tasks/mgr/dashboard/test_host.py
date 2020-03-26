# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase, JList, JObj


class HostControllerTest(DashboardTestCase):

    AUTH_ROLES = ['read-only']

    @DashboardTestCase.RunAs('test', 'test', ['block-manager'])
    def test_access_permissions(self):
        self._get('/api/host')
        self.assertStatus(403)

    def test_host_list(self):
        data = self._get('/api/host')
        self.assertStatus(200)

        for server in data:
            self.assertIn('services', server)
            self.assertIn('hostname', server)
            self.assertIn('ceph_version', server)
            self.assertIsNotNone(server['hostname'])
            self.assertIsNotNone(server['ceph_version'])
            self.assertGreaterEqual(len(server['services']), 1)
            for service in server['services']:
                self.assertIn('type', service)
                self.assertIn('id', service)
                self.assertIsNotNone(service['type'])
                self.assertIsNotNone(service['id'])
