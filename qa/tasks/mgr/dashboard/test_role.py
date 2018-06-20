# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import DashboardTestCase


class RoleTest(DashboardTestCase):

    def test_list_roles(self):
        roles = self._get('/api/role')
        self.assertStatus(200)

        self.assertGreaterEqual(len(roles), 1)
        for role in roles:
            self.assertIn('name', role)
            self.assertIn('scopes_permissions', role)
