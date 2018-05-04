# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import DashboardTestCase, authenticate


class UserTest(DashboardTestCase):
    @authenticate
    def test_get(self):
        self._get("/api/user/admin")
        self.assertStatus(200)
        self.assertJsonBody({"username": "admin"})

    @authenticate
    def test_set(self):
        old_password = self.get_config_key("mgr/dashboard/password")
        self._put("/api/user/admin", {'username': 'admin2', 'password': 'admin2'})
        self.assertStatus(200)
        self.assertJsonBody({"username": "admin2"})

        new_username = self.get_config_key("mgr/dashboard/username")
        new_password = self.get_config_key("mgr/dashboard/password")

        self.assertEqual(new_username, "admin2")
        self.assertNotEqual(new_password, old_password)

        self._ceph_cmd(['dashboard', 'set-login-credentials', 'admin', 'admin'])
