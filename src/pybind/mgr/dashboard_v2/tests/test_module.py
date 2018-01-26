# -*- coding: utf-8 -*-
from __future__ import absolute_import

import errno

from .helper import ControllerTestCase
from ..controllers.auth import Auth


class ModuleTest(ControllerTestCase):

    def test_ping(self):
        self._get("/api/ping")
        self.assertStatus("401 Unauthorized")

    def test_set_login_credentials_cmd(self):
        ret, _, _ = self._mgr_module.handle_command({
            'prefix': 'dashboard set-login-credentials',
            'username': 'admin',
            'password': 'admin'
        })
        self.assertEqual(ret, 0)
        user = self._mgr_module.get_localized_config('username')
        passwd = self._mgr_module.get_localized_config('password')
        self.assertEqual(user, 'admin')
        self.assertEqual(passwd, Auth.password_hash('admin', passwd))

    def test_cmd_not_found(self):
        ret, _, _ = self._mgr_module.handle_command({
            'prefix': 'dashboard non-command'
        })
        self.assertEqual(ret, -errno.EINVAL)
