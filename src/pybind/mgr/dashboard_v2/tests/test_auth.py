# -*- coding: utf-8 -*-

from __future__ import absolute_import

import time

import cherrypy
from cherrypy.lib.sessions import RamSession
from mock import patch

from .helper import ApiControllerTestCase
from ..controllers.auth import Auth

class Ping(object):
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    def ping(self):
        pass


class AuthTest(ApiControllerTestCase):
    @staticmethod
    def setup_server():
        ApiControllerTestCase.setup_test(['Auth'])
        module = ApiControllerTestCase._mgr_module

        cherrypy.tools.autenticate = cherrypy.Tool('before_handler',
                                                   Auth.check_auth)

        cherrypy.tree.mount(Ping(), "/api/test",
                            config={'/': {'tools.autenticate.on': True}})
        module.set_localized_config('session-expire','2')
        module.set_localized_config('username','admin')
        module.set_localized_config('password', Auth.password_hash('admin'))

    def test_login_valid(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
            self.assertStatus('201 Created')
            self.assertBody('{"username": "admin"}')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), 'admin')

    def test_login_invalid(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'inval'})
            self.assertStatus('403 Forbidden')
            self.assertBody('{"detail": "Invalid credentials"}')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), None)

    def test_logout(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), 'admin')
            self._delete("/api/auth")
            self.assertStatus('204 No Content')
            self.assertBody('')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), None)

    def test_session_expire(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
            self.assertStatus('201 Created')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), 'admin')
            self._post("/api/test/ping")
            self.assertStatus('200 OK')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), 'admin')
            time.sleep(3)
            self._post("/api/test/ping")
            self.assertStatus('401 Unauthorized')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), None)

    def test_unauthorized(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/test/ping")
            self.assertStatus('401 Unauthorized')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), None)
