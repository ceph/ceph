# -*- coding: utf-8 -*-

from __future__ import absolute_import

import json
import time

from cherrypy.lib.sessions import RamSession
from cherrypy.test import helper
from mock import patch

from ..module import Module, cherrypy
from ..tools import load_controller

class Ping(object):
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    def ping(self):
        pass


class AuthTest(helper.CPWebCase):
    @staticmethod
    def setup_server():
        module = Module('dashboard', None, None)
        AuthTest.Auth = load_controller(module, 'Auth')

        cherrypy.tools.autenticate = cherrypy.Tool('before_handler',
                                                   AuthTest.Auth.check_auth)

        cherrypy.tree.mount(AuthTest.Auth(), "/api/auth")
        cherrypy.tree.mount(Ping(), "/api/test",
                            config={'/': {'tools.autenticate.on': True}})
        module.set_localized_config('session-expire','2')
        module.set_localized_config('username','admin')
        pass_hash = AuthTest.Auth.password_hash('admin')
        module.set_localized_config('password', pass_hash)

    def _request(self, url, method, data=None):
        if not data:
            b = None
            h = None
        else:
            b = json.dumps(data)
            h = [('Content-Type', 'application/json'),
                 ('Content-Length', str(len(b)))]
        self.getPage(url, method=method, body=b, headers=h)

    def _post(self, url, data=None):
        self._request(url, 'POST', data)

    def _delete(self, url, data=None):
        self._request(url, 'DELETE', data)

    def test_login_valid(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
            self.assertStatus('201 Created')
            self.assertBody('{"username": "admin"}')
            self.assertEquals(sess_mock.get(AuthTest.Auth.SESSION_KEY),
                              'admin')

    def test_login_invalid(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'inval'})
            self.assertStatus('403 Forbidden')
            self.assertBody('{"detail": "Invalid credentials"}')
            self.assertEquals(sess_mock.get(AuthTest.Auth.SESSION_KEY), None)

    def test_logout(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
            self.assertEquals(sess_mock.get(AuthTest.Auth.SESSION_KEY),
                              'admin')
            self._delete("/api/auth")
            self.assertStatus('204 No Content')
            self.assertBody('')
            self.assertEquals(sess_mock.get(AuthTest.Auth.SESSION_KEY), None)

    def test_session_expire(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
            self.assertStatus('201 Created')
            self.assertEquals(sess_mock.get(AuthTest.Auth.SESSION_KEY),
                              'admin')
            self._post("/api/test/ping")
            self.assertStatus('200 OK')
            self.assertEquals(sess_mock.get(AuthTest.Auth.SESSION_KEY),
                              'admin')
            time.sleep(3)
            self._post("/api/test/ping")
            self.assertStatus('401 Unauthorized')
            self.assertEquals(sess_mock.get(AuthTest.Auth.SESSION_KEY), None)

    def test_unauthorized(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/test/ping")
            self.assertStatus('401 Unauthorized')
            self.assertEquals(sess_mock.get(AuthTest.Auth.SESSION_KEY), None)
