# -*- coding: utf-8 -*-

from __future__ import absolute_import

import time
from cherrypy.lib.sessions import RamSession
from cherrypy.test import helper
from mock import patch

from ..auth import Auth
from ..module import Module, cherrypy

class Ping(object):
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    def ping(self):
        pass

class AuthTest(helper.CPWebCase):
    @staticmethod
    def setup_server():
        module = Module('dashboard', None, None)
        auth = Auth(module)
        cherrypy.tools.autenticate = cherrypy.Tool('before_handler', auth.check_auth)
        cherrypy.tree.mount(auth, "/api/auth")
        cherrypy.tree.mount(Ping(), "/api/test",
                            config={'/': {'tools.autenticate.on': True}})
        module.set_localized_config('session-expire','2')
        module.set_localized_config('username','admin')
        module.set_localized_config('password',
            '$2b$12$KunrLI/uq7pqjvwUcAhIZu.B1dAGZ3liB8KFIJUOqZC.5/bEEmBQG')

    def test_login_valid(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self.getPage("/api/auth/login",
                         body="username=admin&password=admin",
                         method='POST')
            self.assertStatus('200 OK')
            self.assertBody('{"username": "admin"}')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), 'admin')

    def test_login_invalid(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self.getPage("/api/auth/login",
                         body="username=admin&password=invalid",
                         method='POST')
            self.assertStatus('403 Forbidden')
            self.assertBody('{"detail": "Invalid credentials"}')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), None)

    def test_logout(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self.getPage("/api/auth/login",
                         body="username=admin&password=admin",
                         method='POST')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), 'admin')
            self.getPage("/api/auth/logout", method='POST')
            self.assertStatus('200 OK')
            self.assertBody('')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), None)

    def test_session_expire(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self.getPage("/api/auth/login",
                         body="username=admin&password=admin",
                         method='POST')
            self.assertStatus('200 OK')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), 'admin')
            self.getPage("/api/test/ping", method='POST')
            self.assertStatus('200 OK')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), 'admin')
            time.sleep(3)
            self.getPage("/api/test/ping", method='POST')
            self.assertStatus('401 Unauthorized')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), None)

    def test_unauthorized(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self.getPage("/api/test/ping", method='POST')
            self.assertStatus('401 Unauthorized')
            self.assertEquals(sess_mock.get(Auth.SESSION_KEY), None)
