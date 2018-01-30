# -*- coding: utf-8 -*-

from __future__ import absolute_import

import time

import cherrypy
from cherrypy.lib.sessions import RamSession
from mock import patch

from .helper import ControllerTestCase
from ..controllers.auth import Auth
from ..tools import Session


class Ping(object):
    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    def ping(self):
        pass


class AuthTest(ControllerTestCase):
    @classmethod
    def setup_test(cls):
        cherrypy.tree.mount(Ping(), "/api/test",
                            config={'/': {'tools.authenticate.on': True}})
        cls._mgr_module.set_config('session-expire', '2')
        cls._mgr_module.set_config('username', 'admin')
        cls._mgr_module.set_config('password', Auth.password_hash('admin'))

    def setUp(self):
        self._mgr_module.set_config('session-expire', '2')
        self._mgr_module.set_config('username', 'admin')
        self._mgr_module.set_config('password', Auth.password_hash('admin'))

    def test_a_set_login_credentials(self):
        Auth.set_login_credentials('admin2', 'admin2')
        user = self._mgr_module.get_config('username')
        passwd = self._mgr_module.get_config('password')
        self.assertEqual(user, 'admin2')
        self.assertEqual(passwd, Auth.password_hash('admin2', passwd))

    def test_login_valid(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
            self.assertStatus('201 Created')
            self.assertJsonBody({"username": "admin"})
            self.assertEqual(sess_mock.get(Session.USERNAME), 'admin')

    def test_login_stay_signed_in(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {
                'username': 'admin',
                'password': 'admin',
                'stay_signed_in': True})
            self.assertStatus('201 Created')
            self.assertEqual(sess_mock.get(
                Session.EXPIRE_AT_BROWSER_CLOSE), False)
            for _, content in self.cookies:
                parts = map(str.strip, content.split(';'))
                parts = {k: v for k, v in (part.split('=') for part in parts)}
                if Session.NAME in parts:
                    self.assertIn('expires', parts)
                    self.assertIn('Max-Age', parts)

    def test_login_not_stay_signed_in(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {
                'username': 'admin',
                'password': 'admin',
                'stay_signed_in': False})
            self.assertStatus('201 Created')
            self.assertEqual(sess_mock.get(
                Session.EXPIRE_AT_BROWSER_CLOSE), True)
            for _, content in self.cookies:
                parts = map(str.strip, content.split(';'))
                parts = {k: v for k, v in (part.split('=') for part in parts)}
                if Session.NAME in parts:
                    self.assertNotIn('expires', parts)
                    self.assertNotIn('Max-Age', parts)

    def test_login_invalid(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'inval'})
            self.assertStatus('403 Forbidden')
            self.assertJsonBody({"detail": "Invalid credentials"})
            self.assertEqual(sess_mock.get(Session.USERNAME), None)

    def test_logout(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
            self.assertEqual(sess_mock.get(Session.USERNAME), 'admin')
            self._delete("/api/auth")
            self.assertStatus('204 No Content')
            self.assertBody('')
            self.assertEqual(sess_mock.get(Session.USERNAME), None)

    def test_session_expire(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
            self.assertStatus('201 Created')
            self.assertEqual(sess_mock.get(Session.USERNAME), 'admin')
            self._post("/api/test/ping")
            self.assertStatus('200 OK')
            self.assertEqual(sess_mock.get(Session.USERNAME), 'admin')
            time.sleep(3)
            self._post("/api/test/ping")
            self.assertStatus('401 Unauthorized')
            self.assertEqual(sess_mock.get(Session.USERNAME), None)

    def test_unauthorized(self):
        sess_mock = RamSession()
        with patch('cherrypy.session', sess_mock, create=True):
            self._post("/api/test/ping")
            self.assertStatus('401 Unauthorized')
            self.assertEqual(sess_mock.get(Session.USERNAME), None)
