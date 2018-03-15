# -*- coding: utf-8 -*-

from __future__ import absolute_import

import time

from .helper import DashboardTestCase


class AuthTest(DashboardTestCase):
    def setUp(self):
        self.reset_session()
        self._ceph_cmd(['dashboard', 'set-session-expire', '2'])
        self._ceph_cmd(['dashboard', 'set-login-credentials', 'admin', 'admin'])

    def test_a_set_login_credentials(self):
        self._ceph_cmd(['dashboard', 'set-login-credentials', 'admin2', 'admin2'])
        self._post("/api/auth", {'username': 'admin2', 'password': 'admin2'})
        self.assertStatus(201)
        self.assertJsonBody({"username": "admin2"})

    def test_login_valid(self):
        self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
        self.assertStatus(201)
        self.assertJsonBody({"username": "admin"})

    def test_login_stay_signed_in(self):
        self._post("/api/auth", {
            'username': 'admin',
            'password': 'admin',
            'stay_signed_in': True})
        self.assertStatus(201)
        self.assertIn('session_id', self.cookies())
        for cookie in self.cookies():
            if cookie.name == 'session_id':
                self.assertIsNotNone(cookie.expires)

    def test_login_not_stay_signed_in(self):
        self._post("/api/auth", {
            'username': 'admin',
            'password': 'admin',
            'stay_signed_in': False})
        self.assertStatus(201)
        self.assertIn('session_id', self.cookies())
        for cookie in self.cookies():
            if cookie.name == 'session_id':
                self.assertIsNone(cookie.expires)

    def test_login_invalid(self):
        self._post("/api/auth", {'username': 'admin', 'password': 'inval'})
        self.assertStatus(403)
        self.assertJsonBody({"detail": "Invalid credentials"})

    def test_logout(self):
        self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
        self._delete("/api/auth")
        self.assertStatus(204)
        self.assertBody('')
        self._get("/api/host")
        self.assertStatus(401)

    def test_session_expire(self):
        self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
        self.assertStatus(201)
        self._get("/api/host")
        self.assertStatus(200)
        time.sleep(3)
        self._get("/api/host")
        self.assertStatus(401)

    def test_unauthorized(self):
        self._get("/api/host")
        self.assertStatus(401)
        self._get("/api")
        self.assertStatus(401)
