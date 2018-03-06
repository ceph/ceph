# -*- coding: utf-8 -*-
# pylint: disable=W0212
from __future__ import absolute_import

import os
import unittest

import requests


class ControllerTestCase(unittest.TestCase):
    DASHBOARD_HOST = os.environ.get('DASHBOARD_V2_HOST', "localhost")
    DASHBOARD_PORT = os.environ.get('DASHBOARD_V2_PORT', 8080)

    def __init__(self, *args, **kwargs):
        self.dashboard_host = kwargs.pop('dashboard_host') \
            if 'dashboard_host' in kwargs else self.DASHBOARD_HOST
        self.dashboard_port = kwargs.pop('dashboard_port') \
            if 'dashboard_port' in kwargs else self.DASHBOARD_PORT
        super(ControllerTestCase, self).__init__(*args, **kwargs)
        self._session = requests.Session()
        self._resp = None

    def _request(self, url, method, data=None):
        url = "http://{}:{}{}".format(self.dashboard_host, self.dashboard_port,
                                      url)
        if method == 'GET':
            self._resp = self._session.get(url)
            return self._resp.json()
        elif method == 'POST':
            self._resp = self._session.post(url, json=data)
        elif method == 'DELETE':
            self._resp = self._session.delete(url, json=data)
        elif method == 'PUT':
            self._resp = self._session.put(url, json=data)
        return None

    def _get(self, url):
        return self._request(url, 'GET')

    def _post(self, url, data=None):
        self._request(url, 'POST', data)

    def _delete(self, url, data=None):
        self._request(url, 'DELETE', data)

    def _put(self, url, data=None):
        self._request(url, 'PUT', data)

    def cookies(self):
        return self._resp.cookies

    def jsonBody(self):
        return self._resp.json()

    def reset_session(self):
        self._session = requests.Session()

    def assertJsonBody(self, data):
        body = self._resp.json()
        self.assertEqual(body, data)

    def assertBody(self, body):
        self.assertEqual(self._resp.text, body)

    def assertStatus(self, status):
        self.assertEqual(self._resp.status_code, status)
