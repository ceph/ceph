# -*- coding: utf-8 -*-

from __future__ import absolute_import

import json

from cherrypy.test import helper

from ..controllers.auth import Auth
from ..module import Module, cherrypy
from ..tools import load_controller

class SimpleCPTest(helper.CPWebCase):
    @staticmethod
    def setup_server():

        cherrypy.tools.autenticate = cherrypy.Tool('before_handler',
                                                   Auth.check_auth)
        module = Module('attic', None, None)
        Ping = load_controller(module, 'Ping')
        Echo = load_controller(module, 'Echo')
        EchoArgs = load_controller(module, 'EchoArgs')
        cherrypy.tree.mount(Ping(), "/api/ping")
        cherrypy.tree.mount(Echo(), "/api/echo2")
        cherrypy.tree.mount(EchoArgs(), "/api/echo1")

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

    def test_ping(self):
        self.getPage("/api/ping")
        self.assertStatus('401 Unauthorized')

    def test_echo(self):
        self._post("/api/echo2", {'msg': 'Hello World'})
        self.assertStatus('201 Created')

    def test_echo_args(self):
        self._post("/api/echo1", {'msg': 'Hello World'})
        self.assertStatus('201 Created')

