# -*- coding: utf-8 -*-

from __future__ import absolute_import

import json

import cherrypy
from cherrypy.test import helper

from ..module import Module
from ..tools import load_controller


class ApiControllerTestCase(helper.CPWebCase):
    @staticmethod
    def setup_test(controllers, authentication=True):
        module = Module('dashboard', None, None)
        ApiControllerTestCase._mgr_module = module
        for ctrl in controllers:
            cls = load_controller(module, ctrl)
            if not authentication:
                cls._cp_config['tools.autenticate.on'] = False
            cherrypy.tree.mount(cls(), '/api/{}'.format(cls._cp_path_))

    def _request(self, url, method, data=None):
        if not data:
            b = None
            h = None
        else:
            b = json.dumps(data)
            h = [('Content-Type', 'application/json'),
                 ('Content-Length', str(len(b)))]
        self.getPage(url, method=method, body=b, headers=h)

    def _get(self, url):
        self._request(url, 'GET')

    def _post(self, url, data=None):
        self._request(url, 'POST', data)

    def _delete(self, url, data=None):
        self._request(url, 'DELETE', data)

    def _put(self, url, data=None):
        self._request(url, 'PUT', data)
