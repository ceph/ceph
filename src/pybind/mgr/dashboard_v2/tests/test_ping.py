# -*- coding: utf-8 -*-

from __future__ import absolute_import

from cherrypy.test import helper

from ..module import Module, cherrypy

class SimpleCPTest(helper.CPWebCase):
    @staticmethod
    def setup_server():
        module = Module('attic', None, None)
        cherrypy.tree.mount(Module.HelloWorld(module), "/api/hello")

    def test_ping(self):
        self.getPage("/api/hello/ping")
        self.assertStatus('200 OK')
        self.assertBody('"pong"')

