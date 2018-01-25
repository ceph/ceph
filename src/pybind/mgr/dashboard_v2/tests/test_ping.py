# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import ApiControllerTestCase

class SimpleCPTest(ApiControllerTestCase):
    @staticmethod
    def setup_server():
        ApiControllerTestCase.setup_test(['Ping', 'Echo', 'EchoArgs'],
                                         authentication=False)

    def test_ping(self):
        self.getPage("/api/ping")
        self.assertStatus('200 OK')

    def test_echo(self):
        self._post("/api/echo2", {'msg': 'Hello World'})
        self.assertStatus('201 Created')

    def test_echo_args(self):
        self._post("/api/echo1", {'msg': 'Hello World'})
        self.assertStatus('201 Created')

