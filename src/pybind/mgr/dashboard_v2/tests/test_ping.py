# -*- coding: utf-8 -*-
# pylint: disable=W0212

from __future__ import absolute_import

from .helper import ControllerTestCase
from ..controllers.ping import Ping


class PingTest(ControllerTestCase):
    @classmethod
    def setup_test(cls):
        Ping._cp_config['tools.autenticate.on'] = False

    def test_ping(self):
        self.getPage("/api/ping")
        self.assertStatus('200 OK')

    def test_echo(self):
        self._post("/api/echo2", {'msg': 'Hello World'})
        self.assertStatus('201 Created')
        self.assertJsonBody({'echo': 'Hello World'})

    def test_echo_args(self):
        self._post("/api/echo1", {'msg': 'Hello World'})
        self.assertStatus('201 Created')
        self.assertJsonBody({'echo': 'Hello World'})
