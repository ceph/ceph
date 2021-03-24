# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from ..controllers import ApiController, RESTController
from . import ControllerTestCase  # pylint: disable=no-name-in-module


@ApiController("/vtest", secure=False)
class VTest(RESTController):
    RESOURCE_ID = "vid"

    def list(self):
        return {'version': ""}

    def get(self):
        return {'version': ""}

    @RESTController.Collection('GET', version="1.0")
    def vmethod(self):
        return {'version': '1.0'}

    @RESTController.Collection('GET', version="2.0")
    def vmethodv2(self):
        return {'version': '2.0'}


class RESTVersioningTest(ControllerTestCase, unittest.TestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([VTest], "/test")

    def test_v1(self):
        for (version, expected_status) in [
                ("1.0", 200),
                ("2.0", 415)
        ]:
            with self.subTest(version=version):
                self._get('/test/api/vtest/vmethod', version=version)
                self.assertStatus(expected_status)

    def test_v2(self):
        for (version, expected_status) in [
                ("2.0", 200),
                ("1.0", 415)
        ]:
            with self.subTest(version=version):
                self._get('/test/api/vtest/vmethodv2', version=version)
                self.assertStatus(expected_status)
