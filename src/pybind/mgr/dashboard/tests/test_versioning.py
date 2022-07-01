# -*- coding: utf-8 -*-

import unittest

from ..controllers._api_router import APIRouter
from ..controllers._rest_controller import RESTController
from ..controllers._version import APIVersion
from ..tests import ControllerTestCase


@APIRouter("/vtest", secure=False)
class VTest(RESTController):
    RESOURCE_ID = "vid"

    @RESTController.MethodMap(version=APIVersion(0, 1))
    def list(self):
        return {'version': ""}

    def get(self):
        return {'version': ""}

    @RESTController.Collection('GET', version=APIVersion(1, 0))
    def vmethod(self):
        return {'version': '1.0'}

    @RESTController.Collection('GET', version=APIVersion(1, 1))
    def vmethodv1_1(self):
        return {'version': '1.1'}

    @RESTController.Collection('GET', version=APIVersion(2, 0))
    def vmethodv2(self):
        return {'version': '2.0'}


class RESTVersioningTest(ControllerTestCase, unittest.TestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([VTest], "/test")

    def test_list(self):
        for (version, expected_status) in [
                ((0, 1), 200),
                ((2, 0), 415)
        ]:
            with self.subTest(version=version):
                self._get('/test/api/vtest', version=APIVersion._make(version))
                self.assertStatus(expected_status)

    def test_v1(self):
        for (version, expected_status) in [
                ((1, 0), 200),
                ((2, 0), 415)
        ]:
            with self.subTest(version=version):
                self._get('/test/api/vtest/vmethod',
                          version=APIVersion._make(version))
                self.assertStatus(expected_status)

    def test_v2(self):
        for (version, expected_status) in [
                ((2, 0), 200),
                ((1, 0), 415)
        ]:
            with self.subTest(version=version):
                self._get('/test/api/vtest/vmethodv2',
                          version=APIVersion._make(version))
                self.assertStatus(expected_status)

    def test_backward_compatibility(self):
        for (version, expected_status) in [
                ((1, 1), 200),
                ((1, 0), 200),
                ((2, 0), 415)
        ]:
            with self.subTest(version=version):
                self._get('/test/api/vtest/vmethodv1_1',
                          version=APIVersion._make(version))
                self.assertStatus(expected_status)
