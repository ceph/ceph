# -*- coding: utf-8 -*-

from __future__ import absolute_import

import unittest

from . import DEFAULT_API_VERSION
from .helper import DashboardTestCase


class VersionReqTest(DashboardTestCase, unittest.TestCase):
    def test_version(self):
        for (version, expected_status) in [
                (DEFAULT_API_VERSION, 200),
                (None, 415),
                ("99.99", 415)
        ]:
            with self.subTest(version=version):
                self._get('/api/summary', version=version)
                self.assertStatus(expected_status)
