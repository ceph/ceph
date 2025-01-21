# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

from __future__ import absolute_import

import time

from .helper import DashboardTestCase


class MotdTest(DashboardTestCase):
    @classmethod
    def tearDownClass(cls):
        cls._ceph_cmd(['dashboard', 'motd', 'clear'])
        super(MotdTest, cls).tearDownClass()

    def setUp(self):
        super(MotdTest, self).setUp()
        self._ceph_cmd(['dashboard', 'motd', 'clear'])

    def test_none(self):
        data = self._get('/ui-api/motd')
        self.assertStatus(200)
        self.assertIsNone(data)

    def test_set(self):
        self._ceph_cmd(['dashboard', 'motd', 'set', 'info', '0', 'foo bar baz'])
        data = self._get('/ui-api/motd')
        self.assertStatus(200)
        self.assertIsInstance(data, dict)

    def test_expired(self):
        self._ceph_cmd(['dashboard', 'motd', 'set', 'info', '2s', 'foo bar baz'])
        time.sleep(5)
        data = self._get('/ui-api/motd')
        self.assertStatus(200)
        self.assertIsNone(data)
