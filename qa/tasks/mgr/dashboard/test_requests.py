# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import DashboardTestCase


class RequestsTest(DashboardTestCase):
    def test_gzip(self):
        self._get('/api/summary')
        self.assertHeaders({
            'Content-Encoding': 'gzip',
            'Content-Type': 'application/json',
        })

    def test_force_no_gzip(self):
        self._get('/api/summary', params=dict(
            headers={'Accept-Encoding': 'identity'}
        ))
        self.assertNotIn('Content-Encoding', self._resp.headers)
        self.assertHeaders({
            'Content-Type': 'application/json',
        })
