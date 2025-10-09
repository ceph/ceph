# -*- coding: utf-8 -*-

from __future__ import absolute_import

from . import DEFAULT_API_VERSION
from .helper import DashboardTestCase


class RequestsTest(DashboardTestCase):
    def test_gzip(self):
        self._get('/api/summary')
        self.assertHeaders({
            'Content-Encoding': 'gzip',
            'Content-Type': 'application/vnd.ceph.api.v{}+json'.format(DEFAULT_API_VERSION)
        })

    def test_force_no_gzip(self):
        self._get('/api/summary', headers={'Accept-Encoding': 'identity'})
        self.assertNotIn('Content-Encoding', self._resp.headers)
        self.assertHeaders({
            'Content-Type': 'application/vnd.ceph.api.v{}+json'.format(DEFAULT_API_VERSION)
        })

    def test_server(self):
        self._get('/api/summary')
        self.assertHeaders({
            'server': 'Ceph-Dashboard',
            'Content-Type': 'application/vnd.ceph.api.v{}+json'.format(DEFAULT_API_VERSION),
            'Content-Security-Policy': "frame-ancestors 'self';",
            'X-Content-Type-Options': 'nosniff',
            'Strict-Transport-Security': 'max-age=63072000; includeSubDomains; preload'
        })
