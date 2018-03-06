# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase, authenticate


class MonitorTest(DashboardTestCase):
    @authenticate
    def test_monitor_default(self):
        data = self._get("/api/monitor")
        self.assertStatus(200)

        self.assertIn('mon_status', data)
        self.assertIn('in_quorum', data)
        self.assertIn('out_quorum', data)
        self.assertIsNotNone(data['mon_status'])
        self.assertIsNotNone(data['in_quorum'])
        self.assertIsNotNone(data['out_quorum'])
