# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import ControllerTestCase, authenticate


class PerfCountersControllerTest(ControllerTestCase):

    @authenticate
    def test_perf_counters_list(self):
        data = self._get('/api/perf_counters')
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        self.assertIn('mon.a', data)
        self.assertIn('mon.b', data)
        self.assertIn('mon.c', data)
        self.assertIn('osd.0', data)
        self.assertIn('osd.1', data)
        self.assertIn('osd.2', data)

    @authenticate
    def test_perf_counters_mon_a_get(self):
        data = self._get('/api/perf_counters/mon/a')
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        self.assertEqual('mon', data['service']['type'])
        self.assertEqual('a', data['service']['id'])
        self.assertIsInstance(data['counters'], list)
        self.assertGreater(len(data['counters']), 0)
        counter = data['counters'][0]
        self.assertIsInstance(counter, dict)
        self.assertIn('description', counter)
        self.assertIn('name', counter)
        self.assertIn('unit', counter)
        self.assertIn('value', counter)
