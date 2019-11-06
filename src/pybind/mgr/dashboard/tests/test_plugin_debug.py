# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import CLICommandTestMixin, ControllerTestCase


class TestPluginDebug(ControllerTestCase, CLICommandTestMixin):
    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        cls.setup_controllers([])

    def setUp(self):
        self.mock_kv_store()

    def test_debug_disabled(self):
        self.exec_cmd('debug', action='disable')

        self._get('/api/unexisting_controller')
        self.assertStatus(404)

        data = self.json_body()
        self.assertGreater(len(data), 0)
        self.assertNotIn('traceback', data)
        self.assertNotIn('version', data)
        self.assertIn('request_id', data)

    def test_debug_enabled(self):
        self.exec_cmd('debug', action='enable')

        self._get('/api/unexisting_controller')
        self.assertStatus(404)

        data = self.json_body()
        self.assertGreater(len(data), 0)
        self.assertIn('traceback', data)
        self.assertIn('version', data)
        self.assertIn('request_id', data)
