# -*- coding: utf-8 -*-
from __future__ import absolute_import

import errno
import unittest

from .. import mgr
from .. import settings
from ..settings import Settings, handle_option_command


class SettingsTest(unittest.TestCase):
    CONFIG_KEY_DICT = {}

    @classmethod
    def setUpClass(cls):
        # pylint: disable=protected-access
        settings.Options.GRAFANA_API_HOST = ('localhost', str)
        settings.Options.GRAFANA_API_PORT = (3000, int)
        settings._OPTIONS_COMMAND_MAP = settings._options_command_map()

    @classmethod
    def mock_set_config(cls, attr, val):
        cls.CONFIG_KEY_DICT[attr] = val

    @classmethod
    def mock_get_config(cls, attr, default):
        return cls.CONFIG_KEY_DICT.get(attr, default)

    def setUp(self):
        self.CONFIG_KEY_DICT.clear()
        mgr.set_config.side_effect = self.mock_set_config
        mgr.get_config.side_effect = self.mock_get_config
        if Settings.GRAFANA_API_HOST != 'localhost':
            Settings.GRAFANA_API_HOST = 'localhost'
        if Settings.GRAFANA_API_PORT != 3000:
            Settings.GRAFANA_API_PORT = 3000

    def test_get_setting(self):
        self.assertEqual(Settings.GRAFANA_API_HOST, 'localhost')

    def test_set_setting(self):
        Settings.GRAFANA_API_HOST = 'grafanahost'
        self.assertEqual(Settings.GRAFANA_API_HOST, 'grafanahost')

    def test_get_cmd(self):
        r, out, err = handle_option_command(
            {'prefix': 'dashboard get-grafana-api-port'})
        self.assertEqual(r, 0)
        self.assertEqual(out, '3000')
        self.assertEqual(err, '')

    def test_set_cmd(self):
        r, out, err = handle_option_command(
            {'prefix': 'dashboard set-grafana-api-port',
             'value': '4000'})
        self.assertEqual(r, 0)
        self.assertEqual(out, 'Option GRAFANA_API_PORT updated')
        self.assertEqual(err, '')

    def test_inv_cmd(self):
        r, out, err = handle_option_command(
            {'prefix': 'dashboard get-non-existent-option'})
        self.assertEqual(r, -errno.ENOSYS)
        self.assertEqual(out, '')
        self.assertEqual(err, "Command not found "
                              "'dashboard get-non-existent-option'")

    def test_sync(self):
        Settings.GRAFANA_API_PORT = 5000
        r, out, err = handle_option_command(
            {'prefix': 'dashboard get-grafana-api-port'})
        self.assertEqual(r, 0)
        self.assertEqual(out, '5000')
        self.assertEqual(err, '')
        r, out, err = handle_option_command(
            {'prefix': 'dashboard set-grafana-api-host',
             'value': 'new-local-host'})
        self.assertEqual(r, 0)
        self.assertEqual(out, 'Option GRAFANA_API_HOST updated')
        self.assertEqual(err, '')
        self.assertEqual(Settings.GRAFANA_API_HOST, 'new-local-host')

    def test_attribute_error(self):
        with self.assertRaises(AttributeError) as ctx:
            _ = Settings.NON_EXISTENT_OPTION

        self.assertEqual(str(ctx.exception),
                         "type object 'Options' has no attribute 'NON_EXISTENT_OPTION'")
