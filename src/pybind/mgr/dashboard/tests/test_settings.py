# -*- coding: utf-8 -*-
from __future__ import absolute_import

import errno
import unittest
from . import KVStoreMockMixin, ControllerTestCase
from .. import settings
from ..controllers.settings import Settings as SettingsController
from ..settings import Settings, handle_option_command


class SettingsTest(unittest.TestCase, KVStoreMockMixin):
    @classmethod
    def setUpClass(cls):
        # pylint: disable=protected-access
        settings.Options.GRAFANA_API_HOST = ('localhost', str)
        settings.Options.GRAFANA_API_PORT = (3000, int)
        settings.Options.GRAFANA_ENABLED = (False, bool)
        settings._OPTIONS_COMMAND_MAP = settings._options_command_map()

    def setUp(self):
        self.mock_kv_store()
        if Settings.GRAFANA_API_HOST != 'localhost':
            Settings.GRAFANA_API_HOST = 'localhost'
        if Settings.GRAFANA_API_PORT != 3000:
            Settings.GRAFANA_API_PORT = 3000

    def test_get_setting(self):
        self.assertEqual(Settings.GRAFANA_API_HOST, 'localhost')
        self.assertEqual(Settings.GRAFANA_API_PORT, 3000)
        self.assertEqual(Settings.GRAFANA_ENABLED, False)

    def test_set_setting(self):
        Settings.GRAFANA_API_HOST = 'grafanahost'
        self.assertEqual(Settings.GRAFANA_API_HOST, 'grafanahost')

        Settings.GRAFANA_API_PORT = 50
        self.assertEqual(Settings.GRAFANA_API_PORT, 50)

        Settings.GRAFANA_ENABLED = True
        self.assertEqual(Settings.GRAFANA_ENABLED, True)

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

    def test_reset_cmd(self):
        r, out, err = handle_option_command(
            {'prefix': 'dashboard reset-grafana-enabled'}
        )
        self.assertEqual(r, 0)
        self.assertEqual(out, 'Option {} reset to default value "{}"'.format(
            'GRAFANA_ENABLED', Settings.GRAFANA_ENABLED))
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


class SettingsControllerTest(ControllerTestCase, KVStoreMockMixin):
    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access

        SettingsController._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([SettingsController])

    def setUp(self):
        self.mock_kv_store()

    def test_settings_list(self):
        self._get('/api/settings')
        data = self.jsonBody()
        self.assertTrue(len(data) > 0)
        self.assertStatus(200)
        self.assertIn('default', data[0].keys())
        self.assertIn('type', data[0].keys())
        self.assertIn('name', data[0].keys())
        self.assertIn('value', data[0].keys())

    def test_rgw_daemon_get(self):
        self._get('/api/settings/grafana-api-username')
        self.assertStatus(200)
        self.assertJsonBody({
            u'default': u'admin',
            u'type': u'str',
            u'name': u'GRAFANA_API_USERNAME',
            u'value': u'admin',
        })

    def test_set(self):
        self._put('/api/settings/GRAFANA_API_USERNAME', {'value': 'foo'},)
        self.assertStatus(200)

        self._get('/api/settings/GRAFANA_API_USERNAME')
        self.assertStatus(200)
        self.assertInJsonBody('default')
        self.assertInJsonBody('type')
        self.assertInJsonBody('name')
        self.assertInJsonBody('value')
        self.assertEqual(self.jsonBody()['value'], 'foo')

    def test_bulk_set(self):
        self._put('/api/settings', {
            'GRAFANA_API_USERNAME': 'foo',
            'GRAFANA_API_HOST': 'somehost',
        })
        self.assertStatus(200)

        self._get('/api/settings/grafana-api-username')
        self.assertStatus(200)
        body = self.jsonBody()
        self.assertEqual(body['value'], 'foo')

        self._get('/api/settings/grafana-api-username')
        self.assertStatus(200)
        self.assertEqual(self.jsonBody()['value'], 'foo')

        self._get('/api/settings/grafana-api-host')
        self.assertStatus(200)
        self.assertEqual(self.jsonBody()['value'], 'somehost')
