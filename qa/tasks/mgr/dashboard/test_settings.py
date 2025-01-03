# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import DashboardTestCase, JAny, JList, JObj


class SettingsTest(DashboardTestCase):
    def setUp(self):
        super(SettingsTest, self).setUp()
        self.settings = self._get('/api/settings')

    def tearDown(self):
        self._put(
            '/api/settings',
            {setting['name']: setting['value']
             for setting in self.settings})

    def test_list_settings(self):
        settings = self._get('/api/settings')
        self.assertGreater(len(settings), 10)
        self.assertSchema(
            settings,
            JList(
                JObj({
                    'default': JAny(none=False),
                    'name': str,
                    'type': str,
                    'value': JAny(none=False)
                })))
        self.assertStatus(200)

    def test_get_setting(self):
        setting = self._get('/api/settings/rgw-api-access-key')
        self.assertSchema(
            setting,
            JObj({
                'default': JAny(none=False),
                'name': str,
                'type': str,
                'value': JAny(none=False)
            }))
        self.assertStatus(200)

    def test_set_setting(self):
        self._put('/api/settings/rgw-api-access-key', {'value': 'foo'})
        self.assertStatus(200)

        value = self._get('/api/settings/rgw-api-access-key')['value']
        self.assertEqual('foo', value)

    def test_bulk_set(self):
        self._put('/api/settings', {
            'RGW_API_ACCESS_KEY': 'dummy-key',
            'RGW_API_SECRET_KEY': 'dummy-secret',
        })
        self.assertStatus(200)

        access_key = self._get('/api/settings/rgw-api-access-key')['value']
        self.assertStatus(200)
        self.assertEqual('dummy-key', access_key)

        secret_key = self._get('/api/settings/rgw-api-secret-key')['value']
        self.assertStatus(200)
        self.assertEqual('dummy-secret', secret_key)
