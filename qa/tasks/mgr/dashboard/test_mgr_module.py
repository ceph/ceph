# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import requests

from .helper import DashboardTestCase, JAny, JObj, JList, JLeaf

logger = logging.getLogger(__name__)


class MgrModuleTestCase(DashboardTestCase):
    MGRS_REQUIRED = 1

    def wait_until_rest_api_accessible(self):
        """
        Wait until the REST API is accessible.
        """

        def _check_connection():
            try:
                # Try reaching an API endpoint successfully.
                self._get('/api/mgr/module')
                if self._resp.status_code == 200:
                    return True
            except requests.ConnectionError:
                pass
            return False

        self.wait_until_true(_check_connection, timeout=30)


class MgrModuleTest(MgrModuleTestCase):
    def test_list_disabled_module(self):
        self._ceph_cmd(['mgr', 'module', 'disable', 'iostat'])
        self.wait_until_rest_api_accessible()
        data = self._get('/api/mgr/module')
        self.assertStatus(200)
        self.assertSchema(
            data,
            JList(
                JObj(sub_elems={
                    'name': JLeaf(str),
                    'enabled': JLeaf(bool),
                    'always_on': JLeaf(bool),
                    'options': JObj(
                        {},
                        allow_unknown=True,
                        unknown_schema=JObj({
                            'name': str,
                            'type': str,
                            'level': str,
                            'flags': int,
                            'default_value': JAny(none=False),
                            'min': JAny(none=False),
                            'max': JAny(none=False),
                            'enum_allowed': JList(str),
                            'see_also': JList(str),
                            'desc': str,
                            'long_desc': str,
                            'tags': JList(str)
                        }))
                })))
        module_info = self.find_object_in_list('name', 'iostat', data)
        self.assertIsNotNone(module_info)
        self.assertFalse(module_info['enabled'])

    def test_list_enabled_module(self):
        self._ceph_cmd(['mgr', 'module', 'enable', 'iostat'])
        self.wait_until_rest_api_accessible()
        data = self._get('/api/mgr/module')
        self.assertStatus(200)
        self.assertSchema(
            data,
            JList(
                JObj(sub_elems={
                    'name': JLeaf(str),
                    'enabled': JLeaf(bool),
                    'always_on': JLeaf(bool),
                    'options': JObj(
                        {},
                        allow_unknown=True,
                        unknown_schema=JObj({
                            'name': str,
                            'type': str,
                            'level': str,
                            'flags': int,
                            'default_value': JAny(none=False),
                            'min': JAny(none=False),
                            'max': JAny(none=False),
                            'enum_allowed': JList(str),
                            'see_also': JList(str),
                            'desc': str,
                            'long_desc': str,
                            'tags': JList(str)
                        }))
                })))
        module_info = self.find_object_in_list('name', 'iostat', data)
        self.assertIsNotNone(module_info)
        self.assertTrue(module_info['enabled'])


class MgrModuleTelemetryTest(MgrModuleTestCase):
    def test_get(self):
        data = self._get('/api/mgr/module/telemetry')
        self.assertStatus(200)
        self.assertSchema(
            data,
            JObj(
                allow_unknown=True,
                sub_elems={
                    'channel_basic': bool,
                    'channel_ident': bool,
                    'channel_crash': bool,
                    'channel_device': bool,
                    'contact': str,
                    'description': str,
                    'enabled': bool,
                    'interval': int,
                    'last_opt_revision': int,
                    'leaderboard': bool,
                    'organization': str,
                    'proxy': str,
                    'url': str
                }))

    def test_put(self):
        self.set_config_key('config/mgr/mgr/telemetry/contact', '')
        self.set_config_key('config/mgr/mgr/telemetry/description', '')
        self.set_config_key('config/mgr/mgr/telemetry/enabled', 'True')
        self.set_config_key('config/mgr/mgr/telemetry/interval', '72')
        self.set_config_key('config/mgr/mgr/telemetry/leaderboard', 'False')
        self.set_config_key('config/mgr/mgr/telemetry/organization', '')
        self.set_config_key('config/mgr/mgr/telemetry/proxy', '')
        self.set_config_key('config/mgr/mgr/telemetry/url', '')
        self._put(
            '/api/mgr/module/telemetry',
            data={
                'config': {
                    'contact': 'tux@suse.com',
                    'description': 'test',
                    'enabled': False,
                    'interval': 4711,
                    'leaderboard': True,
                    'organization': 'SUSE Linux',
                    'proxy': 'foo',
                    'url': 'https://foo.bar/report'
                }
            })
        self.assertStatus(200)
        data = self._get('/api/mgr/module/telemetry')
        self.assertStatus(200)
        self.assertEqual(data['contact'], 'tux@suse.com')
        self.assertEqual(data['description'], 'test')
        self.assertFalse(data['enabled'])
        self.assertEqual(data['interval'], 4711)
        self.assertTrue(data['leaderboard'])
        self.assertEqual(data['organization'], 'SUSE Linux')
        self.assertEqual(data['proxy'], 'foo')
        self.assertEqual(data['url'], 'https://foo.bar/report')
