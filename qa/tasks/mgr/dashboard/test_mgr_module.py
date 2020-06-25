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

    __options_schema = JObj({
        'name': str,
        'type': str,
        'level': str,
        'flags': int,
        'default_value': JAny(none=True),
        'min': JAny(none=False),
        'max': JAny(none=False),
        'enum_allowed': JList(str),
        'desc': str,
        'long_desc': str,
        'tags': JList(str),
        'see_also': JList(str)
    })


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
                            'default_value': JAny(none=True),
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
                            'default_value': JAny(none=True),
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

    def test_module_options(self):
        data = self._get('/api/mgr/module/telemetry/options')
        self.assertStatus(200)
        schema = JObj({
            'channel_basic': self.__options_schema,
            'channel_crash': self.__options_schema,
            'channel_device': self.__options_schema,
            'channel_ident': self.__options_schema,
            'contact': self.__options_schema,
            'description': self.__options_schema,
            'device_url': self.__options_schema,
            'enabled': self.__options_schema,
            'interval': self.__options_schema,
            'last_opt_revision': self.__options_schema,
            'leaderboard': self.__options_schema,
            'log_level': self.__options_schema,
            'log_to_cluster': self.__options_schema,
            'log_to_cluster_level': self.__options_schema,
            'log_to_file': self.__options_schema,
            'organization': self.__options_schema,
            'proxy': self.__options_schema,
            'url': self.__options_schema
        })
        self.assertSchema(data, schema)

    def test_module_enable(self):
        self._post('/api/mgr/module/telemetry/enable')
        self.assertStatus(200)

    def test_disable(self):
        self._post('/api/mgr/module/iostat/disable')
        self.assertStatus(200)

    def test_put(self):
        self.set_config_key('config/mgr/mgr/iostat/log_level', 'critical')
        self.set_config_key('config/mgr/mgr/iostat/log_to_cluster', 'False')
        self.set_config_key('config/mgr/mgr/iostat/log_to_cluster_level', 'info')
        self.set_config_key('config/mgr/mgr/iostat/log_to_file', 'True')
        self._put(
            '/api/mgr/module/iostat',
            data={
                'config': {
                    'log_level': 'debug',
                    'log_to_cluster': True,
                    'log_to_cluster_level': 'warning',
                    'log_to_file': False
                }
            })
        self.assertStatus(200)
        data = self._get('/api/mgr/module/iostat')
        self.assertStatus(200)
        self.assertEqual(data['log_level'], 'debug')
        self.assertTrue(data['log_to_cluster'])
        self.assertEqual(data['log_to_cluster_level'], 'warning')
        self.assertFalse(data['log_to_file'])
