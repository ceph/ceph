# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods
import unittest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch  # type: ignore

from ..services.rgw_client import RgwClient, _parse_frontend_config
from ..settings import Settings
from . import KVStoreMockMixin


class RgwClientTest(unittest.TestCase, KVStoreMockMixin):
    def setUp(self):
        RgwClient._user_instances.clear()  # pylint: disable=protected-access
        self.mock_kv_store()
        self.CONFIG_KEY_DICT.update({
            'RGW_API_ACCESS_KEY': 'klausmustermann',
            'RGW_API_SECRET_KEY': 'supergeheim',
            'RGW_API_HOST': 'localhost',
            'RGW_API_USER_ID': 'rgwadmin'
        })

    def test_ssl_verify(self):
        Settings.RGW_API_SSL_VERIFY = True
        instance = RgwClient.admin_instance()
        self.assertTrue(instance.session.verify)

    def test_no_ssl_verify(self):
        Settings.RGW_API_SSL_VERIFY = False
        instance = RgwClient.admin_instance()
        self.assertFalse(instance.session.verify)

    @patch.object(RgwClient, '_get_daemon_zone_info')
    def test_get_placement_targets_from_default_zone(self, zone_info):
        zone_info.return_value = {
            'placement_pools': [
                {
                    'key': 'default-placement',
                    'val': {
                        'index_pool': 'default.rgw.buckets.index',
                        'storage_classes': {
                            'STANDARD': {
                                'data_pool': 'default.rgw.buckets.data'
                            }
                        },
                        'data_extra_pool': 'default.rgw.buckets.non-ec',
                        'index_type': 0
                    }
                }
            ],
            'realm_id': ''
        }

        instance = RgwClient.admin_instance()
        expected_result = {
            'zonegroup': 'default',
            'placement_targets': [
                {
                    'name': 'default-placement',
                    'data_pool': 'default.rgw.buckets.data'
                }
            ]
        }
        self.assertEqual(expected_result, instance.get_placement_targets())

    @patch.object(RgwClient, '_get_daemon_zone_info')
    @patch.object(RgwClient, '_get_daemon_zonegroup_map')
    def test_get_placement_targets_from_realm_zone(self, zonegroup_map, zone_info):
        zone_info.return_value = {
            'id': 'a0df30ea-4b5b-4830-b143-2bedf684663d',
            'placement_pools': [
                {
                    'key': 'default-placement',
                    'val': {
                        'index_pool': 'default.rgw.buckets.index',
                        'storage_classes': {
                            'STANDARD': {
                                'data_pool': 'default.rgw.buckets.data'
                            }
                        }
                    }
                }
            ],
            'realm_id': 'b5a25d1b-e7ed-4fe5-b461-74f24b8e759b'
        }

        zonegroup_map.return_value = [
            {
                'api_name': 'zonegroup1-realm1',
                'zones': [
                    {
                        'id': '2ef7d0ef-7616-4e9c-8553-b732ebf0592b'
                    },
                    {
                        'id': 'b1d15925-6c8e-408e-8485-5a62cbccfe1f'
                    }
                ]
            },
            {
                'api_name': 'zonegroup2-realm1',
                'zones': [
                    {
                        'id': '645f0f59-8fcc-4e11-95d5-24f289ee8e25'
                    },
                    {
                        'id': 'a0df30ea-4b5b-4830-b143-2bedf684663d'
                    }
                ]
            }
        ]

        instance = RgwClient.admin_instance()
        expected_result = {
            'zonegroup': 'zonegroup2-realm1',
            'placement_targets': [
                {
                    'name': 'default-placement',
                    'data_pool': 'default.rgw.buckets.data'
                }
            ]
        }
        self.assertEqual(expected_result, instance.get_placement_targets())

    @patch.object(RgwClient, '_get_realms_info')
    def test_get_realms(self, realms_info):
        realms_info.side_effect = [
            {
                'default_info': '51de8373-bc24-4f74-a9b7-8e9ef4cb71f7',
                'realms': [
                    'realm1',
                    'realm2'
                ]
            },
            {}
        ]
        instance = RgwClient.admin_instance()

        self.assertEqual(['realm1', 'realm2'], instance.get_realms())
        self.assertEqual([], instance.get_realms())


class RgwClientHelperTest(unittest.TestCase):
    def test_parse_frontend_config_1(self):
        self.assertEqual(_parse_frontend_config('beast port=8000'), (8000, False))

    def test_parse_frontend_config_2(self):
        self.assertEqual(_parse_frontend_config('beast port=80 port=8000'), (80, False))

    def test_parse_frontend_config_3(self):
        self.assertEqual(_parse_frontend_config('beast ssl_port=443 port=8000'), (443, True))

    def test_parse_frontend_config_4(self):
        self.assertEqual(_parse_frontend_config('beast endpoint=192.168.0.100:8000'), (8000, False))

    def test_parse_frontend_config_5(self):
        self.assertEqual(_parse_frontend_config('beast endpoint=[::1]'), (80, False))

    def test_parse_frontend_config_6(self):
        self.assertEqual(_parse_frontend_config(
            'beast ssl_endpoint=192.168.0.100:8443'), (8443, True))

    def test_parse_frontend_config_7(self):
        self.assertEqual(_parse_frontend_config('beast ssl_endpoint=192.168.0.100'), (443, True))

    def test_parse_frontend_config_8(self):
        self.assertEqual(_parse_frontend_config(
            'beast ssl_endpoint=[::1]:8443 endpoint=192.0.2.3:80'), (8443, True))

    def test_parse_frontend_config_9(self):
        self.assertEqual(_parse_frontend_config(
            'beast port=8080 endpoint=192.0.2.3:80'), (8080, False))

    def test_parse_frontend_config_10(self):
        self.assertEqual(_parse_frontend_config(
            'beast ssl_endpoint=192.0.2.3:8443 port=8080'), (8443, True))

    def test_parse_frontend_config_11(self):
        self.assertEqual(_parse_frontend_config('civetweb port=8000s'), (8000, True))

    def test_parse_frontend_config_12(self):
        self.assertEqual(_parse_frontend_config('civetweb port=443s port=8000'), (443, True))

    def test_parse_frontend_config_13(self):
        self.assertEqual(_parse_frontend_config('civetweb port=192.0.2.3:80'), (80, False))

    def test_parse_frontend_config_14(self):
        self.assertEqual(_parse_frontend_config('civetweb port=172.5.2.51:8080s'), (8080, True))

    def test_parse_frontend_config_15(self):
        self.assertEqual(_parse_frontend_config('civetweb port=[::]:8080'), (8080, False))

    def test_parse_frontend_config_16(self):
        self.assertEqual(_parse_frontend_config('civetweb port=ip6-localhost:80s'), (80, True))

    def test_parse_frontend_config_17(self):
        self.assertEqual(_parse_frontend_config('civetweb port=[2001:0db8::1234]:80'), (80, False))

    def test_parse_frontend_config_18(self):
        self.assertEqual(_parse_frontend_config('civetweb port=[::1]:8443s'), (8443, True))

    def test_parse_frontend_config_19(self):
        self.assertEqual(_parse_frontend_config('civetweb port=127.0.0.1:8443s+8000'), (8443, True))

    def test_parse_frontend_config_20(self):
        self.assertEqual(_parse_frontend_config('civetweb port=127.0.0.1:8080+443s'), (8080, False))

    def test_parse_frontend_config_21(self):
        with self.assertRaises(LookupError) as ctx:
            _parse_frontend_config('civetweb port=xyz')
        self.assertEqual(str(ctx.exception),
                         'Failed to determine RGW port from "civetweb port=xyz"')

    def test_parse_frontend_config_22(self):
        with self.assertRaises(LookupError) as ctx:
            _parse_frontend_config('civetweb')
        self.assertEqual(str(ctx.exception), 'Failed to determine RGW port from "civetweb"')

    def test_parse_frontend_config_23(self):
        with self.assertRaises(LookupError) as ctx:
            _parse_frontend_config('mongoose port=8080')
        self.assertEqual(str(ctx.exception),
                         'Failed to determine RGW port from "mongoose port=8080"')
