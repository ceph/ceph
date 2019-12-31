# -*- coding: utf-8 -*-
import unittest

try:
    from mock import patch
except ImportError:
    from unittest.mock import patch

from ..services.rgw_client import RgwClient
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
