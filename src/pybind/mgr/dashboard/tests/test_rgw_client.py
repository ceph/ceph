# -*- coding: utf-8 -*-
import unittest

from .. import mgr
from ..services.rgw_client import RgwClient
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
        mgr.set_module_option('RGW_API_SSL_VERIFY', True)
        instance = RgwClient.admin_instance()
        self.assertTrue(instance.session.verify)

    def test_no_ssl_verify(self):
        mgr.set_module_option('RGW_API_SSL_VERIFY', False)
        instance = RgwClient.admin_instance()
        self.assertFalse(instance.session.verify)
