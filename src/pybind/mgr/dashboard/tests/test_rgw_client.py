# -*- coding: utf-8 -*-
import unittest

from .. import mgr
from ..services.rgw_client import RgwClient


class RgwClientTest(unittest.TestCase):
    settings = {
        'RGW_API_ACCESS_KEY': 'klausmustermann',
        'RGW_API_SECRET_KEY': 'supergeheim',
        'RGW_API_HOST': 'localhost',
        'RGW_API_USER_ID': 'rgwadmin'
    }

    @classmethod
    def mock_set_config(cls, key, val):
        cls.settings[key] = val

    @classmethod
    def mock_get_config(cls, key, default):
        return cls.settings.get(key, default)

    @classmethod
    def setUpClass(cls):
        mgr.get_config.side_effect = cls.mock_get_config
        mgr.set_config.side_effect = cls.mock_set_config

    def setUp(self):
        RgwClient._user_instances.clear()  # pylint: disable=protected-access

    def test_ssl_verify(self):
        mgr.set_config('RGW_API_SSL_VERIFY', True)
        instance = RgwClient.admin_instance()
        self.assertTrue(instance.session.verify)

    def test_no_ssl_verify(self):
        mgr.set_config('RGW_API_SSL_VERIFY', False)
        instance = RgwClient.admin_instance()
        self.assertFalse(instance.session.verify)
