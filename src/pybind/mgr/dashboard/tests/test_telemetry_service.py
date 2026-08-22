# -*- coding: utf-8 -*-
import json
import unittest
from unittest.mock import MagicMock

from .. import mgr
from ..services.auth import AuthType
from ..services.telemetry import DashboardTelemetryService


class TestDashboardTelemetryServiceAuthenticationSignals(unittest.TestCase):

    def setUp(self):
        mgr.get_store = MagicMock(return_value=None)
        mgr.set_store = MagicMock()
        mgr.get_module_option = MagicMock(return_value=False)
        mgr.SSO_DB = None
        mgr.ACCESS_CTRL_DB = None

    def test_increment_login_count(self):
        mgr.get_store = MagicMock(return_value='5')

        DashboardTelemetryService.increment_login_count()

        mgr.set_store.assert_called_once_with(
            DashboardTelemetryService.KV_LOGIN_COUNT, '6'
        )

    def test_get_authentication_user_signals_returns_expected_values(self):
        mgr.get_module_option.return_value = True

        mgr.SSO_DB = MagicMock()
        mgr.SSO_DB.protocol = AuthType.SAML2

        mgr.ACCESS_CTRL_DB = MagicMock()
        mgr.ACCESS_CTRL_DB.users = {
            'user1': MagicMock(),
            'user2': MagicMock(),
        }

        mgr.get_store = MagicMock(
            side_effect=lambda key, *args: (
                None
                if key == DashboardTelemetryService.KV_AUTHENTICATION_USER_SIGNALS
                else '10'
            )
        )

        result = (
            DashboardTelemetryService
            ._detect_and_cache_authentication_user_signals()
        )

        self.assertEqual(
            result,
            {
                'oauth2_enabled': True,
                'saml2_enabled': True,
                'configured_users': 2,
                'login_count': 10,
            }
        )

        mgr.set_store.assert_called_once_with(
            DashboardTelemetryService.KV_AUTHENTICATION_USER_SIGNALS,
            json.dumps(result)
        )

    def test_authentication_user_signals_fallback_when_kv_store_returns_none(self):
        mgr.get_store = MagicMock(return_value=None)

        mgr.SSO_DB = None
        mgr.ACCESS_CTRL_DB = None

        result = (
            DashboardTelemetryService
            ._detect_and_cache_authentication_user_signals()
        )

        self.assertEqual(
            result,
            {
                'oauth2_enabled': False,
                'saml2_enabled': False,
                'configured_users': 0,
                'login_count': 0,
            }
        )

    def test_authentication_user_signals_fallback_when_kv_store_contains_invalid_json(self):
        mgr.get_store = MagicMock(return_value='invalid-json')

        mgr.SSO_DB = None
        mgr.ACCESS_CTRL_DB = None

        # Simulate the fallback performed by get_authentication_user_signals
        # after detecting invalid cached JSON.
        mgr.get_store.side_effect = [
            'invalid-json',
            '0',
        ]

        result = DashboardTelemetryService._detect_and_cache_authentication_user_signals()

        self.assertEqual(
            result,
            {
                'oauth2_enabled': False,
                'saml2_enabled': False,
                'configured_users': 0,
                'login_count': 0,
            }
        )


if __name__ == '__main__':
    unittest.main()