# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods
import json
import queue
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
        # Reset the class-level queue before each test
        DashboardTelemetryService._queue = queue.Queue(maxsize=10_000)

    def test_increment_login_count_enqueues_event(self):
        DashboardTelemetryService.increment_login_count()

        self.assertEqual(DashboardTelemetryService._queue.qsize(), 1)
        self.assertEqual(DashboardTelemetryService._queue.get_nowait(), 'login')

    def test_increment_login_count_does_not_block_when_queue_is_full(self):
        # Fill the queue to capacity
        for _ in range(10_000):
            DashboardTelemetryService._queue.put_nowait('login')

        # Should not raise, should not block
        DashboardTelemetryService.increment_login_count()

        self.assertEqual(DashboardTelemetryService._queue.qsize(), 10_000)

    def test_worker_increments_kv_store_on_login_event(self):
        mgr.get_store = MagicMock(return_value='5')

        DashboardTelemetryService._worker_process_one('login')

        mgr.set_store.assert_called_once_with(
            DashboardTelemetryService.KV_LOGIN_COUNT, '6'
        )

    def test_refresh_authentication_user_signals_returns_expected_values(self):
        mgr.get_module_option.return_value = True

        mgr.SSO_DB = MagicMock()
        mgr.SSO_DB.protocol = AuthType.SAML2

        mgr.ACCESS_CTRL_DB = MagicMock()
        mgr.ACCESS_CTRL_DB.users = {
            'user1': MagicMock(),
            'user2': MagicMock(),
        }

        mgr.get_store = MagicMock(return_value='10')

        result = DashboardTelemetryService.refresh_authentication_user_signals()

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

    def test_refresh_authentication_user_signals_fallback_when_kv_store_returns_none(self):
        mgr.get_store = MagicMock(return_value=None)

        result = DashboardTelemetryService.refresh_authentication_user_signals()

        self.assertEqual(
            result,
            {
                'oauth2_enabled': False,
                'saml2_enabled': False,
                'configured_users': 0,
                'login_count': 0,
            }
        )

    def test_refresh_authentication_user_signals_fallback_when_kv_store_contains_invalid_value(
        self
    ):
        mgr.get_store = MagicMock(return_value='not-an-int')

        result = DashboardTelemetryService.refresh_authentication_user_signals()

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
