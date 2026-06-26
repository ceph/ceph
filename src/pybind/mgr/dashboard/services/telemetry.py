# -*- coding: utf-8 -*-

import json
import logging
from typing import TypedDict

from .. import mgr
from ..plugins.ttl_cache import ttl_cache
from .auth import AuthType
from .sso import load_sso_db

logger = logging.getLogger('services.telemetry')


class AuthenticationUserSignals(TypedDict):
    oauth2_enabled: bool
    saml2_enabled: bool
    configured_users: int
    login_count: int


class DashboardTelemetryService:

    KV_AUTHENTICATION_USER_SIGNALS = (
        'telemetry/metrics/authentication_user_signals'
    )
    KV_LOGIN_COUNT = 'telemetry/login_count'

    @classmethod
    @ttl_cache(300, label='authentication_user_signals')
    def get_authentication_user_signals(cls) -> AuthenticationUserSignals:
        auth_raw = mgr.get_store(cls.KV_AUTHENTICATION_USER_SIGNALS)

        if not auth_raw:
            return cls._detect_and_cache_authentication_user_signals()

        try:
            return json.loads(auth_raw)
        except (TypeError, json.JSONDecodeError):
            logger.warning(
                'telemetry: invalid cached authentication data; recomputing'
            )
            return cls._detect_and_cache_authentication_user_signals()

    @classmethod
    def _detect_and_cache_authentication_user_signals(
        cls
    ) -> AuthenticationUserSignals:

        oauth2_enabled = False
        saml2_enabled = False
        configured_users = 0

        try:
            oauth2_enabled = bool(
                mgr.get_module_option('sso_oauth2')
            )
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                'telemetry: failed to determine OAuth2 status: %s', e
            )

        try:
            load_sso_db()
            saml2_enabled = (
                getattr(mgr.SSO_DB, 'protocol', None) == AuthType.SAML2
            )
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                'telemetry: failed to determine SAML2 status: %s', e
            )

        try:
            from .access_control import load_access_control_db
            load_access_control_db()
            configured_users = len(mgr.ACCESS_CTRL_DB.users)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                'telemetry: failed to determine configured users: %s', e
            )

        result: AuthenticationUserSignals = {
            'oauth2_enabled': oauth2_enabled,
            'saml2_enabled': saml2_enabled,
            'configured_users': configured_users,
            'login_count': cls.get_login_count(),
        }

        mgr.set_store(
            cls.KV_AUTHENTICATION_USER_SIGNALS,
            json.dumps(result)
        )

        return result

    @classmethod
    def refresh_authentication_user_signals(
        cls
    ) -> AuthenticationUserSignals:
        return cls._detect_and_cache_authentication_user_signals()

    @staticmethod
    def get_login_count() -> int:
        """
        Read the persistent login counter.

        Returns cumulative login count stored in the manager KV store.
        """
        try:
            count_raw = mgr.get_store(
                DashboardTelemetryService.KV_LOGIN_COUNT, '0'
            )
            return int(count_raw)
        except (ValueError, TypeError) as e:
            logger.warning('telemetry: failed to read login count: %s', e)
            return 0

    @classmethod
    def increment_login_count(cls):
        """
        Increment the persistent login counter.
        Called after successful authentication.

        Note: This uses a read-modify-write pattern which may lose
        increments during concurrent logins. Minor race is acceptable
        since this is telemetry and login_count is an approximate
        aggregate metric. Telemetry should never break dashboard
        functionality, hence the broad exception handling.
        """
        try:
            count = cls.get_login_count()
            count += 1
            mgr.set_store(cls.KV_LOGIN_COUNT, str(count))
            cls._detect_and_cache_authentication_user_signals()
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('telemetry: failed to increment login count: %s', e)
