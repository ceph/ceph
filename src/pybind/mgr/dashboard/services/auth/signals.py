# -*- coding: utf-8 -*-

import logging
from typing import TypedDict

from ... import mgr
from ..telemetry import TelemetryService
from .auth import AuthType

logger = logging.getLogger(__name__)

LOGIN_METRIC = 'auth.login'


class AuthenticationUserSignals(TypedDict):  # pylint: disable=inherit-non-class
    oauth2_enabled: bool
    saml2_enabled: bool
    configured_users: int
    login_count: int


def collect_authentication_user_signals() -> AuthenticationUserSignals:
    signals: AuthenticationUserSignals = {
        'oauth2_enabled': False,
        'saml2_enabled': False,
        'configured_users': 0,
        'login_count': 0,
    }

    try:
        signals['oauth2_enabled'] = bool(
            mgr.get_module_option('sso_oauth2')
        )
        signals['saml2_enabled'] = (
            mgr.SSO_DB is not None
            and mgr.SSO_DB.protocol == AuthType.SAML2
        )
        signals['configured_users'] = (
            len(mgr.ACCESS_CTRL_DB.users)
            if mgr.ACCESS_CTRL_DB is not None else 0
        )
    except Exception:  # pylint: disable=broad-except
        logger.warning(
            'failed to collect authentication telemetry signals',
            exc_info=True
        )

    signals['login_count'] = TelemetryService.get(LOGIN_METRIC)
    return signals
