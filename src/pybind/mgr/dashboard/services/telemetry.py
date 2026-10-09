# -*- coding: utf-8 -*-

import json
import logging
import queue
import threading
from typing import TypedDict

from .. import mgr

logger = logging.getLogger('services.telemetry')

_LOGIN_EVENT = 'login'


class AuthenticationUserSignals(TypedDict):  # pylint: disable=inherit-non-class
    oauth2_enabled: bool
    saml2_enabled: bool
    configured_users: int
    login_count: int


class DashboardTelemetryService:

    KV_AUTHENTICATION_USER_SIGNALS = (
        'telemetry/metrics/authentication_user_signals'
    )
    KV_LOGIN_COUNT = 'telemetry/login_count'

    # Bounded queue: drops telemetry rather than blocking the login path.
    _queue: queue.Queue = queue.Queue(maxsize=10_000)
    _worker_thread: threading.Thread = None  # type: ignore[assignment]
    _running: bool = False

    # ---------- lifecycle --------------------------------------------------

    @classmethod
    def start_worker(cls):
        """Start the background telemetry worker. Called once at module start."""
        cls._running = True
        cls._worker_thread = threading.Thread(
            target=cls._worker, daemon=True, name='telemetry-worker'
        )
        cls._worker_thread.start()

    @classmethod
    def stop_worker(cls):
        """Stop the background telemetry worker. Called once at module shutdown."""
        cls._running = False
        try:
            cls._queue.put_nowait(None)  # sentinel to unblock the worker
        except queue.Full:
            pass
        if cls._worker_thread is not None:
            cls._worker_thread.join(timeout=5)

    # ---------- worker loop ------------------------------------------------

    @classmethod
    def _worker_process_one(cls, event: str):
        """Process a single telemetry event. Extracted for testability."""
        if event == _LOGIN_EVENT:
            count = cls.get_login_count() + 1
            mgr.set_store(cls.KV_LOGIN_COUNT, str(count))

    @classmethod
    def _worker(cls):
        while cls._running:
            try:
                event = cls._queue.get(timeout=1)
            except queue.Empty:
                continue
            if event is None:  # sentinel — stop
                cls._queue.task_done()
                break
            try:
                cls._worker_process_one(event)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('failed to process telemetry event %r: %s', event, e)
            finally:
                cls._queue.task_done()

    # ---------- producers --------------------------------------------------

    @classmethod
    def increment_login_count(cls):
        """
        Enqueue a login event. Returns immediately; never blocks the login path.
        Drops the event silently when the queue is full.
        """
        try:
            cls._queue.put_nowait(_LOGIN_EVENT)
        except queue.Full:
            logger.warning('queue full; login event dropped')

    # ---------- refresh (called at startup / periodically) -----------------

    @classmethod
    def refresh_authentication_user_signals(
        cls
    ) -> AuthenticationUserSignals:
        return cls._detect_and_cache_authentication_user_signals()

    @classmethod
    def _detect_and_cache_authentication_user_signals(
        cls
    ) -> AuthenticationUserSignals:

        oauth2_enabled = False
        saml2_enabled = False
        configured_users = 0

        try:
            oauth2_enabled = bool(mgr.get_module_option('sso_oauth2'))
            saml2_enabled = (
                mgr.SSO_DB is not None
                and mgr.SSO_DB.protocol.value == 'saml2'
            )
            configured_users = (
                len(mgr.ACCESS_CTRL_DB.users)
                if mgr.ACCESS_CTRL_DB is not None
                else 0
            )
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('failed to detect authentication signals: %s', e)

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

    # ---------- helpers ----------------------------------------------------

    @classmethod
    def get_login_count(cls) -> int:
        try:
            count_raw = mgr.get_store(cls.KV_LOGIN_COUNT, '0')
            return int(count_raw)
        except (ValueError, TypeError) as e:
            logger.warning('failed to read login count: %s', e)
            return 0
