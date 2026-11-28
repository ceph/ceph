# -*- coding: utf-8 -*-
# pylint: disable=protected-access
import json
import queue
import threading
import unittest
from unittest.mock import MagicMock, patch

import cherrypy

from .. import mgr
from ..module import Module
from ..services.auth import AuthType
from ..services.auth.signals import LOGIN_METRIC, collect_authentication_user_signals
from ..services.telemetry import TelemetryService as T
from ..services.telemetry import telemetry


class TestTelemetryService(unittest.TestCase):  # pylint: disable=too-many-public-methods
    def setUp(self):
        self.store = {}
        for attr, mock in (
            ('get_store', MagicMock(side_effect=lambda k, d=None: self.store.get(k, d))),
            ('set_store', MagicMock(side_effect=self.store.__setitem__)),
        ):
            patcher = patch.object(mgr, attr, mock)
            patcher.start()
            self.addCleanup(patcher.stop)
        self._reset_service()
        T._accepting = True
        self.addCleanup(self._reset_service)
        self.addCleanup(self._stop_worker)

    @staticmethod
    def _reset_service():
        T._queue = queue.Queue(maxsize=T.QUEUE_SIZE)
        T._pending = {}
        T._known = {}
        T._dropped = 0
        T._accepting = False
        T._thread = None
        T._stop_event = threading.Event()

    @staticmethod
    def _stop_worker():
        # Never leak a real worker thread, whatever the test did.
        thread = T._thread
        T._stop_event.set()
        if isinstance(thread, threading.Thread):
            thread.join(T.STOP_TIMEOUT)
            assert not thread.is_alive(), 'telemetry worker leaked'

    def _stored(self):
        return json.loads(self.store[T.KV_COUNTERS])

    def test_record_enqueues_without_io(self):
        T.record('m')
        self.assertEqual(T._queue.get_nowait(), ('m', 1))
        mgr.get_store.assert_not_called()
        mgr.set_store.assert_not_called()

    def test_record_ignored_when_not_accepting(self):
        T._accepting = False
        T.record('m')
        self.assertTrue(T._queue.empty())

    def test_record_never_blocks_when_full_and_counts_drops(self):
        for _ in range(T.QUEUE_SIZE):
            T.record('m')
        T.record('m')
        self.assertEqual(T._queue.qsize(), T.QUEUE_SIZE)
        self.assertEqual(T._dropped, 1)

    def test_worker_drains_whole_queue_after_stop_requested(self):
        for _ in range(500):
            T.record('m')
        stop = threading.Event()
        stop.set()
        T._worker(stop)
        self.assertEqual(T._pending, {'m': 500})

    def test_flush_is_additive(self):
        T._aggregate('m', 3)
        T.flush()
        T._aggregate('m', 1)
        T.flush()
        self.assertEqual(self._stored(), {'m': 4})
        self.assertEqual(mgr.set_store.call_count, 2)

    def test_flush_without_events_does_not_write(self):
        self.assertTrue(T.flush())
        mgr.set_store.assert_not_called()

    def test_failed_read_never_overwrites_existing_count(self):
        self.store[T.KV_COUNTERS] = 'not-json'
        T._aggregate('m', 1)
        self.assertFalse(T.flush())
        mgr.set_store.assert_not_called()
        self.assertEqual(T._pending, {'m': 1})
        self.store[T.KV_COUNTERS] = json.dumps({'m': 10})
        self.assertTrue(T.flush())
        self.assertEqual(self._stored(), {'m': 11})

    def test_failed_write_keeps_pending_without_double_count(self):
        T._aggregate('m', 2)
        mgr.set_store.side_effect = RuntimeError('boom')
        self.assertFalse(T.flush())
        self.assertEqual(T._pending, {'m': 2})
        mgr.set_store.side_effect = self.store.__setitem__
        T.flush()
        T.flush()
        self.assertEqual(self._stored(), {'m': 2})

    def test_events_recorded_during_flush_are_not_lost(self):
        def write_and_record(key, value):
            self.store[key] = value
            T._aggregate('m', 1)  # arrives mid-flush
        mgr.set_store.side_effect = write_and_record
        T._aggregate('m', 5)
        T.flush()
        self.assertEqual(self._stored(), {'m': 5})
        self.assertEqual(T._pending, {'m': 1})
        T.flush()
        self.assertEqual(self._stored(), {'m': 6})

    def test_malformed_counter_values_are_not_overwritten(self):
        for bad in ('["m"]', '{"m": "abc"}', '{"m": null}', '{"m": true}', '{"m": 1.5}'):
            self.store[T.KV_COUNTERS] = bad
            T._pending = {'m': 1}
            self.assertFalse(T.flush(), bad)
            mgr.set_store.assert_not_called()
            self.assertEqual(self.store[T.KV_COUNTERS], bad)
            self.assertEqual(T._pending, {'m': 1})

    def test_get_with_malformed_counter_values_does_not_raise(self):
        self.store[T.KV_COUNTERS] = json.dumps({'m': 10})
        self.assertEqual(T.get('m'), 10)
        self.store[T.KV_COUNTERS] = json.dumps({'m': 'abc'})
        T._aggregate('m', 2)
        self.assertEqual(T.get('m'), 12)  # last known total + pending
        self.assertEqual(json.loads(self.store[T.KV_COUNTERS]), {'m': 'abc'})

    def test_get_returns_stored_plus_pending(self):
        self.store[T.KV_COUNTERS] = json.dumps({'m': 10})
        T._aggregate('m', 1)
        self.assertEqual(T.get('m'), 11)

    def test_get_uses_last_known_total_when_read_fails(self):
        self.store[T.KV_COUNTERS] = json.dumps({'m': 10})
        self.assertEqual(T.get('m'), 10)
        self.store[T.KV_COUNTERS] = 'not-json'
        T._aggregate('m', 2)
        self.assertEqual(T.get('m'), 12)

    def test_start_is_idempotent_and_stop_is_repeatable(self):
        T.start()
        first = T._thread
        T.start()
        self.assertIs(T._thread, first)
        self.assertTrue(T.stop())
        self.assertIsNone(T._thread)
        self.assertTrue(T.stop())

    def test_stop_drains_then_flushes(self):
        T.start()
        for _ in range(200):
            T.record('m')
        self.assertTrue(T.stop())
        self.assertEqual(self._stored(), {'m': 200})

    def test_record_after_stop_is_ignored(self):
        T.start()
        T.stop()
        T.record('m')
        self.assertTrue(T._queue.empty())

    def test_stop_reports_failure_when_worker_does_not_exit(self):
        T._thread = MagicMock()
        T._thread.is_alive.return_value = True
        self.assertFalse(T.stop(timeout=0.01))
        self.assertIsNotNone(T._thread)

    def test_repeated_stop_retries_pending_after_failed_final_flush(self):
        T.start()
        for _ in range(3):
            T.record('m')
        mgr.set_store.side_effect = RuntimeError('boom')
        self.assertFalse(T.stop())
        self.assertFalse(T.stop())  # still failing: must not report success
        self.assertIsNotNone(T._thread)
        self.assertEqual(T._pending, {'m': 3})
        self.assertNotIn(T.KV_COUNTERS, self.store)

        mgr.set_store.side_effect = self.store.__setitem__
        self.assertTrue(T.stop())  # retry persists the retained batch once
        self.assertEqual(self._stored(), {'m': 3})
        self.assertIsNone(T._thread)
        self.assertTrue(T.stop())
        self.assertEqual(self._stored(), {'m': 3})

    def test_stop_keeps_rejecting_events_after_failed_flush(self):
        T.start()
        T.record('m')
        mgr.set_store.side_effect = RuntimeError('boom')
        self.assertFalse(T.stop())
        T.record('m')
        self.assertTrue(T._queue.empty())

    def test_start_after_failed_stop_resumes_and_keeps_pending(self):
        T.start()
        T.record('m')
        mgr.set_store.side_effect = RuntimeError('boom')
        self.assertFalse(T.stop())
        mgr.set_store.side_effect = self.store.__setitem__
        T.start()
        T.record('m')
        self.assertTrue(T.stop())
        self.assertEqual(self._stored(), {'m': 2})

    def test_concurrent_start_creates_a_single_worker(self):
        threads = [threading.Thread(target=T.start) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(5)
        workers = [t for t in threading.enumerate() if t.name == 'telemetry-worker']
        self.assertEqual(len(workers), 1)
        self.assertTrue(T.stop())

    def test_concurrent_start_stop_leaves_consistent_state(self):
        def cycle():
            for _ in range(2):
                T.start()
                T.record('m')
                T.stop()

        threads = [threading.Thread(target=cycle) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
            self.assertFalse(t.is_alive())
        self.assertTrue(T.stop())
        self.assertIsNone(T._thread)
        self.assertFalse(T._accepting)
        self.assertEqual(
            [t for t in threading.enumerate() if t.name == 'telemetry-worker'], [])

    def test_producer_racing_with_stop_is_not_lost(self):
        T.start()
        in_put, release = threading.Event(), threading.Event()
        real_put = T._queue.put_nowait

        def slow_put(item):
            in_put.set()
            release.wait(5)
            real_put(item)
        T._queue.put_nowait = slow_put

        producer = threading.Thread(target=T.record, args=('m',))
        producer.start()
        self.assertTrue(in_put.wait(5))  # producer is past the accepting check
        results = []
        stopper = threading.Thread(target=lambda: results.append(T.stop()))
        stopper.start()
        stopper.join(0.2)
        self.assertTrue(stopper.is_alive())  # stop() waits for the in-flight record
        release.set()
        producer.join(5)
        stopper.join(5)
        self.assertEqual(results, [True])
        self.assertEqual(self._stored(), {'m': 1})
        T.record('m')  # after stop: rejected
        self.assertTrue(T._queue.empty())


class TestTelemetryDecorator(unittest.TestCase):

    @patch.object(T, 'record')
    def test_records_after_successful_operation(self, record):
        @telemetry(LOGIN_METRIC)
        def operation():
            return 'success'

        self.assertEqual(operation(), 'success')
        record.assert_called_once_with(LOGIN_METRIC)

    @patch.object(T, 'record')
    def test_does_not_record_when_operation_fails(self, record):
        @telemetry(LOGIN_METRIC)
        def operation():
            raise ValueError('failure')

        with self.assertRaises(ValueError):
            operation()

        record.assert_not_called()

    @patch.object(T, 'record')
    def test_records_on_redirect_when_enabled(self, record):
        @telemetry(LOGIN_METRIC, record_on_redirect=True)
        def operation():
            raise cherrypy.HTTPRedirect('/login')

        with self.assertRaises(cherrypy.HTTPRedirect):
            operation()

        record.assert_called_once_with(LOGIN_METRIC)

    @patch.object(T, 'record')
    def test_does_not_record_on_redirect_when_disabled(self, record):
        @telemetry(LOGIN_METRIC)
        def operation():
            raise cherrypy.HTTPRedirect('/login')

        with self.assertRaises(cherrypy.HTTPRedirect):
            operation()

        record.assert_not_called()

    @patch.object(T, 'record')
    def test_return_not_recorded_when_record_on_return_is_false(self, record):
        @telemetry(LOGIN_METRIC, record_on_return=False)
        def operation():
            return {'is_authenticated': False}

        operation()
        record.assert_not_called()

    @patch.object(T, 'record')
    def test_redirect_recorded_only_when_accepted(self, record):
        @telemetry(LOGIN_METRIC, record_on_return=False,
                   redirect_ok=lambda e: any('access_token=' in u for u in e.urls))
        def operation(url):
            raise cherrypy.HTTPRedirect(url)

        with self.assertRaises(cherrypy.HTTPRedirect):
            operation('/#/sso/404')
        record.assert_not_called()
        with self.assertRaises(cherrypy.HTTPRedirect):
            operation('/#/login?access_token=abc')
        record.assert_called_once_with(LOGIN_METRIC)

    @patch.object(T, 'record')
    def test_failing_redirect_predicate_keeps_redirect(self, record):
        def bad(_redirect):
            raise RuntimeError('boom')

        @telemetry(LOGIN_METRIC, redirect_ok=bad)
        def operation():
            raise cherrypy.HTTPRedirect('/x')

        with self.assertRaises(cherrypy.HTTPRedirect):
            operation()
        record.assert_not_called()

    @patch.object(T, 'record', side_effect=RuntimeError('boom'))
    def test_record_failure_never_breaks_operation(self, _record):
        @telemetry(LOGIN_METRIC)
        def operation():
            return 'ok'

        self.assertEqual(operation(), 'ok')


class TestAuthenticationUserSignals(unittest.TestCase):
    def setUp(self):
        for attr, value in (
            ('get_module_option', MagicMock(return_value=False)),
            ('SSO_DB', None),
            ('ACCESS_CTRL_DB', None),
        ):
            patcher = patch.object(mgr, attr, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    @patch.object(T, 'get', return_value=10)
    def test_shape_and_values(self, get):
        mgr.get_module_option.return_value = True
        mgr.SSO_DB = MagicMock()
        mgr.SSO_DB.protocol = AuthType.SAML2
        mgr.ACCESS_CTRL_DB = MagicMock()
        mgr.ACCESS_CTRL_DB.users = {'u1': 1, 'u2': 2}
        self.assertEqual(collect_authentication_user_signals(), {
            'oauth2_enabled': True, 'saml2_enabled': True,
            'configured_users': 2, 'login_count': 10})
        get.assert_called_once_with(LOGIN_METRIC)

    @patch.object(T, 'get', return_value=0)
    def test_fallback_when_detection_fails(self, _get):
        mgr.get_module_option.side_effect = RuntimeError('boom')
        self.assertEqual(collect_authentication_user_signals(), {
            'oauth2_enabled': False, 'saml2_enabled': False,
            'configured_users': 0, 'login_count': 0})

    @patch.object(T, 'get', return_value=7)
    def test_module_gather_telemetry_exposes_signals(self, _get):
        # This is the method the telemetry module calls via mgr.remote().
        mgr.get_module_option.return_value = True
        mgr.ACCESS_CTRL_DB = MagicMock()
        mgr.ACCESS_CTRL_DB.users = {'u1': 1}
        data = Module.gather_telemetry(MagicMock())
        self.assertEqual(data, {'authentication_user_signals': {
            'oauth2_enabled': True, 'saml2_enabled': False,
            'configured_users': 1, 'login_count': 7}})
        json.dumps(data)  # must be serializable for mgr.remote()

    @patch.object(T, 'get', return_value=5)
    def test_login_count_reported_when_detection_fails(self, _get):
        mgr.get_module_option.side_effect = RuntimeError('boom')
        self.assertEqual(collect_authentication_user_signals(), {
            'oauth2_enabled': False, 'saml2_enabled': False,
            'configured_users': 0, 'login_count': 5})
