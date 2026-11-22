# -*- coding: utf-8 -*-

import json
import logging
import queue
import threading
import time
from functools import wraps
from typing import Callable, Dict, Optional

import cherrypy

from .. import mgr

logger = logging.getLogger('services.telemetry')


def telemetry(name: str, record_on_redirect: bool = False,
              record_on_return: bool = True,
              redirect_ok: Optional[Callable[[cherrypy.HTTPRedirect], bool]] = None):
    """
    Record a telemetry event when the decorated operation succeeds.

    Handlers that succeed by redirecting (OAuth2/SAML2) set record_on_return=False
    and either record_on_redirect=True or a redirect_ok predicate that accepts
    only the success redirect.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
            except cherrypy.HTTPRedirect as e:
                if record_on_redirect or _redirect_accepted(redirect_ok, e):
                    _record_telemetry(name)
                raise

            if record_on_return:
                _record_telemetry(name)
            return result

        return wrapper

    return decorator


def _redirect_accepted(redirect_ok, redirect) -> bool:
    try:
        return redirect_ok is not None and bool(redirect_ok(redirect))
    except Exception:  # pylint: disable=broad-except
        logger.warning('telemetry redirect check failed', exc_info=True)
        return False


def _record_telemetry(name: str) -> None:
    try:
        TelemetryService.record(name)
    except Exception:  # pylint: disable=broad-except
        logger.warning(
            'failed to record telemetry event %r',
            name,
            exc_info=True
        )


class TelemetryService:
    """
    Generic, non-blocking telemetry counters.

    record() only enqueues events in memory.
    flush() persists aggregated counters additively to the mgr KV store.
    """

    KV_COUNTERS = 'telemetry/counters'
    QUEUE_SIZE = 10_000
    FLUSH_INTERVAL = 3600
    STOP_TIMEOUT = 10

    _queue: queue.Queue = queue.Queue(maxsize=QUEUE_SIZE)
    _pending: Dict[str, int] = {}
    _known: Dict[str, int] = {}
    _dropped = 0
    _accepting = False

    _thread: Optional[threading.Thread] = None
    _stop_event = threading.Event()

    _lock = threading.Lock()
    _flush_lock = threading.Lock()
    _lifecycle_lock = threading.Lock()

    @classmethod
    def record(cls, name: str, value: int = 1) -> None:
        with cls._lock:
            if not cls._accepting:
                return
            try:
                cls._queue.put_nowait((name, value))
            except queue.Full:
                cls._dropped += 1
            except Exception:  # pylint: disable=broad-except
                logger.warning(
                    'failed to record telemetry event %r',
                    name,
                    exc_info=True
                )

    @classmethod
    def get(cls, name: str) -> int:
        with cls._flush_lock:
            with cls._lock:
                pending = cls._pending.get(name, 0)
            try:
                cls._load()
            except Exception:  # pylint: disable=broad-except
                logger.warning(
                    'failed to read stored telemetry, using last known total',
                    exc_info=True
                )
            stored = cls._known.get(name, 0)
        return stored + pending

    @classmethod
    def start(cls) -> None:
        with cls._lifecycle_lock:
            if cls._thread is not None and cls._thread.is_alive():
                return

            cls._stop_event = threading.Event()
            with cls._lock:
                cls._accepting = True

            cls._thread = threading.Thread(
                target=cls._worker,
                args=(cls._stop_event,),
                daemon=True,
                name='telemetry-worker'
            )
            cls._thread.start()

    @classmethod
    def stop(cls, timeout: Optional[float] = None) -> bool:
        with cls._lifecycle_lock:
            thread = cls._thread
            if thread is None:
                return True

            with cls._lock:
                cls._accepting = False

            cls._stop_event.set()
            thread.join(
                cls.STOP_TIMEOUT if timeout is None else timeout
            )

            if thread.is_alive():
                logger.error(
                    'telemetry worker did not stop in time; '
                    'queued events may be lost'
                )
                return False

            cls._drain_queue()
            if not cls.flush():
                # Keep _thread set so that a repeated stop() retries the
                # pending counters instead of reporting success.
                return False

            cls._thread = None
            return True

    @classmethod
    def _worker(cls, stop_event: threading.Event) -> None:
        next_flush = time.monotonic() + cls.FLUSH_INTERVAL

        while True:
            try:
                name, value = cls._queue.get(timeout=1)
            except queue.Empty:
                if stop_event.is_set():
                    break
            else:
                try:
                    cls._aggregate(name, value)
                finally:
                    cls._queue.task_done()

            if time.monotonic() >= next_flush:
                cls.flush()
                next_flush = time.monotonic() + cls.FLUSH_INTERVAL

    @classmethod
    def _aggregate(cls, name: str, value: int) -> None:
        with cls._lock:
            cls._pending[name] = cls._pending.get(name, 0) + value

    @classmethod
    def _drain_queue(cls) -> None:
        while True:
            try:
                name, value = cls._queue.get_nowait()
            except queue.Empty:
                return

            try:
                cls._aggregate(name, value)
            finally:
                cls._queue.task_done()

    @classmethod
    def _load(cls) -> Dict[str, int]:
        raw = mgr.get_store(cls.KV_COUNTERS)

        if not raw:
            counters: Dict[str, int] = {}
        else:
            counters = json.loads(raw)
            if not isinstance(counters, dict) or not all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in counters.values()
            ):
                raise ValueError('unexpected telemetry counters format')

        cls._known = dict(counters)
        return counters

    @classmethod
    def flush(cls) -> bool:
        with cls._flush_lock:
            with cls._lock:
                dropped = cls._dropped
                cls._dropped = 0
                batch = cls._pending
                cls._pending = {}

            if dropped:
                logger.warning(
                    'queue full; dropped %d telemetry events',
                    dropped
                )

            if not batch:
                return True

            try:
                counters = cls._load()
                for name, value in batch.items():
                    counters[name] = counters.get(name, 0) + value

                mgr.set_store(cls.KV_COUNTERS, json.dumps(counters))
                cls._known = dict(counters)
                return True

            except Exception:  # pylint: disable=broad-except
                logger.warning(
                    'failed to flush telemetry; retaining batch for retry',
                    exc_info=True
                )
                with cls._lock:
                    for name, value in batch.items():
                        cls._pending[name] = (
                            cls._pending.get(name, 0) + value
                        )
                return False
