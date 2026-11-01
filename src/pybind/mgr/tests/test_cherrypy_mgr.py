import io
import select
import ssl
import threading
import unittest
from unittest import mock
from unittest.mock import MagicMock, call, patch
import cherrypy
import cherrypy_mgr
import logging

from cherrypy_mgr import CherryPyMgr, CherryPyAccessFilter, _SSLBuiltinAdapter, _SSLStreamWriter


class TestCherryPyAccessFilter(unittest.TestCase):
    def setUp(self):
        self.filter = CherryPyAccessFilter(interval=300.0)
        self.monotonic_patcher = mock.patch('cherrypy_mgr.time.monotonic', return_value=100.0)
        self.mock_monotonic = self.monotonic_patcher.start()

    def tearDown(self):
        self.monotonic_patcher.stop()

    def _record(self, name: str, message: str) -> logging.LogRecord:
        return logging.LogRecord(name, logging.INFO, __file__, 0, message, (), None)

    def test_rate_limits_200_within_interval(self):
        record = self._record(
            'cherrypy.access.123',
            '127.0.0.1 - - [time] "GET /metrics HTTP/1.1" 200 1 "" "Prometheus/3.6.0"'
        )

        self.assertTrue(self.filter.filter(record))
        self.assertFalse(self.filter.filter(record))

    def test_non_200_always_passes(self):
        record = self._record(
            'cherrypy.access.123',
            '127.0.0.1 - - [time] "GET /metrics HTTP/1.1" 500 1 "" "Prometheus/3.6.0"'
        )

        self.assertTrue(self.filter.filter(record))
        self.assertTrue(self.filter.filter(record))
        self.assertEqual(self.filter._last, {})

    def test_regex_key_extraction_without_query_params(self):
        record = self._record(
            'cherrypy.access.123',
            '127.0.0.1 - - [time] "GET /metrics HTTP/1.1" 200 1 "" "Prometheus/3.6.0"'
        )

        self.assertTrue(self.filter.filter(record))
        self.assertEqual(self.filter._last, {'/metrics': 100.0})

    def test_regex_key_extraction_with_query_params(self):
        record = self._record(
            'cherrypy.access.123',
            '127.0.0.1 - - [time] "GET /sd/prometheus/sd-config?service=ceph HTTP/1.1" 200 1 "" "Prometheus/3.6.0"'
        )

        self.assertTrue(self.filter.filter(record))
        self.assertEqual(
            self.filter._last,
            {'/sd/prometheus/sd-config?service=ceph': 100.0}
        )

    def test_fallback_when_regex_does_not_match(self):
        message = 'raw" 200 message without an HTTP request pattern'
        record = self._record('cherrypy.access.123', message)

        self.assertTrue(self.filter.filter(record))
        self.assertEqual(
            self.filter._last,
            {'raw" 200 message without an HTTP request pattern': 100.0}
        )


class TestCherryPyMgr(unittest.TestCase):
    def setUp(self):
        CherryPyMgr._trees = {}
        self.patcher_engine = mock.patch('cherrypy_mgr.cherrypy.engine')
        self.mock_engine = self.patcher_engine.start()
        self.mock_engine.state = cherrypy.engine.states.STOPPED

        self.patcher_config = mock.patch('cherrypy_mgr.cherrypy.config')
        self.mock_config = self.patcher_config.start()

        self.patcher_server = mock.patch('cherrypy_mgr.cherrypy.server')
        self.mock_server = self.patcher_server.start()

    def tearDown(self):
        self.patcher_engine.stop()
        self.patcher_config.stop()
        self.patcher_server.stop()
        self.patcher_engine.stop()
    
    @mock.patch('cherrypy_mgr.ServerAdapter')
    @mock.patch('cherrypy_mgr.WSGIServer')
    def test_mount(self, mock_wsgi_server, mock_server_adapter):
        tree = mock.MagicMock(spec=cherrypy._cptree.Tree)
        name = 'test_app'
        bind_addr = ('127.0.0.0', 8080)
        ssl_info = None

        adapter, _ = CherryPyMgr.mount(tree, name, bind_addr, ssl_info)

        self.assertIn(name, CherryPyMgr._trees)
        self.assertEqual(CherryPyMgr._trees[name], tree)
        self.mock_server.unsubscribe.assert_called_once()
        self.mock_engine.autoreload.unsubscribe.assert_called_once()
        self.mock_engine.start.assert_called_once()
        mock_wsgi_server.assert_called_with(
            bind_addr=bind_addr,
            wsgi_app=tree,
            numthreads=30,
            server_name='Ceph-Mgr'
        )
        mock_server_adapter.return_value.start.assert_called_once()

    @mock.patch('cherrypy_mgr.ServerAdapter')
    @mock.patch('cherrypy_mgr.WSGIServer')
    def test_mount_engine_already_started(self, mock_wsgi_server, mock_server_adapter):
        self.mock_engine.state = cherrypy.engine.states.STARTED

        tree = mock.MagicMock(spec=cherrypy._cptree.Tree)
        name = 'another_app'
        bind_addr = ('127.0.0.1', 8082)

        adapter, _ = CherryPyMgr.mount(tree, name, bind_addr)

        self.mock_engine.start.assert_not_called()
        mock_server_adapter.return_value.start.assert_called_once()

    @mock.patch('cherrypy_mgr._SSLBuiltinAdapter')
    @mock.patch('cherrypy_mgr.ServerAdapter')
    @mock.patch('cherrypy_mgr.WSGIServer')
    def test_mount_with_ssl(self, mock_wsgi_server, mock_server_adapter, mock_ssl_adapter):
        tree = mock.MagicMock(spec=cherrypy._cptree.Tree)
        name = 'ssl_app'
        bind_addr = ('127.0.0.1', 8080)
        ssl_info = {
            'cert': '/path/to/cert.pem',
            'key': '/path/to/key.pem',
            'context': 'fake_context'
        }

        CherryPyMgr.mount(tree, name, bind_addr, ssl_info)

        mock_wsgi_server.assert_called_once()
        server_instance = mock_wsgi_server.return_value

        mock_ssl_adapter.assert_called_once_with(ssl_info['cert'], ssl_info['key'])
        self.assertEqual(mock_ssl_adapter.return_value.context, 'fake_context')
        self.assertEqual(server_instance.ssl_adapter, mock_ssl_adapter.return_value)
    
    def test_get_server_config(self):
        tree = cherrypy._cptree.Tree()
        app_one = mock.Mock()
        app_one.config = {'id': 'app_one'}
        
        app_two = mock.Mock()
        app_two.config = {'id': 'app_two'}

        tree.apps['/app_one'] = app_one
        tree.apps['/app_two'] = app_two
        CherryPyMgr._trees['test_app'] = tree

        # get the config of app_two using different mount point formats
        result = CherryPyMgr.get_server_config('test_app', '/app_two')
        self.assertEqual(result, {'id': 'app_two'})
        result = CherryPyMgr.get_server_config('test_app', '/app_two/')
        self.assertEqual(result, {'id': 'app_two'})

        # for app_one, test with mount point '/' and '/app_one'
        result = CherryPyMgr.get_server_config('test_app', '/app_one')
        self.assertEqual(result, {'id': 'app_one'})
        result = CherryPyMgr.get_server_config('test_app', '/')
        self.assertIsNone(result, {'id': 'app_one'})

        # test non-existent app and mount point
        self.assertIsNone(CherryPyMgr.get_server_config('ghost_app'))
        self.assertIsNone(CherryPyMgr.get_server_config('test_app', '/missing'))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_writer(raw_mock):
    """Return a minimal object that exercises _flush_unlocked and _ssl_wait.

    We avoid going through _pyio.BufferedWriter.__init__ (which requires a
    real socket and causes C-level crashes on exception propagation in some
    Python versions).  Instead we construct a plain object whose __class__ is
    _SSLStreamWriter and populate only the attributes the two methods touch:

      _write_buf  — bytearray consumed by _flush_unlocked
      raw         — object whose .write() is called
      _checkClosed — called at the top of _flush_unlocked (no-op here)
    """
    raw_mock.closed = False

    class _Bare(_SSLStreamWriter):
        """Subclass that skips BufferedWriter.__init__ entirely.

        Declaring 'raw' as a plain class attribute shadows the C-level
        property on BufferedWriter so instance assignment works normally.
        """
        raw = None  # shadows the C property; set to mock in __init__

        def __init__(self):
            # Deliberately do NOT call super().__init__() to avoid the
            # C-level buffer setup that requires a real socket descriptor.
            # We must still provide the attributes that _pyio's __del__
            # and close() access so we don't get noisy destructor warnings.
            self._write_buf = bytearray()
            self._write_lock = threading.RLock()
            self.raw = raw_mock
            self._checkClosed = MagicMock()

        def close(self):
            # No-op: skip the parent close() which tries to flush the
            # C-backed buffer and would crash without a real file descriptor.
            pass

    return _Bare()


# ---------------------------------------------------------------------------
# SSL exception hierarchy
# ---------------------------------------------------------------------------

class TestSSLExceptionHierarchy(unittest.TestCase):
    """Confirm the preconditions that make the bug possible."""

    def test_sslwantwriteerror_not_caught_by_blockingio(self):
        """ssl.SSLWantWriteError must NOT be a subclass of io.BlockingIOError.

        This is the invariant that the stock Cheroot _flush_unlocked misses.
        If this ever changes in CPython, the fix is redundant but still safe.
        """
        self.assertFalse(
            issubclass(ssl.SSLWantWriteError, io.BlockingIOError),
            "ssl.SSLWantWriteError is now a BlockingIOError subclass — "
            "_SSLStreamWriter is redundant but still correct.",
        )

    def test_sslwantreaderror_not_caught_by_blockingio(self):
        self.assertFalse(
            issubclass(ssl.SSLWantReadError, io.BlockingIOError),
            "ssl.SSLWantReadError is now a BlockingIOError subclass — "
            "_SSLStreamWriter is redundant but still correct.",
        )

    def test_sslwantwriteerror_is_sslerror(self):
        self.assertTrue(issubclass(ssl.SSLWantWriteError, ssl.SSLError))

    def test_sslwantreaderror_is_sslerror(self):
        self.assertTrue(issubclass(ssl.SSLWantReadError, ssl.SSLError))


# ---------------------------------------------------------------------------
# _SSLStreamWriter._flush_unlocked retry behaviour
# ---------------------------------------------------------------------------

class TestSSLStreamWriterFlush(unittest.TestCase):
    """Verify _SSLStreamWriter._flush_unlocked retry behaviour."""

    def test_want_write_calls_ssl_wait_then_retries(self):
        """SSLWantWriteError must call _ssl_wait(write=True) then retry."""
        raw = MagicMock()
        payload = b'hello world'
        raw.write.side_effect = [
            ssl.SSLWantWriteError(ssl.SSL_ERROR_WANT_WRITE, 'want write'),
            len(payload),
        ]
        writer = _make_writer(raw)
        writer._write_buf.extend(payload)

        with patch.object(writer, '_ssl_wait') as mock_wait:
            writer._flush_unlocked()

        mock_wait.assert_called_once_with(write=True)
        self.assertEqual(raw.write.call_count, 2)
        self.assertEqual(len(writer._write_buf), 0)

    def test_want_write_multiple_retries(self):
        """Multiple consecutive SSLWantWriteErrors each call _ssl_wait."""
        raw = MagicMock()
        payload = b'hello world'
        raw.write.side_effect = [
            ssl.SSLWantWriteError(ssl.SSL_ERROR_WANT_WRITE, 'want write'),
            ssl.SSLWantWriteError(ssl.SSL_ERROR_WANT_WRITE, 'want write again'),
            len(payload),
        ]
        writer = _make_writer(raw)
        writer._write_buf.extend(payload)

        with patch.object(writer, '_ssl_wait') as mock_wait:
            writer._flush_unlocked()

        self.assertEqual(mock_wait.call_count, 2)
        self.assertEqual(mock_wait.call_args_list, [call(write=True), call(write=True)])
        self.assertEqual(raw.write.call_count, 3)
        self.assertEqual(len(writer._write_buf), 0)

    def test_want_read_calls_ssl_wait_then_retries(self):
        """SSLWantReadError (TLS renegotiation) must call _ssl_wait(write=False)."""
        raw = MagicMock()
        payload = b'hello world'
        raw.write.side_effect = [
            ssl.SSLWantReadError(ssl.SSL_ERROR_WANT_READ, 'want read'),
            len(payload),
        ]
        writer = _make_writer(raw)
        writer._write_buf.extend(payload)

        with patch.object(writer, '_ssl_wait') as mock_wait:
            writer._flush_unlocked()

        mock_wait.assert_called_once_with(write=False)
        self.assertEqual(raw.write.call_count, 2)
        self.assertEqual(len(writer._write_buf), 0)

    def test_partial_write_advances_buffer(self):
        """BlockingIOError with characters_written must advance the buffer."""
        raw = MagicMock()
        payload = b'hello world'
        partial = 5  # "hello" written before EAGAIN
        err = BlockingIOError(None, "blocking", partial)
        raw.write.side_effect = [err, len(payload) - partial]

        writer = _make_writer(raw)
        writer._write_buf.extend(payload)

        writer._flush_unlocked()

        self.assertEqual(raw.write.call_count, 2)
        self.assertEqual(len(writer._write_buf), 0)
        # Second call received only the remaining bytes.
        second_call_arg = raw.write.call_args_list[1][0][0]
        self.assertEqual(second_call_arg, bytes(payload[partial:]))

    def test_fatal_oserror_propagates(self):
        """A genuine OSError (e.g. ECONNRESET) must still raise."""
        raw = MagicMock()
        raw.write.side_effect = OSError(104, "Connection reset by peer")

        writer = _make_writer(raw)
        writer._write_buf.extend(b'data')

        with self.assertRaises(OSError):
            writer._flush_unlocked()

    def test_fatal_sslerror_propagates(self):
        """An ssl.SSLError that is not a Want* variant must still raise."""
        raw = MagicMock()
        raw.write.side_effect = ssl.SSLError(ssl.SSL_ERROR_SSL, "bad record mac")

        writer = _make_writer(raw)
        writer._write_buf.extend(b'data')

        with self.assertRaises(ssl.SSLError):
            writer._flush_unlocked()

    def test_empty_buffer_does_not_write(self):
        """_flush_unlocked must not call raw.write when there is nothing to send."""
        raw = MagicMock()
        writer = _make_writer(raw)

        writer._flush_unlocked()

        raw.write.assert_not_called()

    def test_checkClosed_is_called(self):
        """_flush_unlocked must call _checkClosed at entry."""
        raw = MagicMock()
        writer = _make_writer(raw)

        writer._flush_unlocked()

        writer._checkClosed.assert_called_once_with('flush of closed file')


# ---------------------------------------------------------------------------
# _SSLStreamWriter._ssl_wait select() behaviour
# ---------------------------------------------------------------------------

class TestSSLStreamWriterWait(unittest.TestCase):
    """Verify _SSLStreamWriter._ssl_wait select() behaviour."""

    def _make_writer(self):
        raw = MagicMock()
        raw.fileno = MagicMock(return_value=5)  # dummy fd
        return _make_writer(raw)

    def test_wait_write_calls_select_on_write_fd(self):
        """_ssl_wait(write=True) must select() on the write set."""
        writer = self._make_writer()
        with patch('select.select', return_value=([], [5], [])) as mock_select:
            writer._ssl_wait(write=True)
        mock_select.assert_called_once_with(
            [], [5], [], _SSLStreamWriter._SSL_SELECT_TIMEOUT
        )

    def test_wait_read_calls_select_on_read_fd(self):
        """_ssl_wait(write=False) must select() on the read set."""
        writer = self._make_writer()
        with patch('select.select', return_value=([5], [], [])) as mock_select:
            writer._ssl_wait(write=False)
        mock_select.assert_called_once_with(
            [5], [], [], _SSLStreamWriter._SSL_SELECT_TIMEOUT
        )

    def test_wait_write_timeout_raises_sslerror(self):
        """_ssl_wait(write=True) must raise ssl.SSLError when select() times out."""
        writer = self._make_writer()
        with patch('select.select', return_value=([], [], [])):
            with self.assertRaises(ssl.SSLError) as ctx:
                writer._ssl_wait(write=True)
        self.assertEqual(ctx.exception.args[0], ssl.SSL_ERROR_WANT_WRITE)

    def test_wait_read_timeout_raises_sslerror(self):
        """_ssl_wait(write=False) must raise ssl.SSLError when select() times out."""
        writer = self._make_writer()
        with patch('select.select', return_value=([], [], [])):
            with self.assertRaises(ssl.SSLError) as ctx:
                writer._ssl_wait(write=False)
        self.assertEqual(ctx.exception.args[0], ssl.SSL_ERROR_WANT_READ)

    def test_wait_returns_normally_when_socket_ready(self):
        """_ssl_wait must return without raising when select() reports ready."""
        writer = self._make_writer()
        with patch('select.select', return_value=([], [5], [])):
            writer._ssl_wait(write=True)  # must not raise

    def test_ssl_select_timeout_matches_cheroot_default(self):
        """_SSL_SELECT_TIMEOUT must equal Cheroot's HTTPServer.timeout (10s)."""
        self.assertEqual(_SSLStreamWriter._SSL_SELECT_TIMEOUT, 10.0)


# ---------------------------------------------------------------------------
# _SSLBuiltinAdapter.makefile returns the right writer class
# ---------------------------------------------------------------------------

class TestSSLBuiltinAdapterMakefile(unittest.TestCase):
    """Verify _SSLBuiltinAdapter.makefile returns the right class."""

    def _make_adapter(self):
        """Return an _SSLBuiltinAdapter instance without touching the filesystem."""
        with patch('cheroot.ssl.builtin.BuiltinSSLAdapter.__init__', return_value=None):
            return _SSLBuiltinAdapter.__new__(_SSLBuiltinAdapter)

    def test_write_mode_returns_ssl_stream_writer(self):
        from unittest.mock import create_autospec
        adapter = self._make_adapter()
        sock = create_autospec(ssl.SSLSocket, instance=True)
        result = adapter.makefile(sock, mode='w', bufsize=8192)
        self.assertIsInstance(result, _SSLStreamWriter)

    def test_read_mode_does_not_return_ssl_stream_writer(self):
        from cheroot.makefile import StreamReader
        from unittest.mock import create_autospec
        adapter = self._make_adapter()
        sock = create_autospec(ssl.SSLSocket, instance=True)
        result = adapter.makefile(sock, mode='r', bufsize=8192)
        self.assertIsInstance(result, StreamReader)
        self.assertNotIsInstance(result, _SSLStreamWriter)
