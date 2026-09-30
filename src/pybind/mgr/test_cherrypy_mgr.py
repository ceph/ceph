# -*- coding: utf-8 -*-
"""
Unit tests for the SSL-aware stream writer in cherrypy_mgr.

The bug: Cheroot's stock BufferedWriter._flush_unlocked catches only
io.BlockingIOError.  When the socket is an ssl.SSLSocket in non-blocking mode
(FIONBIO=1, which Cheroot always sets after accept), SSLSocket.write() raises
ssl.SSLWantWriteError or ssl.SSLWantReadError on backpressure — neither of
which is a subclass of io.BlockingIOError.  The uncaught exception aborts the
response mid-flight, the kernel sends a TCP RST, and the client sees
ERR_CONTENT_LENGTH_MISMATCH / "unexpected EOF while reading".

These tests confirm:
1. The invariant that triggered the bug still holds (ssl.SSLWantWriteError is
   NOT caught by except io.BlockingIOError).
2. _SSLStreamWriter._flush_unlocked retries instead of propagating
   ssl.SSLWantWriteError / ssl.SSLWantReadError.
3. _SSLStreamWriter._flush_unlocked still propagates genuine errors.
4. _SSLBuiltinAdapter.makefile returns the patched writer class for writes.
"""
import _pyio as pyio
import io
import ssl
import unittest
from unittest.mock import MagicMock, patch

from cherrypy_mgr import _SSLBuiltinAdapter, _SSLStreamWriter


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


class TestSSLStreamWriterFlush(unittest.TestCase):
    """Verify _SSLStreamWriter._flush_unlocked retry behaviour."""

    def _make_writer(self, raw_mock):
        """Return a fully-initialised _SSLStreamWriter whose raw is raw_mock.

        _pyio.BufferedWriter only needs the raw object to implement
        readable() -> False and writable() -> True.  We bypass
        StreamWriter.__init__ (which calls socket.SocketIO) by going through
        pyio.BufferedWriter.__init__ directly.
        """
        raw_mock.readable = MagicMock(return_value=False)
        raw_mock.writable = MagicMock(return_value=True)
        raw_mock.readinto = MagicMock(return_value=0)
        raw_mock.closed = False  # _checkClosed inspects raw.closed
        writer = pyio.BufferedWriter.__new__(_SSLStreamWriter)
        pyio.BufferedWriter.__init__(writer, raw_mock, 8192)
        return writer

    # ------------------------------------------------------------------
    # SSLWantWriteError: retry until success
    # ------------------------------------------------------------------

    def test_want_write_retries_and_succeeds(self):
        """SSLWantWriteError on the first call must cause a retry, not a raise."""
        raw = MagicMock()
        payload = b'hello world'
        raw.write.side_effect = [
            ssl.SSLWantWriteError(ssl.SSL_ERROR_WANT_WRITE, 'want write'),
            len(payload),
        ]
        writer = self._make_writer(raw)
        writer._write_buf.extend(payload)

        writer._flush_unlocked()

        self.assertEqual(raw.write.call_count, 2)
        self.assertEqual(len(writer._write_buf), 0)

    def test_want_write_multiple_retries(self):
        """Multiple consecutive SSLWantWriteErrors must all be retried."""
        raw = MagicMock()
        payload = b'hello world'
        raw.write.side_effect = [
            ssl.SSLWantWriteError(ssl.SSL_ERROR_WANT_WRITE, 'want write'),
            ssl.SSLWantWriteError(ssl.SSL_ERROR_WANT_WRITE, 'want write again'),
            len(payload),
        ]
        writer = self._make_writer(raw)
        writer._write_buf.extend(payload)

        writer._flush_unlocked()

        self.assertEqual(raw.write.call_count, 3)
        self.assertEqual(len(writer._write_buf), 0)

    # ------------------------------------------------------------------
    # SSLWantReadError: retry (TLS renegotiation mid-write)
    # ------------------------------------------------------------------

    def test_want_read_retries_and_succeeds(self):
        """SSLWantReadError during a write (TLS renegotiation) must also retry."""
        raw = MagicMock()
        payload = b'hello world'
        raw.write.side_effect = [
            ssl.SSLWantReadError(ssl.SSL_ERROR_WANT_READ, 'want read'),
            len(payload),
        ]
        writer = self._make_writer(raw)
        writer._write_buf.extend(payload)

        writer._flush_unlocked()

        self.assertEqual(raw.write.call_count, 2)
        self.assertEqual(len(writer._write_buf), 0)

    # ------------------------------------------------------------------
    # Partial writes (plain non-blocking socket via BlockingIOError)
    # ------------------------------------------------------------------

    def test_partial_write_advances_buffer(self):
        """io.BlockingIOError with characters_written must advance the buffer."""
        raw = MagicMock()
        payload = b'hello world'
        partial = 5  # "hello" written before EAGAIN
        err = io.BlockingIOError(None, "blocking", partial)
        raw.write.side_effect = [err, len(payload) - partial]

        writer = self._make_writer(raw)
        writer._write_buf.extend(payload)

        writer._flush_unlocked()

        self.assertEqual(raw.write.call_count, 2)
        self.assertEqual(len(writer._write_buf), 0)
        # Second call received only the remaining bytes.
        second_call_arg = raw.write.call_args_list[1][0][0]
        self.assertEqual(second_call_arg, bytes(payload[partial:]))

    # ------------------------------------------------------------------
    # Fatal errors still propagate
    # ------------------------------------------------------------------

    def test_fatal_oserror_propagates(self):
        """A genuine OSError (e.g. ECONNRESET) must still raise."""
        raw = MagicMock()
        raw.write.side_effect = OSError(104, "Connection reset by peer")

        writer = self._make_writer(raw)
        writer._write_buf.extend(b'data')

        with self.assertRaises(OSError):
            writer._flush_unlocked()

    def test_fatal_sslerror_propagates(self):
        """An ssl.SSLError that is not a Want* variant must still raise."""
        raw = MagicMock()
        raw.write.side_effect = ssl.SSLError(ssl.SSL_ERROR_SSL, "bad record mac")

        writer = self._make_writer(raw)
        writer._write_buf.extend(b'data')

        with self.assertRaises(ssl.SSLError):
            writer._flush_unlocked()

    # ------------------------------------------------------------------
    # No-op on empty buffer
    # ------------------------------------------------------------------

    def test_empty_buffer_does_not_write(self):
        """_flush_unlocked must not call raw.write when there is nothing to send."""
        raw = MagicMock()
        writer = self._make_writer(raw)
        # _write_buf is empty by default after __init__

        writer._flush_unlocked()

        raw.write.assert_not_called()


class TestSSLBuiltinAdapterMakefile(unittest.TestCase):
    """Verify _SSLBuiltinAdapter.makefile returns the right class."""

    def _make_adapter(self):
        """Return an _SSLBuiltinAdapter instance without touching the filesystem."""
        with patch('cheroot.ssl.builtin.BuiltinSSLAdapter.__init__', return_value=None):
            return _SSLBuiltinAdapter.__new__(_SSLBuiltinAdapter)

    def test_write_mode_returns_ssl_stream_writer(self):
        from unittest.mock import create_autospec
        import socket
        adapter = self._make_adapter()
        # makefile calls _SSLStreamWriter(sock, mode, bufsize) which chains to
        # socket.SocketIO; give it a real-enough socket mock.
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


if __name__ == '__main__':
    unittest.main()
