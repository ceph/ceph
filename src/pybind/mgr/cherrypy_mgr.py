"""
CherryPyMgr is a utility class to encapsulate the CherryPy server instance
into a standalone component. Unlike standard cherrypy which relies on global state
and a single engine, CherryPyMgr allows for multiple independent server instances
to be created and managed within the same process. So we can run multiple servers
in each modules without worrying about their global state interfering with each other.

Usage:
    # Create a tree and mount your WSGI app on it
    from cherrypy import _cptree
    tree = _cptree.Tree()
    tree.mount(my_wsgi_app, config=config)

    # Mount your WSGI app on the manager
    adapter, app = CherryPyMgr.mount(
        tree,
        'my-app',
        addr,
        ssl_info={'cert': 'path/to/cert.pem', 'key': 'path/to/key.pem', 'context': ssl_context}
        )

    # The adapter can be used to stop the server when needed
    adapter.stop()

Each mounted app is stored in the class variable _trees, which allows us to retrieve
the server config for each app when needed. This will let us dynamically update the
server configuration for each app without affecting the others.

Usage:
    config = CherryPyMgr.get_server_config(name='my-app', mount_point='/')
    if config:
        # Do something with the config
"""
import logging
import select
import ssl
import cherrypy
import re
import threading
import time
from cherrypy.process.servers import ServerAdapter
from cheroot.wsgi import Server as WSGIServer
from cheroot.ssl.builtin import BuiltinSSLAdapter
from cheroot.makefile import StreamWriter
from cherrypy._cptree import Tree
from typing import Any, Tuple, Optional, Dict

logger = logging.getLogger(__name__)


class CherryPyAccessFilter(logging.Filter):
    """Rate-limits access log to one entry per endpoint per 300 s.
    Non-200 responses always pass through."""
    _PATH_RE = re.compile(r'"[A-Z]+\s+(/[^\s]*)')

    def __init__(self, interval: float = 300.0) -> None:
        super().__init__()
        self.interval = interval
        self._last: Dict[str, float] = {}
        self._calls = 0
        self._lock = threading.Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        if not record.name.startswith('cherrypy.access'):
            return True
        msg = record.getMessage()
        if '" 200 ' not in msg:
            return True
        m = self._PATH_RE.search(msg)
        key = m.group(1) if m else msg
        now = time.monotonic()
        with self._lock:
            self._calls += 1
            if self._calls % 500 == 0:
                cutoff = now - self.interval
                self._last = {
                    path: ts for path, ts in self._last.items()
                    if ts >= cutoff
                }
            last = self._last.get(key)
            if last is not None and now - last < self.interval:
                return False
            self._last[key] = now
        return True


class CherryPyErrorFilter(logging.Filter):
    """
    Filters out specific, noisy CherryPy connection errors
    that do not indicate a service failure.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        blocked = [
            'TLSV1_ALERT_DECRYPT_ERROR'
        ]
        msg = record.getMessage()
        return not any(m in msg for m in blocked)


class _SSLStreamWriter(StreamWriter):
    """StreamWriter that correctly handles ssl.SSLWantWriteError and
    ssl.SSLWantReadError in non-blocking mode.

    Cheroot's stock ``BufferedWriter._flush_unlocked`` only catches
    ``io.BlockingIOError``.  When the underlying socket is a Python
    ``ssl.SSLSocket`` in non-blocking mode (Cheroot sets FIONBIO=1
    after accept), ``SSLSocket.write()`` may raise
    ``ssl.SSLWantWriteError`` or ``ssl.SSLWantReadError`` instead —
    neither of which is a subclass of ``io.BlockingIOError``.  The
    uncaught exception bubbles all the way up to Cheroot's connection
    handler, which closes the socket and sends a TCP RST to the client
    mid-response.  The client sees ``ERR_CONTENT_LENGTH_MISMATCH`` /
    ``unexpected EOF while reading``.

    The fix: intercept the two SSL want-* exceptions in the same
    ``_flush_unlocked`` loop and treat them as n=0 (no bytes consumed
    yet), so the loop retries the write, exactly what
    ``io.BlockingIOError`` does for a plain non-blocking socket.
    """

    # How long to wait in select() before giving up on a stalled write.
    # Matches Cheroot's default HTTPServer.timeout so behaviour is consistent
    # with how Cheroot handles plain-socket timeouts.
    _SSL_SELECT_TIMEOUT = 10.0

    # Declare the C-level attribute so mypy can resolve it on this subclass.
    _write_buf: bytearray

    def _flush_unlocked(self) -> None:
        self._checkClosed('flush of closed file')
        while self._write_buf:
            try:
                n = self.raw.write(bytes(self._write_buf))
            except ssl.SSLWantWriteError:
                # TLS record layer cannot write yet (send buffer full).
                # Block in select() until the socket is writable, then
                # retry.  This yields the Cheroot worker thread to the OS
                # scheduler instead of spinning at 100% CPU.
                self._ssl_wait(write=True)
                continue
            except ssl.SSLWantReadError:
                # TLS renegotiation: the SSL layer needs to read before it
                # can complete the write.  Wait for the socket to be
                # readable, then retry.
                self._ssl_wait(write=False)
                continue
            except BlockingIOError as e:
                # Plain non-blocking socket returned EAGAIN; some bytes
                # may have been written (characters_written).
                n = e.characters_written
            del self._write_buf[:n]

    def _ssl_wait(self, write: bool) -> None:
        """Block until the underlying SSL socket is ready for I/O.

        Raises ``ssl.SSLError`` (timeout) if the socket does not become
        ready within ``_SSL_SELECT_TIMEOUT`` seconds, which causes
        Cheroot's connection handler to close the connection cleanly.
        """
        fd = self.raw.fileno()  # socket.SocketIO exposes the underlying fd
        if write:
            _, ready, _ = select.select([], [fd], [], self._SSL_SELECT_TIMEOUT)
        else:
            ready, _, _ = select.select([fd], [], [], self._SSL_SELECT_TIMEOUT)
        if not ready:
            raise ssl.SSLError(
                ssl.SSL_ERROR_WANT_WRITE if write else ssl.SSL_ERROR_WANT_READ,
                "SSL socket not ready after %ss" % self._SSL_SELECT_TIMEOUT,
            )


class _SSLBuiltinAdapter(BuiltinSSLAdapter):
    """BuiltinSSLAdapter that uses the SSL-aware stream writer."""

    def makefile(self, sock: ssl.SSLSocket, mode: str = 'r',
                 bufsize: int = -1) -> Any:
        from cheroot.makefile import StreamReader
        if 'r' in mode:
            return StreamReader(sock, mode, bufsize)
        return _SSLStreamWriter(sock, mode, bufsize)


class CherryPyMgr:
    _trees: Dict[str, Tree] = {}

    @classmethod
    def mount(
        cls,
        tree: Tree,
        name: str,
        bind_addr: Tuple[str, int],
        ssl_info: Optional[Dict[str, Any]] = None
    ) -> Tuple[ServerAdapter, Any]:
        """
        :param bind_addr: Tuple (host, port)
        :param ssl_info: Dict containing {'cert': path, 'key': path, 'context': ssl_context}
        """
        cls._trees[name] = tree

        is_engine_running = cherrypy.engine.state in (
            cherrypy.engine.states.STARTED,
            cherrypy.engine.states.STARTING
        )

        if not is_engine_running:
            if hasattr(cherrypy, 'server'):
                cherrypy.server.unsubscribe()
            if hasattr(cherrypy.engine, 'autoreload'):
                cherrypy.engine.autoreload.unsubscribe()
            if hasattr(cherrypy.engine, 'signal_handler'):
                cherrypy.engine.signal_handler.unsubscribe()

            cherrypy.config.update({
                'engine.autoreload.on': False,
                'checker.on': False,
                'tools.log_headers.on': False,
                'log.screen': False
            })
            try:
                cherrypy.engine.start()
                logger.info('Cherrypy engine started successfully.')
            except Exception as e:
                logger.error(f'Failed to start cherrypy engine: {e}')
                raise

        cls.configure_logging()
        adapter = cls.create_adapter(tree, bind_addr, ssl_info)
        cls.subscribe_adapter(adapter)
        adapter.start()

        return adapter, tree

    @classmethod
    def get_server_config(
        cls,
        name: str,
        mount_point: str = '/'
    ) -> Optional[Dict]:
        if name in cls._trees:
            tree = cls._trees[name]
            if mount_point in tree.apps:
                return tree.apps[mount_point].config
            if mount_point == '/' and '' in tree.apps:
                return tree.apps[''].config
            stripped = mount_point.rstrip('/')
            if stripped in tree.apps:
                return tree.apps[stripped].config
        return None

    @classmethod
    def unregister(cls, name: str) -> None:
        cls._trees.pop(name, None)

    @staticmethod
    def configure_logging() -> None:
        cherrypy.log.access_log.propagate = True
        cherrypy.log.error_log.propagate = False

        error_log = logging.getLogger('cherrypy.error')

        # make sure we only add the filter once
        has_filter = any(isinstance(f, CherryPyErrorFilter) for f in error_log.filters)
        if not has_filter:
            error_log.addFilter(CherryPyErrorFilter())

        access_filter = CherryPyAccessFilter()

        root_log = logging.getLogger()

        for handler in root_log.handlers:
            if not any(isinstance(f, CherryPyAccessFilter) for f in handler.filters):
                handler.addFilter(access_filter)

    @staticmethod
    def create_adapter(
        app: Any,
        bind_addr: Tuple[str, int],
        ssl_info: Optional[Dict[str, Any]] = None,
    ) -> ServerAdapter:
        server = WSGIServer(
            bind_addr=bind_addr,
            wsgi_app=app,
            numthreads=30,
            server_name='Ceph-Mgr'
        )

        if ssl_info:
            ssl_adapter = _SSLBuiltinAdapter(ssl_info['cert'], ssl_info['key'])
            if ssl_info.get('context'):
                ssl_adapter.context = ssl_info['context']
            server.ssl_adapter = ssl_adapter

        adapter = ServerAdapter(cherrypy.engine, server, bind_addr)
        return adapter

    @staticmethod
    def subscribe_adapter(adapter: ServerAdapter) -> None:
        adapter.subscribe()
