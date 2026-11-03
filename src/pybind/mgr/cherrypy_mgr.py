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

    # With 'defer_handshake': True in ssl_info, each connection's TLS handshake
    # runs on the worker thread that serves it rather than on the accept thread
    # (see DeferredHandshakeSSLAdapter)

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
import cherrypy
import re
import socket
import ssl
import threading
import time
from cherrypy.process.servers import ServerAdapter
from cheroot.server import HTTPConnection
from cheroot.wsgi import Server as WSGIServer
from cheroot.ssl.builtin import BuiltinSSLAdapter
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


class DeferredHandshakeSSLAdapter(BuiltinSSLAdapter):
    """
    A BuiltinSSLAdapter that leaves the TLS handshake to the worker thread
    serving the connection.

    cheroot accepts every connection and calls wrap() on a single thread,
    and BuiltinSSLAdapter.wrap() completes the handshake there. A server
    that gets many new connections, or a few slow clients, then has them
    wait in line for that thread. Here wrap() only sets up the TLS state,
    and DeferredHandshakeConnection completes the handshake on the worker
    thread, before it reads the request.

    The WSGI environ lacks the SSL_* entries describing the negotiated
    session, which are only known after the handshake.
    """

    def wrap(self, sock: Any) -> Tuple[Any, Dict[str, str]]:
        try:
            s = self.context.wrap_socket(sock, server_side=True,
                                         do_handshake_on_connect=False)
        except OSError:
            # the client is gone already
            sock.close()
            return None, {}
        return s, {'wsgi.url_scheme': 'https', 'HTTPS': 'on'}


class DeferredHandshakeConnection(HTTPConnection):
    """
    An HTTPConnection that completes the TLS handshake DeferredHandshakeSSLAdapter
    left undone, on the worker thread, before it reads the first request.

    Like BuiltinSSLAdapter.wrap(), it drops a connection whose handshake
    fails without logging an error (a client that gave up, or one that
    speaks some other protocol), and answers a client that sent plain HTTP
    with a 400.
    """
    _handshake_done = False

    def communicate(self) -> bool:
        if not self._handshake_done:
            self._handshake_done = True
            try:
                self.socket.do_handshake()
            except ssl.SSLError as e:
                if getattr(e, 'reason', None) == 'HTTP_REQUEST':
                    self._reply_plain_http()
                logger.debug('TLS handshake with %s failed: %s', getattr(self, 'remote_addr', '?'), e)
                return False
            except OSError as e:
                logger.debug('TLS handshake with %s failed: %s', getattr(self, 'remote_addr', '?'), e)
                return False
        return bool(super().communicate())

    def _reply_plain_http(self) -> None:
        # The response BuiltinSSLAdapter's caller sends, in plain text: the
        # TLS layer never came up, so write past it to the socket itself.
        msg = 'The client sent a plain HTTP request, but this server only speaks HTTPS on this port.'
        response = (f'{self.server.protocol} 400 Bad Request\r\n'
                    f'Content-Length: {len(msg)}\r\n'
                    'Content-Type: text/plain\r\n\r\n'
                    f'{msg}')
        try:
            socket.socket.sendall(self.socket, response.encode('ISO-8859-1'))
        except OSError:
            pass


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
        :param ssl_info: Dict containing {'cert': path, 'key': path, 'context': ssl_context},
            and optionally 'defer_handshake': True (see DeferredHandshakeSSLAdapter)
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
            adapter_class = BuiltinSSLAdapter
            if ssl_info.get('defer_handshake'):
                adapter_class = DeferredHandshakeSSLAdapter
                server.ConnectionClass = DeferredHandshakeConnection
            ssl_adapter = adapter_class(ssl_info['cert'], ssl_info['key'])
            if ssl_info.get('context'):
                ssl_adapter.context = ssl_info['context']
            server.ssl_adapter = ssl_adapter

        adapter = ServerAdapter(cherrypy.engine, server, bind_addr)
        return adapter

    @staticmethod
    def subscribe_adapter(adapter: ServerAdapter) -> None:
        adapter.subscribe()
