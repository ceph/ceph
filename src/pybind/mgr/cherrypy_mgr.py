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
import cherrypy
from cherrypy.process.servers import ServerAdapter
from cheroot.wsgi import Server as WSGIServer
from cheroot.ssl.builtin import BuiltinSSLAdapter
from cherrypy._cptree import Tree
from typing import Any, Tuple, Optional, Dict

logger = logging.getLogger(__name__)


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
        cherrypy.log.access_log.propagate = False
        cherrypy.log.error_log.propagate = False

        error_log = logging.getLogger('cherrypy.error')

        # make sure we only add the filter once
        has_filter = any(isinstance(f, CherryPyErrorFilter) for f in error_log.filters)
        if not has_filter:
            error_log.addFilter(CherryPyErrorFilter())

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
            ssl_adapter = BuiltinSSLAdapter(ssl_info['cert'], ssl_info['key'])
            if ssl_info.get('context'):
                ssl_adapter.context = ssl_info['context']
            server.ssl_adapter = ssl_adapter

        adapter = ServerAdapter(cherrypy.engine, server, bind_addr)
        return adapter

    @staticmethod
    def subscribe_adapter(adapter: ServerAdapter) -> None:
        adapter.subscribe()
