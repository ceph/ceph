# -*- coding: utf-8 -*-
"""
ceph dashboard mgr plugin (based on CherryPy)
"""
from __future__ import absolute_import

import collections
import errno
import logging
import os
import socket
import tempfile
import threading
import time

from mgr_module import MgrModule, MgrStandbyModule, Option, CLIWriteCommand
from mgr_util import get_default_addr, ServerConfigException, verify_tls_files, \
    create_self_signed_cert

try:
    import cherrypy
    from cherrypy._cptools import HandlerWrapperTool
except ImportError:
    # To be picked up and reported by .can_run()
    cherrypy = None

from .services.sso import load_sso_db

if cherrypy is not None:
    from .cherrypy_backports import patch_cherrypy
    patch_cherrypy(cherrypy.__version__)

# pylint: disable=wrong-import-position
from . import mgr
from .controllers import generate_routes, json_error_page
from .grafana import push_local_dashboards
from .tools import NotificationQueue, RequestLoggingTool, TaskManager, \
                   prepare_url_prefix, str_to_bool
from .services.auth import AuthManager, AuthManagerTool, JwtManager
from .services.sso import SSO_COMMANDS, \
                          handle_sso_command
from .services.exception import dashboard_exception_handler
from .settings import options_command_list, options_schema_list, \
                      handle_option_command

from .plugins import PLUGIN_MANAGER
from .plugins import feature_toggles, debug  # noqa # pylint: disable=unused-import


PLUGIN_MANAGER.hook.init()


# cherrypy likes to sys.exit on error.  don't let it take us down too!
# pylint: disable=W0613
def os_exit_noop(*args):
    pass


# pylint: disable=W0212
os._exit = os_exit_noop


logger = logging.getLogger(__name__)


class CherryPyConfig(object):
    """
    Class for common server configuration done by both active and
    standby module, especially setting up SSL.
    """

    def __init__(self):
        self._stopping = threading.Event()
        self._url_prefix = ""

        self.cert_tmp = None
        self.pkey_tmp = None

    def shutdown(self):
        self._stopping.set()

    @property
    def url_prefix(self):
        return self._url_prefix

    @staticmethod
    def update_cherrypy_config(config):
        PLUGIN_MANAGER.hook.configure_cherrypy(config=config)
        cherrypy.config.update(config)

    # pylint: disable=too-many-branches
    def _configure(self):
        """
        Configure CherryPy and initialize self.url_prefix

        :returns our URI
        """
        server_addr = self.get_localized_module_option(  # type: ignore
            'server_addr', get_default_addr())
        ssl = self.get_localized_module_option('ssl', True)  # type: ignore
        if not ssl:
            server_port = self.get_localized_module_option('server_port', 8080)  # type: ignore
        else:
            server_port = self.get_localized_module_option('ssl_server_port', 8443)  # type: ignore

        if server_addr is None:
            raise ServerConfigException(
                'no server_addr configured; '
                'try "ceph config set mgr mgr/{}/{}/server_addr <ip>"'
                .format(self.module_name, self.get_mgr_id()))  # type: ignore
        self.log.info('server: ssl=%s host=%s port=%d', 'yes' if ssl else 'no',  # type: ignore
                      server_addr, server_port)

        # Initialize custom handlers.
        cherrypy.tools.authenticate = AuthManagerTool()
        cherrypy.tools.plugin_hooks_filter_request = cherrypy.Tool(
            'before_handler',
            lambda: PLUGIN_MANAGER.hook.filter_request_before_handler(request=cherrypy.request),
            priority=1)
        cherrypy.tools.request_logging = RequestLoggingTool()
        cherrypy.tools.dashboard_exception_handler = HandlerWrapperTool(dashboard_exception_handler,
                                                                        priority=31)

        cherrypy.log.access_log.propagate = False
        cherrypy.log.error_log.propagate = False

        # Apply the 'global' CherryPy configuration.
        config = {
            'engine.autoreload.on': False,
            'server.socket_host': server_addr,
            'server.socket_port': int(server_port),
            'error_page.default': json_error_page,
            'tools.request_logging.on': True,
            'tools.gzip.on': True,
            'tools.gzip.mime_types': [
                # text/html and text/plain are the default types to compress
                'text/html', 'text/plain',
                # We also want JSON and JavaScript to be compressed
                'application/json',
                'application/javascript',
            ],
            'tools.json_in.on': True,
            'tools.json_in.force': False,
            'tools.plugin_hooks_filter_request.on': True,
        }

        if ssl:
            # SSL initialization
            cert = self.get_store("crt")  # type: ignore
            if cert is not None:
                self.cert_tmp = tempfile.NamedTemporaryFile()
                self.cert_tmp.write(cert.encode('utf-8'))
                self.cert_tmp.flush()  # cert_tmp must not be gc'ed
                cert_fname = self.cert_tmp.name
            else:
                cert_fname = self.get_localized_module_option('crt_file')  # type: ignore

            pkey = self.get_store("key")  # type: ignore
            if pkey is not None:
                self.pkey_tmp = tempfile.NamedTemporaryFile()
                self.pkey_tmp.write(pkey.encode('utf-8'))
                self.pkey_tmp.flush()  # pkey_tmp must not be gc'ed
                pkey_fname = self.pkey_tmp.name
            else:
                pkey_fname = self.get_localized_module_option('key_file')  # type: ignore

            verify_tls_files(cert_fname, pkey_fname)

            config['server.ssl_module'] = 'builtin'
            config['server.ssl_certificate'] = cert_fname
            config['server.ssl_private_key'] = pkey_fname

        self.update_cherrypy_config(config)

        self._url_prefix = prepare_url_prefix(self.get_module_option(  # type: ignore
            'url_prefix', default=''))

        uri = "{0}://{1}:{2}{3}/".format(
            'https' if ssl else 'http',
            socket.getfqdn(server_addr if server_addr != '::' else ''),
            server_port,
            self.url_prefix
        )

        return uri

    def await_configuration(self):
        """
        Block until configuration is ready (i.e. all needed keys are set)
        or self._stopping is set.

        :returns URI of configured webserver
        """
        while not self._stopping.is_set():
            try:
                uri = self._configure()
            except ServerConfigException as e:
                self.log.info(  # type: ignore
                    "Config not ready to serve, waiting: {0}".format(e)
                )
                # Poll until a non-errored config is present
                self._stopping.wait(5)
            else:
                self.log.info("Configured CherryPy, starting engine...")  # type: ignore
                return uri


class Module(MgrModule, CherryPyConfig):
    """
    dashboard module entrypoint
    """

    COMMANDS = [
        {
            'cmd': 'dashboard set-jwt-token-ttl '
                   'name=seconds,type=CephInt',
            'desc': 'Set the JWT token TTL in seconds',
            'perm': 'w'
        },
        {
            'cmd': 'dashboard get-jwt-token-ttl',
            'desc': 'Get the JWT token TTL in seconds',
            'perm': 'r'
        },
        {
            "cmd": "dashboard create-self-signed-cert",
            "desc": "Create self signed certificate",
            "perm": "w"
        },
        {
            "cmd": "dashboard grafana dashboards update",
            "desc": "Push dashboards to Grafana",
            "perm": "w",
        },
    ]
    COMMANDS.extend(options_command_list())
    COMMANDS.extend(SSO_COMMANDS)
    PLUGIN_MANAGER.hook.register_commands()

    MODULE_OPTIONS = [
        Option(name='server_addr', type='str', default=get_default_addr()),
        Option(name='server_port', type='int', default=8080),
        Option(name='ssl_server_port', type='int', default=8443),
        Option(name='jwt_token_ttl', type='int', default=28800),
        Option(name='password', type='str', default=''),
        Option(name='url_prefix', type='str', default=''),
        Option(name='username', type='str', default=''),
        Option(name='key_file', type='str', default=''),
        Option(name='crt_file', type='str', default=''),
        Option(name='ssl', type='bool', default=True),
        Option(name='standby_behaviour', type='str', default='redirect',
               enum_allowed=['redirect', 'error']),
        Option(name='standby_error_status_code', type='int', default=500,
               min=400, max=599)
    ]
    MODULE_OPTIONS.extend(options_schema_list())
    for options in PLUGIN_MANAGER.hook.get_options() or []:
        MODULE_OPTIONS.extend(options)

    __pool_stats = collections.defaultdict(lambda: collections.defaultdict(
        lambda: collections.deque(maxlen=10)))  # type: dict

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        CherryPyConfig.__init__(self)

        mgr.init(self)

        self._stopping = threading.Event()
        self.shutdown_event = threading.Event()

        self.ACCESS_CTRL_DB = None
        self.SSO_DB = None

    @classmethod
    def can_run(cls):
        if cherrypy is None:
            return False, "Missing dependency: cherrypy"

        if not os.path.exists(cls.get_frontend_path()):
            return False, "Frontend assets not found: incomplete build?"

        return True, ""

    @classmethod
    def get_frontend_path(cls):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, 'frontend/dist')

    def serve(self):

        if 'COVERAGE_ENABLED' in os.environ:
            import coverage
            __cov = coverage.Coverage(config_file="{}/.coveragerc"
                                      .format(os.path.dirname(__file__)),
                                      data_suffix=True)
            __cov.start()
            cherrypy.engine.subscribe('after_request', __cov.save)
            cherrypy.engine.subscribe('stop', __cov.stop)

        AuthManager.initialize()
        load_sso_db()

        uri = self.await_configuration()
        if uri is None:
            # We were shut down while waiting
            return

        # Publish the URI that others may use to access the service we're
        # about to start serving
        self.set_uri(uri)

        mapper, parent_urls = generate_routes(self.url_prefix)

        config = {}
        for purl in parent_urls:
            config[purl] = {
                'request.dispatch': mapper
            }

        cherrypy.tree.mount(None, config=config)

        PLUGIN_MANAGER.hook.setup()

        cherrypy.engine.start()
        NotificationQueue.start_queue()
        TaskManager.init()
        logger.info('Engine started.')
        update_dashboards = str_to_bool(
            self.get_module_option('GRAFANA_UPDATE_DASHBOARDS', 'False'))
        if update_dashboards:
            logger.info('Starting Grafana dashboard task')
            TaskManager.run(
                'grafana/dashboards/update',
                {},
                push_local_dashboards,
                kwargs=dict(tries=10, sleep=60),
            )
        # wait for the shutdown event
        self.shutdown_event.wait()
        self.shutdown_event.clear()
        NotificationQueue.stop()
        cherrypy.engine.stop()
        logger.info('Engine stopped')

    def shutdown(self):
        super(Module, self).shutdown()
        CherryPyConfig.shutdown(self)
        logger.info('Stopping engine...')
        self.shutdown_event.set()

    @CLIWriteCommand("dashboard set-ssl-certificate",
                     "name=mgr_id,type=CephString,req=false")
    def set_ssl_certificate(self, mgr_id=None, inbuf=None):
        if inbuf is None:
            return -errno.EINVAL, '',\
                   'Please specify the certificate file with "-i" option'
        if mgr_id is not None:
            self.set_store('{}/crt'.format(mgr_id), inbuf)
        else:
            self.set_store('crt', inbuf)
        return 0, 'SSL certificate updated', ''

    @CLIWriteCommand("dashboard set-ssl-certificate-key",
                     "name=mgr_id,type=CephString,req=false")
    def set_ssl_certificate_key(self, mgr_id=None, inbuf=None):
        if inbuf is None:
            return -errno.EINVAL, '',\
                   'Please specify the certificate key file with "-i" option'
        if mgr_id is not None:
            self.set_store('{}/key'.format(mgr_id), inbuf)
        else:
            self.set_store('key', inbuf)
        return 0, 'SSL certificate key updated', ''

    def handle_command(self, inbuf, cmd):
        # pylint: disable=too-many-return-statements
        res = handle_option_command(cmd)
        if res[0] != -errno.ENOSYS:
            return res
        res = handle_sso_command(cmd)
        if res[0] != -errno.ENOSYS:
            return res
        if cmd['prefix'] == 'dashboard set-jwt-token-ttl':
            self.set_module_option('jwt_token_ttl', str(cmd['seconds']))
            return 0, 'JWT token TTL updated', ''
        if cmd['prefix'] == 'dashboard get-jwt-token-ttl':
            ttl = self.get_module_option('jwt_token_ttl', JwtManager.JWT_TOKEN_TTL)
            return 0, str(ttl), ''
        if cmd['prefix'] == 'dashboard create-self-signed-cert':
            self.create_self_signed_cert()
            return 0, 'Self-signed certificate created', ''
        if cmd['prefix'] == 'dashboard grafana dashboards update':
            push_local_dashboards()
            return 0, 'Grafana dashboards updated', ''

        return (-errno.EINVAL, '', 'Command not found \'{0}\''
                .format(cmd['prefix']))

    def create_self_signed_cert(self):
        cert, pkey = create_self_signed_cert('IT', 'ceph-dashboard')
        self.set_store('crt', cert)
        self.set_store('key', pkey)

    def notify(self, notify_type, notify_id):
        NotificationQueue.new_notification(notify_type, notify_id)

    def get_updated_pool_stats(self):
        df = self.get('df')
        pool_stats = {p['id']: p['stats'] for p in df['pools']}
        now = time.time()
        for pool_id, stats in pool_stats.items():
            for stat_name, stat_val in stats.items():
                self.__pool_stats[pool_id][stat_name].append((now, stat_val))

        return self.__pool_stats


class StandbyModule(MgrStandbyModule, CherryPyConfig):
    def __init__(self, *args, **kwargs):
        super(StandbyModule, self).__init__(*args, **kwargs)
        CherryPyConfig.__init__(self)
        self.shutdown_event = threading.Event()

        # We can set the global mgr instance to ourselves even though
        # we're just a standby, because it's enough for logging.
        mgr.init(self)

    def serve(self):
        uri = self.await_configuration()
        if uri is None:
            # We were shut down while waiting
            return

        module = self

        class Root(object):
            @cherrypy.expose
            def default(self, *args, **kwargs):
                if module.get_module_option('standby_behaviour', 'redirect') == 'redirect':
                    active_uri = module.get_active_uri()
                    if active_uri:
                        module.log.info("Redirecting to active '%s'", active_uri)
                        raise cherrypy.HTTPRedirect(active_uri)
                    else:
                        template = """
                    <html>
                        <!-- Note: this is only displayed when the standby
                             does not know an active URI to redirect to, otherwise
                             a simple redirect is returned instead -->
                        <head>
                            <title>Ceph</title>
                            <meta http-equiv="refresh" content="{delay}">
                        </head>
                        <body>
                            No active ceph-mgr instance is currently running
                            the dashboard. A failover may be in progress.
                            Retrying in {delay} seconds...
                        </body>
                    </html>
                        """
                        return template.format(delay=5)
                else:
                    status = module.get_module_option('standby_error_status_code', 500)
                    raise cherrypy.HTTPError(status, message="Keep on looking")

        cherrypy.tree.mount(Root(), "{}/".format(self.url_prefix), {})
        self.log.info("Starting engine...")
        cherrypy.engine.start()
        self.log.info("Engine started...")
        # Wait for shutdown event
        self.shutdown_event.wait()
        self.shutdown_event.clear()
        cherrypy.engine.stop()
        self.log.info("Engine stopped.")

    def shutdown(self):
        CherryPyConfig.shutdown(self)

        self.log.info("Stopping engine...")
        self.shutdown_event.set()
        self.log.info("Stopped engine...")
