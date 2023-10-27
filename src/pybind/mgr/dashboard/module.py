# -*- coding: utf-8 -*-
"""
ceph dashboard mgr plugin (based on CherryPy)
"""
import collections
import errno
import logging
import os
import socket
import ssl
import sys
import tempfile
import threading
import time
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

if TYPE_CHECKING:
    if sys.version_info >= (3, 8):
        from typing import Literal
    else:
        from typing_extensions import Literal

from mgr_module import CLIReadCommand, CLIWriteCommand, HandleCommandResult, \
    MgrModule, MgrStandbyModule, NotifyType, Option, _get_localized_key
from mgr_util import ServerConfigException, build_url, \
    create_self_signed_cert, get_default_addr, verify_tls_files

from . import mgr
from .controllers import Router, json_error_page
from .grafana import push_local_dashboards
from .services.auth import AuthManager, AuthManagerTool, JwtManager
from .services.exception import dashboard_exception_handler
from .services.rgw_client import configure_rgw_credentials
from .services.sso import SSO_COMMANDS, handle_sso_command
from .settings import Settings, handle_option_command, options_command_list, options_schema_list
from .tools import NotificationQueue, RequestLoggingTool, TaskManager, \
    prepare_url_prefix, str_to_bool

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
from .plugins import PLUGIN_MANAGER, debug, feature_toggles, motd  # isort:skip # noqa E501 # pylint: disable=unused-import

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
        use_ssl = self.get_localized_module_option('ssl', True)  # type: ignore
        if not use_ssl:
            server_port = self.get_localized_module_option('server_port', 8080)  # type: ignore
        else:
            server_port = self.get_localized_module_option('ssl_server_port', 8443)  # type: ignore

        if server_addr is None:
            raise ServerConfigException(
                'no server_addr configured; '
                'try "ceph config set mgr mgr/{}/{}/server_addr <ip>"'
                .format(self.module_name, self.get_mgr_id()))  # type: ignore
        self.log.info('server: ssl=%s host=%s port=%d', 'yes' if use_ssl else 'no',  # type: ignore
                      server_addr, server_port)

        # Initialize custom handlers.
        cherrypy.tools.authenticate = AuthManagerTool()
        self.configure_cors()
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
                'application/*+json',
                'application/javascript',
            ],
            'tools.json_in.on': True,
            'tools.json_in.force': True,
            'tools.plugin_hooks_filter_request.on': True,
        }

        if use_ssl:
            # SSL initialization
            cert = self.get_localized_store("crt")  # type: ignore
            if cert is not None:
                self.cert_tmp = tempfile.NamedTemporaryFile()
                self.cert_tmp.write(cert.encode('utf-8'))
                self.cert_tmp.flush()  # cert_tmp must not be gc'ed
                cert_fname = self.cert_tmp.name
            else:
                cert_fname = self.get_localized_module_option('crt_file')  # type: ignore

            pkey = self.get_localized_store("key")  # type: ignore
            if pkey is not None:
                self.pkey_tmp = tempfile.NamedTemporaryFile()
                self.pkey_tmp.write(pkey.encode('utf-8'))
                self.pkey_tmp.flush()  # pkey_tmp must not be gc'ed
                pkey_fname = self.pkey_tmp.name
            else:
                pkey_fname = self.get_localized_module_option('key_file')  # type: ignore

            verify_tls_files(cert_fname, pkey_fname)

            # Create custom SSL context to disable TLS 1.0 and 1.1.
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.load_cert_chain(cert_fname, pkey_fname)
            if sys.version_info >= (3, 7):
                if Settings.UNSAFE_TLS_v1_2:
                    context.minimum_version = ssl.TLSVersion.TLSv1_2
                else:
                    context.minimum_version = ssl.TLSVersion.TLSv1_3
            else:
                if Settings.UNSAFE_TLS_v1_2:
                    context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
                else:
                    context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1 | ssl.OP_NO_TLSv1_2

            config['server.ssl_module'] = 'builtin'
            config['server.ssl_certificate'] = cert_fname
            config['server.ssl_private_key'] = pkey_fname
            config['server.ssl_context'] = context

        self.update_cherrypy_config(config)

        self._url_prefix = prepare_url_prefix(self.get_module_option(  # type: ignore
            'url_prefix', default=''))

        if server_addr in ['::', '0.0.0.0']:
            server_addr = self.get_mgr_ip()  # type: ignore
        base_url = build_url(
            scheme='https' if use_ssl else 'http',
            host=server_addr,
            port=server_port,
        )
        uri = f'{base_url}{self.url_prefix}/'
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

    def configure_cors(self):
        """
        Allow CORS requests if the cross_origin_url option is set.
        """
        cross_origin_url = mgr.get_localized_module_option('cross_origin_url', '')
        if cross_origin_url:
            cherrypy.tools.CORS = cherrypy.Tool('before_handler', self.cors_tool)
            config = {
                'tools.CORS.on': True,
            }
            self.update_cherrypy_config(config)

    def cors_tool(self):
        '''
        Handle both simple and complex CORS requests

        Add CORS headers to each response. If the request is a CORS preflight
        request swap out the default handler with a simple, single-purpose handler
        that verifies the request and provides a valid CORS response.
        '''
        req_head = cherrypy.request.headers
        resp_head = cherrypy.response.headers

        # Always set response headers necessary for 'simple' CORS.
        req_header_cross_origin_url = req_head.get('Access-Control-Allow-Origin')
        cross_origin_urls = mgr.get_localized_module_option('cross_origin_url', '')
        cross_origin_url_list = [url.strip() for url in cross_origin_urls.split(',')]
        if req_header_cross_origin_url in cross_origin_url_list:
            resp_head['Access-Control-Allow-Origin'] = req_header_cross_origin_url
        resp_head['Access-Control-Expose-Headers'] = 'GET, POST'
        resp_head['Access-Control-Allow-Credentials'] = 'true'

        # Non-simple CORS preflight request; short-circuit the normal handler.
        if cherrypy.request.method == 'OPTIONS':
            req_header_origin_url = req_head.get('Origin')
            if req_header_origin_url in cross_origin_url_list:
                resp_head['Access-Control-Allow-Origin'] = req_header_origin_url
            ac_method = req_head.get('Access-Control-Request-Method', None)

            allowed_methods = ['GET', 'POST', 'PUT']
            allowed_headers = [
                'Content-Type',
                'Authorization',
                'Accept',
                'Access-Control-Allow-Origin'
            ]

            if ac_method and ac_method in allowed_methods:
                resp_head['Access-Control-Allow-Methods'] = ', '.join(allowed_methods)
                resp_head['Access-Control-Allow-Headers'] = ', '.join(allowed_headers)

                resp_head['Connection'] = 'keep-alive'
                resp_head['Access-Control-Max-Age'] = '3600'

            # CORS requests should short-circuit the other tools.
            cherrypy.response.body = ''.encode('utf8')
            cherrypy.response.status = 200
            cherrypy.serving.request.handler = None

            # Needed to avoid the auth_tool check.
            if cherrypy.request.config.get('tools.sessions.on', False):
                cherrypy.session['token'] = True
            return True


if TYPE_CHECKING:
    SslConfigKey = Literal['crt', 'key']


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
        Option(name='url_prefix', type='str', default=''),
        Option(name='key_file', type='str', default=''),
        Option(name='crt_file', type='str', default=''),
        Option(name='ssl', type='bool', default=True),
        Option(name='standby_behaviour', type='str', default='redirect',
               enum_allowed=['redirect', 'error']),
        Option(name='standby_error_status_code', type='int', default=500,
               min=400, max=599),
        Option(name='redirect_resolve_ip_addr', type='bool', default=False),
        Option(name='cross_origin_url', type='str', default=''),
    ]
    MODULE_OPTIONS.extend(options_schema_list())
    for options in PLUGIN_MANAGER.hook.get_options() or []:
        MODULE_OPTIONS.extend(options)

    NOTIFY_TYPES = [NotifyType.clog]

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
        self.health_checks = {}

    @classmethod
    def can_run(cls):
        if cherrypy is None:
            return False, "Missing dependency: cherrypy"

        if not os.path.exists(cls.get_frontend_path()):
            return False, ("Frontend assets not found at '{}': incomplete build?"
                           .format(cls.get_frontend_path()))

        return True, ""

    @classmethod
    def get_frontend_path(cls):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(current_dir, 'frontend/dist')
        if os.path.exists(path):
            return path
        else:
            path = os.path.join(current_dir,
                                '../../../../build',
                                'src/pybind/mgr/dashboard',
                                'frontend/dist')
            return os.path.abspath(path)

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

        mapper, parent_urls = Router.generate_routes(self.url_prefix)

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

    def _set_ssl_item(self, item_label: str, item_key: 'SslConfigKey' = 'crt',
                      mgr_id: Optional[str] = None, inbuf: Optional[str] = None):
        if inbuf is None:
            return -errno.EINVAL, '', f'Please specify the {item_label} with "-i" option'

        if mgr_id is not None:
            self.set_store(_get_localized_key(mgr_id, item_key), inbuf)
        else:
            self.set_store(item_key, inbuf)
        return 0, f'SSL {item_label} updated', ''

    @CLIWriteCommand("dashboard set-ssl-certificate")
    def set_ssl_certificate(self, mgr_id: Optional[str] = None, inbuf: Optional[str] = None):
        return self._set_ssl_item('certificate', 'crt', mgr_id, inbuf)

    @CLIWriteCommand("dashboard set-ssl-certificate-key")
    def set_ssl_certificate_key(self, mgr_id: Optional[str] = None, inbuf: Optional[str] = None):
        return self._set_ssl_item('certificate key', 'key', mgr_id, inbuf)

    @CLIWriteCommand("dashboard create-self-signed-cert")
    def set_mgr_created_self_signed_cert(self):
        cert, pkey = create_self_signed_cert('IT', 'ceph-dashboard')
        result = HandleCommandResult(*self.set_ssl_certificate(inbuf=cert))
        if result.retval != 0:
            return result

        result = HandleCommandResult(*self.set_ssl_certificate_key(inbuf=pkey))
        if result.retval != 0:
            return result
        return 0, 'Self-signed certificate created', ''

    @CLIWriteCommand("dashboard set-rgw-credentials")
    def set_rgw_credentials(self):
        try:
            configure_rgw_credentials()
        except Exception as error:
            return -errno.EINVAL, '', str(error)

        return 0, 'RGW credentials configured', ''

    @CLIWriteCommand("dashboard set-login-banner")
    def set_login_banner(self, inbuf: str):
        '''
        Set the custom login banner read from -i <file>
        '''
        item_label = 'login banner file'
        if inbuf is None:
            return HandleCommandResult(
                -errno.EINVAL,
                stderr=f'Please specify the {item_label} with "-i" option'
            )
        mgr.set_store('custom_login_banner', inbuf)
        return HandleCommandResult(stdout=f'{item_label} added')

    @CLIReadCommand("dashboard get-login-banner")
    def get_login_banner(self):
        '''
        Get the custom login banner text
        '''
        banner_text = mgr.get_store('custom_login_banner')
        if banner_text is None:
            return HandleCommandResult(stdout='No login banner set')
        else:
            return HandleCommandResult(stdout=banner_text)

    @CLIWriteCommand("dashboard unset-login-banner")
    def unset_login_banner(self):
        '''
        Unset the custom login banner
        '''
        mgr.set_store('custom_login_banner', None)
        return HandleCommandResult(stdout='Login banner removed')

    def handle_command(self, inbuf, cmd):
        # pylint: disable=too-many-return-statements
        res = handle_option_command(cmd, inbuf)
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
        if cmd['prefix'] == 'dashboard grafana dashboards update':
            push_local_dashboards()
            return 0, 'Grafana dashboards updated', ''

        return (-errno.EINVAL, '', 'Command not found \'{0}\''
                .format(cmd['prefix']))

    def notify(self, notify_type: NotifyType, notify_id):
        NotificationQueue.new_notification(str(notify_type), notify_id)

    def get_updated_pool_stats(self):
        df = self.get('df')
        pool_stats = {p['id']: p['stats'] for p in df['pools']}
        now = time.time()
        for pool_id, stats in pool_stats.items():
            for stat_name, stat_val in stats.items():
                self.__pool_stats[pool_id][stat_name].append((now, stat_val))

        return self.__pool_stats

    def config_notify(self):
        """
        This method is called whenever one of our config options is changed.
        """
        PLUGIN_MANAGER.hook.config_notify()

    def refresh_health_checks(self):
        self.set_health_checks(self.health_checks)


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

                    if cherrypy.request.path_info.startswith('/api/prometheus_receiver'):
                        module.log.debug("Suppressed redirecting alert to active '%s'",
                                         active_uri)
                        cherrypy.response.status = 204
                        return None

                    if active_uri:
                        if module.get_module_option('redirect_resolve_ip_addr'):
                            p_result = urlparse(active_uri)
                            hostname = str(p_result.hostname)
                            fqdn_netloc = p_result.netloc.replace(
                                hostname, socket.getfqdn(hostname))
                            active_uri = p_result._replace(netloc=fqdn_netloc).geturl()

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
