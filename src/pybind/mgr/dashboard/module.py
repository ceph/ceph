# -*- coding: utf-8 -*-
"""
openATTIC mgr plugin (based on CherryPy)
"""
from __future__ import absolute_import

import collections
import errno
from distutils.version import StrictVersion
from distutils.util import strtobool
import os
import socket
import tempfile
import threading
import time
from uuid import uuid4
from OpenSSL import crypto, SSL

from mgr_module import MgrModule, MgrStandbyModule

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin

try:
    import cherrypy
except ImportError:
    # To be picked up and reported by .can_run()
    cherrypy = None


# The SSL code in CherryPy 3.5.0 is buggy.  It was fixed long ago,
# but 3.5.0 is still shipping in major linux distributions
# (Fedora 27, Ubuntu Xenial), so we must monkey patch it to get SSL working.
if cherrypy is not None:
    v = StrictVersion(cherrypy.__version__)
    # It was fixed in 3.7.0.  Exact lower bound version is probably earlier,
    # but 3.5.0 is what this monkey patch is tested on.
    if v >= StrictVersion("3.5.0") and v < StrictVersion("3.7.0"):
        from cherrypy.wsgiserver.wsgiserver2 import HTTPConnection,\
                                                    CP_fileobject

        def fixed_init(hc_self, server, sock, makefile=CP_fileobject):
            hc_self.server = server
            hc_self.socket = sock
            hc_self.rfile = makefile(sock, "rb", hc_self.rbufsize)
            hc_self.wfile = makefile(sock, "wb", hc_self.wbufsize)
            hc_self.requests_seen = 0

        HTTPConnection.__init__ = fixed_init

# When the CherryPy server in 3.2.2 (and later) starts it attempts to verify
# that the ports its listening on are in fact bound. When using the any address
# "::" it tries both ipv4 and ipv6, and in some environments (e.g. kubernetes)
# ipv6 isn't yet configured / supported and CherryPy throws an uncaught
# exception.
if cherrypy is not None:
    v = StrictVersion(cherrypy.__version__)
    # the issue was fixed in 3.2.3. it's present in 3.2.2 (current version on
    # centos:7) and back to at least 3.0.0.
    if StrictVersion("3.1.2") <= v < StrictVersion("3.2.3"):
        # https://github.com/cherrypy/cherrypy/issues/1100
        from cherrypy.process import servers
        servers.wait_for_occupied_port = lambda host, port: None

if 'COVERAGE_ENABLED' in os.environ:
    import coverage
    _cov = coverage.Coverage(config_file="{}/.coveragerc".format(os.path.dirname(__file__)))
    _cov.start()

# pylint: disable=wrong-import-position
from . import logger, mgr
from .controllers import generate_routes, json_error_page
from .controllers.auth import Auth
from .tools import SessionExpireAtBrowserCloseTool, NotificationQueue, \
                   RequestLoggingTool, TaskManager
from .settings import options_command_list, options_schema_list, \
                      handle_option_command


# cherrypy likes to sys.exit on error.  don't let it take us down too!
# pylint: disable=W0613
def os_exit_noop(*args):
    pass


# pylint: disable=W0212
os._exit = os_exit_noop


def prepare_url_prefix(url_prefix):
    """
    return '' if no prefix, or '/prefix' without slash in the end.
    """
    url_prefix = urljoin('/', url_prefix)
    return url_prefix.rstrip('/')


class ServerConfigException(Exception):
    pass


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

    # pylint: disable=too-many-branches
    def _configure(self):
        """
        Configure CherryPy and initialize self.url_prefix

        :returns our URI
        """
        server_addr = self.get_localized_config('server_addr', '::')
        ssl = strtobool(self.get_localized_config('ssl', 'True'))
        def_server_port = 8443
        if not ssl:
            def_server_port = 8080

        server_port = self.get_localized_config('server_port', def_server_port)
        if server_addr is None:
            raise ServerConfigException(
                'no server_addr configured; '
                'try "ceph config set mgr mgr/{}/{}/server_addr <ip>"'
                .format(self.module_name, self.get_mgr_id()))
        self.log.info('server_addr: %s server_port: %s', server_addr,
                      server_port)

        # Initialize custom handlers.
        cherrypy.tools.authenticate = cherrypy.Tool('before_handler', Auth.check_auth)
        cherrypy.tools.session_expire_at_browser_close = SessionExpireAtBrowserCloseTool()
        cherrypy.tools.request_logging = RequestLoggingTool()

        # Apply the 'global' CherryPy configuration.
        config = {
            'engine.autoreload.on': False,
            'server.socket_host': server_addr,
            'server.socket_port': int(server_port),
            'error_page.default': json_error_page,
            'tools.request_logging.on': True
        }

        if ssl:
            # SSL initialization
            cert = self.get_store("crt")
            if cert is not None:
                self.cert_tmp = tempfile.NamedTemporaryFile()
                self.cert_tmp.write(cert.encode('utf-8'))
                self.cert_tmp.flush()  # cert_tmp must not be gc'ed
                cert_fname = self.cert_tmp.name
            else:
                cert_fname = self.get_localized_config('crt_file')

            pkey = self.get_store("key")
            if pkey is not None:
                self.pkey_tmp = tempfile.NamedTemporaryFile()
                self.pkey_tmp.write(pkey.encode('utf-8'))
                self.pkey_tmp.flush()  # pkey_tmp must not be gc'ed
                pkey_fname = self.pkey_tmp.name
            else:
                pkey_fname = self.get_localized_config('key_file')

            if not cert_fname or not pkey_fname:
                raise ServerConfigException('no certificate configured')
            if not os.path.isfile(cert_fname):
                raise ServerConfigException('certificate %s does not exist' % cert_fname)
            if not os.path.isfile(pkey_fname):
                raise ServerConfigException('private key %s does not exist' % pkey_fname)

            # Do some validations to the private key and certificate:
            # - Check the type and format
            # - Check the certificate expiration date
            # - Check the consistency of the private key
            # - Check that the private key and certificate match up
            try:
                with open(cert_fname) as f:
                    x509 = crypto.load_certificate(crypto.FILETYPE_PEM, f.read())
                    if x509.has_expired():
                        self.log.warning(
                            'Certificate {} has been expired'.format(cert_fname))
            except (ValueError, crypto.Error) as e:
                raise ServerConfigException(
                    'Invalid certificate {}: {}'.format(cert_fname, str(e)))
            try:
                with open(pkey_fname) as f:
                    pkey = crypto.load_privatekey(crypto.FILETYPE_PEM, f.read())
                    pkey.check()
            except (ValueError, crypto.Error) as e:
                raise ServerConfigException(
                    'Invalid private key {}: {}'.format(pkey_fname, str(e)))
            try:
                context = SSL.Context(SSL.TLSv1_METHOD)
                context.use_certificate_file(cert_fname, crypto.FILETYPE_PEM)
                context.use_privatekey_file(pkey_fname, crypto.FILETYPE_PEM)
                context.check_privatekey()
            except crypto.Error as e:
                self.log.warning(
                    'Private key {} and certificate {} do not match up: {}'.format(
                        pkey_fname, cert_fname, str(e)))

            config['server.ssl_module'] = 'builtin'
            config['server.ssl_certificate'] = cert_fname
            config['server.ssl_private_key'] = pkey_fname

        cherrypy.config.update(config)

        self._url_prefix = prepare_url_prefix(self.get_config('url_prefix',
                                                              default=''))

        uri = "{0}://{1}:{2}{3}/".format(
            'https' if ssl else 'http',
            socket.getfqdn() if server_addr == "::" else server_addr,
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
                self.log.info("Config not ready to serve, waiting: {0}".format(
                    e
                ))
                # Poll until a non-errored config is present
                self._stopping.wait(5)
            else:
                self.log.info("Configured CherryPy, starting engine...")
                return uri


class Module(MgrModule, CherryPyConfig):
    """
    dashboard module entrypoint
    """

    COMMANDS = [
        {
            'cmd': 'dashboard set-login-credentials '
                   'name=username,type=CephString '
                   'name=password,type=CephString',
            'desc': 'Set the login credentials',
            'perm': 'w'
        },
        {
            'cmd': 'dashboard set-session-expire '
                   'name=seconds,type=CephInt',
            'desc': 'Set the session expire timeout',
            'perm': 'w'
        },
        {
            "cmd": "dashboard create-self-signed-cert",
            "desc": "Create self signed certificate",
            "perm": "w"
        },
    ]
    COMMANDS.extend(options_command_list())

    OPTIONS = [
        {'name': 'server_addr'},
        {'name': 'server_port'},
        {'name': 'session-expire'},
        {'name': 'password'},
        {'name': 'url_prefix'},
        {'name': 'username'},
        {'name': 'key_file'},
        {'name': 'crt_file'},
        {'name': 'ssl'}
    ]
    OPTIONS.extend(options_schema_list())

    __pool_stats = collections.defaultdict(lambda: collections.defaultdict(
        lambda: collections.deque(maxlen=10)))

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        CherryPyConfig.__init__(self)

        mgr.init(self)

        self._stopping = threading.Event()
        self.shutdown_event = threading.Event()

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
            _cov.start()

        uri = self.await_configuration()
        if uri is None:
            # We were shut down while waiting
            return

        # Publish the URI that others may use to access the service we're
        # about to start serving
        self.set_uri(uri)

        mapper = generate_routes(self.url_prefix)

        config = {
            self.url_prefix or '/': {
                'tools.staticdir.on': True,
                'tools.staticdir.dir': self.get_frontend_path(),
                'tools.staticdir.index': 'index.html'
            },
            '{}/api'.format(self.url_prefix): {'request.dispatch': mapper}
        }
        cherrypy.tree.mount(None, config=config)

        cherrypy.engine.start()
        NotificationQueue.start_queue()
        TaskManager.init()
        logger.info('Engine started.')
        # wait for the shutdown event
        self.shutdown_event.wait()
        self.shutdown_event.clear()
        NotificationQueue.stop()
        cherrypy.engine.stop()
        if 'COVERAGE_ENABLED' in os.environ:
            _cov.stop()
            _cov.save()
        logger.info('Engine stopped')

    def shutdown(self):
        super(Module, self).shutdown()
        CherryPyConfig.shutdown(self)
        logger.info('Stopping engine...')
        self.shutdown_event.set()

    def handle_command(self, inbuf, cmd):
        res = handle_option_command(cmd)
        if res[0] != -errno.ENOSYS:
            return res
        if cmd['prefix'] == 'dashboard set-login-credentials':
            Auth.set_login_credentials(cmd['username'], cmd['password'])
            return 0, 'Username and password updated', ''
        elif cmd['prefix'] == 'dashboard set-session-expire':
            self.set_config('session-expire', str(cmd['seconds']))
            return 0, 'Session expiration timeout updated', ''
        elif cmd['prefix'] == 'dashboard create-self-signed-cert':
            self.create_self_signed_cert()
            return 0, 'Self-signed certificate created', ''

        return (-errno.EINVAL, '', 'Command not found \'{0}\''
                .format(cmd['prefix']))

    def create_self_signed_cert(self):
        # create a key pair
        pkey = crypto.PKey()
        pkey.generate_key(crypto.TYPE_RSA, 2048)

        # create a self-signed cert
        cert = crypto.X509()
        cert.get_subject().O = "IT"
        cert.get_subject().CN = "ceph-dashboard"
        cert.set_serial_number(int(uuid4()))
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(10*365*24*60*60)
        cert.set_issuer(cert.get_subject())
        cert.set_pubkey(pkey)
        cert.sign(pkey, 'sha512')

        cert = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
        self.set_store('crt', cert.decode('utf-8'))

        pkey = crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey)
        self.set_store('key', pkey.decode('utf-8'))

    def notify(self, notify_type, notify_id):
        NotificationQueue.new_notification(notify_type, notify_id)

    def get_updated_pool_stats(self):
        df = self.get('df')
        pool_stats = dict([(p['id'], p['stats']) for p in df['pools']])
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
            def index(self):
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
                        the dashboard.  A failover may be in progress.
                        Retrying in {delay} seconds...
                    </body>
                </html>
                    """
                    return template.format(delay=5)

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
