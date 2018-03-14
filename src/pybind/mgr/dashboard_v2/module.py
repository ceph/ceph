# -*- coding: utf-8 -*-
"""
openATTIC mgr plugin (based on CherryPy)
"""
from __future__ import absolute_import

import errno
import os
import socket
try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin
try:
    import cherrypy
except ImportError:
    # To be picked up and reported by .can_run()
    cherrypy = None

from mgr_module import MgrModule, MgrStandbyModule

if 'COVERAGE_ENABLED' in os.environ:
    import coverage
    _cov = coverage.Coverage(config_file="{}/.coveragerc".format(os.path.dirname(__file__)))
    _cov.start()

# pylint: disable=wrong-import-position
from . import logger, mgr
from .controllers.auth import Auth
from .tools import load_controllers, json_error_page, SessionExpireAtBrowserCloseTool, \
                   NotificationQueue, RequestLoggingTool
from .settings import options_command_list, handle_option_command


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


class Module(MgrModule):
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
        }
    ]
    COMMANDS.extend(options_command_list())

    @property
    def url_prefix(self):
        return self._url_prefix

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        mgr.init(self)
        self._url_prefix = ''

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

    def configure_cherrypy(self):
        server_addr = self.get_localized_config('server_addr', '::')
        server_port = self.get_localized_config('server_port', '8080')
        if server_addr is None:
            raise RuntimeError(
                'no server_addr configured; '
                'try "ceph config-key put mgr/{}/{}/server_addr <ip>"'
                .format(self.module_name, self.get_mgr_id()))
        self.log.info('server_addr: %s server_port: %s', server_addr,
                      server_port)

        self._url_prefix = prepare_url_prefix(self.get_config('url_prefix',
                                                              default=''))

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
        cherrypy.config.update(config)

        config = {
            '/': {
                'tools.staticdir.on': True,
                'tools.staticdir.dir': self.get_frontend_path(),
                'tools.staticdir.index': 'index.html'
            }
        }

        # Publish the URI that others may use to access the service we're
        # about to start serving
        self.set_uri("http://{0}:{1}{2}/".format(
            socket.getfqdn() if server_addr == "::" else server_addr,
            server_port,
            self.url_prefix
        ))

        cherrypy.tree.mount(Module.ApiRoot(self), '{}/api'.format(self.url_prefix))
        cherrypy.tree.mount(Module.StaticRoot(), '{}/'.format(self.url_prefix), config=config)

    def serve(self):
        if 'COVERAGE_ENABLED' in os.environ:
            _cov.start()
        self.configure_cherrypy()

        cherrypy.engine.start()
        NotificationQueue.start_queue()
        logger.info('Waiting for engine...')
        cherrypy.engine.block()
        if 'COVERAGE_ENABLED' in os.environ:
            _cov.stop()
            _cov.save()
        logger.info('Engine done')

    def shutdown(self):
        super(Module, self).shutdown()
        logger.info('Stopping server...')
        NotificationQueue.stop()
        cherrypy.engine.exit()
        logger.info('Stopped server')

    def handle_command(self, cmd):
        res = handle_option_command(cmd)
        if res[0] != -errno.ENOSYS:
            return res
        if cmd['prefix'] == 'dashboard set-login-credentials':
            Auth.set_login_credentials(cmd['username'], cmd['password'])
            return 0, 'Username and password updated', ''
        elif cmd['prefix'] == 'dashboard set-session-expire':
            self.set_config('session-expire', str(cmd['seconds']))
            return 0, 'Session expiration timeout updated', ''

        return (-errno.EINVAL, '', 'Command not found \'{0}\''
                .format(cmd['prefix']))

    def notify(self, notify_type, notify_id):
        NotificationQueue.new_notification(notify_type, notify_id)

    class ApiRoot(object):

        _cp_config = {
            'tools.sessions.on': True,
            'tools.authenticate.on': True
        }

        def __init__(self, mgrmod):
            self.ctrls = load_controllers()
            logger.debug('Loaded controllers: %s', self.ctrls)

            first_level_ctrls = [ctrl for ctrl in self.ctrls
                                 if '/' not in ctrl._cp_path_]
            multi_level_ctrls = set(self.ctrls).difference(first_level_ctrls)

            for ctrl in first_level_ctrls:
                logger.info('Adding controller: %s -> /api/%s', ctrl.__name__,
                            ctrl._cp_path_)
                inst = ctrl()
                setattr(Module.ApiRoot, ctrl._cp_path_, inst)

            for ctrl in multi_level_ctrls:
                path_parts = ctrl._cp_path_.split('/')
                path = '/'.join(path_parts[:-1])
                key = path_parts[-1]
                parent_ctrl_classes = [c for c in self.ctrls
                                       if c._cp_path_ == path]
                if len(parent_ctrl_classes) != 1:
                    logger.error('No parent controller found for %s! '
                                 'Please check your path in the ApiController '
                                 'decorator!', ctrl)
                else:
                    inst = ctrl()
                    setattr(parent_ctrl_classes[0], key, inst)

        @cherrypy.expose
        def index(self):
            tpl = """API Endpoints:<br>
            <ul>
            {lis}
            </ul>
            """
            endpoints = ['<li><a href="{}">{}</a></li>'.format(ctrl._cp_path_, ctrl.__name__) for
                         ctrl in self.ctrls]
            return tpl.format(lis='\n'.join(endpoints))

    class StaticRoot(object):
        pass


class StandbyModule(MgrStandbyModule):
    def serve(self):
        server_addr = self.get_localized_config('server_addr', '::')
        server_port = self.get_localized_config('server_port', '7000')
        if server_addr is None:
            msg = 'no server_addr configured; try "ceph config-key set ' \
                  'mgr/dashboard/server_addr <ip>"'
            raise RuntimeError(msg)
        self.log.info("server_addr: %s server_port: %s",
                      server_addr, server_port)
        cherrypy.config.update({
            'server.socket_host': server_addr,
            'server.socket_port': int(server_port),
            'engine.autoreload.on': False
        })

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

        url_prefix = prepare_url_prefix(self.get_config('url_prefix',
                                                        default=''))
        cherrypy.tree.mount(Root(), "{}/".format(url_prefix), {})
        self.log.info("Starting engine...")
        cherrypy.engine.start()
        self.log.info("Waiting for engine...")
        cherrypy.engine.wait(state=cherrypy.engine.states.STOPPED)
        self.log.info("Engine done.")

    def shutdown(self):
        self.log.info("Stopping server...")
        cherrypy.engine.wait(state=cherrypy.engine.states.STARTED)
        cherrypy.engine.stop()
        self.log.info("Stopped server")
