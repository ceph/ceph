# -*- coding: utf-8 -*-
"""
openATTIC mgr plugin (based on CherryPy)
"""
from __future__ import absolute_import

import errno
import os
try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin

import cherrypy
from mgr_module import MgrModule

if 'COVERAGE_ENABLED' in os.environ:
    import coverage
    _cov = coverage.Coverage(config_file="{}/.coveragerc".format(os.path.dirname(__file__)))
    _cov.start()

# pylint: disable=wrong-import-position
from .controllers.auth import Auth
from .tools import load_controllers, json_error_page, SessionExpireAtBrowserCloseTool, \
                   NotificationQueue
from . import logger


# cherrypy likes to sys.exit on error.  don't let it take us down too!
# pylint: disable=W0613
def os_exit_noop(*args):
    pass


# pylint: disable=W0212
os._exit = os_exit_noop


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

    @property
    def url_prefix(self):
        return self._url_prefix

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        logger.logger = self._logger
        self._url_prefix = ''

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

        def prepare_url_prefix(url_prefix):
            """
            return '' if no prefix, or '/prefix' without slash in the end.
            """
            url_prefix = urljoin('/', url_prefix)
            return url_prefix.rstrip('/')

        self._url_prefix = prepare_url_prefix(self.get_config('url_prefix',
                                                              default=''))

        # Initialize custom handlers.
        cherrypy.tools.authenticate = cherrypy.Tool('before_handler', Auth.check_auth)
        cherrypy.tools.session_expire_at_browser_close = SessionExpireAtBrowserCloseTool()

        # Apply the 'global' CherryPy configuration.
        config = {
            'engine.autoreload.on': False,
            'server.socket_host': server_addr,
            'server.socket_port': int(server_port),
            'error_page.default': json_error_page
        }
        cherrypy.config.update(config)

        current_dir = os.path.dirname(os.path.abspath(__file__))
        fe_dir = os.path.join(current_dir, 'frontend/dist')
        config = {
            '/': {
                'tools.staticdir.on': True,
                'tools.staticdir.dir': fe_dir,
                'tools.staticdir.index': 'index.html'
            }
        }

        cherrypy.tree.mount(Module.ApiRoot(self), '/api')
        cherrypy.tree.mount(Module.StaticRoot(), '/', config=config)

    def serve(self):
        if 'COVERAGE_ENABLED' in os.environ:
            _cov.start()
        self.configure_cherrypy()

        cherrypy.engine.start()
        NotificationQueue.start_queue()
        logger.info('Waiting for engine...')
        self.log.info('Waiting for engine...')
        cherrypy.engine.block()
        if 'COVERAGE_ENABLED' in os.environ:
            _cov.stop()
            _cov.save()
        logger.info('Engine done')

    def shutdown(self):
        logger.info('Stopping server...')
        NotificationQueue.stop()
        cherrypy.engine.exit()
        logger.info('Stopped server')

    def handle_command(self, cmd):
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

        def __init__(self, mgrmod):
            self.ctrls = load_controllers(mgrmod)
            logger.debug('Loaded controllers: %s', self.ctrls)
            for ctrl in self.ctrls:
                logger.info('Adding controller: %s -> /api/%s', ctrl.__name__,
                            ctrl._cp_path_)
                ins = ctrl()
                setattr(Module.ApiRoot, ctrl._cp_path_, ins)

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
