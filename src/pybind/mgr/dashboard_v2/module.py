# -*- coding: utf-8 -*-
"""
openATTIC mgr plugin (based on CherryPy)
"""
from __future__ import absolute_import

import errno
import os

import cherrypy
from mgr_module import MgrModule

from .controllers.auth import Auth
from .tools import load_controllers, json_error_page


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
        }
    ]

    def configure_cherrypy(self, in_unittest=False):
        server_addr = self.get_localized_config('server_addr', '::')
        server_port = self.get_localized_config('server_port', '8080')
        if server_addr is None:
            raise RuntimeError(
                'no server_addr configured; '
                'try "ceph config-key put mgr/{}/{}/server_addr <ip>"'
                .format(self.module_name, self.get_mgr_id()))
        self.log.info('server_addr: %s server_port: %s', server_addr,
                      server_port)

        if not in_unittest:
            cherrypy.config.update({
                'server.socket_host': server_addr,
                'server.socket_port': int(server_port),
                'engine.autoreload.on': False,
                'error_page.default': json_error_page
            })
        cherrypy.tools.authenticate = cherrypy.Tool('before_handler',
                                                    Auth.check_auth)

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
        self.configure_cherrypy()

        cherrypy.engine.start()
        self.log.info('Waiting for engine...')
        cherrypy.engine.block()
        self.log.info('Engine done')

    def shutdown(self):
        self.log.info('Stopping server...')
        cherrypy.engine.exit()
        self.log.info('Stopped server')

    def handle_command(self, cmd):
        if cmd['prefix'] == 'dashboard set-login-credentials':
            Auth.set_login_credentials(cmd['username'], cmd['password'])
            return 0, 'Username and password updated', ''

        return (-errno.EINVAL, '', 'Command not found \'{0}\''
                .format(cmd['prefix']))

    class ApiRoot(object):

        def __init__(self, mgrmod):
            self.ctrls = load_controllers(mgrmod)
            mgrmod.log.debug('Loaded controllers: {}'.format(self.ctrls))
            for ctrl in self.ctrls:
                mgrmod.log.info('Adding controller: {} -> /api/{}'
                                .format(ctrl.__name__, ctrl._cp_path_))
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
