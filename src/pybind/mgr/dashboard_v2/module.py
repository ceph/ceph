# -*- coding: utf-8 -*-

"""
openATTIC mgr plugin (based on CherryPy)
"""

import bcrypt
import os
import cherrypy
from cherrypy import tools

from mgr_module import MgrModule

# cherrypy likes to sys.exit on error.  don't let it take us down too!
def os_exit_noop(*args):
    pass

os._exit = os_exit_noop

"""
openATTIC CherryPy Module
"""
class Module(MgrModule):

    """
    Hello.
    """

    COMMANDS = [
        {
            "cmd": "dashboard set-login-credentials "
                   "name=username,type=CephString "
                   "name=password,type=CephString",
            "desc": "Set the login credentials",
            "perm": "w"
        }
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

    def serve(self):
        server_addr = self.get_localized_config('server_addr', '::')
        server_port = self.get_localized_config('server_port', '8080')
        if server_addr is None:
            raise RuntimeError(
                'no server_addr configured; '
                'try "ceph config-key put mgr/{}/{}/server_addr <ip>"'.format(
                self.module_name, self.get_mgr_id()))
        self.log.info("server_addr: %s server_port: %s" % (server_addr, server_port))

        cherrypy.config.update({'server.socket_host': server_addr,
                                'server.socket_port': int(server_port),
                               })
        cherrypy.tree.mount(Module.HelloWorld(self), "/")
        cherrypy.engine.start()
        self.log.info("Waiting for engine...")
        cherrypy.engine.block()
        self.log.info("Engine done.")

    def shutdown(self):
        self.log.info("Stopping server...")
        cherrypy.engine.exit()
        self.log.info("Stopped server")

    def handle_command(self, cmd):
        if cmd['prefix'] == 'dashboard set-login-credentials':
            self.set_localized_config('username', cmd['username'])
            hashed_passwd = bcrypt.hashpw(cmd['password'], bcrypt.gensalt())
            self.set_localized_config('password', hashed_passwd)
            return 0, 'Username and password updated', ''
        else:
            return (-errno.EINVAL, '', 'Command not found \'{0}\''.format(
                    cmd['prefix']))

    class HelloWorld(object):

        """

        Hello World.

        """

        def __init__(self, module):
            self.module = module
            self.log = module.log
            self.log.warn("Initiating WebServer CherryPy")

        @cherrypy.expose
        def index(self):
            """
            WS entrypoint
            """

            return "Hello World!"

        @cherrypy.expose
        @tools.json_out()
        def ping(self):
            """
            Ping endpoint
            """

            return "pong"
