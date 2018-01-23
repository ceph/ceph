# -*- coding: utf-8 -*-

"""
openATTIC mgr plugin (based on CherryPy)
"""

import cherrypy
from cherrypy import tools

from mgr_module import MgrModule

"""
openATTIC CherryPy Module
"""


class Module(MgrModule):

    """
    Hello.
    """

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
        cherrypy.engine.wait(state=cherrypy.engine.states.STOPPED)

    def shutdown(self):
        cherrypy.engine.wait(state=cherrypy.engine.states.STARTED)
        cherrypy.engine.stop()

    def handle_command(self, cmd):
        pass

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
