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
        cherrypy.config.update({'server.socket_host': '0.0.0.0',
                                'server.socket_port': 8080,
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
