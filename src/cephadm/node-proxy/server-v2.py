import cherrypy
from redfish_dell import RedfishDell
from reporter import Reporter
from util import logger
import sys

# for devel purposes
import os
DEVEL_ENV_VARS = ['REDFISH_HOST',
                  'REDFISH_USERNAME',
                  'REDFISH_PASSWORD']
for env_var in DEVEL_ENV_VARS:
    if os.environ.get(env_var) is None:
        print(f"{env_var} environment variable must be set.")
        sys.exit(1)

log = logger(__name__)

# must be passed as arguments
host = os.environ.get('REDFISH_HOST')
username = os.environ.get('REDFISH_USERNAME')
password = os.environ.get('REDFISH_PASSWORD')

# create the redfish system and the obsever
log.info("Server initialization...")
system = RedfishDell(host=host, username=username, password=password, system_endpoint='/Systems/System.Embedded.1')
reporter_agent = Reporter(system, "http://127.0.0.1:8000")


class Memory:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self):
        return {'memory': system.get_memory()}


class Network:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self):
        return {'network': system.get_network()}


class Processors:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self):
        return {'processors': system.get_processors()}


class Storage:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self):
        return {'storage': system.get_storage()}


class Status:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self):
        return {'status': system.get_status()}


class System:
    exposed = True
    memory = Memory()
    network = Network()
    processors = Processors()
    storage = Storage()
    status = Status()
    # actions = Actions()
    # control = Control()


class Shutdown:
    exposed = True

    def POST(self):
        _stop()
        cherrypy.engine.exit()
        return 'Server shutdown...'


def _stop():
    system.stop_update_loop()
    system.client.logout()
    reporter_agent.stop()


class Start:
    exposed = True

    def POST(self):
        system.start_client()
        system.start_update_loop()
        reporter_agent.run()
        return 'node-proxy daemon started'


class Stop:
    exposed = True

    def POST(self):
        _stop()
        return 'node-proxy daemon stopped'


class API:
    exposed = True

    system = System()
    shutdown = Shutdown()
    start = Start()
    stop = Stop()

    def GET(self):
        return 'use /system'


if __name__ == '__main__':
    cherrypy.config.update({
        'server.socket_port': 8080
    })
    config = {'/': {
        'request.methods_with_bodies': ('POST', 'PUT', 'PATCH'),
        'request.dispatch': cherrypy.dispatch.MethodDispatcher()
    }}
    system.start_update_loop()
    reporter_agent.run()
    cherrypy.quickstart(API(), config=config)
