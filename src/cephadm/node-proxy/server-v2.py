import cherrypy
from redfish_dell import RedfishDell
from reporter import Reporter
from util import Config, Logger
from typing import Dict
import sys

# for devel purposes
import os
DEVEL_ENV_VARS = ['REDFISH_HOST',
                  'REDFISH_USERNAME',
                  'REDFISH_PASSWORD']

DEFAULT_CONFIG = {
    'reporter': {
        'check_interval': 5,
        'push_data_max_retries': 30,
    },
    'system': {
        'refresh_interval': 5
    },
    'server': {
        'port': 8080,
    },
    'logging': {
        'level': 20,
    }
}

for env_var in DEVEL_ENV_VARS:
    if os.environ.get(env_var) is None:
        print(f"{env_var} environment variable must be set.")
        sys.exit(1)

config = Config(default_config=DEFAULT_CONFIG)

log = Logger(__name__, level=config.logging['level'])
# must be passed as arguments
host = os.environ.get('REDFISH_HOST')
username = os.environ.get('REDFISH_USERNAME')
password = os.environ.get('REDFISH_PASSWORD')

# create the redfish system and the obsever
log.logger.info("Server initialization...")
system = RedfishDell(host=host,
                     username=username,
                     password=password,
                     system_endpoint='/Systems/System.Embedded.1',
                     config=config)
reporter_agent = Reporter(system, "http://127.0.0.1:8000")


class Memory:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
        return {'memory': system.get_memory()}


class Network:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
        return {'network': system.get_network()}


class Processors:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
        return {'processors': system.get_processors()}


class Storage:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
        return {'storage': system.get_storage()}


class Status:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
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

    def POST(self) -> str:
        _stop()
        cherrypy.engine.exit()
        return 'Server shutdown...'


def _stop() -> None:
    system.stop_update_loop()
    system.client.logout()
    reporter_agent.stop()


class Start:
    exposed = True

    def POST(self) -> str:
        system.start_client()
        system.start_update_loop()
        reporter_agent.run()
        return 'node-proxy daemon started'


class Stop:
    exposed = True

    def POST(self) -> str:
        _stop()
        return 'node-proxy daemon stopped'


class ConfigReload:
    exposed = True

    def __init__(self, config: cherrypy.config) -> None:
        self.config = config

    def POST(self) -> str:
        self.config['node_proxy'].reload()
        return 'node-proxy config reloaded'


class API:
    exposed = True

    system = System()
    shutdown = Shutdown()
    start = Start()
    stop = Stop()
    config_reload = ConfigReload(cherrypy.config)

    def GET(self) -> str:
        return 'use /system'


if __name__ == '__main__':
    cherrypy.config.update({
        'node_proxy': config,
        'server.socket_port': config.__dict__['server']['port']
    })
    c = {'/': {
        'request.methods_with_bodies': ('POST', 'PUT', 'PATCH'),
        'request.dispatch': cherrypy.dispatch.MethodDispatcher()
    }}
    system.start_update_loop()
    reporter_agent.run()
    cherrypy.quickstart(API(), config=c)
