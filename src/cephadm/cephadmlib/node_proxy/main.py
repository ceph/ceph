import cherrypy
from threading import Thread
from .redfish_dell import RedfishDell
from .reporter import Reporter
from .util import Config, Logger
from typing import Dict, Any, Optional
from .basesystem import BaseSystem
import sys
import argparse
import json

DEFAULT_CONFIG = {
    'reporter': {
        'check_interval': 5,
        'push_data_max_retries': 30,
        'endpoint': 'https://127.0.0.1:7150/node-proxy/data',
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


class Memory:
    exposed = True

    def __init__(self, backend: BaseSystem) -> None:
        self.backend = backend

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
        return {'memory': self.backend.get_memory()}


class Network:
    exposed = True

    def __init__(self, backend: BaseSystem) -> None:
        self.backend = backend

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
        return {'network': self.backend.get_network()}


class Processors:
    exposed = True

    def __init__(self, backend: BaseSystem) -> None:
        self.backend = backend

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
        return {'processors': self.backend.get_processors()}


class Storage:
    exposed = True

    def __init__(self, backend: BaseSystem) -> None:
        self.backend = backend

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
        return {'storage': self.backend.get_storage()}


class Status:
    exposed = True

    def __init__(self, backend: BaseSystem) -> None:
        self.backend = backend

    @cherrypy.tools.json_out()
    def GET(self) -> Dict[str, Dict[str, Dict]]:
        return {'status': self.backend.get_status()}


class System:
    exposed = True

    def __init__(self, backend: BaseSystem) -> None:
        self.memory = Memory(backend)
        self.network = Network(backend)
        self.processors = Processors(backend)
        self.storage = Storage(backend)
        self.status = Status(backend)
        # actions = Actions()
        # control = Control()


class Shutdown:
    exposed = True

    def __init__(self, backend: BaseSystem, reporter: Reporter) -> None:
        self.backend = backend
        self.reporter = reporter

    def POST(self) -> str:
        _stop(self.backend, self.reporter)
        cherrypy.engine.exit()
        return 'Server shutdown...'


def _stop(backend: BaseSystem, reporter: Reporter) -> None:
    backend.stop_update_loop()
    backend.client.logout()
    reporter.stop()


class Start:
    exposed = True

    def __init__(self, backend: BaseSystem, reporter: Reporter) -> None:
        self.backend = backend
        self.reporter = reporter

    def POST(self) -> str:
        self.backend.start_client()
        # self.backend.start_update_loop()
        self.reporter.run()
        return 'node-proxy daemon started'


class Stop:
    exposed = True

    def __init__(self, backend: BaseSystem, reporter: Reporter) -> None:
        self.backend = backend
        self.reporter = reporter

    def POST(self) -> str:
        _stop(self.backend, self.reporter)
        return 'node-proxy daemon stopped'


class ConfigReload:
    exposed = True

    def __init__(self, config: Config) -> None:
        self.config = config

    def POST(self) -> str:
        self.config.reload()
        return 'node-proxy config reloaded'


class Flush:
    exposed = True

    def __init__(self, backend: BaseSystem) -> None:
        self.backend = backend

    def POST(self) -> str:
        self.backend.flush()
        return 'node-proxy data flushed'


class Admin:
    exposed = False

    def __init__(self, backend: BaseSystem, config: Config, reporter: Reporter) -> None:
        self.reload = ConfigReload(config)
        self.flush = Flush(backend)
        self.shutdown = Shutdown(backend, reporter)
        self.start = Start(backend, reporter)
        self.stop = Stop(backend, reporter)


class API:
    exposed = True

    def __init__(self,
                 backend: BaseSystem,
                 reporter: Reporter,
                 config: Config) -> None:

        self.system = System(backend)
        self.admin = Admin(backend, config, reporter)

    def GET(self) -> str:
        return 'use /system or /admin endpoints'


class NodeProxy(Thread):
    def __init__(self, **kw: Dict[str, Any]) -> None:
        super().__init__()
        for k, v in kw.items():
            setattr(self, k, v)
        self.exc: Optional[Exception] = None

    def run(self) -> None:
        try:
            self.main()
        except Exception as e:
            self.exc = e
            return

    def check_status(self) -> bool:
        if self.__dict__.get('system') and not self.system.run:
            raise RuntimeError("node-proxy encountered an error.")
        if self.exc:
            raise self.exc
        return True

    def main(self) -> None:
        # TODO: add a check and fail if host/username/password/data aren't passed

        config = Config('/etc/ceph/node-proxy.yml', default_config=DEFAULT_CONFIG)

        log = Logger(__name__, level=config.__dict__['logging']['level'])

        self.data = json.loads(self.__dict__['data'])

        # create the redfish system and the obsever
        log.logger.info(f"Server initialization...")
        try:
            self.system = RedfishDell(host=self.__dict__['host'],
                                      username=self.__dict__['username'],
                                      password=self.__dict__['password'],
                                      system_endpoint='/Systems/System.Embedded.1',
                                      config=config)
        except RuntimeError:
            log.logger.error("Can't initialize the redfish system.")
            raise

        try:
            reporter_agent = Reporter(self.system,
                                      self.data,
                                      f"https://{self.__dict__['mgr_target_ip']}:{self.__dict__['mgr_target_port']}/node-proxy/data")
        except RuntimeError:
            log.logger.error("Can't initialize the reporter.")
            raise

        cherrypy.config.update({
            'node_proxy': config,
            'server.socket_port': config.__dict__['server']['port']
        })
        c = {'/': {
            'request.methods_with_bodies': ('POST', 'PUT', 'PATCH'),
            'request.dispatch': cherrypy.dispatch.MethodDispatcher()
        }}
        reporter_agent.run()
        cherrypy.quickstart(API(self.system, reporter_agent, config), config=c)
