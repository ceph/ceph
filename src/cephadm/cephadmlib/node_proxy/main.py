import cherrypy
from cherrypy._cpserver import Server
from threading import Thread, Event
from .redfishdellsystem import RedfishDellSystem
from .reporter import Reporter
from .util import Config, Logger
from typing import Dict, Any, Optional, List
from .basesystem import BaseSystem
import traceback

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


@cherrypy.tools.auth_basic(on=True)
@cherrypy.tools.allow(methods=['PUT'])
@cherrypy.tools.json_out()
class Admin():
    def __init__(self, api: 'API') -> None:
        self.api = api

    @cherrypy.expose
    def start(self) -> Dict[str, str]:
        self.api.backend.start_client()
        # self.backend.start_update_loop()
        self.api.reporter.run()
        return {"ok": "node-proxy daemon started"}

    @cherrypy.expose
    def reload(self) -> Dict[str, str]:
        self.api.config.reload()
        return {"ok": "node-proxy config reloaded"}

    def _stop(self) -> None:
        self.api.backend.stop_update_loop()
        self.api.backend.client.logout()
        self.api.reporter.stop()

    @cherrypy.expose
    def stop(self) -> Dict[str, str]:
        self._stop()
        return {"ok": "node-proxy daemon stopped"}

    @cherrypy.expose
    def shutdown(self) -> Dict[str, str]:
        self._stop()
        cherrypy.engine.exit()
        return {"ok": "Server shutdown."}

    @cherrypy.expose
    def flush(self) -> Dict[str, str]:
        self.api.backend.flush()
        return {"ok": "node-proxy data flushed"}


class API(Server):
    def __init__(self,
                 backend: BaseSystem,
                 reporter: Reporter,
                 config: Config,
                 addr: str = '0.0.0.0',
                 port: int = 0) -> None:
        super().__init__()
        self.log = Logger(__name__)
        self.backend = backend
        self.reporter = reporter
        self.config = config
        self.socket_port = self.config.__dict__['server']['port'] if not port else port
        self.socket_host = addr
        self.subscribe()

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def memory(self) -> Dict[str, Any]:
        return {'memory': self.backend.get_memory()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def network(self) -> Dict[str, Any]:
        return {'network': self.backend.get_network()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def processors(self) -> Dict[str, Any]:
        return {'processors': self.backend.get_processors()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def storage(self) -> Dict[str, Any]:
        return {'storage': self.backend.get_storage()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def power(self) -> Dict[str, Any]:
        return {'power': self.backend.get_power()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def fans(self) -> Dict[str, Any]:
        return {'fans': self.backend.get_fans()}

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def firmwares(self) -> Dict[str, Any]:
        return {'firmwares': self.backend.get_firmwares()}

    def _cp_dispatch(self, vpath: List[str]) -> "API":
        if vpath[0] == 'led':
            if cherrypy.request.method == 'GET':
                return self.get_led
            if cherrypy.request.method == 'PATCH':
                return self.set_led
        return self

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def get_led(self, **kw: Dict[str, Any]) -> Dict[str, Any]:
        return self.backend.get_led()

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['PATCH'])
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.auth_basic(on=True)
    def set_led(self, **kw: Dict[str, Any]) -> Dict[str, Any]:
        data = cherrypy.request.json
        rc = self.backend.set_led(data)

        if rc != 200:
            cherrypy.response.status = rc
            result = {"state": f"error: please, verify the data you sent."}
        else:
            result = {"state": data["state"].lower()}
        return result

    def stop(self) -> None:
        self.unsubscribe()
        super().stop()


class NodeProxy(Thread):
    def __init__(self, **kw: Dict[str, Any]) -> None:
        super().__init__()
        for k, v in kw.items():
            setattr(self, k, v)
        self.exc: Optional[Exception] = None
        self.cp_shutdown_event = Event()
        self.log = Logger(__name__)

    def run(self) -> None:
        try:
            self.main()
        except Exception as e:
            self.exc = e
            return

    def check_auth(self, realm: str, username: str, password: str) -> bool:
        return self.__dict__['username'] == username and \
            self.__dict__['password'] == password

    def check_status(self) -> bool:
        if self.__dict__.get('system') and not self.system.run:
            raise RuntimeError("node-proxy encountered an error.")
        if self.exc:
            traceback.print_tb(self.exc.__traceback__)
            self.log.logger.error(f"{self.exc.__class__.__name__}: {self.exc}")
            raise self.exc
        return True

    def start_api(self) -> None:
        cherrypy.server.unsubscribe()
        cherrypy.engine.start()
        self.reporter_agent.run()
        self.cp_shutdown_event.wait()
        self.cp_shutdown_event.clear()
        cherrypy.engine.stop()
        cherrypy.server.httpserver = None
        self.log.logger.info("node-proxy shutdown.")

    def main(self) -> None:
        # TODO: add a check and fail if host/username/password/data aren't passed
        self.config = Config('/etc/ceph/node-proxy.yml', default_config=DEFAULT_CONFIG)
        self.log = Logger(__name__, level=self.config.__dict__['logging']['level'])

        # create the redfish system and the obsever
        self.log.logger.info(f"Server initialization...")
        try:
            self.system = RedfishDellSystem(host=self.__dict__['host'],
                                            port=self.__dict__.get('port', 443),
                                            username=self.__dict__['username'],
                                            password=self.__dict__['password'],
                                            config=self.config)
        except RuntimeError:
            self.log.logger.error("Can't initialize the redfish system.")
            raise

        try:
            self.reporter_agent = Reporter(self.system,
                                           self.__dict__['cephx'],
                                           f"https://{self.__dict__['mgr_target_ip']}:{self.__dict__['mgr_target_port']}/node-proxy/data")
        except RuntimeError:
            self.log.logger.error("Can't initialize the reporter.")
            raise
        self.api = API(self.system,
                       self.reporter_agent,
                       self.config)
        self.admin = Admin(self.api)
        self.configure()
        self.start_api()

    def configure(self) -> None:
        cherrypy.config.update({
            'environment': 'production',
            'engine.autoreload.on': False,
        })
        config = {'/': {
            'request.methods_with_bodies': ('POST', 'PUT', 'PATCH'),
            'tools.trailing_slash.on': False,
            'tools.auth_basic.realm': 'localhost',
            'tools.auth_basic.checkpassword': self.check_auth
        }}
        cherrypy.tree.mount(self.api, '/', config=config)
        cherrypy.tree.mount(self.admin, '/admin', config=config)
        self.api.ssl_certificate = self.__dict__['ssl_crt_path']
        self.api.ssl_private_key = self.__dict__['ssl_key_path']

    def shutdown(self) -> None:
        self.log.logger.info("Shutting node-proxy down...")
        self.cp_shutdown_event.set()
