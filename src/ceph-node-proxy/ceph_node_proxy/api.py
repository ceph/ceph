import cherrypy  # type: ignore
from urllib.error import HTTPError
from cherrypy._cpserver import Server  # type: ignore
from threading import Thread, Event
from typing import Dict, Any, List
from ceph_node_proxy.util import Config, get_logger, write_tmp_file
from ceph_node_proxy.basesystem import BaseSystem
from ceph_node_proxy.reporter import Reporter
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ceph_node_proxy.main import NodeProxyManager


@cherrypy.tools.auth_basic(on=True)
@cherrypy.tools.allow(methods=['PUT'])
@cherrypy.tools.json_out()
class Admin():
    def __init__(self, api: 'API') -> None:
        self.api = api

    @cherrypy.expose
    def start(self) -> Dict[str, str]:
        self.api.backend.start()
        self.api.reporter.run()
        return {'ok': 'node-proxy daemon started'}

    @cherrypy.expose
    def reload(self) -> Dict[str, str]:
        self.api.config.reload()
        return {'ok': 'node-proxy config reloaded'}

    def _stop(self) -> None:
        self.api.backend.shutdown()
        self.api.reporter.shutdown()

    @cherrypy.expose
    def stop(self) -> Dict[str, str]:
        self._stop()
        return {'ok': 'node-proxy daemon stopped'}

    @cherrypy.expose
    def shutdown(self) -> Dict[str, str]:
        self._stop()
        cherrypy.engine.exit()
        return {'ok': 'Server shutdown.'}

    @cherrypy.expose
    def flush(self) -> Dict[str, str]:
        self.api.backend.flush()
        return {'ok': 'node-proxy data flushed'}


class API(Server):
    def __init__(self,
                 backend: 'BaseSystem',
                 reporter: 'Reporter',
                 config: 'Config',
                 addr: str = '0.0.0.0',
                 port: int = 0) -> None:
        super().__init__()
        self.log = get_logger(__name__)
        self.backend = backend
        self.reporter = reporter
        self.config = config
        self.socket_port = self.config.__dict__['api']['port'] if not port else port
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

    def _cp_dispatch(self, vpath: List[str]) -> 'API':
        if vpath[0] == 'led' and len(vpath) > 1:  # /led/{type}/{id}
            _type = vpath[1]
            cherrypy.request.params['type'] = _type
            vpath.pop(1)  # /led/{id} or # /led
            if _type == 'drive' and len(vpath) > 1:  # /led/{id}
                _id = vpath[1]
                vpath.pop(1)  # /led
                cherrypy.request.params['id'] = _id
            vpath[0] = '_led'
        # /<endpoint>
        return self

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.auth_basic(on=True)
    def shutdown(self, **kw: Any) -> int:
        data: Dict[str, bool] = cherrypy.request.json

        if 'force' not in data.keys():
            msg = "The key 'force' wasn't passed."
            self.log.debug(msg)
            raise cherrypy.HTTPError(400, msg)
        try:
            result: int = self.backend.shutdown_host(force=data['force'])
        except HTTPError as e:
            raise cherrypy.HTTPError(e.code, e.reason)
        return result

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.auth_basic(on=True)
    def powercycle(self, **kw: Any) -> int:
        try:
            result: int = self.backend.powercycle()
        except HTTPError as e:
            raise cherrypy.HTTPError(e.code, e.reason)
        return result

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'PATCH'])
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.tools.auth_basic(on=True)
    def _led(self, **kw: Any) -> Dict[str, Any]:
        method: str = cherrypy.request.method
        led_type: Optional[str] = kw.get('type')
        id_drive: Optional[str] = kw.get('id')
        result: Dict[str, Any] = dict()

        if not led_type:
            msg = "the led type must be provided (either 'chassis' or 'drive')."
            self.log.debug(msg)
            raise cherrypy.HTTPError(400, msg)

        if led_type == 'drive':
            id_drive_required = not id_drive
            if id_drive_required or id_drive not in self.backend.get_storage():
                msg = 'A valid device ID must be provided.'
                self.log.debug(msg)
                raise cherrypy.HTTPError(400, msg)

        try:
            if method == 'PATCH':
                data: Dict[str, Any] = cherrypy.request.json

                if 'state' not in data or data['state'] not in ['on', 'off']:
                    msg = "Invalid data. 'state' must be provided and have a valid value (on|off)."
                    self.log.error(msg)
                    raise cherrypy.HTTPError(400, msg)

                func: Any = (self.backend.device_led_on if led_type == 'drive' and data['state'] == 'on' else
                             self.backend.device_led_off if led_type == 'drive' and data['state'] == 'off' else
                             self.backend.chassis_led_on if led_type != 'drive' and data['state'] == 'on' else
                             self.backend.chassis_led_off if led_type != 'drive' and data['state'] == 'off' else None)

            else:
                func = self.backend.get_device_led if led_type == 'drive' else self.backend.get_chassis_led

            result = func(id_drive) if led_type == 'drive' else func()

        except HTTPError as e:
            raise cherrypy.HTTPError(e.code, e.reason)
        return result

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
            result = {'state': 'error: please, verify the data you sent.'}
        else:
            result = {'state': data['state'].lower()}
        return result

    def stop(self) -> None:
        self.unsubscribe()
        super().stop()


class NodeProxyApi(Thread):
    def __init__(self, node_proxy_mgr: 'NodeProxyManager') -> None:
        super().__init__()
        self.log = get_logger(__name__)
        self.cp_shutdown_event = Event()
        self.node_proxy_mgr = node_proxy_mgr
        self.username = self.node_proxy_mgr.username
        self.password = self.node_proxy_mgr.password
        self.ssl_crt = self.node_proxy_mgr.api_ssl_crt
        self.ssl_key = self.node_proxy_mgr.api_ssl_key
        self.system = self.node_proxy_mgr.system
        self.reporter_agent = self.node_proxy_mgr.reporter_agent
        self.config = self.node_proxy_mgr.config
        self.api = API(self.system, self.reporter_agent, self.config)

    def check_auth(self, realm: str, username: str, password: str) -> bool:
        return self.username == username and \
            self.password == password

    def shutdown(self) -> None:
        self.log.info('Stopping node-proxy API...')
        self.cp_shutdown_event.set()

    def run(self) -> None:
        self.log.info('node-proxy API configuration...')
        cherrypy.config.update({
            'environment': 'production',
            'engine.autoreload.on': False,
            'log.screen': True,
        })
        config = {'/': {
            'request.methods_with_bodies': ('POST', 'PUT', 'PATCH'),
            'tools.trailing_slash.on': False,
            'tools.auth_basic.realm': 'localhost',
            'tools.auth_basic.checkpassword': self.check_auth
        }}
        cherrypy.tree.mount(self.api, '/', config=config)
        # cherrypy.tree.mount(admin, '/admin', config=config)

        ssl_crt = write_tmp_file(self.ssl_crt,
                                 prefix_name='listener-crt-')
        ssl_key = write_tmp_file(self.ssl_key,
                                 prefix_name='listener-key-')

        self.api.ssl_certificate = ssl_crt.name
        self.api.ssl_private_key = ssl_key.name

        cherrypy.server.unsubscribe()
        try:
            cherrypy.engine.start()
            self.log.info('node-proxy API started.')
            self.cp_shutdown_event.wait()
            self.cp_shutdown_event.clear()
            cherrypy.engine.exit()
            cherrypy.server.httpserver = None
            self.log.info('node-proxy API shutdown.')
        except Exception as e:
            self.log.error(f'node-proxy API error: {e}')
