import threading
import time
import errno
from typing import TYPE_CHECKING, Tuple, Any, Optional
from cherrypy import _cptree
from cherrypy.process.servers import ServerAdapter
from cherrypy_mgr import CherryPyMgr

from cephadm.agent import AgentEndpoint
from cephadm.services.service_discovery import ServiceDiscovery
from mgr_util import test_port_allocation, PortAlreadyInUse
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


class CephadmHttpServer(threading.Thread):
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.agent = AgentEndpoint(mgr)
        self.service_discovery = ServiceDiscovery(mgr)
        self.cherrypy_shutdown_event = threading.Event()
        self.cherrypy_restart_event = threading.Event()
        self._service_discovery_port = self.mgr.service_discovery_port
        security_enabled, _, _ = self.mgr._get_security_config()
        self.security_enabled = security_enabled
        self.agent_adapter = None
        self.sd_adapter = None
        super().__init__(target=self.run)

    def config_update(self) -> None:
        self.service_discovery_port = self.mgr.service_discovery_port
        security_enabled, _, _ = self.mgr._get_security_config()
        if self.security_enabled != security_enabled:
            self.security_enabled = security_enabled
            self.restart()

    @property
    def service_discovery_port(self) -> int:
        return self._service_discovery_port

    @service_discovery_port.setter
    def service_discovery_port(self, value: int) -> None:
        if self._service_discovery_port == value:
            return

        try:
            test_port_allocation(self.mgr.get_mgr_ip(), value)
        except PortAlreadyInUse:
            raise OrchestratorError(f'Service discovery port {value} is already in use. Listening on old port {self._service_discovery_port}.')
        except Exception as e:
            raise OrchestratorError(f'Cannot check service discovery port ip:{self.mgr.get_mgr_ip()} port:{value} error:{e}')

        self.mgr.log.info(f'Changing service discovery port from {self._service_discovery_port} to {value}...')
        self._service_discovery_port = value
        self.restart()

    def restart(self) -> None:
        self.cherrypy_restart_event.set()

    def _stop_adapters(self) -> None:
        adapters_to_stop = {
            'service-discovery': getattr(self, 'sd_adapter', None),
            'cephadm-agent': getattr(self, 'agent_adapter', None)
        }
        for name, adapter in adapters_to_stop.items():
            if adapter:
                try:
                    adapter.stop()
                    adapter.unsubscribe()
                    CherryPyMgr.unregister(name)
                except Exception as e:
                    self.mgr.log.error(f'Failed to stop {name} adapter: {e}')

        self.sd_adapter = None
        self.agent_adapter = None

    def run(self) -> None:
        def _mount_server(
            name: str,
            bind_addr: Tuple[str, int],
            main_app: Any,
            main_path: str,
            main_config: dict,
            ssl_info: Optional[dict] = None,
            extra_mounts: Optional[Any] = None,
            logger: Optional[Any] = None
        ) -> ServerAdapter:
            if logger:
                logger.info(f'Starting {name} server on {bind_addr[0]}:{bind_addr[1]}...')

            tree = _cptree.Tree()
            tree.mount(main_app, main_path, config=main_config)

            for app, path, conf in extra_mounts or []:
                tree.mount(app, path, config=conf)

            adapter, _ = CherryPyMgr.mount(
                tree,
                name,
                bind_addr,
                ssl_info=ssl_info
            )
            return adapter

        def start_servers() -> None:
            # start service discovery server
            sd_port = self._service_discovery_port
            sd_ip = self.mgr.get_mgr_ip()
            sd_config, sd_ssl_info = self.service_discovery.configure(
                sd_port,
                sd_ip,
                self.security_enabled
            )
            self.sd_adapter = _mount_server(
                name='service-discovery',
                bind_addr=(sd_ip, sd_port),
                main_app=self.service_discovery,
                main_path='/sd',
                main_config=sd_config,
                ssl_info=sd_ssl_info,
                logger=self.mgr.log
            )

            # start agent server
            agent_config, agent_ssl_info, agent_mounts, bind_addr = self.agent.configure()
            self.agent_adapter = _mount_server(
                name='cephadm-agent',
                bind_addr=bind_addr,
                main_app=self.agent,
                main_path='/',
                main_config=agent_config,
                ssl_info=agent_ssl_info,
                extra_mounts=agent_mounts,
                logger=self.mgr.log
            )

        try:
            start_servers()
            self.mgr.log.info('Cherrypy server started successfully.')
        except Exception as e:
            self.mgr.log.error(f'Failed to start cherrypy server: {e}')
            self._stop_adapters()
            return

        while not self.cherrypy_shutdown_event.is_set():
            if self.cherrypy_restart_event.wait(timeout=0.5):
                self.cherrypy_restart_event.clear()
                self.mgr.log.debug('Restarting cherrypy server...')
                self._stop_adapters()

                retries = 10
                for attempt in range(retries):
                    try:
                        start_servers()
                        self.mgr.log.debug('Cherrypy server restarted successfully.')
                        break
                    except OSError as e:
                        if e.errno == errno.EADDRINUSE:
                            self.mgr.log.warning(f'Port already in use when restarting cherrypy server (attempt {attempt + 1}/{retries}): {e}')
                            time.sleep(1)
                        else:
                            self.mgr.log.error(f'Failed to restart cherrypy server (attempt {attempt + 1}/{retries}): {e}')
                            self._stop_adapters()
                            break
                    except Exception as e:
                        self.mgr.log.error(f'Failed to restart cherrypy server (attempt {attempt + 1}/{retries}): {e}')
                        self._stop_adapters()
                        break
                else:
                    self.mgr.log.error('Exceeded maximum retries to restart cherrypy server. Please check the server status and resolve any port conflicts.')
                    continue
        self._stop_adapters()

    def shutdown(self) -> None:
        self.mgr.log.debug('Stopping cherrypy engine...')
        self.cherrypy_shutdown_event.set()
