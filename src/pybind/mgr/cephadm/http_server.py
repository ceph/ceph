import cherrypy
import threading
import logging
from typing import TYPE_CHECKING

from cephadm.agent import AgentEndpoint
from cephadm.service_discovery import ServiceDiscovery
from mgr_util import test_port_allocation, PortAlreadyInUse
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


def cherrypy_filter(record: logging.LogRecord) -> bool:
    blocked = [
        'TLSV1_ALERT_DECRYPT_ERROR'
    ]
    msg = record.getMessage()
    return not any([m for m in blocked if m in msg])


logging.getLogger('cherrypy.error').addFilter(cherrypy_filter)
cherrypy.log.access_log.propagate = False


class CephadmHttpServer(threading.Thread):
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.agent = AgentEndpoint(mgr)
        self.service_discovery = ServiceDiscovery(mgr)
        self.cherrypy_shutdown_event = threading.Event()
        self._service_discovery_port = self.mgr.service_discovery_port
        self.secure_monitoring_stack = self.mgr.secure_monitoring_stack
        super().__init__(target=self.run)

    def configure_cherrypy(self) -> None:
        cherrypy.config.update({
            'environment': 'production',
            'engine.autoreload.on': False,
        })

    def configure(self) -> None:
        self.configure_cherrypy()
        self.agent.configure()
        self.service_discovery.configure(self.mgr.service_discovery_port,
                                         self.mgr.get_mgr_ip(),
                                         self.secure_monitoring_stack)

    def config_update(self) -> None:
        self.service_discovery_port = self.mgr.service_discovery_port
        if self.secure_monitoring_stack != self.mgr.secure_monitoring_stack:
            self.secure_monitoring_stack = self.mgr.secure_monitoring_stack
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
        cherrypy.engine.stop()
        cherrypy.server.httpserver = None
        self.configure()
        cherrypy.engine.start()

    def run(self) -> None:
        try:
            self.mgr.log.debug('Starting cherrypy engine...')
            self.configure()
            cherrypy.server.unsubscribe()  # disable default server
            cherrypy.engine.start()
            self.mgr.log.debug('Cherrypy engine started.')
            self.mgr._kick_serve_loop()
            # wait for the shutdown event
            self.cherrypy_shutdown_event.wait()
            self.cherrypy_shutdown_event.clear()
            cherrypy.engine.stop()
            cherrypy.server.httpserver = None
            self.mgr.log.debug('Cherrypy engine stopped.')
        except Exception as e:
            self.mgr.log.error(f'Failed to run cephadm http server: {e}')

    def shutdown(self) -> None:
        self.mgr.log.debug('Stopping cherrypy engine...')
        self.cherrypy_shutdown_event.set()
