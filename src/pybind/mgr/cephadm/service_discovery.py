try:
    import cherrypy
    from cherrypy._cpserver import Server
except ImportError:
    # to avoid sphinx build crash
    class Server:  # type: ignore
        pass

import logging
import orchestrator  # noqa
from mgr_module import ServiceInfoT
from mgr_util import build_url
from typing import Dict, List, TYPE_CHECKING, cast, Collection, Callable, NamedTuple
from cephadm.services.monitoring import AlertmanagerService, NodeExporterService, PrometheusService

from cephadm.services.ingress import IngressSpec
from cephadm.ssl_cert_utils import SSLCerts

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


def cherrypy_filter(record: logging.LogRecord) -> int:
    blocked = [
        'TLSV1_ALERT_DECRYPT_ERROR'
    ]
    msg = record.getMessage()
    return not any([m for m in blocked if m in msg])


logging.getLogger('cherrypy.error').addFilter(cherrypy_filter)
cherrypy.log.access_log.propagate = False


class Route(NamedTuple):
    name: str
    route: str
    controller: Callable


class ServiceDiscovery:

    KV_STORE_SD_ROOT_CERT = 'service_discovery/root/cert'
    KV_STORE_SD_ROOT_KEY = 'service_discovery/root/key'

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.ssl_certs = SSLCerts()

    def configure_routes(self, server: Server) -> None:
        ROUTES = [
            Route('index', '/', server.index),
            Route('sd-config', '/prometheus/sd-config', server.get_sd_config),
            Route('rules', '/prometheus/rules', server.get_prometheus_rules),
        ]
        d = cherrypy.dispatch.RoutesDispatcher()
        for route in ROUTES:
            d.connect(**route._asdict())
        conf = {'/': {'request.dispatch': d}}
        cherrypy.tree.mount(None, '/sd', config=conf)

    def configure_tls(self, server: Server) -> None:
        old_cert = self.mgr.get_store(self.KV_STORE_SD_ROOT_CERT)
        old_key = self.mgr.get_store(self.KV_STORE_SD_ROOT_KEY)
        if old_key and old_cert:
            self.ssl_certs.load_root_credentials(old_cert, old_key)
        else:
            self.ssl_certs.generate_root_cert(self.mgr.get_mgr_ip())
            self.mgr.set_store(self.KV_STORE_SD_ROOT_CERT, self.ssl_certs.get_root_cert())
            self.mgr.set_store(self.KV_STORE_SD_ROOT_KEY, self.ssl_certs.get_root_key())

        host = self.mgr.get_hostname()
        addr = self.mgr.get_mgr_ip()
        server.ssl_certificate, server.ssl_private_key = self.ssl_certs.generate_cert_files(host, addr)

    def configure(self, port: int, addr: str) -> None:
        # we create a new server to enforce TLS/SSL config refresh
        self.root_server = Root(self.mgr, port, addr)
        self.configure_tls(self.root_server)
        self.configure_routes(self.root_server)


class Root(Server):

    # collapse everything to '/'
    def _cp_dispatch(self, vpath: str) -> 'Root':
        cherrypy.request.path = ''
        return self

    def stop(self) -> None:
        # we must call unsubscribe before stopping the server,
        # otherwise the port is not released and we will get
        # an exception when trying to restart it
        self.unsubscribe()
        super().stop()

    def __init__(self, mgr: "CephadmOrchestrator", port: int, host: str):
        self.mgr = mgr
        super().__init__()
        self.socket_port = port
        self.socket_host = host
        self.subscribe()

    @cherrypy.expose
    def index(self) -> str:
        return '''<!DOCTYPE html>
<html>
<head><title>Cephadm HTTP Endpoint</title></head>
<body>
<h2>Cephadm Service Discovery Endpoints</h2>
<p><a href='prometheus/sd-config?service=mgr-prometheus'>mgr/Prometheus http sd-config</a></p>
<p><a href='prometheus/sd-config?service=alertmanager'>Alertmanager http sd-config</a></p>
<p><a href='prometheus/sd-config?service=node-exporter'>Node exporter http sd-config</a></p>
<p><a href='prometheus/sd-config?service=haproxy'>HAProxy http sd-config</a></p>
<p><a href='prometheus/rules'>Prometheus rules</a></p>
</body>
</html>'''

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_sd_config(self, service: str) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for the specified service."""
        if service == 'mgr-prometheus':
            return self.prometheus_sd_config()
        elif service == 'alertmanager':
            return self.alertmgr_sd_config()
        elif service == 'node-exporter':
            return self.node_exporter_sd_config()
        elif service == 'haproxy':
            return self.haproxy_sd_config()
        else:
            return []

    def prometheus_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for prometheus service."""
        servers = self.mgr.list_servers()
        targets = []
        for server in servers:
            hostname = server.get('hostname', '')
            for service in cast(List[ServiceInfoT], server.get('services', [])):
                if service['type'] != 'mgr':
                    continue
                port = self.mgr.get_module_option_ex(
                    'prometheus', 'server_port', PrometheusService.DEFAULT_MGR_PROMETHEUS_PORT)
                targets.append(f'{hostname}:{port}')
        return [{"targets": targets, "labels": {}}]

    def alertmgr_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for mgr alertmanager service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_service('alertmanager'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = dd.ports[0] if dd.ports else AlertmanagerService.DEFAULT_SERVICE_PORT
            srv_entries.append('{}'.format(build_url(host=addr, port=port).lstrip('/')))
        return [{"targets": srv_entries, "labels": {}}]

    def node_exporter_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for node-exporter service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_service('node-exporter'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = dd.ports[0] if dd.ports else NodeExporterService.DEFAULT_SERVICE_PORT
            srv_entries.append({
                'targets': [build_url(host=addr, port=port).lstrip('/')],
                'labels': {'instance': dd.hostname}
            })
        return srv_entries

    def haproxy_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for haproxy service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_type('ingress'):
            if dd.service_name() in self.mgr.spec_store:
                spec = cast(IngressSpec, self.mgr.spec_store[dd.service_name()].spec)
                assert dd.hostname is not None
                if dd.daemon_type == 'haproxy':
                    addr = self.mgr.inventory.get_addr(dd.hostname)
                    srv_entries.append({
                        'targets': [f"{build_url(host=addr, port=spec.monitor_port).lstrip('/')}"],
                        'labels': {'instance': dd.service_name()}
                    })
        return srv_entries

    @cherrypy.expose(alias='prometheus/rules')
    def get_prometheus_rules(self) -> str:
        """Return currently configured prometheus rules as Yaml."""
        cherrypy.response.headers['Content-Type'] = 'text/plain'
        with open(self.mgr.prometheus_alerts_path, 'r', encoding='utf-8') as f:
            return f.read()
