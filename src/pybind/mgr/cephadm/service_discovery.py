try:
    import cherrypy
    from cherrypy._cpserver import Server
except ImportError:
    # to avoid sphinx build crash
    class Server:  # type: ignore
        pass

import logging
import socket

import orchestrator  # noqa
from mgr_module import ServiceInfoT
from mgr_util import build_url
from typing import Dict, List, TYPE_CHECKING, cast, Collection, Callable, NamedTuple, Optional
from cephadm.services.monitoring import AlertmanagerService, NodeExporterService, PrometheusService
import secrets

from cephadm.services.ingress import IngressSpec
from cephadm.ssl_cert_utils import SSLCerts
from cephadm.services.cephadmservice import CephExporterService
from cephadm.services.nvmeof import NvmeofService

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
        self.username: Optional[str] = None
        self.password: Optional[str] = None

    def validate_password(self, realm: str, username: str, password: str) -> bool:
        return (password == self.password and username == self.username)

    def configure_routes(self, server: Server, enable_auth: bool) -> None:
        ROUTES = [
            Route('index', '/', server.index),
            Route('sd-config', '/prometheus/sd-config', server.get_sd_config),
            Route('rules', '/prometheus/rules', server.get_prometheus_rules),
        ]
        d = cherrypy.dispatch.RoutesDispatcher()
        for route in ROUTES:
            d.connect(**route._asdict())
        if enable_auth:
            conf = {
                '/': {
                    'request.dispatch': d,
                    'tools.auth_basic.on': True,
                    'tools.auth_basic.realm': 'localhost',
                    'tools.auth_basic.checkpassword': self.validate_password
                }
            }
        else:
            conf = {'/': {'request.dispatch': d}}
        cherrypy.tree.mount(None, '/sd', config=conf)

    def enable_auth(self) -> None:
        self.username = self.mgr.get_store('service_discovery/root/username')
        self.password = self.mgr.get_store('service_discovery/root/password')
        if not self.password or not self.username:
            self.username = 'admin'  # TODO(redo): what should be the default username
            self.password = secrets.token_urlsafe(20)
            self.mgr.set_store('service_discovery/root/password', self.password)
            self.mgr.set_store('service_discovery/root/username', self.username)

    def configure_tls(self, server: Server) -> None:
        old_cert = self.mgr.get_store(self.KV_STORE_SD_ROOT_CERT)
        old_key = self.mgr.get_store(self.KV_STORE_SD_ROOT_KEY)
        if old_key and old_cert:
            self.ssl_certs.load_root_credentials(old_cert, old_key)
        else:
            self.ssl_certs.generate_root_cert(self.mgr.get_mgr_ip())
            self.mgr.set_store(self.KV_STORE_SD_ROOT_CERT, self.ssl_certs.get_root_cert())
            self.mgr.set_store(self.KV_STORE_SD_ROOT_KEY, self.ssl_certs.get_root_key())
        addr = self.mgr.get_mgr_ip()
        host_fqdn = socket.getfqdn(addr)
        server.ssl_certificate, server.ssl_private_key = self.ssl_certs.generate_cert_files(
            host_fqdn, addr)

    def configure(self, port: int, addr: str, enable_security: bool) -> None:
        # we create a new server to enforce TLS/SSL config refresh
        self.root_server = Root(self.mgr, port, addr)
        self.root_server.ssl_certificate = None
        self.root_server.ssl_private_key = None
        if enable_security:
            self.enable_auth()
            self.configure_tls(self.root_server)
        self.configure_routes(self.root_server, enable_security)


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

    def __init__(self, mgr: "CephadmOrchestrator", port: int = 0, host: str = ''):
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
<p><a href='prometheus/sd-config?service=ceph-exporter'>Ceph exporter http sd-config</a></p>
<p><a href='prometheus/sd-config?service=nvmeof'>NVMeoF http sd-config</a></p>
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
        elif service == 'ceph-exporter':
            return self.ceph_exporter_sd_config()
        elif service == 'nvmeof':
            return self.nvmeof_sd_config()
        else:
            return []

    def prometheus_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for prometheus service."""
        servers = self.mgr.list_servers()
        targets = []
        for server in servers:
            hostname = server.get('hostname', '')
            for service in cast(List[ServiceInfoT], server.get('services', [])):
                if service['type'] != 'mgr' or service['id'] != self.mgr.get_mgr_id():
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

    def ceph_exporter_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for ceph-exporter service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_service('ceph-exporter'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = dd.ports[0] if dd.ports else CephExporterService.DEFAULT_SERVICE_PORT
            srv_entries.append({
                'targets': [build_url(host=addr, port=port).lstrip('/')],
                'labels': {'instance': dd.hostname}
            })
        return srv_entries

    def nvmeof_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for nvmeof service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_type('nvmeof'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = NvmeofService.PROMETHEUS_PORT
            srv_entries.append({
                'targets': [build_url(host=addr, port=port).lstrip('/')],
                'labels': {'instance': dd.hostname}
            })
        return srv_entries

    @cherrypy.expose(alias='prometheus/rules')
    def get_prometheus_rules(self) -> str:
        """Return currently configured prometheus rules as Yaml."""
        cherrypy.response.headers['Content-Type'] = 'text/plain'
        with open(self.mgr.prometheus_alerts_path, 'r', encoding='utf-8') as f:
            return f.read()
