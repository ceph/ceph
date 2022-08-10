try:
    import cherrypy
    from cherrypy._cpserver import Server
except ImportError:
    # to avoid sphinx build crash
    class Server:  # type: ignore
        pass

import logging
import tempfile
from typing import Dict, List, TYPE_CHECKING, cast, Collection

from orchestrator import OrchestratorError
from mgr_module import ServiceInfoT
from mgr_util import verify_tls_files, build_url
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


class ServiceDiscovery:

    KV_STORE_SD_ROOT_CERT = 'service_discovery/root/cert'
    KV_STORE_SD_ROOT_KEY = 'service_discovery/root/key'

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.ssl_certs = SSLCerts()
        self.server_port = self.mgr.service_discovery_port
        self.server_addr = '::'

    def configure_routes(self) -> None:

        root_server = Root(self.mgr,
                           self.server_port,
                           self.server_addr,
                           self.cert_file.name,
                           self.key_file.name)

        # configure routes
        d = cherrypy.dispatch.RoutesDispatcher()
        d.connect(name='index', route='/', controller=root_server.index)
        d.connect(name='index', route='/sd', controller=root_server.index)
        d.connect(name='index', route='/sd/', controller=root_server.index)
        d.connect(name='sd-config', route='/sd/prometheus/sd-config',
                  controller=root_server.get_sd_config)
        d.connect(name='rules', route='/sd/prometheus/rules',
                  controller=root_server.get_prometheus_rules)
        cherrypy.tree.mount(None, '/', config={'/': {'request.dispatch': d}})

    def configure_tls(self) -> None:
        try:
            old_cert = self.mgr.get_store(self.KV_STORE_SD_ROOT_CERT)
            old_key = self.mgr.get_store(self.KV_STORE_SD_ROOT_KEY)
            if not old_key or not old_cert:
                raise OrchestratorError('No old credentials for service discovery found')
            self.ssl_certs.load_root_credentials(old_cert, old_key)
        except (OrchestratorError, KeyError, ValueError):
            self.ssl_certs.generate_root_cert(self.mgr.get_mgr_ip())
            self.mgr.set_store(self.KV_STORE_SD_ROOT_CERT, self.ssl_certs.get_root_cert())
            self.mgr.set_store(self.KV_STORE_SD_ROOT_KEY, self.ssl_certs.get_root_key())

        cert, key = self.ssl_certs.generate_cert(self.mgr.get_mgr_ip())
        self.key_file = tempfile.NamedTemporaryFile()
        self.key_file.write(key.encode('utf-8'))
        self.key_file.flush()  # pkey_tmp must not be gc'ed
        self.cert_file = tempfile.NamedTemporaryFile()
        self.cert_file.write(cert.encode('utf-8'))
        self.cert_file.flush()  # cert_tmp must not be gc'ed
        verify_tls_files(self.cert_file.name, self.key_file.name)

    def configure(self) -> None:
        self.configure_tls()
        self.configure_routes()


class Root(Server):

    # collapse everything to '/'
    def _cp_dispatch(self, vpath: str) -> 'Root':
        cherrypy.request.path = ''
        return self

    def __init__(self, mgr: "CephadmOrchestrator",
                 port: int = 0,
                 host: str = '',
                 ssl_ca_cert: str = '',
                 ssl_priv_key: str = ''):
        self.mgr = mgr
        super().__init__()
        self.socket_port = port
        self._socket_host = host
        self.ssl_certificate = ssl_ca_cert
        self.ssl_private_key = ssl_priv_key
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
                port = self.mgr.get_module_option_ex('prometheus', 'server_port', 9283)
                targets.append(f'{hostname}:{port}')
        return [{"targets": targets, "labels": {}}]

    def alertmgr_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for mgr alertmanager service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_service('alertmanager'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = dd.ports[0] if dd.ports else 9093
            srv_entries.append('{}'.format(build_url(host=addr, port=port).lstrip('/')))
        return [{"targets": srv_entries, "labels": {}}]

    def node_exporter_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for node-exporter service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_service('node-exporter'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = dd.ports[0] if dd.ports else 9100
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
