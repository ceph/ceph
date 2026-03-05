import cherrypy
import logging

import orchestrator  # noqa
from mgr_util import build_url
from typing import Dict, List, TYPE_CHECKING, cast, Collection, Callable, NamedTuple, Optional, IO, Tuple
from cephadm.services.nfs import NFSService
from cephadm.services.ingress import IngressService
from cephadm.services.monitoring import AlertmanagerService, NodeExporterService, PrometheusService
import secrets
from mgr_util import verify_tls_files
import tempfile

from cephadm.services.cephadmservice import CephExporterService
from cephadm.services.nvmeof import NvmeofService
from cephadm.services.service_registry import service_registry

from ceph.deployment.service_spec import SMBSpec

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


logger = logging.getLogger(__name__)


CEPHADM_SVC_DISCOVERY_CERT_DURATION = (365 * 5)


class Route(NamedTuple):
    name: str
    route: str
    controller: Callable


class ServiceDiscovery:

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.username: Optional[str] = None
        self.password: Optional[str] = None
        self.key_file: IO[bytes]
        self.cert_file: IO[bytes]

    def validate_password(self, realm: str, username: str, password: str) -> bool:
        return (password == self.password and username == self.username)

    def get_cherrypy_config(self, enable_auth: bool) -> Dict:
        config = {
            '/': {
                'environment': 'production',
                'tools.gzip.on': True,
                'engine.autoreload.on': False,
            }
        }
        if enable_auth:
            config['/'].update({
                'tools.auth_basic.on': True,
                'tools.auth_basic.realm': 'localhost',
                'tools.auth_basic.checkpassword': self.validate_password,
            })
        return config

    def configure_routes(self, root: 'Root') -> cherrypy.dispatch.RoutesDispatcher:
        ROUTES = [
            Route('index', '/', root.index),
            Route('sd-config', '/prometheus/sd-config', root.get_sd_config),
            Route('rules', '/prometheus/rules', root.get_prometheus_rules),
        ]
        d = cherrypy.dispatch.RoutesDispatcher()
        for route in ROUTES:
            d.connect(**route._asdict())
        return d

    def enable_auth(self) -> None:
        self.username = self.mgr.get_store('service_discovery/root/username')
        self.password = self.mgr.get_store('service_discovery/root/password')
        if not self.password or not self.username:
            self.username = 'admin'  # TODO(redo): what should be the default username
            self.password = secrets.token_urlsafe(20)
            self.mgr.set_store('service_discovery/root/password', self.password)
            self.mgr.set_store('service_discovery/root/username', self.username)

    def configure_tls(self) -> Dict[str, str]:
        addr = self.mgr.get_mgr_ip()
        host = self.mgr.get_hostname()
        tls_pair = self.mgr.cert_mgr.generate_cert(host, addr, duration_in_days=CEPHADM_SVC_DISCOVERY_CERT_DURATION)
        self.cert_file = tempfile.NamedTemporaryFile()
        self.cert_file.write(tls_pair.cert.encode('utf-8'))
        self.cert_file.flush()  # cert_tmp must not be gc'ed

        self.key_file = tempfile.NamedTemporaryFile()
        self.key_file.write(tls_pair.key.encode('utf-8'))
        self.key_file.flush()  # pkey_tmp must not be gc'ed

        verify_tls_files(self.cert_file.name, self.key_file.name)
        return {
            'cert': self.cert_file.name,
            'key': self.key_file.name,
        }

    def configure(self, port: int, addr: str, enable_security: bool) -> Tuple[Dict, Optional[Dict[str, str]]]:
        # we create a new server to enforce TLS/SSL config refresh
        self.root_server = Root(self.mgr)
        ssl_info = None
        if enable_security:
            self.enable_auth()
            ssl_info = self.configure_tls()
        config = self.get_cherrypy_config(enable_security)
        dispatcher = self.configure_routes(self.root_server)
        config['/'].update({'request.dispatch': dispatcher})
        return config, ssl_info


class Root:

    # collapse everything to '/'
    def _cp_dispatch(self, vpath: str) -> 'Root':
        cherrypy.request.path = ''
        return self

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr = mgr

    @cherrypy.expose
    def index(self) -> str:
        return self.mgr.get_store('service_discovery/index') or '''<!DOCTYPE html>
<html>
<head><title>Cephadm HTTP Endpoint</title></head>
<body>
<h2>Cephadm Service Discovery Endpoints</h2>
<p><a href='prometheus/sd-config?service=ceph'>mgr/Prometheus http sd-config</a></p>
<p><a href='prometheus/sd-config?service=alertmanager'>Alertmanager http sd-config</a></p>
<p><a href='prometheus/sd-config?service=node-exporter'>Node exporter http sd-config</a></p>
<p><a href='prometheus/sd-config?service=haproxy'>HAProxy http sd-config</a></p>
<p><a href='prometheus/sd-config?service=ceph-exporter'>Ceph exporter http sd-config</a></p>
<p><a href='prometheus/sd-config?service=nvmeof'>NVMeoF http sd-config</a></p>
<p><a href='prometheus/sd-config?service=nfs'>NFS http sd-config</a></p>
<p><a href='prometheus/sd-config?service=smb'>SMB http sd-config</a></p>
<p><a href='prometheus/rules'>Prometheus rules</a></p>
</body>
</html>'''

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_sd_config(self, service: str) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for the specified service."""

        if service.startswith("container"):
            return self.container_sd_config(service)

        service_to_config = {
            'mgr-prometheus': self.prometheus_sd_config,
            'ceph': self.prometheus_sd_config,
            'alertmanager': self.alertmgr_sd_config,
            'node-exporter': self.node_exporter_sd_config,
            'haproxy': self.haproxy_sd_config,
            'ingress': self.haproxy_sd_config,
            'ceph-exporter': self.ceph_exporter_sd_config,
            'nvmeof': self.nvmeof_sd_config,
            'nfs': self.nfs_sd_config,
            'smb': self.smb_sd_config,
        }
        return service_to_config.get(service, lambda: [])()

    def prometheus_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for prometheus service.
        Targets should be a length one list containing only the active mgr
        """
        targets = []
        mgr_daemons = self.mgr.cache.get_daemons_by_service('mgr')
        host = service_registry.get_service('mgr').get_active_daemon(mgr_daemons).hostname or ''
        fqdn = self.mgr.get_fqdn(host)
        port = self.mgr.get_module_option_ex(
            'prometheus', 'server_port', PrometheusService.DEFAULT_MGR_PROMETHEUS_PORT)
        targets.append(f'{fqdn}:{port}')
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
                assert dd.hostname is not None
                if dd.daemon_type == 'haproxy':
                    ingress = cast(IngressService, service_registry.get_service('ingress'))
                    monitor_ip, monitor_port = ingress.get_monitoring_details(dd.service_name(), dd.hostname)
                    addr = monitor_ip or dd.ip or self.mgr.inventory.get_addr(dd.hostname)
                    srv_entries.append({
                        'targets': [f"{build_url(host=addr, port=monitor_port).lstrip('/')}"],
                        'labels': {'ingress': dd.service_name(), 'instance': dd.hostname}
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

    def nfs_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for nfs service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_type('nfs'):
            assert dd.hostname is not None
            nfs = cast(NFSService, service_registry.get_service('nfs'))
            monitoring_ip, monitoring_port = nfs.get_monitoring_details(dd.service_name(), dd.hostname)
            addr = monitoring_ip or dd.ip or self.mgr.inventory.get_addr(dd.hostname)
            port = monitoring_port or NFSService.DEFAULT_EXPORTER_PORT
            srv_entries.append({
                'targets': [build_url(host=addr, port=port).lstrip('/')],
                'labels': {'instance': dd.hostname}
            })
        return srv_entries

    def smb_sd_config(self) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for smb service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_type('smb'):
            assert dd.hostname is not None
            try:
                spec = cast(SMBSpec, self.mgr.spec_store[dd.service_name()].spec)
            except KeyError:
                logger.warning("no spec found for %s", dd.service_name())
                continue
            # TODO: needs updating once ip control/colocation is present
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = spec.metrics_exporter_port()
            srv_entries.append({
                'targets': [build_url(host=addr, port=port).lstrip('/')],
                'labels': {'instance': dd.hostname}
            })
        return srv_entries

    def container_sd_config(self, service: str) -> List[Dict[str, Collection[str]]]:
        """Return <http_sd_config> compatible prometheus config for a container service."""
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_service(service):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            if not dd.ports:
                continue
            port = dd.ports[0]
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
