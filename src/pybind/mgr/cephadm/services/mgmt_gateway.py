import logging
from typing import List, Any, Tuple, Dict, cast

from orchestrator import DaemonDescription
from ceph.deployment.service_spec import MgmtGatewaySpec, GrafanaSpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec, get_dashboard_endpoints
from cephadm.ssl_cert_utils import SSLCerts

logger = logging.getLogger(__name__)


class MgmtGatewayService(CephadmService):
    TYPE = 'mgmt-gateway'
    SVC_TEMPLATE_PATH = 'services/mgmt-gateway/nginx.conf.j2'
    EXTERNAL_SVC_TEMPLATE_PATH = 'services/mgmt-gateway/external_server.conf.j2'
    INTERNAL_SVC_TEMPLATE_PATH = 'services/mgmt-gateway/internal_server.conf.j2'
    INTERNAL_SERVICE_PORT = 29443

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def get_service_endpoints(self, service_name: str) -> List[str]:
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_service(service_name):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = dd.ports[0] if dd.ports else None
            srv_entries.append(f'{addr}:{port}')
        return srv_entries

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        if daemon_descrs:
            return daemon_descrs[0]
        # if empty list provided, return empty Daemon Desc
        return DaemonDescription()

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        # we adjust the standby behaviour so rev-proxy can pick correctly the active instance
        self.mgr.set_module_option_ex('dashboard', 'standby_error_status_code', '503')
        self.mgr.set_module_option_ex('dashboard', 'standby_behaviour', 'error')

    def get_certificates(self, svc_spec: MgmtGatewaySpec, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str, str, str]:
        self.ssl_certs = SSLCerts()
        old_cert = self.mgr.cert_key_store.get_cert('mgmt_gw_root_cert')
        old_key = self.mgr.cert_key_store.get_key('mgmt_gw_root_key')
        if old_cert and old_key:
            self.ssl_certs.load_root_credentials(old_cert, old_key)
        else:
            self.ssl_certs.generate_root_cert(self.mgr.get_mgr_ip())
            self.mgr.cert_key_store.save_cert('mgmt_gw_root_cert', self.ssl_certs.get_root_cert())
            self.mgr.cert_key_store.save_key('mgmt_gw_root_key', self.ssl_certs.get_root_key())

        node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
        host_fqdn = self._inventory_get_fqdn(daemon_spec.host)
        internal_cert, internal_pkey = self.ssl_certs.generate_cert(host_fqdn, node_ip)
        cert = svc_spec.ssl_certificate
        pkey = svc_spec.ssl_certificate_key
        if not (cert and pkey):
            # In case the user has not provided certificates then we generate self-signed ones
            cert, pkey = self.ssl_certs.generate_cert(host_fqdn, node_ip)

        return internal_cert, internal_pkey, cert, pkey

    def get_mgmt_gateway_deps(self) -> List[str]:
        # url_prefix for the following services depends on the presence of mgmt-gateway
        deps: List[str] = []
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('prometheus')]
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('alertmanager')]
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('grafana')]
        # secure_monitoring_stack affects the protocol used by monitoring services
        deps += [f'secure_monitoring_stack:{self.mgr.secure_monitoring_stack}']
        for dd in self.mgr.cache.get_daemons_by_service('mgr'):
            # we consider mgr a dep even if the dashboard is disabled
            # in order to be consistent with _calc_daemon_deps().
            deps.append(dd.name())

        return deps

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        svc_spec = cast(MgmtGatewaySpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        dashboard_endpoints, dashboard_scheme = get_dashboard_endpoints(self)
        scheme = 'https' if self.mgr.secure_monitoring_stack else 'http'

        prometheus_endpoints = self.get_service_endpoints('prometheus')
        alertmanager_endpoints = self.get_service_endpoints('alertmanager')
        grafana_endpoints = self.get_service_endpoints('grafana')
        try:
            grafana_spec = cast(GrafanaSpec, self.mgr.spec_store['grafana'].spec)
            grafana_protocol = grafana_spec.protocol
        except Exception:
            grafana_protocol = 'https'  # defualt to https just for UT

        main_context = {
            'dashboard_endpoints': dashboard_endpoints,
            'prometheus_endpoints': prometheus_endpoints,
            'alertmanager_endpoints': alertmanager_endpoints,
            'grafana_endpoints': grafana_endpoints
        }
        external_server_context = {
            'spec': svc_spec,
            'dashboard_scheme': dashboard_scheme,
            'grafana_scheme': grafana_protocol,
            'prometheus_scheme': scheme,
            'alertmanager_scheme': scheme,
            'dashboard_endpoints': dashboard_endpoints,
            'prometheus_endpoints': prometheus_endpoints,
            'alertmanager_endpoints': alertmanager_endpoints,
            'grafana_endpoints': grafana_endpoints
        }
        internal_server_context = {
            'spec': svc_spec,
            'internal_port': self.INTERNAL_SERVICE_PORT,
            'grafana_scheme': grafana_protocol,
            'prometheus_scheme': scheme,
            'alertmanager_scheme': scheme,
            'prometheus_endpoints': prometheus_endpoints,
            'alertmanager_endpoints': alertmanager_endpoints,
            'grafana_endpoints': grafana_endpoints
        }

        internal_cert, internal_pkey, cert, pkey = self.get_certificates(svc_spec, daemon_spec)
        daemon_config = {
            "files": {
                "nginx.conf": self.mgr.template.render(self.SVC_TEMPLATE_PATH, main_context),
                "nginx_external_server.conf": self.mgr.template.render(self.EXTERNAL_SVC_TEMPLATE_PATH, external_server_context),
                "nginx_internal_server.conf": self.mgr.template.render(self.INTERNAL_SVC_TEMPLATE_PATH, internal_server_context),
                "nginx_internal.crt": internal_cert,
                "nginx_internal.key": internal_pkey
            }
        }
        if not svc_spec.disable_https:
            daemon_config["files"]["nginx.crt"] = cert
            daemon_config["files"]["nginx.key"] = pkey

        return daemon_config, sorted(self.get_mgmt_gateway_deps())

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before mgmt-gateway daemon is removed.
        """
        # reset the standby dashboard redirection behaviour
        self.mgr.set_module_option_ex('dashboard', 'standby_error_status_code', '500')
        self.mgr.set_module_option_ex('dashboard', 'standby_behaviour', 'redirect')
