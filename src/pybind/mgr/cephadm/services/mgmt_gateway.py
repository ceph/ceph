import logging
from typing import List, Any, Tuple, Dict, cast, Optional, TYPE_CHECKING

from orchestrator import DaemonDescription
from ceph.deployment.service_spec import MgmtGatewaySpec, GrafanaSpec, ServiceSpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec, get_dashboard_endpoints
from .service_registry import register_cephadm_service

if TYPE_CHECKING:
    from ..module import CephadmOrchestrator

logger = logging.getLogger(__name__)

INTERNAL_CERT_LABEL = 'internal'


@register_cephadm_service
class MgmtGatewayService(CephadmService):
    TYPE = 'mgmt-gateway'
    SVC_TEMPLATE_PATH = 'services/mgmt-gateway/nginx.conf.j2'
    EXTERNAL_SVC_TEMPLATE_PATH = 'services/mgmt-gateway/external_server.conf.j2'
    INTERNAL_SVC_TEMPLATE_PATH = 'services/mgmt-gateway/internal_server.conf.j2'
    INTERNAL_SERVICE_PORT = 29443

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        super().register_for_certificates(daemon_spec)
        self.mgr.cert_mgr.register_self_signed_cert_key_pair(MgmtGatewayService.TYPE, INTERNAL_CERT_LABEL)
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

    def get_mgmt_gw_ip(self, svc_spec: MgmtGatewaySpec, daemon_spec: CephadmDaemonDeploySpec) -> str:
        if svc_spec.virtual_ip is not None:
            return svc_spec.virtual_ip
        else:
            return self.mgr.inventory.get_addr(daemon_spec.host)

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        # we adjust the standby behaviour so rev-proxy can pick correctly the active instance
        self.mgr.set_module_option_ex('dashboard', 'standby_error_status_code', '503')
        self.mgr.set_module_option_ex('dashboard', 'standby_behaviour', 'error')

    def get_service_discovery_endpoints(self) -> List[str]:
        sd_endpoints = []
        for dd in self.mgr.cache.get_daemons_by_service('mgr'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            sd_endpoints.append(f"{addr}:{self.mgr.service_discovery_port}")
        return sd_endpoints

    @classmethod
    def get_dependencies(cls, mgr: "CephadmOrchestrator",
                         spec: ServiceSpec,
                         daemon_type: Optional[str] = None) -> List[str]:
        # url_prefix for the following services depends on the presence of mgmt-gateway
        parent_deps = super().get_dependencies(mgr, spec, daemon_type)
        mgmt_gw_deps = [
            f'{d.name()}:{d.ports[0]}' if d.ports else d.name()
            for service in ['prometheus', 'alertmanager', 'grafana', 'oauth2-proxy']
            for d in mgr.cache.get_daemons_by_service(service)
        ]
        # dashboard and service discovery urls depend on the mgr daemons
        mgmt_gw_deps += [
            f'{d.name()}'
            for service in ['mgr']
            for d in mgr.cache.get_daemons_by_service(service)
        ]
        return parent_deps + mgmt_gw_deps

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        svc_spec = cast(MgmtGatewaySpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        scheme = 'https'
        dashboard_endpoints, dashboard_scheme = get_dashboard_endpoints(self)
        prometheus_endpoints = self.get_service_endpoints('prometheus')
        alertmanager_endpoints = self.get_service_endpoints('alertmanager')
        grafana_endpoints = self.get_service_endpoints('grafana')
        oauth2_proxy_endpoints = self.get_service_endpoints('oauth2-proxy')
        service_discovery_endpoints = self.get_service_discovery_endpoints()
        try:
            grafana_spec = cast(GrafanaSpec, self.mgr.spec_store['grafana'].spec)
            grafana_protocol = grafana_spec.protocol
        except Exception:
            grafana_protocol = 'https'  # default to https just for UT

        main_context = {
            'dashboard_endpoints': dashboard_endpoints,
            'prometheus_endpoints': prometheus_endpoints,
            'alertmanager_endpoints': alertmanager_endpoints,
            'grafana_endpoints': grafana_endpoints,
            'oauth2_proxy_endpoints': oauth2_proxy_endpoints,
            'service_discovery_endpoints': service_discovery_endpoints
        }
        server_context = {
            'spec': svc_spec,
            'internal_port': self.INTERNAL_SERVICE_PORT,
            'dashboard_scheme': dashboard_scheme,
            'dashboard_endpoints': dashboard_endpoints,
            'grafana_scheme': grafana_protocol,
            'prometheus_scheme': scheme,
            'alertmanager_scheme': scheme,
            'prometheus_endpoints': prometheus_endpoints,
            'alertmanager_endpoints': alertmanager_endpoints,
            'grafana_endpoints': grafana_endpoints,
            'service_discovery_endpoints': service_discovery_endpoints,
            'enable_oauth2_proxy': bool(oauth2_proxy_endpoints),
        }

        internal_cert, internal_pkey = self.get_self_signed_certificates_with_label(svc_spec, daemon_spec, INTERNAL_CERT_LABEL)
        daemon_config = {
            "files": {
                "nginx.conf": self.mgr.template.render(self.SVC_TEMPLATE_PATH, main_context),
                "nginx_external_server.conf": self.mgr.template.render(self.EXTERNAL_SVC_TEMPLATE_PATH, server_context),
                "nginx_internal_server.conf": self.mgr.template.render(self.INTERNAL_SVC_TEMPLATE_PATH, server_context),
                "nginx_internal.crt": internal_cert,
                "nginx_internal.key": internal_pkey,
                "ca.crt": self.mgr.cert_mgr.get_root_ca()
            }
        }

        if svc_spec.ssl:
            ip = self.get_mgmt_gw_ip(svc_spec, daemon_spec)
            tls_pair = self.get_certificates(daemon_spec, [ip])
            daemon_config["files"]["nginx.crt"] = tls_pair.cert
            daemon_config["files"]["nginx.key"] = tls_pair.key

        return daemon_config, sorted(MgmtGatewayService.get_dependencies(self.mgr, svc_spec))

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        """
        Called before mgmt-gateway daemon is removed.
        """
        # reset the standby dashboard redirection behaviour
        assert daemon.hostname  # for mypy
        super().post_remove(daemon, is_failed_deploy=is_failed_deploy)
        self.mgr.cert_mgr.rm_self_signed_cert_key_pair(MgmtGatewayService.TYPE, daemon.hostname, label=INTERNAL_CERT_LABEL)
        self.mgr.set_module_option_ex('dashboard', 'standby_error_status_code', '500')
        self.mgr.set_module_option_ex('dashboard', 'standby_behaviour', 'redirect')
