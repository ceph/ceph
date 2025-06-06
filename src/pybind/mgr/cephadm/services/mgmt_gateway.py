import logging
from typing import List, Any, Tuple, Dict, cast, Optional, TYPE_CHECKING

from orchestrator import DaemonDescription
from ceph.deployment.service_spec import MgmtGatewaySpec, GrafanaSpec, ServiceSpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec, get_dashboard_endpoints
from .service_registry import register_cephadm_service

if TYPE_CHECKING:
    from ..module import CephadmOrchestrator

logger = logging.getLogger(__name__)


@register_cephadm_service
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

    def get_mgmt_gw_ip(self, svc_spec: MgmtGatewaySpec, daemon_spec: CephadmDaemonDeploySpec) -> str:
        if svc_spec.virtual_ip is not None:
            return svc_spec.virtual_ip
        else:
            return self.mgr.inventory.get_addr(daemon_spec.host)

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        # we adjust the standby behaviour so rev-proxy can pick correctly the active instance
        self.mgr.set_module_option_ex('dashboard', 'standby_error_status_code', '503')
        self.mgr.set_module_option_ex('dashboard', 'standby_behaviour', 'error')

    def get_external_certificates(self, svc_spec: MgmtGatewaySpec, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str]:
        cert = self.mgr.cert_mgr.get_cert('mgmt_gw_cert')
        key = self.mgr.cert_mgr.get_key('mgmt_gw_key')
        user_made = False
        if not (cert and key):
            # not available on store, check if provided on the spec
            if svc_spec.ssl_cert and svc_spec.ssl_key:
                user_made = True
                cert = svc_spec.ssl_cert
                key = svc_spec.ssl_key
            else:
                # not provided on the spec, let's generate self-sigend certificates
                ip = self.get_mgmt_gw_ip(svc_spec, daemon_spec)
                # we don't include the host_fqdn in case of using a virtual_ip
                # because we may have several instances of the mgmt-gateway running
                # on different hosts
                host_fqdn = [] if svc_spec.virtual_ip else [self.mgr.get_fqdn(daemon_spec.host)]
                cert, key = self.mgr.cert_mgr.generate_cert(host_fqdn, ip)
            # save certificates
            if cert and key:
                self.mgr.cert_mgr.save_cert('mgmt_gw_cert', cert, user_made=user_made)
                self.mgr.cert_mgr.save_key('mgmt_gw_key', key, user_made=user_made)
            else:
                logger.error("Failed to obtain certificate and key from mgmt-gateway.")
        return cert, key

    def get_internal_certificates(self, svc_spec: MgmtGatewaySpec, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str]:
        ip = self.get_mgmt_gw_ip(svc_spec, daemon_spec)
        host_fqdn = self.mgr.get_fqdn(daemon_spec.host)
        return self.mgr.cert_mgr.generate_cert(host_fqdn, ip)

    def get_service_discovery_endpoints(self) -> List[str]:
        sd_endpoints = []
        for dd in self.mgr.cache.get_daemons_by_service('mgr'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            sd_endpoints.append(f"{addr}:{self.mgr.service_discovery_port}")
        return sd_endpoints

    @classmethod
    def get_dependencies(cls, mgr: "CephadmOrchestrator",
                         spec: Optional[ServiceSpec] = None,
                         daemon_type: Optional[str] = None) -> List[str]:
        # url_prefix for the following services depends on the presence of mgmt-gateway
        deps = [
            f'{d.name()}:{d.ports[0]}' if d.ports else d.name()
            for service in ['prometheus', 'alertmanager', 'grafana', 'oauth2-proxy']
            for d in mgr.cache.get_daemons_by_service(service)
        ]
        # dashboard and service discovery urls depend on the mgr daemons
        deps += [
            f'{d.name()}'
            for service in ['mgr']
            for d in mgr.cache.get_daemons_by_service(service)
        ]
        return deps

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
            grafana_protocol = 'https'  # defualt to https just for UT

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

        internal_cert, internal_pkey = self.get_internal_certificates(svc_spec, daemon_spec)
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
            cert, key = self.get_external_certificates(svc_spec, daemon_spec)
            daemon_config["files"]["nginx.crt"] = cert
            daemon_config["files"]["nginx.key"] = key

        return daemon_config, sorted(MgmtGatewayService.get_dependencies(self.mgr))

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        """
        Called before mgmt-gateway daemon is removed.
        """
        # reset the standby dashboard redirection behaviour
        self.mgr.set_module_option_ex('dashboard', 'standby_error_status_code', '500')
        self.mgr.set_module_option_ex('dashboard', 'standby_behaviour', 'redirect')
        # delete cert/key entires for this mgmt-gateway daemon
        self.mgr.cert_mgr.rm_cert('mgmt_gw_cert')
        self.mgr.cert_mgr.rm_key('mgmt_gw_key')
