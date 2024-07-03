import logging
from typing import TYPE_CHECKING, List, Any, Tuple, Dict, cast, Optional

from orchestrator import DaemonDescription
from ceph.deployment.service_spec import MgmtGatewaySpec, GrafanaSpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec, get_dashboard_endpoints
from mgr_util import build_url

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


def get_mgmt_gw_internal_endpoint(mgr: "CephadmOrchestrator") -> Optional[str]:
    mgmt_gw_daemons = mgr.cache.get_daemons_by_service('mgmt-gateway')
    if not mgmt_gw_daemons:
        return None

    dd = mgmt_gw_daemons[0]
    assert dd.hostname is not None
    mgmt_gw_addr = mgr.get_fqdn(dd.hostname)
    mgmt_gw_internal_endpoint = build_url(scheme='https', host=mgmt_gw_addr, port=MgmtGatewayService.INTERNAL_SERVICE_PORT)
    return f'{mgmt_gw_internal_endpoint}/internal'


def get_mgmt_gw_external_endpoint(mgr: "CephadmOrchestrator") -> Optional[str]:
    mgmt_gw_daemons = mgr.cache.get_daemons_by_service('mgmt-gateway')
    if not mgmt_gw_daemons:
        return None

    dd = mgmt_gw_daemons[0]
    assert dd.hostname is not None
    mgmt_gw_port = dd.ports[0] if dd.ports else None
    mgmt_gw_addr = mgr.get_fqdn(dd.hostname)
    mgmt_gw_spec = cast(MgmtGatewaySpec, mgr.spec_store['mgmt-gateway'].spec)
    protocol = 'http' if mgmt_gw_spec.disable_https else 'https'
    mgmt_gw_external_endpoint = build_url(scheme=protocol, host=mgmt_gw_addr, port=mgmt_gw_port)
    return mgmt_gw_external_endpoint


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

    def get_external_certificates(self, svc_spec: MgmtGatewaySpec, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str]:
        cert = self.mgr.cert_key_store.get_cert('mgmt_gw_cert')
        key = self.mgr.cert_key_store.get_key('mgmt_gw_key')
        if not (cert and key):
            # not available on store, check if provided on the spec
            if svc_spec.ssl_certificate and svc_spec.ssl_certificate_key:
                cert = svc_spec.ssl_certificate
                key = svc_spec.ssl_certificate_key
            else:
                # not provided on the spec, let's generate self-sigend certificates
                addr = self.mgr.inventory.get_addr(daemon_spec.host)
                host_fqdn = self.mgr.get_fqdn(daemon_spec.host)
                cert, key = self.mgr.cert_mgr.generate_cert(host_fqdn, addr)
            # save certificates
            if cert and key:
                self.mgr.cert_key_store.save_cert('mgmt_gw_cert', cert)
                self.mgr.cert_key_store.save_key('mgmt_gw_key', key)
            else:
                logger.error("Failed to obtain certificate and key from mgmt-gateway.")
        return cert, key

    def get_internal_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str]:
        node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
        host_fqdn = self.mgr.get_fqdn(daemon_spec.host)
        return self.mgr.cert_mgr.generate_cert(host_fqdn, node_ip)

    def get_mgmt_gateway_deps(self) -> List[str]:
        # url_prefix for the following services depends on the presence of mgmt-gateway
        deps: List[str] = []
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('prometheus')]
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('alertmanager')]
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('grafana')]
        for dd in self.mgr.cache.get_daemons_by_service('mgr'):
            # we consider mgr a dep even if the dashboard is disabled
            # in order to be consistent with _calc_daemon_deps().
            deps.append(dd.name())

        return deps

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        svc_spec = cast(MgmtGatewaySpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        scheme = 'https'
        dashboard_endpoints, dashboard_scheme = get_dashboard_endpoints(self)
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
            'grafana_endpoints': grafana_endpoints
        }

        cert, key = self.get_external_certificates(svc_spec, daemon_spec)
        internal_cert, internal_pkey = self.get_internal_certificates(daemon_spec)
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
        if not svc_spec.disable_https:
            daemon_config["files"]["nginx.crt"] = cert
            daemon_config["files"]["nginx.key"] = key

        return daemon_config, sorted(self.get_mgmt_gateway_deps())

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before mgmt-gateway daemon is removed.
        """
        # reset the standby dashboard redirection behaviour
        self.mgr.set_module_option_ex('dashboard', 'standby_error_status_code', '500')
        self.mgr.set_module_option_ex('dashboard', 'standby_behaviour', 'redirect')
        if daemon.hostname is not None:
            # delete cert/key entires for this mgmt-gateway daemon
            self.mgr.cert_key_store.rm_cert('mgmt_gw_cert')
            self.mgr.cert_key_store.rm_key('mgmt_gw_key')
