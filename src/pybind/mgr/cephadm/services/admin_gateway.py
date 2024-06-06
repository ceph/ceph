import logging
from typing import List, Any, Tuple, Dict, Optional, cast

from mgr_util import build_url
from orchestrator import DaemonDescription
from ceph.deployment.service_spec import AdminGatewaySpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec, get_dashboard_eps
from cephadm.ssl_cert_utils import SSLCerts

logger = logging.getLogger(__name__)


class AdminGatewayService(CephadmService):
    TYPE = 'admin-gateway'
    SVC_TEMPLATE_PATH = 'services/admin-gateway/nginx.conf.j2'
    EXTERNAL_SVC_TEMPLATE_PATH = 'services/admin-gateway/external_server.conf.j2'
    INTERNAL_SVC_TEMPLATE_PATH = 'services/admin-gateway/internal_server.conf.j2'
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
        dd = self.get_active_daemon(daemon_descrs)
        assert dd.hostname is not None
        addr = self._inventory_get_fqdn(dd.hostname)
        spec = cast(AdminGatewaySpec, self.mgr.spec_store[dd.service_name()].spec)

        # Grafana has to be configured by using the 'external' URL
        protocol = 'http' if spec.disable_https else 'https'
        port = dd.ports[0] if dd.ports else None
        admin_gw_external_ep = build_url(scheme=protocol, host=addr, port=port)
        self._set_value_on_dashboard(
            'Grafana',
            'dashboard get-grafana-api-url',
            'dashboard set-grafana-api-url',
            f'{admin_gw_external_ep}/grafana'
        )

        # configure prometheus
        admin_gw_internal_ep = build_url(scheme='https', host=addr, port=self.INTERNAL_SERVICE_PORT)
        self._set_value_on_dashboard(
            'Prometheus',
            'dashboard get-prometheus-api-host',
            'dashboard set-prometheus-api-host',
            f'{admin_gw_internal_ep}/internal/prometheus'
        )

        # configure alertmanager
        self._set_value_on_dashboard(
            'AlertManager',
            'dashboard get-alertmanager-api-host',
            'dashboard set-alertmanager-api-host',
            f'{admin_gw_internal_ep}/internal/alertmanager'
        )

        # Disable SSL verification on all the monitoring services
        # sicne we are using our own self-signed certificates
        self._set_value_on_dashboard(
            'Alertmanager',
            'dashboard get-alertmanager-api-ssl-verify',
            'dashboard set-alertmanager-api-ssl-verify',
            'false'
        )
        self._set_value_on_dashboard(
            'Prometheus',
            'dashboard get-prometheus-api-ssl-verify',
            'dashboard set-prometheus-api-ssl-verify',
            'false'
        )
        self._set_value_on_dashboard(
            'Grafana',
            'dashboard get-grafana-api-ssl-verify',
            'dashboard set-grafana-api-ssl-verify',
            'false'
        )

        # we adjust the standby behaviour so rev-proxy can pick correctly the active instance
        self.mgr.set_module_option_ex('dashboard', 'standby_error_status_code', '503')
        self.mgr.set_module_option_ex('dashboard', 'standby_behaviour', 'error')

    def get_certificates(self, svc_spec: AdminGatewaySpec, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str, str, str]:

        def read_certificate(spec_field: Optional[List[str]]) -> str:
            cert = ''
            if isinstance(spec_field, list):
                cert = '\n'.join(spec_field)
            elif isinstance(spec_field, str):
                cert = spec_field
            return cert

        # TODO(redo): store/load these certificates by using the new support and check the posibility
        # to have a "centrilized" certificate mangaer for all cephadm components so we use the same
        # root CA fo sign all of them
        #
        # PD: a this moment we are generating new certificates each time the service is reconfigured
        self.ssl_certs = SSLCerts()
        self.ssl_certs.generate_root_cert(self.mgr.get_mgr_ip())
        node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
        host_fqdn = self._inventory_get_fqdn(daemon_spec.host)
        internal_cert, internal_pkey = self.ssl_certs.generate_cert(host_fqdn, node_ip)
        cert = read_certificate(svc_spec.ssl_certificate)
        pkey = read_certificate(svc_spec.ssl_certificate_key)
        if not (cert and pkey):
            # In case the user has not provided certificates then we generate self-signed ones
            cert, pkey = self.ssl_certs.generate_cert(host_fqdn, node_ip)

        return internal_cert, internal_pkey, cert, pkey

    def get_admin_gatway_deps(self) -> List[str]:
        # url_prefix for the following services depends on the presence of admin-gateway
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
        svc_spec = cast(AdminGatewaySpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        dashboard_eps, dashboard_scheme = get_dashboard_eps(self)
        scheme = 'https' if self.mgr.secure_monitoring_stack else 'http'

        prometheus_eps = self.get_service_endpoints('prometheus')
        alertmanager_eps = self.get_service_endpoints('alertmanager')
        grafana_eps = self.get_service_endpoints('grafana')
        main_context = {
            'dashboard_eps': dashboard_eps,
            'prometheus_eps': prometheus_eps,
            'alertmanager_eps': alertmanager_eps,
            'grafana_eps': grafana_eps
        }
        external_server_context = {
            'spec': svc_spec,
            'dashboard_scheme': dashboard_scheme,
            'grafana_scheme': 'https',  # TODO(redo): fixme, get current value of grafana scheme
            'prometheus_scheme': scheme,
            'alertmanager_scheme': scheme,
            'dashboard_eps': dashboard_eps,
            'prometheus_eps': prometheus_eps,
            'alertmanager_eps': alertmanager_eps,
            'grafana_eps': grafana_eps
        }
        internal_server_context = {
            'spec': svc_spec,
            'internal_port': self.INTERNAL_SERVICE_PORT,
            'grafana_scheme': 'https',  # TODO(redo): fixme, get current value of grafana scheme
            'prometheus_scheme': scheme,
            'alertmanager_scheme': scheme,
            'prometheus_eps': prometheus_eps,
            'alertmanager_eps': alertmanager_eps,
            'grafana_eps': grafana_eps
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

        return daemon_config, sorted(self.get_admin_gatway_deps())

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before admin-gateway daemon is removed.
        """
        # reset the standby dashboard redirection behaviour
        self.mgr.set_module_option_ex('dashboard', 'standby_error_status_code', '500')
        self.mgr.set_module_option_ex('dashboard', 'standby_behaviour', 'redirect')
