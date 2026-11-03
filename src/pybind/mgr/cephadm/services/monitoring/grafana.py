from typing import TYPE_CHECKING
import errno
import os
from typing import List, Any, Tuple, Dict, Optional, cast
from mgr_module import HandleCommandResult
from ..service_registry import register_cephadm_service
from cephadm.tlsobject_types import TLSCredentials
from orchestrator import DaemonDescription
from ceph.deployment.service_spec import GrafanaSpec, ServiceSpec, MgmtGatewaySpec
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec, CephadmService, DaemonDeployContext
from mgr_util import build_url
from ... import utils
import logging

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from ...module import CephadmOrchestrator


@register_cephadm_service
class GrafanaService(CephadmService):
    TYPE = 'grafana'
    DEFAULT_SERVICE_PORT = 3000

    def generate_data_sources(self, security_enabled: bool, mgmt_gw_enabled: bool, cert: str, pkey: str) -> str:
        prometheus_user, prometheus_password = self.mgr._get_prometheus_credentials()
        root_cert = self.mgr.cert_mgr.get_root_ca()
        oneline_root_cert = '\\n'.join([line.strip() for line in root_cert.splitlines()])
        oneline_cert = '\\n'.join([line.strip() for line in cert.splitlines()])
        oneline_key = '\\n'.join([line.strip() for line in pkey.splitlines()])
        prom_services = self.generate_prom_services(security_enabled, mgmt_gw_enabled)
        return self.mgr.template.render('services/grafana/ceph-dashboard.yml.j2',
                                        {'hosts': prom_services,
                                         'prometheus_user': prometheus_user,
                                         'prometheus_password': prometheus_password,
                                         'cephadm_root_ca': oneline_root_cert,
                                         'cert': oneline_cert,
                                         'key': oneline_key,
                                         'security_enabled': security_enabled,
                                         'mgmt_gw_enabled': mgmt_gw_enabled,
                                         'loki_host': self.get_loki_host()})

    def generate_grafana_ini(self,
                             daemon_spec: CephadmDaemonDeploySpec,
                             mgmt_gw_enabled: bool,
                             oauth2_enabled: bool) -> str:

        spec: GrafanaSpec = cast(GrafanaSpec, self.mgr.spec_store.active_specs[daemon_spec.service_name])
        grafana_port = daemon_spec.ports[0] if daemon_spec.ports else self.DEFAULT_SERVICE_PORT
        grafana_ip = daemon_spec.ip if daemon_spec.ip else ''
        if spec.only_bind_port_on_networks and spec.networks:
            assert daemon_spec.host is not None
            ip_to_bind_to = self.mgr.get_first_matching_network_ip(daemon_spec.host, spec)
            if ip_to_bind_to:
                daemon_spec.port_ips = {str(grafana_port): ip_to_bind_to}
                grafana_ip = ip_to_bind_to

        if not grafana_ip:
            # Grafana 11.1+ validates http_addr with net.ParseIP; hostnames such as
            # localhost fail in grafana-apiserver. Use a literal address (bind all IPv4).
            # Check if the primary manager or orchestrator is configured for IPv6
            if self.mgr.get_mgr_ip().startswith('::') or ':' in self.mgr.get_mgr_ip():
                grafana_ip = '::'
            else:
                grafana_ip = '0.0.0.0'

        domain = self.mgr.get_fqdn(daemon_spec.host)
        mgmt_gw_ips = []
        if mgmt_gw_enabled:
            mgmt_gw_daemons = self.mgr.cache.get_daemons_by_service('mgmt-gateway')
            if mgmt_gw_daemons:
                dd = mgmt_gw_daemons[0]
                assert dd.hostname
                mgmt_gw_spec = cast(MgmtGatewaySpec, self.mgr.spec_store['mgmt-gateway'].spec)
                # TODO(redo): should we resolve the virtual_ip to a name if possible?
                domain = mgmt_gw_spec.virtual_ip or self.mgr.get_fqdn(dd.hostname)  # give prio to VIP if configured
                mgmt_gw_ips = [self.mgr.inventory.get_addr(dd.hostname) for dd in mgmt_gw_daemons]  # type: ignore

        return self.mgr.template.render('services/grafana/grafana.ini.j2', {
            'anonymous_access': spec.anonymous_access,
            'initial_admin_password': spec.initial_admin_password,
            'protocol': spec.protocol,
            'http_port': grafana_port,
            'http_addr': grafana_ip,
            'domain': domain,
            'mgmt_gw_enabled': mgmt_gw_enabled,
            'oauth2_enabled': oauth2_enabled,
            'mgmt_gw_ips': ','.join(mgmt_gw_ips),
        })

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:

        deps = []  # type: List[str]
        security_enabled, mgmt_gw_enabled, _ = mgr._get_security_config()
        deps.append(f'secure_monitoring_stack:{mgr.secure_monitoring_stack}')

        # in case security is enabled we have to reconfig when prom user/pass changes
        prometheus_user, prometheus_password = mgr._get_prometheus_credentials()
        if security_enabled and prometheus_user and prometheus_password:
            deps.append(f'cred:{utils.config_hash(prometheus_user + prometheus_password)}')

        # adding a dependency for mgmt-gateway because the usage of url_prefix relies on its presence.
        # another dependency is added for oauth-proxy as Grafana login is delegated to this service when enabled.
        for service in ['prometheus', 'loki', 'mgmt-gateway', 'oauth2-proxy']:
            deps += [d.name() for d in mgr.cache.get_daemons_by_service(service)]

        return sorted(deps)

    def generate_prom_services(self, security_enabled: bool, mgmt_gw_enabled: bool) -> List[str]:

        # in case mgmt-gw is enabeld we only use one url pointing to the internal
        # mgmt gw for dashboard which will take care of HA in this case
        if mgmt_gw_enabled:
            return [f'{self.mgr.get_mgmt_gw_internal_endpoint()}/prometheus']

        prom_services = []  # type: List[str]
        for dd in self.mgr.cache.get_daemons_by_service('prometheus'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)
            port = dd.ports[0] if dd.ports else 9095
            protocol = 'https' if security_enabled else 'http'
            prom_services.append(build_url(scheme=protocol, host=addr, port=port))

        return prom_services

    def get_loki_host(self) -> str:
        daemons = self.mgr.cache.get_daemons_by_service('loki')
        for i, dd in enumerate(daemons):
            assert dd.hostname is not None
            if i == 0:
                addr = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)
                return build_url(scheme='http', host=addr, port=3100)

        return ''

    def get_grafana_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> TLSCredentials:
        host_ips = [self.mgr.inventory.get_addr(daemon_spec.host)]
        host_fqdns = [self.mgr.get_fqdn(daemon_spec.host), 'grafana_servers']
        return self.get_certificates(daemon_spec, ips=host_ips, fqdns=host_fqdns)

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type

        tls_pair = self.get_grafana_certificates(daemon_spec)
        if not tls_pair.cert or not tls_pair.key:
            # this will lead to an error in the daemon as certificates are needed
            logger.error(f'Cannot generate the needed certificates to deploy Grafana on {daemon_spec.host}')

        security_enabled, mgmt_gw_enabled, oauth2_enabled = self.mgr._get_security_config()
        grafana_ini = self.generate_grafana_ini(daemon_spec, mgmt_gw_enabled, oauth2_enabled)
        grafana_data_sources = self.generate_data_sources(security_enabled, mgmt_gw_enabled, tls_pair.cert, tls_pair.key)
        # the path of the grafana dashboards are assumed from the providers.yml.j2 file by grafana
        grafana_dashboards_path = self.mgr.grafana_dashboards_path or '/etc/grafana/dashboards/ceph-dashboard/'

        if 'dashboard' in self.mgr.get('mgr_map')['modules']:
            self.mgr.check_mon_command({
                'prefix': 'dashboard set-grafana-api-ssl-verify',
                'value': 'false'})

        config_file = {
            'files': {
                "grafana.ini": grafana_ini,
                'provisioning/datasources/ceph-dashboard.yml': grafana_data_sources,
                'certs/cert_file': '# generated by cephadm\n%s' % tls_pair.cert,
                'certs/cert_key': '# generated by cephadm\n%s' % tls_pair.key,
                'provisioning/dashboards/default.yml': self.mgr.template.render(
                    'services/grafana/providers.yml.j2', {
                        'grafana_dashboards_path': grafana_dashboards_path
                    }
                )
            }
        }

        grafana_spec = cast(
            GrafanaSpec,
            spec if spec is not None else self.mgr.spec_store.active_specs[daemon_spec.service_name],
        )
        if 'dashboard' in self.mgr.get('mgr_map')['modules'] and grafana_spec.initial_admin_password:
            self.mgr.check_mon_command({'prefix': 'dashboard set-grafana-api-password'}, inbuf=grafana_spec.initial_admin_password)

        # include dashboards, if present in the container
        if os.path.exists(grafana_dashboards_path):
            files = os.listdir(grafana_dashboards_path)
            for file_name in files:
                with open(os.path.join(grafana_dashboards_path, file_name), 'r', encoding='utf-8') as f:
                    dashboard = f.read()
                    config_file['files'][f'/etc/grafana/provisioning/dashboards/{file_name}'] = dashboard

        return config_file, self.get_dependencies(
            self.mgr, grafana_spec, daemon_spec.daemon_type)

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        # Use the least-created one as the active daemon
        if daemon_descrs:
            return daemon_descrs[-1]
        # if empty list provided, return empty Daemon Desc
        return DaemonDescription()

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        # TODO: signed cert
        dd = self.get_active_daemon(daemon_descrs)
        assert dd.hostname is not None
        addr = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)
        port = dd.ports[0] if dd.ports else self.DEFAULT_SERVICE_PORT
        spec = cast(GrafanaSpec, self.mgr.spec_store[dd.service_name()].spec)

        mgmt_gw_external_endpoint = self.mgr.get_mgmt_gw_external_endpoint()
        if mgmt_gw_external_endpoint is not None:
            self._set_value_on_dashboard(
                'Grafana',
                'dashboard get-grafana-api-url',
                'dashboard set-grafana-api-url',
                f'{mgmt_gw_external_endpoint}/grafana'
            )
            self._set_value_on_dashboard(
                'Grafana',
                'dashboard get-grafana-api-ssl-verify',
                'dashboard set-grafana-api-ssl-verify',
                'false'
            )
        else:
            service_url = build_url(scheme=spec.protocol, host=addr, port=port)
            self._set_value_on_dashboard(
                'Grafana',
                'dashboard get-grafana-api-url',
                'dashboard set-grafana-api-url',
                service_url
            )

    def reset_config(self, daemon: DaemonDescription) -> None:

        if daemon.hostname is None:
            return
        try:
            current_api_host = self.mgr.check_mon_command({"prefix": "dashboard get-grafana-api-url"}).stdout.strip()
            daemon_addr = daemon.ip if daemon.ip else self.mgr.get_fqdn(daemon.hostname)
            daemon_port = daemon.ports[0] if daemon.ports else self.DEFAULT_SERVICE_PORT
            service_url = build_url(scheme='https', host=daemon_addr, port=daemon_port)

            if current_api_host == service_url:
                remaining_daemons = [d for d in self.mgr.cache.get_daemons_by_service(self.TYPE)
                                     if d.name() != daemon.name()]
                if remaining_daemons:
                    self.config_dashboard(remaining_daemons)
                    logger.info("Updated dashboard API settings to point to the remaining daemon")
                else:
                    self.mgr.check_mon_command({"prefix": "dashboard reset-grafana-api-url"})
                    self.mgr.check_mon_command({"prefix": "dashboard reset-grafana-api-ssl-verify"})
                    logger.info("Reset dashboard API settings as no Grafana daemons are remaining")
            else:
                logger.info(f"Grafana daemon {daemon.name()} removed; no changes")
        except Exception as e:
            logger.error(f"Error in Grafana pre_remove: {str(e)}")

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before grafana daemon is removed.
        """
        self.reset_config(daemon)

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Grafana', 1)
        if warn and not force:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)
        return HandleCommandResult(0, warn_message, '')
