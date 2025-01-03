import errno
import logging
import os
import socket
from typing import List, Any, Tuple, Dict, Optional, cast

from mgr_module import HandleCommandResult

from orchestrator import DaemonDescription
from ceph.deployment.service_spec import AlertManagerSpec, GrafanaSpec, ServiceSpec, \
    SNMPGatewaySpec, PrometheusSpec, MgmtGatewaySpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec, get_dashboard_urls
from mgr_util import verify_tls, ServerConfigException, build_url, get_cert_issuer_info, password_hash
from ceph.deployment.utils import wrap_ipv6
from .. import utils

logger = logging.getLogger(__name__)


class GrafanaService(CephadmService):
    TYPE = 'grafana'
    DEFAULT_SERVICE_PORT = 3000

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

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

    def calculate_grafana_deps(self, security_enabled: bool) -> List[str]:

        deps = []  # type: List[str]
        deps.append(f'secure_monitoring_stack:{self.mgr.secure_monitoring_stack}')

        # in case security is enabled we have to reconfig when prom user/pass changes
        prometheus_user, prometheus_password = self.mgr._get_prometheus_credentials()
        if security_enabled and prometheus_user and prometheus_password:
            deps.append(f'{utils.md5_hash(prometheus_user + prometheus_password)}')

        # adding a dependency for mgmt-gateway because the usage of url_prefix relies on its presence.
        # another dependency is added for oauth-proxy as Grafana login is delegated to this service when enabled.
        for service in ['prometheus', 'loki', 'mgmt-gateway', 'oauth2-proxy']:
            deps += [d.name() for d in self.mgr.cache.get_daemons_by_service(service)]

        return deps

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

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type

        cert, pkey = self.prepare_certificates(daemon_spec)
        security_enabled, mgmt_gw_enabled, oauth2_enabled = self.mgr._get_security_config()
        deps = self.calculate_grafana_deps(security_enabled)
        grafana_ini = self.generate_grafana_ini(daemon_spec, mgmt_gw_enabled, oauth2_enabled)
        grafana_data_sources = self.generate_data_sources(security_enabled, mgmt_gw_enabled, cert, pkey)
        # the path of the grafana dashboards are assumed from the providers.yml.j2 file by grafana
        grafana_dashboards_path = self.mgr.grafana_dashboards_path or '/etc/grafana/dashboards/ceph-dashboard/'

        config_file = {
            'files': {
                "grafana.ini": grafana_ini,
                'provisioning/datasources/ceph-dashboard.yml': grafana_data_sources,
                'certs/cert_file': '# generated by cephadm\n%s' % cert,
                'certs/cert_key': '# generated by cephadm\n%s' % pkey,
                'provisioning/dashboards/default.yml': self.mgr.template.render(
                    'services/grafana/providers.yml.j2', {
                        'grafana_dashboards_path': grafana_dashboards_path
                    }
                )
            }
        }

        spec: GrafanaSpec = cast(GrafanaSpec, self.mgr.spec_store.active_specs[daemon_spec.service_name])
        if 'dashboard' in self.mgr.get('mgr_map')['modules'] and spec.initial_admin_password:
            self.mgr.check_mon_command({'prefix': 'dashboard set-grafana-api-password'}, inbuf=spec.initial_admin_password)

        # include dashboards, if present in the container
        if os.path.exists(grafana_dashboards_path):
            files = os.listdir(grafana_dashboards_path)
            for file_name in files:
                with open(os.path.join(grafana_dashboards_path, file_name), 'r', encoding='utf-8') as f:
                    dashboard = f.read()
                    config_file['files'][f'/etc/grafana/provisioning/dashboards/{file_name}'] = dashboard

        return config_file, sorted(deps)

    def prepare_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str]:
        cert = self.mgr.cert_key_store.get_cert('grafana_cert', host=daemon_spec.host)
        pkey = self.mgr.cert_key_store.get_key('grafana_key', host=daemon_spec.host)
        certs_present = (cert and pkey)
        is_valid_certificate = False
        (org, cn) = (None, None)
        if certs_present:
            try:
                (org, cn) = get_cert_issuer_info(cert)
                verify_tls(cert, pkey)
                is_valid_certificate = True
            except ServerConfigException as e:
                logger.warning(f'Provided grafana TLS certificates are invalid: {e}')

        if is_valid_certificate:
            # let's clear health error just in case it was set
            self.mgr.remove_health_warning('CEPHADM_CERT_ERROR')
            return cert, pkey

        # certificate is not valid, to avoid overwriting user generated
        # certificates we only re-generate in case of self signed certificates
        # that were originally generated by cephadm or in case cert/key are empty.
        if not certs_present or (org == 'Ceph' and cn == 'cephadm'):
            logger.info('Regenerating cephadm self-signed grafana TLS certificates')
            host_fqdn = socket.getfqdn(daemon_spec.host)
            node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
            cert, pkey = self.mgr.cert_mgr.generate_cert([host_fqdn, "grafana_servers"], node_ip)
            # cert, pkey = create_self_signed_cert('Ceph', host_fqdn)
            self.mgr.cert_key_store.save_cert('grafana_cert', cert, host=daemon_spec.host)
            self.mgr.cert_key_store.save_key('grafana_key', pkey, host=daemon_spec.host)
            if 'dashboard' in self.mgr.get('mgr_map')['modules']:
                self.mgr.check_mon_command({
                    'prefix': 'dashboard set-grafana-api-ssl-verify',
                    'value': 'false',
                })
            self.mgr.remove_health_warning('CEPHADM_CERT_ERROR')  # clear if any
        else:
            # the certificate was not generated by cephadm, we cannot overwrite
            # it by new self-signed ones. Let's warn the user to fix the issue
            err_msg = """
            Detected invalid grafana certificates. Set mgr/cephadm/grafana_crt
            and mgr/cephadm/grafana_key to valid certificates or reset their value
            to an empty string in case you want cephadm to generate self-signed Grafana
            certificates.

            Once done, run the following command to reconfig the daemon:

               > ceph orch daemon reconfig <grafana-daemon>

            """
            self.mgr.set_health_warning(
                'CEPHADM_CERT_ERROR', 'Invalid grafana certificate: ', 1, [err_msg])

        return cert, pkey

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

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before grafana daemon is removed.
        """
        if daemon.hostname is not None:
            # delete cert/key entires for this grafana daemon
            self.mgr.cert_key_store.rm_cert('grafana_cert', host=daemon.hostname)
            self.mgr.cert_key_store.rm_key('grafana_key', host=daemon.hostname)

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Grafana', 1)
        if warn and not force:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)
        return HandleCommandResult(0, warn_message, '')


class AlertmanagerService(CephadmService):
    TYPE = 'alertmanager'
    DEFAULT_SERVICE_PORT = 9093
    USER_CFG_KEY = 'alertmanager/web_user'
    PASS_CFG_KEY = 'alertmanager/web_password'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def get_alertmanager_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str]:
        node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
        host_fqdn = self.mgr.get_fqdn(daemon_spec.host)
        cert, key = self.mgr.cert_mgr.generate_cert([host_fqdn, "alertmanager_servers"], node_ip)
        return cert, key

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        deps: List[str] = []
        default_webhook_urls: List[str] = []

        spec = cast(AlertManagerSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        try:
            secure = spec.secure
        except AttributeError:
            secure = False
        user_data = spec.user_data
        if 'default_webhook_urls' in user_data and isinstance(
                user_data['default_webhook_urls'], list):
            default_webhook_urls.extend(user_data['default_webhook_urls'])

        # add a dependency since url_prefix depends on the existence of mgmt-gateway
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('mgmt-gateway')]
        # add a dependency since enbling basic-auth (or not) depends on the existence of 'oauth2-proxy'
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('oauth2-proxy')]

        security_enabled, mgmt_gw_enabled, oauth2_enabled = self.mgr._get_security_config()
        if mgmt_gw_enabled:
            dashboard_urls = [f'{self.mgr.get_mgmt_gw_internal_endpoint()}/dashboard']
        else:
            dashboard_urls = get_dashboard_urls(self)
            # scan all mgrs to generate deps and to get standbys too.
            for dd in self.mgr.cache.get_daemons_by_service('mgr'):
                # we consider mgr a dep even if the dashboard is disabled
                # in order to be consistent with _calc_daemon_deps().
                # when mgmt_gw is enabled there's no need for mgr dep as
                # mgmt-gw wil route to the active mgr automatically
                deps.append(dd.name())

        snmp_gateway_urls: List[str] = []
        for dd in self.mgr.cache.get_daemons_by_service('snmp-gateway'):
            assert dd.hostname is not None
            assert dd.ports
            addr = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)
            deps.append(dd.name())

            snmp_gateway_urls.append(build_url(scheme='http', host=addr,
                                     port=dd.ports[0], path='/alerts'))

        context = {
            'security_enabled': security_enabled,
            'dashboard_urls': dashboard_urls,
            'default_webhook_urls': default_webhook_urls,
            'snmp_gateway_urls': snmp_gateway_urls,
            'secure': secure,
        }
        yml = self.mgr.template.render('services/alertmanager/alertmanager.yml.j2', context)

        peers = []
        port = 9094
        for dd in self.mgr.cache.get_daemons_by_service('alertmanager'):
            assert dd.hostname is not None
            deps.append(dd.name())
            addr = self.mgr.get_fqdn(dd.hostname)
            peers.append(build_url(host=addr, port=port).lstrip('/'))

        deps.append(f'secure_monitoring_stack:{self.mgr.secure_monitoring_stack}')
        if security_enabled:
            alertmanager_user, alertmanager_password = self.mgr._get_alertmanager_credentials()
            if alertmanager_user and alertmanager_password:
                deps.append(f'{utils.md5_hash(alertmanager_user + alertmanager_password)}')
            cert, key = self.get_alertmanager_certificates(daemon_spec)
            context = {
                'enable_mtls': mgmt_gw_enabled,
                'enable_basic_auth': not oauth2_enabled,
                'alertmanager_web_user': alertmanager_user,
                'alertmanager_web_password': password_hash(alertmanager_password),
            }
            return {
                "files": {
                    "alertmanager.yml": yml,
                    'alertmanager.crt': cert,
                    'alertmanager.key': key,
                    'web.yml': self.mgr.template.render('services/alertmanager/web.yml.j2', context),
                    'root_cert.pem': self.mgr.cert_mgr.get_root_ca()
                },
                'peers': peers,
                'web_config': '/etc/alertmanager/web.yml',
                'use_url_prefix': mgmt_gw_enabled
            }, sorted(deps)
        else:
            return {
                "files": {
                    "alertmanager.yml": yml
                },
                "peers": peers,
                'use_url_prefix': mgmt_gw_enabled
            }, sorted(deps)

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        # TODO: if there are multiple daemons, who is the active one?
        if daemon_descrs:
            return daemon_descrs[0]
        # if empty list provided, return empty Daemon Desc
        return DaemonDescription()

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        dd = self.get_active_daemon(daemon_descrs)
        assert dd.hostname is not None
        addr = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)
        port = dd.ports[0] if dd.ports else self.DEFAULT_SERVICE_PORT
        security_enabled, mgmt_gw_enabled, _ = self.mgr._get_security_config()
        protocol = 'https' if security_enabled else 'http'
        if mgmt_gw_enabled:
            self._set_value_on_dashboard(
                'AlertManager',
                'dashboard get-alertmanager-api-host',
                'dashboard set-alertmanager-api-host',
                f'{self.mgr.get_mgmt_gw_internal_endpoint()}/alertmanager'
            )
            self._set_value_on_dashboard(
                'Alertmanager',
                'dashboard get-alertmanager-api-ssl-verify',
                'dashboard set-alertmanager-api-ssl-verify',
                'false'
            )
        else:
            service_url = build_url(scheme=protocol, host=addr, port=port)
            self._set_value_on_dashboard(
                'AlertManager',
                'dashboard get-alertmanager-api-host',
                'dashboard set-alertmanager-api-host',
                service_url
            )

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Alertmanager', 1)
        if warn and not force:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)
        return HandleCommandResult(0, warn_message, '')


class PrometheusService(CephadmService):
    TYPE = 'prometheus'
    DEFAULT_SERVICE_PORT = 9095
    DEFAULT_MGR_PROMETHEUS_PORT = 9283
    USER_CFG_KEY = 'prometheus/web_user'
    PASS_CFG_KEY = 'prometheus/web_password'

    def config(self, spec: ServiceSpec) -> None:
        # make sure module is enabled
        mgr_map = self.mgr.get('mgr_map')
        if 'prometheus' not in mgr_map.get('services', {}):
            self.mgr.check_mon_command({
                'prefix': 'mgr module enable',
                'module': 'prometheus'
            })
            # we shouldn't get here (mon will tell the mgr to respawn), but no
            # harm done if we do.

    def get_prometheus_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str]:
        node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
        host_fqdn = self.mgr.get_fqdn(daemon_spec.host)
        cert, key = self.mgr.cert_mgr.generate_cert([host_fqdn, 'prometheus_servers'], node_ip)
        return cert, key

    def prepare_create(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def generate_config(
            self,
            daemon_spec: CephadmDaemonDeploySpec,
    ) -> Tuple[Dict[str, Any], List[str]]:

        assert self.TYPE == daemon_spec.daemon_type
        spec = cast(PrometheusSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        try:
            retention_time = spec.retention_time if spec.retention_time else '15d'
        except AttributeError:
            retention_time = '15d'

        try:
            targets = spec.targets
        except AttributeError:
            logger.warning('Prometheus targets not found in the spec. Using empty list.')
            targets = []

        try:
            retention_size = spec.retention_size if spec.retention_size else '0'
        except AttributeError:
            # default to disabled
            retention_size = '0'

        # build service discovery end-point
        security_enabled, mgmt_gw_enabled, oauth2_enabled = self.mgr._get_security_config()
        port = self.mgr.service_discovery_port
        mgr_addr = wrap_ipv6(self.mgr.get_mgr_ip())

        protocol = 'https' if security_enabled else 'http'
        self.mgr.get_mgmt_gw_internal_endpoint()
        if mgmt_gw_enabled:
            service_discovery_url_prefix = f'{self.mgr.get_mgmt_gw_internal_endpoint()}'
        else:
            service_discovery_url_prefix = f'{protocol}://{mgr_addr}:{port}'
        srv_end_point = f'{service_discovery_url_prefix}/sd/prometheus/sd-config?'

        node_exporter_cnt = len(self.mgr.cache.get_daemons_by_service('node-exporter'))
        alertmgr_cnt = len(self.mgr.cache.get_daemons_by_service('alertmanager'))
        haproxy_cnt = len(self.mgr.cache.get_daemons_by_type('ingress'))
        node_exporter_sd_url = f'{srv_end_point}service=node-exporter' if node_exporter_cnt > 0 else None
        alertmanager_sd_url = f'{srv_end_point}service=alertmanager' if alertmgr_cnt > 0 else None
        haproxy_sd_url = f'{srv_end_point}service=haproxy' if haproxy_cnt > 0 else None
        mgr_prometheus_sd_url = f'{srv_end_point}service=mgr-prometheus'  # always included
        ceph_exporter_sd_url = f'{srv_end_point}service=ceph-exporter'  # always included
        nvmeof_sd_url = f'{srv_end_point}service=nvmeof'  # always included
        mgmt_gw_enabled = len(self.mgr.cache.get_daemons_by_service('mgmt-gateway')) > 0
        nfs_sd_url = f'{srv_end_point}service=nfs'  # always included
        smb_sd_url = f'{srv_end_point}service=smb'  # always included

        alertmanager_user, alertmanager_password = self.mgr._get_alertmanager_credentials()
        prometheus_user, prometheus_password = self.mgr._get_prometheus_credentials()
        federate_path = self.get_target_cluster_federate_path(targets)
        cluster_credentials: Dict[str, Any] = {}
        cluster_credentials_files: Dict[str, Any] = {'files': {}}
        FSID = self.mgr._cluster_fsid
        if targets:
            if 'dashboard' in self.mgr.get('mgr_map')['modules']:
                cluster_credentials_files, cluster_credentials = self.mgr.remote(
                    'dashboard', 'get_cluster_credentials_files', targets
                )
            else:
                logger.error("dashboard module not found")

        # generate the prometheus configuration
        context = {
            'alertmanager_url_prefix': '/alertmanager' if mgmt_gw_enabled else '/',
            'alertmanager_web_user': alertmanager_user,
            'alertmanager_web_password': alertmanager_password,
            'security_enabled': security_enabled,
            'service_discovery_username': self.mgr.http_server.service_discovery.username,
            'service_discovery_password': self.mgr.http_server.service_discovery.password,
            'mgr_prometheus_sd_url': mgr_prometheus_sd_url,
            'node_exporter_sd_url': node_exporter_sd_url,
            'alertmanager_sd_url': alertmanager_sd_url,
            'haproxy_sd_url': haproxy_sd_url,
            'ceph_exporter_sd_url': ceph_exporter_sd_url,
            'nvmeof_sd_url': nvmeof_sd_url,
            'external_prometheus_targets': targets,
            'cluster_fsid': FSID,
            'nfs_sd_url': nfs_sd_url,
            'smb_sd_url': smb_sd_url,
            'clusters_credentials': cluster_credentials,
            'federate_path': federate_path
        }

        ip_to_bind_to = ''
        if spec.only_bind_port_on_networks and spec.networks:
            assert daemon_spec.host is not None
            ip_to_bind_to = self.mgr.get_first_matching_network_ip(daemon_spec.host, spec) or ''
            if ip_to_bind_to:
                daemon_spec.port_ips = {str(port): ip_to_bind_to}

        web_context = {
            'enable_mtls': mgmt_gw_enabled,
            'enable_basic_auth': not oauth2_enabled,
            'prometheus_web_user': prometheus_user,
            'prometheus_web_password': password_hash(prometheus_password),
        }

        if security_enabled:
            # Following key/cert are needed for:
            # 1- run the prometheus server (web.yml config)
            # 2- use mTLS to scrape node-exporter (prometheus acts as client)
            # 3- use mTLS to send alerts to alertmanager (prometheus acts as client)
            cert, key = self.get_prometheus_certificates(daemon_spec)
            r: Dict[str, Any] = {
                'files': {
                    'prometheus.yml': self.mgr.template.render('services/prometheus/prometheus.yml.j2', context),
                    'root_cert.pem': self.mgr.cert_mgr.get_root_ca(),
                    'web.yml': self.mgr.template.render('services/prometheus/web.yml.j2', web_context),
                    'prometheus.crt': cert,
                    'prometheus.key': key,
                },
                'retention_time': retention_time,
                'retention_size': retention_size,
                'ip_to_bind_to': ip_to_bind_to,
                'web_config': '/etc/prometheus/web.yml',
                'use_url_prefix': mgmt_gw_enabled
            }
            r['files'].update(cluster_credentials_files['files'])
        else:
            r = {
                'files': {
                    'prometheus.yml': self.mgr.template.render('services/prometheus/prometheus.yml.j2', context)
                },
                'retention_time': retention_time,
                'retention_size': retention_size,
                'ip_to_bind_to': ip_to_bind_to,
                'use_url_prefix': mgmt_gw_enabled
            }

        # include alerts, if present in the container
        if os.path.exists(self.mgr.prometheus_alerts_path):
            with open(self.mgr.prometheus_alerts_path, 'r', encoding='utf-8') as f:
                alerts = f.read()
            r['files']['/etc/prometheus/alerting/ceph_alerts.yml'] = alerts

        # Include custom alerts if present in key value store. This enables the
        # users to add custom alerts. Write the file in any case, so that if the
        # content of the key value store changed, that file is overwritten
        # (emptied in case they value has been removed from the key value
        # store). This prevents the necessity to adapt `cephadm` binary to
        # remove the file.
        #
        # Don't use the template engine for it as
        #
        #   1. the alerts are always static and
        #   2. they are a template themselves for the Go template engine, which
        #      use curly braces and escaping that is cumbersome and unnecessary
        #      for the user.
        #
        r['files']['/etc/prometheus/alerting/custom_alerts.yml'] = \
            self.mgr.get_store('services/prometheus/alerting/custom_alerts.yml', '')

        return r, sorted(self.calculate_deps())

    def calculate_deps(self) -> List[str]:
        deps = []  # type: List[str]
        port = cast(int, self.mgr.get_module_option_ex('prometheus', 'server_port', self.DEFAULT_MGR_PROMETHEUS_PORT))
        deps.append(str(port))
        deps.append(str(self.mgr.service_discovery_port))
        deps.append(f'secure_monitoring_stack:{self.mgr.secure_monitoring_stack}')
        security_enabled, mgmt_gw_enabled, _ = self.mgr._get_security_config()

        if not mgmt_gw_enabled:
            # add an explicit dependency on the active manager. This will force to
            # re-deploy prometheus if the mgr has changed (due to a fail-over i.e).
            # when mgmt_gw is enabled there's no need for such dep as mgmt-gw wil
            # route to the active mgr automatically
            deps.append(self.mgr.get_active_mgr().name())

        if security_enabled:
            alertmanager_user, alertmanager_password = self.mgr._get_alertmanager_credentials()
            prometheus_user, prometheus_password = self.mgr._get_prometheus_credentials()
            if prometheus_user and prometheus_password:
                deps.append(f'{utils.md5_hash(prometheus_user + prometheus_password)}')
            if alertmanager_user and alertmanager_password:
                deps.append(f'{utils.md5_hash(alertmanager_user + alertmanager_password)}')

        # add a dependency since url_prefix depends on the existence of mgmt-gateway
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('mgmt-gateway')]
        # add a dependency since enbling basic-auth (or not) depends on the existence of 'oauth2-proxy'
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('oauth2-proxy')]

        # add dependency on ceph-exporter daemons
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('ceph-exporter')]
        deps += [s for s in ['node-exporter', 'alertmanager'] if self.mgr.cache.get_daemons_by_service(s)]
        if len(self.mgr.cache.get_daemons_by_type('ingress')) > 0:
            deps.append('ingress')
        return deps

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        # TODO: if there are multiple daemons, who is the active one?
        if daemon_descrs:
            return daemon_descrs[0]
        # if empty list provided, return empty Daemon Desc
        return DaemonDescription()

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        dd = self.get_active_daemon(daemon_descrs)
        assert dd.hostname is not None
        addr = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)
        port = dd.ports[0] if dd.ports else self.DEFAULT_SERVICE_PORT
        security_enabled, mgmt_gw_enabled, _ = self.mgr._get_security_config()
        protocol = 'https' if security_enabled else 'http'
        if mgmt_gw_enabled:
            self._set_value_on_dashboard(
                'Prometheus',
                'dashboard get-prometheus-api-host',
                'dashboard set-prometheus-api-host',
                f'{self.mgr.get_mgmt_gw_internal_endpoint()}/prometheus'
            )
            self._set_value_on_dashboard(
                'Prometheus',
                'dashboard get-prometheus-api-ssl-verify',
                'dashboard set-prometheus-api-ssl-verify',
                'false'
            )
        else:
            service_url = build_url(scheme=protocol, host=addr, port=port)
            self._set_value_on_dashboard(
                'Prometheus',
                'dashboard get-prometheus-api-host',
                'dashboard set-prometheus-api-host',
                service_url
            )

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Prometheus', 1)
        if warn and not force:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)
        return HandleCommandResult(0, warn_message, '')

    def get_target_cluster_federate_path(self, targets: List[str]) -> str:
        for target in targets:
            if ':' in target:
                return '/federate'
        return '/prometheus/federate'


class NodeExporterService(CephadmService):
    TYPE = 'node-exporter'
    DEFAULT_SERVICE_PORT = 9100

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def get_node_exporter_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str]:
        node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
        host_fqdn = self.mgr.get_fqdn(daemon_spec.host)
        cert, key = self.mgr.cert_mgr.generate_cert(host_fqdn, node_ip)
        return cert, key

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        deps = []
        deps += [d.name() for d in self.mgr.cache.get_daemons_by_service('mgmt-gateway')]
        deps += [f'secure_monitoring_stack:{self.mgr.secure_monitoring_stack}']
        security_enabled, mgmt_gw_enabled, _ = self.mgr._get_security_config()
        if security_enabled:
            cert, key = self.get_node_exporter_certificates(daemon_spec)
            r = {
                'files': {
                    'web.yml': self.mgr.template.render('services/node-exporter/web.yml.j2',
                                                        {'enable_mtls': mgmt_gw_enabled}),
                    'root_cert.pem': self.mgr.cert_mgr.get_root_ca(),
                    'node_exporter.crt': cert,
                    'node_exporter.key': key,
                },
                'web_config': '/etc/node-exporter/web.yml'
            }
        else:
            r = {}

        return r, deps

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        # since node exporter runs on each host and cannot compromise data, no extra checks required
        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        out = f'It is presumed safe to stop {names}'
        return HandleCommandResult(0, out, '')


class LokiService(CephadmService):
    TYPE = 'loki'
    DEFAULT_SERVICE_PORT = 3100

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        deps: List[str] = []

        yml = self.mgr.template.render('services/loki.yml.j2')
        return {
            "files": {
                "loki.yml": yml
            }
        }, sorted(deps)


class PromtailService(CephadmService):
    TYPE = 'promtail'
    DEFAULT_SERVICE_PORT = 9080

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        deps: List[str] = []

        daemons = self.mgr.cache.get_daemons_by_service('loki')
        loki_host = ''
        for i, dd in enumerate(daemons):
            assert dd.hostname is not None
            if i == 0:
                loki_host = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)

            deps.append(dd.name())

        context = {
            'client_hostname': loki_host,
        }

        yml = self.mgr.template.render('services/promtail.yml.j2', context)
        return {
            "files": {
                "promtail.yml": yml
            }
        }, sorted(deps)


class SNMPGatewayService(CephadmService):
    TYPE = 'snmp-gateway'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        deps: List[str] = []

        spec = cast(SNMPGatewaySpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        config = {
            "destination": spec.snmp_destination,
            "snmp_version": spec.snmp_version,
        }
        if spec.snmp_version == 'V2c':
            community = spec.credentials.get('snmp_community', None)
            assert community is not None

            config.update({
                "snmp_community": community
            })
        else:
            # SNMP v3 settings can be either authNoPriv or authPriv
            auth_protocol = 'SHA' if not spec.auth_protocol else spec.auth_protocol

            auth_username = spec.credentials.get('snmp_v3_auth_username', None)
            auth_password = spec.credentials.get('snmp_v3_auth_password', None)
            assert auth_username is not None
            assert auth_password is not None
            assert spec.engine_id is not None

            config.update({
                "snmp_v3_auth_protocol": auth_protocol,
                "snmp_v3_auth_username": auth_username,
                "snmp_v3_auth_password": auth_password,
                "snmp_v3_engine_id": spec.engine_id,
            })
            # authPriv adds encryption
            if spec.privacy_protocol:
                priv_password = spec.credentials.get('snmp_v3_priv_password', None)
                assert priv_password is not None

                config.update({
                    "snmp_v3_priv_protocol": spec.privacy_protocol,
                    "snmp_v3_priv_password": priv_password,
                })

        logger.debug(
            f"Generated configuration for '{self.TYPE}' service. Dependencies={deps}")

        return config, sorted(deps)
