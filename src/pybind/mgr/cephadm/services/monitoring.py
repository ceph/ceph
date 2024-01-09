import errno
import ipaddress
import logging
import os
import socket
from typing import List, Any, Tuple, Dict, Optional, cast
from urllib.parse import urlparse

from mgr_module import HandleCommandResult

from orchestrator import DaemonDescription
from ceph.deployment.service_spec import AlertManagerSpec, GrafanaSpec, ServiceSpec, \
    SNMPGatewaySpec, PrometheusSpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec
from mgr_util import verify_tls, ServerConfigException, create_self_signed_cert, build_url, get_cert_issuer_info, password_hash
from ceph.deployment.utils import wrap_ipv6

logger = logging.getLogger(__name__)


class GrafanaService(CephadmService):
    TYPE = 'grafana'
    DEFAULT_SERVICE_PORT = 3000

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        prometheus_user, prometheus_password = self.mgr._get_prometheus_credentials()
        deps = []  # type: List[str]
        if self.mgr.secure_monitoring_stack and prometheus_user and prometheus_password:
            deps.append(f'{hash(prometheus_user + prometheus_password)}')
        deps.append(f'secure_monitoring_stack:{self.mgr.secure_monitoring_stack}')

        prom_services = []  # type: List[str]
        for dd in self.mgr.cache.get_daemons_by_service('prometheus'):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self._inventory_get_fqdn(dd.hostname)
            port = dd.ports[0] if dd.ports else 9095
            protocol = 'https' if self.mgr.secure_monitoring_stack else 'http'
            prom_services.append(build_url(scheme=protocol, host=addr, port=port))

            deps.append(dd.name())

        daemons = self.mgr.cache.get_daemons_by_service('loki')
        loki_host = ''
        for i, dd in enumerate(daemons):
            assert dd.hostname is not None
            if i == 0:
                addr = dd.ip if dd.ip else self._inventory_get_fqdn(dd.hostname)
                loki_host = build_url(scheme='http', host=addr, port=3100)

            deps.append(dd.name())

        root_cert = self.mgr.http_server.service_discovery.ssl_certs.get_root_cert()
        oneline_root_cert = '\\n'.join([line.strip() for line in root_cert.splitlines()])
        grafana_data_sources = self.mgr.template.render('services/grafana/ceph-dashboard.yml.j2',
                                                        {'hosts': prom_services,
                                                         'prometheus_user': prometheus_user,
                                                         'prometheus_password': prometheus_password,
                                                         'cephadm_root_ca': oneline_root_cert,
                                                         'security_enabled': self.mgr.secure_monitoring_stack,
                                                         'loki_host': loki_host})

        spec: GrafanaSpec = cast(
            GrafanaSpec, self.mgr.spec_store.active_specs[daemon_spec.service_name])

        grafana_port = daemon_spec.ports[0] if daemon_spec.ports else self.DEFAULT_SERVICE_PORT
        grafana_ip = daemon_spec.ip if daemon_spec.ip else ''

        if spec.only_bind_port_on_networks and spec.networks:
            assert daemon_spec.host is not None
            ip_to_bind_to = self.mgr.get_first_matching_network_ip(daemon_spec.host, spec)
            if ip_to_bind_to:
                daemon_spec.port_ips = {str(grafana_port): ip_to_bind_to}
                grafana_ip = ip_to_bind_to

        grafana_ini = self.mgr.template.render(
            'services/grafana/grafana.ini.j2', {
                'anonymous_access': spec.anonymous_access,
                'initial_admin_password': spec.initial_admin_password,
                'http_port': grafana_port,
                'protocol': spec.protocol,
                'http_addr': grafana_ip
            })

        if 'dashboard' in self.mgr.get('mgr_map')['modules'] and spec.initial_admin_password:
            self.mgr.check_mon_command(
                {'prefix': 'dashboard set-grafana-api-password'}, inbuf=spec.initial_admin_password)

        # the path of the grafana dashboards are assumed from the providers.yml.j2 file by grafana
        grafana_dashboards_path = self.mgr.grafana_dashboards_path or '/etc/grafana/dashboards/ceph-dashboard/'
        grafana_providers = self.mgr.template.render(
            'services/grafana/providers.yml.j2', {
                'grafana_dashboards_path': grafana_dashboards_path
            }
        )

        cert, pkey = self.prepare_certificates(daemon_spec)
        config_file = {
            'files': {
                "grafana.ini": grafana_ini,
                'provisioning/datasources/ceph-dashboard.yml': grafana_data_sources,
                'certs/cert_file': '# generated by cephadm\n%s' % cert,
                'certs/cert_key': '# generated by cephadm\n%s' % pkey,
                'provisioning/dashboards/default.yml': grafana_providers
            }
        }

        # include dashboards, if present in the container
        if os.path.exists(grafana_dashboards_path):
            files = os.listdir(grafana_dashboards_path)
            for file_name in files:
                with open(os.path.join(grafana_dashboards_path, file_name), 'r', encoding='utf-8') as f:
                    dashboard = f.read()
                    config_file['files'][f'/etc/grafana/provisioning/dashboards/{file_name}'] = dashboard

        return config_file, sorted(deps)

    def prepare_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str]:
        cert_path = f'{daemon_spec.host}/grafana_crt'
        key_path = f'{daemon_spec.host}/grafana_key'
        cert = self.mgr.get_store(cert_path)
        pkey = self.mgr.get_store(key_path)
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
            cert, pkey = create_self_signed_cert('Ceph', host_fqdn)
            self.mgr.set_store(cert_path, cert)
            self.mgr.set_store(key_path, pkey)
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
        addr = dd.ip if dd.ip else self._inventory_get_fqdn(dd.hostname)
        port = dd.ports[0] if dd.ports else self.DEFAULT_SERVICE_PORT
        spec = cast(GrafanaSpec, self.mgr.spec_store[dd.service_name()].spec)
        service_url = build_url(scheme=spec.protocol, host=addr, port=port)
        self._set_service_url_on_dashboard(
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
            cert_path = f'{daemon.hostname}/grafana_crt'
            key_path = f'{daemon.hostname}/grafana_key'
            self.mgr.set_store(cert_path, None)
            self.mgr.set_store(key_path, None)

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

        # dashboard(s)
        dashboard_urls: List[str] = []
        snmp_gateway_urls: List[str] = []
        mgr_map = self.mgr.get('mgr_map')
        port = None
        proto = None  # http: or https:
        url = mgr_map.get('services', {}).get('dashboard', None)
        if url:
            p_result = urlparse(url.rstrip('/'))
            hostname = socket.getfqdn(p_result.hostname)

            try:
                ip = ipaddress.ip_address(hostname)
            except ValueError:
                pass
            else:
                if ip.version == 6:
                    hostname = f'[{hostname}]'

            dashboard_urls.append(
                f'{p_result.scheme}://{hostname}:{p_result.port}{p_result.path}')
            proto = p_result.scheme
            port = p_result.port

        # scan all mgrs to generate deps and to get standbys too.
        # assume that they are all on the same port as the active mgr.
        for dd in self.mgr.cache.get_daemons_by_service('mgr'):
            # we consider mgr a dep even if the dashboard is disabled
            # in order to be consistent with _calc_daemon_deps().
            deps.append(dd.name())
            if not port:
                continue
            if dd.daemon_id == self.mgr.get_mgr_id():
                continue
            assert dd.hostname is not None
            addr = self._inventory_get_fqdn(dd.hostname)
            dashboard_urls.append(build_url(scheme=proto, host=addr, port=port).rstrip('/'))

        for dd in self.mgr.cache.get_daemons_by_service('snmp-gateway'):
            assert dd.hostname is not None
            assert dd.ports
            addr = dd.ip if dd.ip else self._inventory_get_fqdn(dd.hostname)
            deps.append(dd.name())

            snmp_gateway_urls.append(build_url(scheme='http', host=addr,
                                     port=dd.ports[0], path='/alerts'))

        context = {
            'secure_monitoring_stack': self.mgr.secure_monitoring_stack,
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
            addr = self._inventory_get_fqdn(dd.hostname)
            peers.append(build_url(host=addr, port=port).lstrip('/'))

        deps.append(f'secure_monitoring_stack:{self.mgr.secure_monitoring_stack}')

        if self.mgr.secure_monitoring_stack:
            alertmanager_user, alertmanager_password = self.mgr._get_alertmanager_credentials()
            if alertmanager_user and alertmanager_password:
                deps.append(f'{hash(alertmanager_user + alertmanager_password)}')
            node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
            host_fqdn = self._inventory_get_fqdn(daemon_spec.host)
            cert, key = self.mgr.http_server.service_discovery.ssl_certs.generate_cert(
                host_fqdn, node_ip)
            context = {
                'alertmanager_web_user': alertmanager_user,
                'alertmanager_web_password': password_hash(alertmanager_password),
            }
            return {
                "files": {
                    "alertmanager.yml": yml,
                    'alertmanager.crt': cert,
                    'alertmanager.key': key,
                    'web.yml': self.mgr.template.render('services/alertmanager/web.yml.j2', context),
                    'root_cert.pem': self.mgr.http_server.service_discovery.ssl_certs.get_root_cert()
                },
                'peers': peers,
                'web_config': '/etc/alertmanager/web.yml'
            }, sorted(deps)
        else:
            return {
                "files": {
                    "alertmanager.yml": yml
                },
                "peers": peers
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
        addr = dd.ip if dd.ip else self._inventory_get_fqdn(dd.hostname)
        port = dd.ports[0] if dd.ports else self.DEFAULT_SERVICE_PORT
        protocol = 'https' if self.mgr.secure_monitoring_stack else 'http'
        service_url = build_url(scheme=protocol, host=addr, port=port)
        self._set_service_url_on_dashboard(
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

        try:
            enable_admin_api = spec.enable_admin_api
        except AttributeError:
            enable_admin_api = False

        # build service discovery end-point
        port = self.mgr.service_discovery_port
        mgr_addr = wrap_ipv6(self.mgr.get_mgr_ip())
        protocol = 'https' if self.mgr.secure_monitoring_stack else 'http'
        srv_end_point = f'{protocol}://{mgr_addr}:{port}/sd/prometheus/sd-config?'

        node_exporter_cnt = len(self.mgr.cache.get_daemons_by_service('node-exporter'))
        alertmgr_cnt = len(self.mgr.cache.get_daemons_by_service('alertmanager'))
        haproxy_cnt = len(self.mgr.cache.get_daemons_by_type('ingress'))
        node_exporter_sd_url = f'{srv_end_point}service=node-exporter' if node_exporter_cnt > 0 else None
        alertmanager_sd_url = f'{srv_end_point}service=alertmanager' if alertmgr_cnt > 0 else None
        haproxy_sd_url = f'{srv_end_point}service=haproxy' if haproxy_cnt > 0 else None
        mgr_prometheus_sd_url = f'{srv_end_point}service=mgr-prometheus'  # always included
        ceph_exporter_sd_url = f'{srv_end_point}service=ceph-exporter'  # always included
        nvmeof_sd_url = f'{srv_end_point}service=nvmeof'  # always included

        alertmanager_user, alertmanager_password = self.mgr._get_alertmanager_credentials()
        prometheus_user, prometheus_password = self.mgr._get_prometheus_credentials()
        FSID = self.mgr._cluster_fsid

        # generate the prometheus configuration
        context = {
            'alertmanager_web_user': alertmanager_user,
            'alertmanager_web_password': alertmanager_password,
            'secure_monitoring_stack': self.mgr.secure_monitoring_stack,
            'service_discovery_username': self.mgr.http_server.service_discovery.username,
            'service_discovery_password': self.mgr.http_server.service_discovery.password,
            'mgr_prometheus_sd_url': mgr_prometheus_sd_url,
            'node_exporter_sd_url': node_exporter_sd_url,
            'alertmanager_sd_url': alertmanager_sd_url,
            'haproxy_sd_url': haproxy_sd_url,
            'ceph_exporter_sd_url': ceph_exporter_sd_url,
            'nvmeof_sd_url': nvmeof_sd_url,
            'external_prometheus_targets': targets,
            'cluster_fsid': FSID
        }

        ip_to_bind_to = ''
        if spec.only_bind_port_on_networks and spec.networks:
            assert daemon_spec.host is not None
            ip_to_bind_to = self.mgr.get_first_matching_network_ip(daemon_spec.host, spec) or ''
            if ip_to_bind_to:
                daemon_spec.port_ips = {str(port): ip_to_bind_to}

        web_context = {
            'prometheus_web_user': prometheus_user,
            'prometheus_web_password': password_hash(prometheus_password),
        }

        if self.mgr.secure_monitoring_stack:
            cfg_key = 'mgr/prometheus/root/cert'
            cmd = {'prefix': 'config-key get', 'key': cfg_key}
            ret, mgr_prometheus_rootca, err = self.mgr.mon_command(cmd)
            if ret != 0:
                logger.error(f'mon command to get config-key {cfg_key} failed: {err}')
            else:
                node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
                host_fqdn = self._inventory_get_fqdn(daemon_spec.host)
                cert, key = self.mgr.http_server.service_discovery.ssl_certs.generate_cert(host_fqdn, node_ip)
                r: Dict[str, Any] = {
                    'files': {
                        'prometheus.yml': self.mgr.template.render('services/prometheus/prometheus.yml.j2', context),
                        'root_cert.pem': self.mgr.http_server.service_discovery.ssl_certs.get_root_cert(),
                        'mgr_prometheus_cert.pem': mgr_prometheus_rootca,
                        'web.yml': self.mgr.template.render('services/prometheus/web.yml.j2', web_context),
                        'prometheus.crt': cert,
                        'prometheus.key': key,
                    },
                    'retention_time': retention_time,
                    'retention_size': retention_size,
                    'ip_to_bind_to': ip_to_bind_to,
                    'enable_admin_api': enable_admin_api,
                    'web_config': '/etc/prometheus/web.yml'
                }
        else:
            r = {
                'files': {
                    'prometheus.yml': self.mgr.template.render('services/prometheus/prometheus.yml.j2', context)
                },
                'retention_time': retention_time,
                'retention_size': retention_size,
                'ip_to_bind_to': ip_to_bind_to,
                'enable_admin_api': enable_admin_api,
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
        # add an explicit dependency on the active manager. This will force to
        # re-deploy prometheus if the mgr has changed (due to a fail-over i.e).
        deps.append(self.mgr.get_active_mgr().name())
        if self.mgr.secure_monitoring_stack:
            alertmanager_user, alertmanager_password = self.mgr._get_alertmanager_credentials()
            prometheus_user, prometheus_password = self.mgr._get_prometheus_credentials()
            if prometheus_user and prometheus_password:
                deps.append(f'{hash(prometheus_user + prometheus_password)}')
            if alertmanager_user and alertmanager_password:
                deps.append(f'{hash(alertmanager_user + alertmanager_password)}')
        deps.append(f'secure_monitoring_stack:{self.mgr.secure_monitoring_stack}')
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
        addr = dd.ip if dd.ip else self._inventory_get_fqdn(dd.hostname)
        port = dd.ports[0] if dd.ports else self.DEFAULT_SERVICE_PORT
        protocol = 'https' if self.mgr.secure_monitoring_stack else 'http'
        service_url = build_url(scheme=protocol, host=addr, port=port)
        self._set_service_url_on_dashboard(
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


class NodeExporterService(CephadmService):
    TYPE = 'node-exporter'
    DEFAULT_SERVICE_PORT = 9100

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        deps = [f'secure_monitoring_stack:{self.mgr.secure_monitoring_stack}']
        if self.mgr.secure_monitoring_stack:
            node_ip = self.mgr.inventory.get_addr(daemon_spec.host)
            host_fqdn = self._inventory_get_fqdn(daemon_spec.host)
            cert, key = self.mgr.http_server.service_discovery.ssl_certs.generate_cert(
                host_fqdn, node_ip)
            r = {
                'files': {
                    'web.yml': self.mgr.template.render('services/node-exporter/web.yml.j2', {}),
                    'root_cert.pem': self.mgr.http_server.service_discovery.ssl_certs.get_root_cert(),
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
                loki_host = dd.ip if dd.ip else self._inventory_get_fqdn(dd.hostname)

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
