from typing import TYPE_CHECKING
import errno
import os
from typing import List, Any, Tuple, Dict, Optional, cast
import time
import requests
from mgr_module import HandleCommandResult
from ..service_registry import register_cephadm_service
from cephadm.services.service_registry import service_registry
from cephadm.tlsobject_types import TLSCredentials
from orchestrator import DaemonDescription
from ceph.deployment.service_spec import ServiceSpec, PrometheusSpec
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec, CephadmService, DaemonDeployContext, next_action_for_mgmt_stack_service
from mgr_util import build_url, password_hash
from ceph.deployment.utils import wrap_ipv6
from ... import utils
from .common import get_field_from_spec
import logging

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from ...module import CephadmOrchestrator


@register_cephadm_service
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

    def get_prometheus_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> TLSCredentials:
        host_ips = [self.mgr.inventory.get_addr(daemon_spec.host)]
        host_fqdns = [self.mgr.get_fqdn(daemon_spec.host), 'prometheus_servers']
        return self.get_certificates(daemon_spec, ips=host_ips, fqdns=host_fqdns)

    def get_service_discovery_cfg(self, security_enabled: bool, mgmt_gw_enabled: bool) -> Dict[str, List[str]]:
        """
        Retrieves the service discovery URLs for the services that require monitoring

        Note: we always add the 'ceph' Prometheus target as it corresponds to the prometheus-mgr module target

        Returns:
            Dict[str, List[str]]: A dictionary where the keys represent service categories (e.g., "nfs", "node-exporterr") and
                                  the values are a list of service-discovery URLs used to get the corresponding service targets.
        """

        def sd_urls(svc: str, prefixes: List[str]) -> list[str]:
            return [f'{p}/sd/prometheus/sd-config?service={svc}' for p in prefixes]

        if mgmt_gw_enabled:
            service_discovery_url_prefixes = [f'{self.mgr.get_mgmt_gw_internal_endpoint()}']
        else:
            port = self.mgr.service_discovery_port
            protocol = 'https' if security_enabled else 'http'
            service_discovery_url_prefixes = [f'{protocol}://{wrap_ipv6(ip)}:{port}'
                                              for ip in self.mgr._get_mgr_ips()]

        services_to_monitor = ['ceph', *(
            s for s in service_registry.get_services_requiring_monitoring()
            if self.mgr.cache.get_daemons_by_service(s) or self.mgr.cache.get_daemons_by_type(s)
        )]

        return {s: sd_urls(s, service_discovery_url_prefixes) for s in services_to_monitor}

    def configure_alerts(self, r: Dict) -> None:
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

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type

        spec = cast(PrometheusSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        retention_time = get_field_from_spec(spec, 'retention_time', '15d')
        retention_size = get_field_from_spec(spec, 'retention_size', '0')
        targets = get_field_from_spec(spec, 'targets', [])
        remote_write_url = get_field_from_spec(spec, 'remote_write_url', '')
        remote_write_allowed_metrics = get_field_from_spec(spec, 'remote_write_allowed_metrics', '')

        # build service discovery end-point
        security_enabled, mgmt_gw_enabled, oauth2_enabled = self.mgr._get_security_config()
        alertmanager_user, alertmanager_password = self.mgr._get_alertmanager_credentials()
        federate_path = self.get_target_cluster_federate_path(targets)
        cluster_credentials: Dict[str, Any] = {}
        cluster_credentials_files: Dict[str, Any] = {'files': {}}
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
            'security_enabled': security_enabled,
            'alertmanager_web_user': alertmanager_user,
            'alertmanager_web_password': alertmanager_password,
            'service_discovery_username': self.mgr.http_server.service_discovery.username,
            'service_discovery_password': self.mgr.http_server.service_discovery.password,
            'service_discovery_cfg': self.get_service_discovery_cfg(security_enabled, mgmt_gw_enabled),
            'external_prometheus_targets': targets,
            'remote_write_url': remote_write_url,
            'remote_write_allowed_metrics': remote_write_allowed_metrics,
            'cluster_fsid': self.mgr._cluster_fsid,
            'clusters_credentials': cluster_credentials,
            'federate_path': federate_path
        }

        ip_to_bind_to = ''
        if spec.only_bind_port_on_networks and spec.networks:
            assert daemon_spec.host is not None
            ip_to_bind_to = self.mgr.get_first_matching_network_ip(daemon_spec.host, spec) or ''
            if ip_to_bind_to:
                daemon_spec.port_ips = {str(self.mgr.service_discovery_port): ip_to_bind_to}

        files = {
            'prometheus.yml': self.mgr.template.render('services/prometheus/prometheus.yml.j2', context)
        }

        # check if the prometheus.yml already exists in the config-key store,
        # if not we need to set the initial config-key with the default template content.
        # If it already exists, we need not override user config changes.
        r, outs, err = self.mgr.mon_command({
            'prefix': 'config-key get',
            'key': 'mgr/cephadm/services/prometheus/prometheus.yml'
        })
        if r == -errno.ENOENT:
            loader = self.mgr.template.engine.env.loader
            assert loader is not None

            raw_template, _, _ = loader.get_source(
                self.mgr.template.engine.env,
                'services/prometheus/prometheus.yml.j2'
            )
            self.mgr.check_mon_command({
                'prefix': 'config-key set',
                'key': 'mgr/cephadm/services/prometheus/prometheus.yml',
                'val': raw_template
            })

        r: Dict[str, Any] = {
            'files': files,
            'retention_time': retention_time,
            'retention_size': retention_size,
            'ip_to_bind_to': ip_to_bind_to,
            'use_url_prefix': mgmt_gw_enabled
        }
        if security_enabled:
            # Following key/cert are needed for:
            # 1- run the prometheus server (web.yml config)
            # 2- use mTLS to scrape node-exporter (prometheus acts as client)
            # 3- use mTLS to send alerts to alertmanager (prometheus acts as client)
            prometheus_user, prometheus_password = self.mgr._get_prometheus_credentials()
            web_context = {
                'enable_mtls': mgmt_gw_enabled,
                'enable_basic_auth': not oauth2_enabled,
                'prometheus_web_user': prometheus_user,
                'prometheus_web_password': password_hash(prometheus_password),
            }
            tls_pair = self.get_prometheus_certificates(daemon_spec)
            files.update({
                'root_cert.pem': self.mgr.cert_mgr.get_root_ca(),
                'web.yml': self.mgr.template.render('services/prometheus/web.yml.j2', web_context),
                'prometheus.crt': tls_pair.cert,
                'prometheus.key': tls_pair.key,
                **cluster_credentials_files['files']
            })
            r.update({'web_config': '/etc/prometheus/web.yml'})

        self.configure_alerts(r)

        return r, self.get_dependencies(
            self.mgr, spec, daemon_spec.daemon_type)

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        deps = []  # type: List[str]
        deps.append(str(mgr.service_discovery_port))
        deps.append(f'secure_monitoring_stack:{mgr.secure_monitoring_stack}')
        security_enabled, mgmt_gw_enabled, _ = mgr._get_security_config()
        if security_enabled:
            alertmanager_user, alertmanager_password = mgr._get_alertmanager_credentials()
            prometheus_user, prometheus_password = mgr._get_prometheus_credentials()
            if prometheus_user and prometheus_password:
                deps.append(f'prom-cred:{utils.config_hash(prometheus_user + prometheus_password)}')
            if alertmanager_user and alertmanager_password:
                deps.append(f'alert-cred:{utils.config_hash(alertmanager_user + alertmanager_password)}')

        # Adding other services as deps (with corresponding justification):
        # mgmt-gateway : url_prefix depends on the existence of mgmt-gateway
        # oauth2-proxy : enbling basic-auth (or not) depends on the existence of 'oauth2-proxy'
        prometheus_svc_deps = service_registry.get_services_requiring_monitoring() + ['mgmt-gateway', 'oauth2-proxy']
        for svc in prometheus_svc_deps:
            configured = bool(mgr.cache.get_daemons_by_service(svc)) or bool(mgr.cache.get_daemons_by_type(svc))
            deps.append(f'{svc}_configured:{configured}')

        if not mgmt_gw_enabled:
            # Ceph mgrs are dependency because when mgmt-gateway is not enabled the service-discovery depends on mgrs ips
            deps += mgr.cache.get_daemons_by_types(['mgr'])

        if spec:
            prometheus_spec = cast(PrometheusSpec, spec)

            deps.append(f'remote_write_url:{prometheus_spec.remote_write_url}')
            deps.append(f'remote_write_metrics:{prometheus_spec.remote_write_allowed_metrics}')

        return sorted(deps)

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

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before Prometheus daemon is removed
        """
        MAX_RETRIES = 5
        RETRY_INTERVAL = 5
        if daemon.hostname is None:
            return
        try:
            current_api_host = self.mgr.check_mon_command({"prefix": "dashboard get-prometheus-api-host"}).stdout.strip()
            daemon_addr = daemon.ip if daemon.ip else self.mgr.get_fqdn(daemon.hostname)
            daemon_port = daemon.ports[0] if daemon.ports else self.DEFAULT_SERVICE_PORT
            service_url = build_url(scheme="http", host=daemon_addr, port=daemon_port)

            if current_api_host == service_url:
                remaining_daemons = [
                    d for d in self.mgr.cache.get_daemons_by_service(self.TYPE)
                    if d.name() != daemon.name()
                ]
                if remaining_daemons:
                    self.config_dashboard(remaining_daemons)
                    logger.info("Updated Dashboard Settings to point to remaining Prometheus daemons")
                    for attempt in range(MAX_RETRIES):
                        try:
                            response = requests.get(f"{service_url}/api/v1/rules", timeout=5)
                            if response.status_code == 200:
                                logger.info(f"Prometheus daemon is ready at {service_url}.")
                                break
                        except Exception as e:
                            logger.info(f"Retry {attempt + 1}: Waiting for Prometheus daemon at {service_url}: {e}")
                        time.sleep(RETRY_INTERVAL)
                    else:
                        logger.warning("Prometheus daemon did not become ready after retries.")
                else:
                    self.mgr.check_mon_command({"prefix": "dashboard reset-prometheus-api-host"})
                    self.mgr.check_mon_command({"prefix": "dashboard reset-prometheus-api-ssl-verify"})
                    logger.info("Reset Prometheus API settings as no daemons are remaining")
            else:
                logger.info("Prometheus daemon removed; no changes to dashboard API settings")
        except Exception as e:
            logger.error(f"Error in Prometheus pre_remove {str(e)}")

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

    def choose_next_action(
        self,
        scheduled_action: utils.Action,
        daemon_type: Optional[str],
        spec: Optional[ServiceSpec],
        curr_deps: List[str],
        last_deps: List[str],
        daemon: Optional[DaemonDescription] = None,
    ) -> utils.NextDaemonStep:
        """Given the scheduled_action, service spec, daemon_type, and
        current and previous dependency lists return the next action that
        this service would prefer cephadm take.
        """
        return next_action_for_mgmt_stack_service(
            scheduled_action, daemon_type, spec, curr_deps, last_deps
        )
