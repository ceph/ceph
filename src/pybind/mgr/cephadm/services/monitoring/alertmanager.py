from typing import TYPE_CHECKING
import errno
from typing import List, Any, Tuple, Dict, Optional, cast
from mgr_module import HandleCommandResult
from ..service_registry import register_cephadm_service
from cephadm.tlsobject_types import TLSCredentials
from orchestrator import DaemonDescription
from ceph.deployment.service_spec import AlertManagerSpec, ServiceSpec
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec, CephadmService, DaemonDeployContext, get_dashboard_urls, next_action_for_mgmt_stack_service
from mgr_util import build_url, password_hash
from ... import utils
import logging

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from ...module import CephadmOrchestrator


@register_cephadm_service
class AlertmanagerService(CephadmService):
    TYPE = 'alertmanager'
    DEFAULT_SERVICE_PORT = 9093
    USER_CFG_KEY = 'alertmanager/web_user'
    PASS_CFG_KEY = 'alertmanager/web_password'

    @property
    def needs_monitoring(self) -> bool:
        return True

    def get_alertmanager_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> TLSCredentials:
        host_ips = [self.mgr.inventory.get_addr(daemon_spec.host)]
        host_fqdns = [self.mgr.get_fqdn(daemon_spec.host), 'alertmanager_servers']
        return self.get_certificates(daemon_spec, ips=host_ips, fqdns=host_fqdns)

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        deps = []
        deps.append(f'secure_monitoring_stack:{mgr.secure_monitoring_stack}')
        deps += mgr.cache.get_daemons_by_types(['alertmanager', 'snmp-gateway', 'mgmt-gateway', 'oauth2-proxy'])
        security_enabled, mgmt_gw_enabled, _ = mgr._get_security_config()
        if security_enabled:
            alertmanager_user, alertmanager_password = mgr._get_alertmanager_credentials()
            if alertmanager_user and alertmanager_password:
                alertmgr_cred_hash = f'cred:{utils.config_hash(alertmanager_user + alertmanager_password)}'
                deps.append(alertmgr_cred_hash)

        if not mgmt_gw_enabled:
            deps += mgr.cache.get_daemons_by_types(['mgr'])

        return sorted(deps)

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type
        webhook_urls: List[str] = []

        spec = cast(AlertManagerSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        try:
            secure = spec.secure
        except AttributeError:
            secure = False
        user_data = spec.user_data
        if 'default_webhook_urls' in user_data and isinstance(
                user_data['default_webhook_urls'], list):
            webhook_urls.extend(user_data['default_webhook_urls'])
        if 'webhook_urls' in user_data and isinstance(
                user_data['webhook_urls'], list):
            webhook_urls.extend(user_data['webhook_urls'])

        security_enabled, mgmt_gw_enabled, oauth2_enabled = self.mgr._get_security_config()
        if mgmt_gw_enabled:
            dashboard_urls = [f'{self.mgr.get_mgmt_gw_internal_endpoint()}/dashboard']
        else:
            dashboard_urls = get_dashboard_urls(self)

        snmp_gateway_urls: List[str] = []
        for dd in self.mgr.cache.get_daemons_by_service('snmp-gateway'):
            assert dd.hostname is not None
            assert dd.ports
            addr = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)
            snmp_gateway_urls.append(build_url(scheme='http', host=addr,
                                     port=dd.ports[0], path='/alerts'))

        context = {
            'enable_mtls': mgmt_gw_enabled,
            'dashboard_urls': dashboard_urls,
            'webhook_urls': webhook_urls,
            'snmp_gateway_urls': snmp_gateway_urls,
            'secure': secure,
        }
        yml = self.mgr.template.render('services/alertmanager/alertmanager.yml.j2', context)

        peers = []
        port = 9094
        for dd in self.mgr.cache.get_daemons_by_service('alertmanager'):
            assert dd.hostname is not None
            addr = self.mgr.get_fqdn(dd.hostname)
            peers.append(build_url(host=addr, port=port).lstrip('/'))

        ip_to_bind_to = ''
        if spec.only_bind_port_on_networks and spec.networks:
            assert daemon_spec.host is not None
            ip_to_bind_to = self.mgr.get_first_matching_network_ip(daemon_spec.host, spec) or ''
            if ip_to_bind_to:
                daemon_spec.port_ips = {str(port): ip_to_bind_to}

        deps = self.get_dependencies(self.mgr, spec, daemon_spec.daemon_type)
        if security_enabled:
            alertmanager_user, alertmanager_password = self.mgr._get_alertmanager_credentials()
            tls_pair = self.get_alertmanager_certificates(daemon_spec)
            context = {
                'enable_mtls': mgmt_gw_enabled,
                'enable_basic_auth': not oauth2_enabled,
                'alertmanager_web_user': alertmanager_user,
                'alertmanager_web_password': password_hash(alertmanager_password),
            }
            return {
                "files": {
                    "alertmanager.yml": yml,
                    'alertmanager.crt': tls_pair.cert,
                    'alertmanager.key': tls_pair.key,
                    'web.yml': self.mgr.template.render('services/alertmanager/web.yml.j2', context),
                    'root_cert.pem': self.mgr.cert_mgr.get_root_ca()
                },
                'peers': peers,
                'web_config': '/etc/alertmanager/web.yml',
                'use_url_prefix': mgmt_gw_enabled,
                'ip_to_bind_to': ip_to_bind_to
            }, deps
        else:
            return {
                "files": {
                    "alertmanager.yml": yml
                },
                "peers": peers,
                'use_url_prefix': mgmt_gw_enabled,
                'ip_to_bind_to': ip_to_bind_to
            }, deps

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

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before Alertmanager is removed
        """
        if daemon.hostname is None:
            return
        try:
            current_api_host = self.mgr.check_mon_command({"prefix": "dashboard get-alertmanager-api-host"}).stdout.strip()
            daemon_addr = daemon.ip if daemon.ip else self.mgr.get_fqdn(daemon.hostname)
            daemon_port = daemon.ports[0] if daemon.ports else self.DEFAULT_SERVICE_PORT
            service_url = build_url(scheme='http', host=daemon_addr, port=daemon_port)

            if current_api_host == service_url:
                # This is the active daemon, update or reset the settings
                remaining_daemons = [
                    d for d in self.mgr.cache.get_daemons_by_service(self.TYPE)
                    if d.name() != daemon.name()
                ]
                if remaining_daemons:
                    self.config_dashboard(remaining_daemons)
                    logger.info("Updated dashboard API settings to point to a remaining Alertmanager daemon")
                else:
                    self.mgr.check_mon_command({"prefix": "dashboard reset-alertmanager-api-host"})
                    self.mgr.check_mon_command({"prefix": "dashboard reset-alertmanager-api-ssl-verify"})
                    logger.info("Reset dashboard API settings as no Alertmnager daemons are remaining")
            else:
                logger.info(f"Alertmanager {daemon.name()} removed; no changes to dashboard API settings")
        except Exception as e:
            logger.error(f"Error in Alertmanager pre_remove: {str(e)}")

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Alertmanager', 1)
        if warn and not force:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)
        return HandleCommandResult(0, warn_message, '')

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
