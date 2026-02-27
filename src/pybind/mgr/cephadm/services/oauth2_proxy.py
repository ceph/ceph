import logging
from typing import List, Any, Tuple, Dict, cast, Optional, TYPE_CHECKING
from copy import copy

from orchestrator import DaemonDescription
from ceph.deployment.service_spec import OAuth2ProxySpec, MgmtGatewaySpec, ServiceSpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec
from .service_registry import register_cephadm_service

if TYPE_CHECKING:
    from ..module import CephadmOrchestrator

logger = logging.getLogger(__name__)


@register_cephadm_service
class OAuth2ProxyService(CephadmService):
    TYPE = 'oauth2-proxy'
    SVC_TEMPLATE_PATH = 'services/oauth2-proxy/oauth2-proxy.conf.j2'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        super().prepare_create(daemon_spec)
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    @classmethod
    def get_dependencies(cls, mgr: "CephadmOrchestrator",
                         spec: Optional[ServiceSpec] = None,
                         daemon_type: Optional[str] = None) -> List[str]:
        # adding dependency as redirect_url calculation depends on the mgmt-gateway
        deps = [
            f'{d.name()}:{d.ports[0]}' if d.ports else d.name()
            for service in ['mgmt-gateway']
            for d in mgr.cache.get_daemons_by_service(service)
        ]
        return deps

    def get_service_ips_and_hosts(self, service_name: str) -> List[str]:
        entries = set()
        if 'mgmt-gateway' in self.mgr.spec_store:
            mgmt_gw_spec = cast(MgmtGatewaySpec, self.mgr.spec_store['mgmt-gateway'].spec)
            if mgmt_gw_spec.virtual_ip is not None:
                entries.add(mgmt_gw_spec.virtual_ip)
        for dd in self.mgr.cache.get_daemons_by_service(service_name):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            entries.add(dd.hostname)
            entries.add(addr)
        return sorted(list(entries))

    def get_redirect_url(self) -> Optional[str]:
        external_endpoint = self.mgr.get_mgmt_gw_external_endpoint()
        return f"{external_endpoint}/oauth2/callback" if external_endpoint else None

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        if daemon_descrs:
            return daemon_descrs[0]
        # if empty list provided, return empty Daemon Desc
        return DaemonDescription()

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        svc_spec = cast(OAuth2ProxySpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        allowlist_domains = copy(svc_spec.allowlist_domains) or []
        allowlist_domains += self.get_service_ips_and_hosts('mgmt-gateway')
        context = {
            'spec': svc_spec,
            'cookie_secret': svc_spec.cookie_secret,
            'allowlist_domains': allowlist_domains,
            'redirect_url': svc_spec.redirect_url or self.get_redirect_url()
        }

        tls_pair = self.get_certificates(daemon_spec)
        daemon_config = {
            "files": {
                "oauth2-proxy.conf": self.mgr.template.render(self.SVC_TEMPLATE_PATH, context),
                "oauth2-proxy.crt": tls_pair.cert,
                "oauth2-proxy.key": tls_pair.key,
            }
        }

        return daemon_config, sorted(OAuth2ProxyService.get_dependencies(self.mgr))
