import logging
from typing import List, Any, Tuple, Dict, Optional, cast

from mgr_util import build_url
from orchestrator import DaemonDescription
from ceph.deployment.service_spec import OAuth2ProxySpec, MgmtGatewaySpec
from cephadm.services.cephadmservice import CephadmService, CephadmDaemonDeploySpec
from cephadm.ssl_cert_utils import SSLCerts

logger = logging.getLogger(__name__)


class OAuth2ProxyService(CephadmService):
    TYPE = 'oauth2-proxy'
    SVC_TEMPLATE_PATH = 'services/oauth2-proxy/oauth2-proxy.conf.j2'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        return daemon_spec

    def get_redirect_url(self) -> Optional[str]:
        # TODO(redo): check how can we create several servers for HA
        admin_gateway_proxy_eps = self.get_service_endpoints('mgmt-gateway')
        spec = cast(MgmtGatewaySpec, self.mgr.spec_store['mgmt-gateway'].spec)
        protocol = 'http' if spec.disable_https else 'https'
        return f'{protocol}://{admin_gateway_proxy_eps[0]}' if admin_gateway_proxy_eps else None

    def get_service_endpoints(self, service_name: str) -> List[str]:
        srv_entries = []
        for dd in self.mgr.cache.get_daemons_by_service(service_name):
            assert dd.hostname is not None
            addr = dd.ip if dd.ip else self.mgr.inventory.get_addr(dd.hostname)
            port = dd.ports[0] if dd.ports else None
            ep = f'{addr}:{port}' if port is not None else f'{addr}'
            srv_entries.append(ep)
        return srv_entries

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        if daemon_descrs:
            return daemon_descrs[0]
        # if empty list provided, return empty Daemon Desc
        return DaemonDescription()

    def get_certificates(self, svc_spec: OAuth2ProxySpec, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[str, str, str, str]:

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

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        # TODO: get_certificates
        svc_spec = cast(OAuth2ProxySpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        context = {
            'spec': svc_spec,
            'redirect_url': self.get_redirect_url(),
        }

        internal_cert, internal_pkey, cert, pkey = self.get_certificates(svc_spec, daemon_spec)
        daemon_config = {
            "files": {
                "oauth2-proxy.conf": self.mgr.template.render(self.SVC_TEMPLATE_PATH, context),
            }
        }

        return daemon_config, []
