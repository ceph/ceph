import json
import logging
import socket
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
import ipaddress
from urllib.parse import urlparse
from typing import TYPE_CHECKING, List, Callable, TypeVar, \
    Optional, Dict, Any, Tuple, NewType, cast

from mgr_module import HandleCommandResult, MonCommandFailed

from ceph.deployment.service_spec import ArgumentList, GeneralArgList, InitContainerSpec, ServiceSpec, CertificateSource, RequiresCertificatesEntry
from ceph.deployment.utils import wrap_ipv6
from mgr_util import build_url
from orchestrator import (
    OrchestratorError,
    DaemonDescription,
    DaemonDescriptionStatus,
    HostSpec
)
from orchestrator._interface import daemon_type_to_service
from cephadm import utils
from cephadm.tlsobject_types import TLSObjectScope, TLSCredentials, EMPTY_TLS_CREDENTIALS
from ceph.deployment.tls_utils import (
    extract_ips_and_fqdns_from_cert,
    parse_tls_pem_bundle,
    contains_private_key,
    contains_multiple_pem_blocks,
    SSLConfigException,
)

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)

ServiceSpecs = TypeVar('ServiceSpecs', bound=ServiceSpec)
AuthEntity = NewType('AuthEntity', str)

# the release that added 'profile rgw'
RGW_PROFILE_RELEASE = utils.ceph_release_to_major('umbrella')


def get_auth_entity(daemon_type: str, daemon_id: str, host: str = "") -> AuthEntity:
    """
    Map the daemon id to a cephx keyring entity name
    """
    # despite this mapping entity names to daemons, self.TYPE within
    # the CephService class refers to service types, not daemon types
    if daemon_type in ['rgw', 'rbd-mirror', 'cephfs-mirror', "iscsi", 'nvmeof', 'ingress', 'ceph-exporter']:
        return AuthEntity(f'client.{daemon_type}.{daemon_id}')
    elif daemon_type in ['crash', 'agent', 'node-proxy']:
        if host == "":
            raise OrchestratorError(
                f'Host not provided to generate <{daemon_type}> auth entity name')
        return AuthEntity(f'client.{daemon_type}.{host}')
    elif daemon_type == 'mon':
        return AuthEntity('mon.')
    elif daemon_type in ['mgr', 'osd', 'mds']:
        return AuthEntity(f'{daemon_type}.{daemon_id}')
    else:
        raise OrchestratorError(f"unknown daemon type {daemon_type}")


def simplified_keyring(entity: str, contents: str) -> str:
    # strip down keyring
    #  - don't include caps (auth get includes them; get-or-create does not)
    #  - use pending key if present
    key = None
    for line in contents.splitlines():
        if ' = ' not in line:
            continue
        line = line.strip()
        (ls, rs) = line.split(' = ', 1)
        if ls == 'key' and not key:
            key = rs
        if ls == 'pending key':
            key = rs
    keyring = f'[{entity}]\nkey = {key}\n'
    return keyring


def _get_dashboard_server_addr(svc: 'CephadmService', daemon_id: str, hostname: str) -> str:
    """Get the address where the dashboard listens for a given mgr daemon.

    Reads the dashboard module's server_addr config (per-daemon first, then
    global fallback via the localized option lookup). If the address is a
    wildcard (``::`` or ``0.0.0.0``) or not configured, returns the host's
    FQDN from the cephadm inventory.
    """
    server_addr = svc.mgr._ceph_get_module_option('dashboard', 'server_addr', daemon_id)
    if server_addr and str(server_addr) not in ('::', '0.0.0.0'):
        return str(server_addr)
    return svc.mgr.get_fqdn(hostname)


def get_dashboard_endpoints(svc: 'CephadmService') -> Tuple[List[str], Optional[str]]:
    dashboard_endpoints: List[str] = []
    port = None
    protocol = None
    mgr_map = svc.mgr.get('mgr_map')
    url = mgr_map.get('services', {}).get('dashboard', None)
    if url:
        p_result = urlparse(url.rstrip('/'))
        protocol = p_result.scheme
        port = p_result.port
        for dd in svc.mgr.cache.get_daemons_by_service('mgr'):
            if not port:
                continue
            assert dd.hostname is not None
            assert dd.daemon_id is not None
            server_addr = svc.mgr._ceph_get_module_option('dashboard', 'server_addr', dd.daemon_id)
            if server_addr and str(server_addr) not in ('::', '0.0.0.0'):
                addr = str(server_addr)
            else:
                # Use daemon/inventory IPs (same as other mgmt-gateway upstreams),
                # not FQDNs, so nginx can reach the dashboard on the Ceph network
                # when DNS resolves elsewhere. IPv6 literals are bracketed.
                addr = dd.ip if dd.ip else svc.mgr.inventory.get_addr(dd.hostname)
            dashboard_endpoints.append(f'{wrap_ipv6(addr)}:{port}')

    return dashboard_endpoints, protocol


def get_dashboard_urls(svc: 'CephadmService') -> List[str]:
    # dashboard(s)
    dashboard_urls: List[str] = []
    mgr_map = svc.mgr.get('mgr_map')
    port = None
    proto = None  # http: or https:
    url = mgr_map.get('services', {}).get('dashboard', None)
    if url:
        p_result = urlparse(url.rstrip('/'))
        server_addr = svc.mgr._ceph_get_module_option('dashboard', 'server_addr', svc.mgr.get_mgr_id())
        if server_addr and str(server_addr) not in ('::', '0.0.0.0'):
            hostname = str(server_addr)
        else:
            hostname = socket.getfqdn(p_result.hostname)
        try:
            ip = ipaddress.ip_address(hostname)
        except ValueError:
            pass
        else:
            if ip.version == 6:
                hostname = f'[{hostname}]'
        dashboard_urls.append(f'{p_result.scheme}://{hostname}:{p_result.port}{p_result.path}')
        proto = p_result.scheme
        port = p_result.port

    # assume that they are all dashboards on the same port as the active mgr.
    for dd in svc.mgr.cache.get_daemons_by_service('mgr'):
        if not port:
            continue
        if dd.daemon_id == svc.mgr.get_mgr_id():
            continue
        assert dd.hostname is not None
        assert dd.daemon_id is not None
        addr = _get_dashboard_server_addr(svc, dd.daemon_id, dd.hostname)
        dashboard_urls.append(build_url(scheme=proto, host=addr, port=port).rstrip('/'))

    return dashboard_urls


class CephadmDaemonDeploySpec:
    # typing.NamedTuple + Generic is broken in py36
    def __init__(self, host: str, daemon_id: str,
                 service_name: str,
                 network: Optional[str] = None,
                 keyring: Optional[str] = None,
                 extra_args: Optional[List[str]] = None,
                 ceph_conf: str = '',
                 extra_files: Optional[Dict[str, Any]] = None,
                 daemon_type: Optional[str] = None,
                 ip: Optional[str] = None,
                 ports: Optional[List[int]] = None,
                 port_ips: Optional[Dict[str, str]] = None,
                 rank: Optional[int] = None,
                 rank_generation: Optional[int] = None,
                 extra_container_args: Optional[ArgumentList] = None,
                 extra_entrypoint_args: Optional[ArgumentList] = None,
                 init_containers: Optional[List[InitContainerSpec]] = None,
                 ):
        """
        A data struction to encapsulate `cephadm deploy ...
        """
        self.host: str = host
        self.daemon_id = daemon_id
        self.service_name = service_name
        daemon_type = daemon_type or (service_name.split('.')[0])
        assert daemon_type is not None
        self.daemon_type: str = daemon_type

        # mons
        self.network = network

        # for run_cephadm.
        self.keyring: Optional[str] = keyring

        # FIXME: finish removing this
        # For run_cephadm. Would be great to have more expressive names.
        # self.extra_args: List[str] = extra_args or []
        assert not extra_args

        self.ceph_conf = ceph_conf
        self.extra_files = extra_files or {}

        # TCP ports used by the daemon
        self.ports: List[int] = ports or []
        # mapping of ports to IP addresses for ports
        # we know we will only bind to on a specific IP.
        # Useful for allowing multiple daemons to bind
        # to the same port on different IPs on the same node
        self.port_ips: Dict[str, str] = port_ips or {}
        self.ip: Optional[str] = ip

        # values to be populated during generate_config calls
        # and then used in _run_cephadm
        self.final_config: Dict[str, Any] = {}
        self.deps: List[str] = []

        self.rank: Optional[int] = rank
        self.rank_generation: Optional[int] = rank_generation

        self.extra_container_args = extra_container_args
        self.extra_entrypoint_args = extra_entrypoint_args
        self.init_containers = init_containers

    def __setattr__(self, name: str, value: Any) -> None:
        if value is not None and name in ('extra_container_args', 'extra_entrypoint_args'):
            for v in value:
                tname = str(type(v))
                if 'ArgumentSpec' not in tname:
                    raise TypeError(f"{name} is not all ArgumentSpec values: {v!r}(is {type(v)} in {value!r}")

        super().__setattr__(name, value)

    def name(self) -> str:
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def entity_name(self) -> str:
        return get_auth_entity(self.daemon_type, self.daemon_id, host=self.host)

    def config_get_files(self) -> Dict[str, Any]:
        files = self.extra_files
        if self.ceph_conf:
            files['config'] = self.ceph_conf

        return files

    @staticmethod
    def from_daemon_description(dd: DaemonDescription) -> 'CephadmDaemonDeploySpec':
        assert dd.hostname
        assert dd.daemon_id
        assert dd.daemon_type
        return CephadmDaemonDeploySpec(
            host=dd.hostname,
            daemon_id=dd.daemon_id,
            daemon_type=dd.daemon_type,
            service_name=dd.service_name(),
            ip=dd.ip,
            ports=dd.ports,
            rank=dd.rank,
            rank_generation=dd.rank_generation,
            extra_container_args=dd.extra_container_args,
            extra_entrypoint_args=dd.extra_entrypoint_args,
        )

    def to_daemon_description(self, status: DaemonDescriptionStatus, status_desc: str) -> DaemonDescription:
        return DaemonDescription(
            daemon_type=self.daemon_type,
            daemon_id=self.daemon_id,
            service_name=self.service_name,
            hostname=self.host,
            status=status,
            status_desc=status_desc,
            ip=self.ip,
            ports=self.ports,
            rank=self.rank,
            rank_generation=self.rank_generation,
            extra_container_args=cast(GeneralArgList, self.extra_container_args),
            extra_entrypoint_args=cast(GeneralArgList, self.extra_entrypoint_args),
        )

    @property
    def extra_args(self) -> List[str]:
        return []


@dataclass
class DaemonDeployContext:
    daemon_spec: CephadmDaemonDeploySpec
    service_spec: Optional[ServiceSpec] = None


class CephadmService(metaclass=ABCMeta):
    """
    Base class for service types. Often providing a create() and config() fn.
    """

    @property
    def needs_monitoring(self) -> bool:
        return False

    @property
    def requires_certificates(self) -> bool:
        return self.TYPE in ServiceSpec.REQUIRES_CERTIFICATES

    @property
    def allows_user_certificates(self) -> bool:
        config = ServiceSpec.REQUIRES_CERTIFICATES.get(self.TYPE)
        return config is not None and bool(config.get("user_cert_allowed", False))

    @property
    def SCOPE(self) -> TLSObjectScope:
        entry: Optional[RequiresCertificatesEntry] = ServiceSpec.REQUIRES_CERTIFICATES.get(self.TYPE)
        if not entry:
            return TLSObjectScope.UNKNOWN
        return TLSObjectScope(entry['scope'])

    @property
    def requires_ca_cert(self) -> bool:
        config = ServiceSpec.REQUIRES_CERTIFICATES.get(self.TYPE)
        return config is not None and bool(config.get("requires_ca_cert", False))

    @property
    def cert_name(self) -> str:
        return f"{self.TYPE.replace('-', '_')}_ssl_cert"

    @property
    def key_name(self) -> str:
        return f"{self.TYPE.replace('-', '_')}_ssl_key"

    @property
    def ca_cert_name(self) -> Optional[str]:
        if self.requires_ca_cert:
            return f"{self.TYPE.replace('-', '_')}_ssl_ca_cert"
        return None

    @property
    @abstractmethod
    def TYPE(self) -> str:
        pass

    @classmethod
    def _include_tls_dependencies(
        cls,
        spec: Optional[ServiceSpec],
        daemon_type: Optional[str] = None,
    ) -> bool:
        return bool(spec and getattr(spec, 'ssl', False))

    @classmethod
    def get_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        """Return the complete dependency set.

        This is the public entry point for dependency calculation. It combines
        common dependencies with the service-specific dependencies returned by
        ``_get_service_dependencies()``.
        """
        deps = cls._get_service_dependencies(mgr, spec, daemon_type)

        if spec is None:
            return sorted(deps)

        if not cls._include_tls_dependencies(spec, daemon_type):
            return sorted(deps)

        cert_source = getattr(spec, 'certificate_source', None)
        if cert_source:
            deps.append(f'certificate_source: {cert_source}')
        if spec.ssl_cert and spec.ssl_key:
            deps.append(f'ssl_cert: {str(utils.config_hash(spec.ssl_cert))}')
            deps.append(f'ssl_key: {str(utils.config_hash(spec.ssl_key))}')
        if spec.ssl_ca_cert:
            deps.append(f'ssl_ca_cert: {str(utils.config_hash(spec.ssl_ca_cert))}')

        return sorted(deps)

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        """Return service-specific dependencies.

        Services should override this hook rather than ``get_dependencies`` so
        common dependencies are always included.
        """
        return []

    @classmethod
    def sorted_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        """A version of get_dependencies that guarantees that the returned
        list is in sorted order.
        """
        return sorted(cls.get_dependencies(mgr, spec, daemon_type))

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr

    def get_self_signed_certificates_with_label(self, svc_spec: ServiceSpec, daemon_spec: CephadmDaemonDeploySpec, label: str, ip_addr: Optional[str] = None) -> TLSCredentials:
        svc_name = svc_spec.service_name()
        ip = ip_addr or self.mgr.inventory.get_addr(daemon_spec.host)
        host_fqdn = self.mgr.get_fqdn(daemon_spec.host)
        tls_creds = self.mgr.cert_mgr.get_self_signed_tls_credentials(svc_name, daemon_spec.host, label)
        if not tls_creds:
            tls_creds = self.mgr.cert_mgr.generate_cert(host_fqdn, ip)
            self.mgr.cert_mgr.save_self_signed_cert_key_pair(svc_name, tls_creds, host=daemon_spec.host, label=label)
        return tls_creds

    def _get_certificates_from_spec_ssl_certificates(
        self,
        svc_spec: ServiceSpec,
        daemon_spec: CephadmDaemonDeploySpec,
        feature: Optional[str] = None
    ) -> TLSCredentials:
        return EMPTY_TLS_CREDENTIALS

    def get_certificates(self,
                         daemon_spec: CephadmDaemonDeploySpec,
                         ips: List[str] = [],
                         fqdns: List[str] = [],
                         custom_sans: List[str] = [],
                         ca_cert_required: bool = False,
                         feature: Optional[str] = None
                         ) -> TLSCredentials:

        svc_spec = cast(ServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        if not self.requires_certificates or not svc_spec.ssl:
            return EMPTY_TLS_CREDENTIALS

        return self.get_certificates_generic(
            svc_spec=svc_spec,
            daemon_spec=daemon_spec,
            cert_source_attr='certificate_source',
            cert_attr='ssl_cert',
            cert_name=self.cert_name,
            key_attr='ssl_key',
            key_name=self.key_name,
            ca_cert_attr='ssl_ca_cert' if ca_cert_required else None,
            ca_cert_name=self.ca_cert_name if ca_cert_required else None,
            ips=ips,
            fqdns=fqdns,
            custom_sans=custom_sans,
            feature=feature,
        )

    def get_certificates_generic(
        self,
        svc_spec: ServiceSpec,
        daemon_spec: CephadmDaemonDeploySpec,
        cert_source_attr: str,
        cert_attr: str,
        cert_name: str,
        key_attr: str,
        key_name: str,
        ca_cert_attr: Optional[str] = None,
        ca_cert_name: Optional[str] = None,
        custom_sans: Optional[List[str]] = None,
        ips: Optional[List[str]] = None,
        fqdns: Optional[List[str]] = None,
        feature: Optional[str] = None,
    ) -> TLSCredentials:

        ips = ips or [self.mgr.inventory.get_addr(daemon_spec.host)]
        fqdns = fqdns or [self.mgr.get_fqdn(daemon_spec.host)]
        custom_sans = custom_sans or []

        cert_source = getattr(svc_spec, cert_source_attr, None)
        logger.debug(f'Getting certificate for {svc_spec.service_name()} using source: {cert_source}')

        if feature is not None:
            return self._get_certificates_from_spec_ssl_certificates(svc_spec, daemon_spec, feature)
        elif cert_source == CertificateSource.INLINE.value:
            return self._get_certificates_from_spec(svc_spec, daemon_spec, cert_attr, key_attr, cert_name, key_name, ca_cert_attr, ca_cert_name)
        elif cert_source == CertificateSource.REFERENCE.value:
            return self._get_certificates_from_certmgr_store(svc_spec, fqdns, cert_name, key_name, ca_cert_name)
        elif cert_source == CertificateSource.CEPHADM_SIGNED.value:
            return self._get_cephadm_signed_certificates(svc_spec, daemon_spec, ips, fqdns, custom_sans)
        else:
            logger.error(f'Invalid cert_source: {cert_source}')
            return EMPTY_TLS_CREDENTIALS

    def _get_certificates_from_spec(
        self,
        svc_spec: ServiceSpec,
        daemon_spec: CephadmDaemonDeploySpec,
        cert_attr: str,
        key_attr: str,
        cert_name: str,
        key_name: str,
        ca_cert_attr: Optional[str] = None,
        ca_cert_name: Optional[str] = None
    ) -> TLSCredentials:
        """
        Fetch and persist the TLS certificate and key for a service spec.

        Supports fullchain PEM input: when the ``cert_attr`` field on the spec
        contains a combined PEM blob (private key + one or more certificate
        blocks), the key is automatically extracted and stored separately so
        that downstream consumers always receive clean cert-only and key-only
        values.  The ``key_attr`` field must be empty when a fullchain PEM is
        used; an error is raised if both are provided simultaneously.

        Returns:
            A TLSCredentials if both are available; otherwise EMPTY_TLS_CREDENTIALS.
        """
        cert = getattr(svc_spec, cert_attr, None)
        key = getattr(svc_spec, key_attr, None)
        needs_ca = bool(ca_cert_attr and ca_cert_name)
        ca_cert: Optional[str] = None
        if needs_ca:
            assert ca_cert_attr
            ca_cert = getattr(svc_spec, ca_cert_attr, None)

        service_name = svc_spec.service_name()
        host = daemon_spec.host

        # --- Fullchain PEM auto-detection ------------------------------------
        # Enterprise CAs often output key + chain as a single blob.  When
        # the cert field embeds a private key we split it here so the rest of
        # the stack remains unaware of multi-block PEM input. A cert blob with
        # multiple CERTIFICATE blocks but no embedded key (e.g. leaf +
        # intermediates) is also normalised here, even if a separate key
        # field is set.
        if cert and contains_private_key(cert) and key:
            logger.error(
                f"Service '{service_name}': '{cert_attr}' is a fullchain PEM "
                f"(contains an embedded private key) but '{key_attr}' is also "
                f"set. Please supply a fullchain PEM without a separate key, "
                f"or a plain certificate PEM with the key field."
            )
            return EMPTY_TLS_CREDENTIALS
        if cert and (contains_private_key(cert) or contains_multiple_pem_blocks(cert)):
            try:
                cert, split_key = parse_tls_pem_bundle(cert)
                logger.debug(
                    "certmgr: split fullchain PEM from spec for service '%s' "
                    "(cert_attr=%s)", service_name, cert_attr
                )
            except SSLConfigException as exc:
                logger.error(
                    f"Service '{service_name}': failed to parse fullchain PEM "
                    f"from '{cert_attr}': {exc}"
                )
                return EMPTY_TLS_CREDENTIALS
            if split_key:
                key = split_key
        # ---------------------------------------------------------------------

        missing = []
        if not cert:
            missing.append(cert_attr)
        if not key:
            missing.append(key_attr)
        if needs_ca and not ca_cert:
            assert ca_cert_attr
            missing.append(ca_cert_attr)
        if missing:
            logger.error(
                f"Cannot get required TLS fields {', '.join(missing)} for service "
                f"'{service_name}' (cert/key names: {cert_name}/{key_name})"
            )
            return EMPTY_TLS_CREDENTIALS
        # Save TLS credentials
        if needs_ca:
            assert ca_cert_name and ca_cert
            self.mgr.cert_mgr.save_cert(ca_cert_name, ca_cert, service_name, host, user_made=True)
        assert cert and key
        self.mgr.cert_mgr.save_cert(cert_name, cert, service_name, host, user_made=True)
        self.mgr.cert_mgr.save_key(key_name, key, service_name, host, user_made=True)
        return TLSCredentials(cert=cert, key=key, ca_cert=ca_cert)

    def _get_certificates_from_certmgr_store(
        self,
        svc_spec: ServiceSpec,
        fqdns: List[str],
        cert_name: str,
        key_name: str,
        ca_cert_name: Optional[str] = None
    ) -> TLSCredentials:
        host = fqdns[0] if fqdns else None
        cert = self.mgr.cert_mgr.get_cert(cert_name, svc_spec.service_name(), host)
        key = self.mgr.cert_mgr.get_key(key_name, svc_spec.service_name(), host)
        ca_cert = self.mgr.cert_mgr.get_cert(ca_cert_name, svc_spec.service_name(), host) if ca_cert_name else ''
        if cert and key:
            if ca_cert_name and not ca_cert:
                logger.error(f'Failed to get CA cert {ca_cert_name} for service {svc_spec.service_name()} host: {host} from the certmgr store.')
                return EMPTY_TLS_CREDENTIALS
            return TLSCredentials(cert=cert, key=key, ca_cert=ca_cert)
        else:
            logger.error(f'Failed to get cert/key {cert_name} for service {svc_spec.service_name()} host: {host} from the certmgr store.')
            return EMPTY_TLS_CREDENTIALS

    def _get_cephadm_signed_certificates(
        self,
        svc_spec: ServiceSpec,
        daemon_spec: CephadmDaemonDeploySpec,
        ips: List[str],
        fqdns: List[str],
        custom_sans: List[str],
    ) -> TLSCredentials:

        custom_sans = custom_sans or svc_spec.custom_sans or []
        ips = ips or [self.mgr.inventory.get_addr(daemon_spec.host)]
        fqdns = fqdns or [self.mgr.get_fqdn(daemon_spec.host)]
        tls_creds = self.mgr.cert_mgr.get_self_signed_tls_credentials(svc_spec.service_name(), daemon_spec.host)
        if tls_creds:
            combined_fqdns = sorted(set(s.lower() for s in fqdns + custom_sans))
            cert_ips, cert_fqdns = extract_ips_and_fqdns_from_cert(tls_creds.cert)
            if sorted(cert_ips) == sorted(ips) and sorted(cert_fqdns) == sorted(combined_fqdns):
                # Nothing has changed, use the stored certifiactes
                return tls_creds

        # Either there were not certs or ips/fqdns have changed generate new cets
        tls_creds = self.mgr.cert_mgr.generate_cert(fqdns, ips, custom_sans)
        self.mgr.cert_mgr.save_self_signed_cert_key_pair(svc_spec.service_name(), tls_creds, host=daemon_spec.host)

        return tls_creds

    def allow_colo(self) -> bool:
        """
        Return True if multiple daemons of the same type can colocate on
        the same host.
        """
        return False

    def primary_daemon_type(self, spec: Optional[ServiceSpec] = None) -> str:
        """
        This is the type of the primary (usually only) daemon to be deployed.
        """
        return self.TYPE

    def per_host_daemon_type(self, spec: Optional[ServiceSpec] = None) -> Optional[str]:
        """
        If defined, this type of daemon will be deployed once for each host
        containing one or more daemons of the primary type.
        """
        return None

    def ranked(self, spec: ServiceSpec) -> bool:
        """
        If True, we will assign a stable rank (0, 1, ...) and monotonically increasing
        generation (0, 1, ...) to each daemon we create/deploy.
        """
        return False

    def fence_old_ranks(self,
                        spec: ServiceSpec,
                        rank_map: Dict[int, Dict[int, Optional[str]]],
                        num_ranks: int) -> None:
        assert False

    def make_daemon_spec(
            self,
            host: str,
            daemon_id: str,
            network: str,
            spec: ServiceSpecs,
            daemon_type: Optional[str] = None,
            ports: Optional[List[int]] = None,
            ip: Optional[str] = None,
            rank: Optional[int] = None,
            rank_generation: Optional[int] = None,
    ) -> CephadmDaemonDeploySpec:
        return CephadmDaemonDeploySpec(
            host=host,
            daemon_id=daemon_id,
            service_name=spec.service_name(),
            network=network,
            daemon_type=daemon_type,
            ports=ports,
            ip=ip,
            rank=rank,
            rank_generation=rank_generation,
            extra_container_args=spec.extra_container_args if hasattr(
                spec, 'extra_container_args') else None,
            extra_entrypoint_args=spec.extra_entrypoint_args if hasattr(
                spec, 'extra_entrypoint_args') else None,
            init_containers=getattr(spec, 'init_containers', None),
        )

    def register_for_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> None:
        if not self.requires_certificates:
            return
        spec = self.mgr.spec_store[daemon_spec.service_name].spec
        if spec.is_using_certificates_source(CertificateSource.CEPHADM_SIGNED):
            self.mgr.cert_mgr.register_self_signed_cert_key_pair(spec.service_name())

    def prepare_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> None:
        self.register_for_certificates(daemon_spec)
        self.reconcile_certificates(daemon_spec)

    def reconcile_certificates(self, daemon_spec: CephadmDaemonDeploySpec) -> None:
        """Garbage-collect stale TLS objects when the certificate source has changed."""
        if not self.requires_certificates:
            return
        spec = self.mgr.spec_store[daemon_spec.service_name].spec
        cert_source = getattr(spec, 'certificate_source', None)
        svc_name = spec.service_name()
        host = daemon_spec.host

        # Inline-saved certs/keys are persisted in the certmgr store as user_made=True
        # but editable=False. These should be garbage-collected once the service no
        # longer uses INLINE.
        if cert_source in (CertificateSource.REFERENCE.value, CertificateSource.CEPHADM_SIGNED.value):
            self.mgr.cert_mgr.rm_inline_saved_cert_key_pair(
                self.cert_name,
                self.key_name,
                service_name=svc_name,
                host=host,
                ca_cert_name=self.ca_cert_name,
            )

        # Cephadm-signed certs/keys are stored under cephadm-signed_* entities and
        # should be removed when the service no longer uses CEPHADM_SIGNED.
        if cert_source != CertificateSource.CEPHADM_SIGNED.value:
            self.mgr.cert_mgr.try_rm_self_signed_cert_key_pair(svc_name, host)

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        self.prepare_certificates(daemon_spec)
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)
        return daemon_spec

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        raise NotImplementedError()

    def config(self, spec: ServiceSpec) -> None:
        """
        Configure the cluster for this service. Only called *once* per
        service apply. Not for every daemon.
        """
        pass

    def daemon_check_post(self, daemon_descrs: List[DaemonDescription]) -> None:
        """The post actions needed to be done after daemons are checked"""
        if self.mgr.config_dashboard:
            if 'dashboard' in self.mgr.get('mgr_map')['modules']:
                self.config_dashboard(daemon_descrs)
            else:
                logger.debug('Dashboard is not enabled. Skip configuration.')

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        """Config dashboard settings."""
        raise NotImplementedError()

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        # if this is called for a service type where it hasn't explicitly been
        # defined, return empty Daemon Desc
        return DaemonDescription()

    def get_keyring_with_caps(self, entity: AuthEntity, caps: List[str]) -> str:
        # try with newer cipher first, it's possible this isn't supported
        # early in an upgrade
        ret, keyring, err = self.mgr.mon_command({
            'prefix': 'auth get-or-create',
            'entity': entity,
            'caps': caps,
            'key_type': utils.ROTATION_CIPHER
        })
        if err:
            ret, keyring, err = self.mgr.mon_command({
                'prefix': 'auth get-or-create',
                'entity': entity,
                'caps': caps,
            })
        if err:
            ret, out, err = self.mgr.mon_command({
                'prefix': 'auth caps',
                'entity': entity,
                'caps': caps,
            })
            if err:
                self.mgr.log.warning(f"Unable to update caps for {entity}")

            # get keyring anyway
            ret, keyring, err = self.mgr.mon_command({
                'prefix': 'auth get',
                'entity': entity,
            })
            if err:
                raise OrchestratorError(f"Unable to fetch keyring for {entity}: {err}")
        return simplified_keyring(entity, keyring)

    def _set_value_on_dashboard(self,
                                service_name: str,
                                get_mon_cmd: str,
                                set_mon_cmd: str,
                                new_value: str) -> None:
        """A helper to get and set values via Dashboard's MON command.
           If result of get_mon_cmd differs from the new_value, set_mon_cmd will
           be sent to set the service_url.
        """
        def get_set_cmd_dicts(out: str) -> List[dict]:
            cmd_dict = {
                'prefix': set_mon_cmd,
                'value': new_value
            }
            return [cmd_dict] if new_value != out else []

        self._check_and_set_dashboard(
            service_name=service_name,
            get_cmd=get_mon_cmd,
            get_set_cmd_dicts=get_set_cmd_dicts
        )

    def _check_and_set_dashboard(self,
                                 service_name: str,
                                 get_cmd: str,
                                 get_set_cmd_dicts: Callable[[str], List[dict]]) -> None:
        """A helper to set configs in the Dashboard.

        The method is useful for the pattern:
            - Getting a config from Dashboard by using a Dashboard command. e.g. current iSCSI
              gateways.
            - Parse or deserialize previous output. e.g. Dashboard command returns a JSON string.
            - Determine if the config need to be update. NOTE: This step is important because if a
              Dashboard command modified Ceph config, cephadm's config_notify() is called. Which
              kicks the serve() loop and the logic using this method is likely to be called again.
              A config should be updated only when needed.
            - Update a config in Dashboard by using a Dashboard command.

        :param service_name: the service name to be used for logging
        :type service_name: str
        :param get_cmd: Dashboard command prefix to get config. e.g. dashboard get-grafana-api-url
        :type get_cmd: str
        :param get_set_cmd_dicts: function to create a list, and each item is a command dictionary.
            e.g.
            [
                {
                   'prefix': 'dashboard iscsi-gateway-add',
                   'service_url': 'http://admin:admin@aaa:5000',
                   'name': 'aaa'
                },
                {
                    'prefix': 'dashboard iscsi-gateway-add',
                    'service_url': 'http://admin:admin@bbb:5000',
                    'name': 'bbb'
                }
            ]
            The function should return empty list if no command need to be sent.
        :type get_set_cmd_dicts: Callable[[str], List[dict]]
        """

        try:
            _, out, _ = self.mgr.check_mon_command({
                'prefix': get_cmd
            })
        except MonCommandFailed as e:
            logger.warning('Failed to get Dashboard config for %s: %s', service_name, e)
            return
        cmd_dicts = get_set_cmd_dicts(out.strip())
        for cmd_dict in list(cmd_dicts):
            try:
                inbuf = cmd_dict.pop('inbuf', None)
                _, out, _ = self.mgr.check_mon_command(cmd_dict, inbuf)
            except MonCommandFailed as e:
                logger.warning('Failed to set Dashboard config for %s: %s', service_name, e)

    def ok_to_stop_osd(
            self,
            osds: List[str],
            known: Optional[List[str]] = None,  # output argument
            force: bool = False) -> HandleCommandResult:
        r = HandleCommandResult(*self.mgr.mon_command({
            'prefix': "osd ok-to-stop",
            'ids': osds,
            'max': self.mgr.max_parallel_osd_upgrades,
        }))
        j = None
        try:
            j = json.loads(r.stdout)
        except json.decoder.JSONDecodeError:
            self.mgr.log.warning("osd ok-to-stop didn't return structured result")
            raise
        if r.retval:
            return r
        if known is not None and j and j.get('ok_to_stop'):
            self.mgr.log.debug(f"got {j}")
            known.extend([f'osd.{x}' for x in j.get('osds', [])])
        return HandleCommandResult(
            0,
            f'{",".join(["osd.%s" % o for o in osds])} {"is" if len(osds) == 1 else "are"} safe to restart',
            ''
        )

    def ok_to_stop(
            self,
            daemon_ids: List[str],
            force: bool = False,
            known: Optional[List[str]] = None    # output argument
    ) -> HandleCommandResult:
        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        out = f'It appears safe to stop {",".join(names)}'
        err = f'It is NOT safe to stop {",".join(names)} at this time'

        if self.TYPE not in ['mon', 'osd', 'mds']:
            logger.debug(out)
            return HandleCommandResult(0, out)

        if self.TYPE == 'osd':
            return self.ok_to_stop_osd(daemon_ids, known, force)

        r = HandleCommandResult(*self.mgr.mon_command({
            'prefix': f'{self.TYPE} ok-to-stop',
            'ids': daemon_ids,
        }))

        if r.retval:
            err = f'{err}: {r.stderr}' if r.stderr else err
            logger.debug(err)
            return HandleCommandResult(r.retval, r.stdout, err)

        out = f'{out}: {r.stdout}' if r.stdout else out
        logger.debug(out)
        return HandleCommandResult(r.retval, out, r.stderr)

    def _enough_daemons_to_stop(self, daemon_type: str, daemon_ids: List[str], service: str, low_limit: int, alert: bool = False) -> Tuple[bool, str]:
        # Provides a warning about if it possible or not to stop <n> daemons in a service
        names = [f'{daemon_type}.{d_id}' for d_id in daemon_ids]
        number_of_running_daemons = len(
            [daemon
             for daemon in self.mgr.cache.get_daemons_by_type(daemon_type)
             if daemon.status == DaemonDescriptionStatus.running])
        if (number_of_running_daemons - len(daemon_ids)) >= low_limit:
            return False, f'It is presumed safe to stop {names}'

        num_daemons_left = number_of_running_daemons - len(daemon_ids)

        def plural(count: int) -> str:
            return 'daemon' if count == 1 else 'daemons'

        left_count = "no" if num_daemons_left == 0 else num_daemons_left

        if alert:
            out = (f'ALERT: Cannot stop {names} in {service} service. '
                   f'Not enough remaining {service} daemons. '
                   f'Please deploy at least {low_limit + 1} {service} daemons before stopping {names}. ')
        else:
            out = (f'WARNING: Stopping {len(daemon_ids)} out of {number_of_running_daemons} daemons in {service} service. '
                   f'Service will not be operational with {left_count} {plural(num_daemons_left)} left. '
                   f'At least {low_limit} {plural(low_limit)} must be running to guarantee service. ')
        return True, out

    def pre_remove(self, daemon: DaemonDescription) -> None:
        """
        Called before the daemon is removed.
        """
        assert daemon.daemon_type is not None
        assert self.TYPE == daemon_type_to_service(daemon.daemon_type)
        logger.debug(f'Pre remove daemon {self.TYPE}.{daemon.daemon_id}')

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        """
        Called after the daemon is removed.
        """

        def _cleanup_tls_creds_for_host(svc_name: str, host: str, cert_source: Optional[str], other_daemons_in_service: bool) -> None:

            # Daemons can be removed after the service has switched certificate sources. Let's Clean up
            # any cephadm-signed leftovers for this host regardless of the *current* source:
            self.mgr.cert_mgr.try_rm_self_signed_cert_key_pair(svc_name, host)

            # Inline-saved cleanup depends on TLS object scope:
            # - HOST scope: safe to remove for this host when no other daemons remain on this host
            # - SERVICE/GLOBAL scope: only remove when this is the last daemon of the service
            if self.SCOPE in (TLSObjectScope.SERVICE, TLSObjectScope.GLOBAL):
                if other_daemons_in_service:
                    return
                self.mgr.cert_mgr.rm_inline_saved_cert_key_pair(
                    self.cert_name,
                    self.key_name,
                    service_name=svc_name,
                    host=None,
                    ca_cert_name=self.ca_cert_name,
                )
            else:  # Host Scope
                self.mgr.cert_mgr.rm_inline_saved_cert_key_pair(
                    self.cert_name,
                    self.key_name,
                    service_name=svc_name,
                    host=host,
                    ca_cert_name=self.ca_cert_name,
                )

            if cert_source == CertificateSource.REFERENCE.value:
                # It's a reference cert/key to the certmgr so we must keep them as user may want to use them later
                logger.info(
                    "Certificate source is 'reference'; user-provided certmgr entries (if present) will be kept for service: %s, host: %s",
                    svc_name, host
                )

        assert daemon.daemon_type is not None
        assert daemon.hostname
        assert self.TYPE == daemon_type_to_service(daemon.daemon_type)
        logger.debug(f'Post remove daemon {self.TYPE}.{daemon.daemon_id}')

        if not self.requires_certificates:
            # no certificates cleanup actions are needed
            return

        svc_name = daemon.service_name()
        if svc_name not in self.mgr.spec_store:
            return

        host = daemon.hostname
        spec = self.mgr.spec_store[svc_name].spec

        # If TLS is disabled, we may not have a meaningful source anymore
        cert_source = spec.certificate_source if spec.ssl else None

        daemons = [
            d for d in self.mgr.cache.get_daemons_by_service(svc_name)
            if d.name() != daemon.name()
        ]
        remaining_on_host = [d for d in daemons if d.hostname == host]
        if remaining_on_host:
            logger.info(
                "Not removing TLS cert/key for %s on %s: still %d daemons present",
                svc_name, host, len(remaining_on_host)
            )
            return

        _cleanup_tls_creds_for_host(svc_name, host, cert_source, other_daemons_in_service=bool(daemons))

    def purge(self, service_name: str) -> None:
        """Called to carry out any purge tasks following service removal"""
        logger.debug(f'Purge called for {self.TYPE} - no action taken')

    def ignore_possible_stray(
        self, service_type: str, daemon_id: str, name: str
    ) -> bool:
        """Called to decide if a possible stray service should be ignored
        because it "virtually" belongs to a service.
        This is mainly needed when properly managed services spawn layered ceph
        services with different names (for example).
        """
        return False

    def get_blocking_daemon_hosts(self, service_name: str) -> List[HostSpec]:
        return []

    def pre_daemon_service_config(self, spec: ServiceSpec) -> None:
        return

    def has_placement_changed(self, deps: List[str], spec: ServiceSpec) -> bool:
        return False

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
        if curr_deps == last_deps:
            return utils.NextDaemonStep(scheduled_action)
        sym_diff = set(curr_deps).symmetric_difference(last_deps)
        logger.info(
            'Reconfigure wanted %s: deps %r -> %r (diff %r)',
            spec.service_name() if spec else daemon_type,
            last_deps,
            curr_deps,
            sym_diff,
        )
        return utils.NextDaemonStep(utils.Action.RECONFIG)


class CephService(CephadmService):

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        cephadm_config = self.get_config_and_keyring(
            daemon_spec.daemon_type,
            daemon_spec.daemon_id,
            host=daemon_spec.host,
            keyring=daemon_spec.keyring,
            extra_ceph_config=daemon_spec.ceph_conf)

        if daemon_spec.config_get_files():
            cephadm_config.update({'files': daemon_spec.config_get_files()})

        return cephadm_config, self.get_dependencies(
            self.mgr, spec, daemon_spec.daemon_type)

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        super().post_remove(daemon, is_failed_deploy=is_failed_deploy)
        if daemon.daemon_type != 'nfs':
            self.remove_keyring(daemon)

    def get_auth_entity(self, daemon_id: str, host: str = "", rados_user: str = '') -> AuthEntity:
        return get_auth_entity(self.TYPE, daemon_id, host=host)

    def get_config_and_keyring(self,
                               daemon_type: str,
                               daemon_id: str,
                               host: str,
                               keyring: Optional[str] = None,
                               extra_ceph_config: Optional[str] = None
                               ) -> Dict[str, Any]:
        # keyring
        if not keyring:
            entity: AuthEntity = self.get_auth_entity(daemon_id, host=host)
            ret, keyring, err = self.mgr.check_mon_command({
                'prefix': 'auth get',
                'entity': entity,
            })
        config = self.mgr.get_minimal_ceph_conf()

        if extra_ceph_config:
            config += extra_ceph_config

        return {
            'config': config,
            'keyring': keyring,
        }

    def remove_keyring(self, daemon: DaemonDescription) -> None:
        assert daemon.daemon_id is not None
        assert daemon.hostname is not None
        daemon_id: str = daemon.daemon_id
        host: str = daemon.hostname

        assert daemon.daemon_type != 'mon'

        entity = self.get_auth_entity(daemon_id, host=host)

        logger.info(f'Removing keyring for {entity}')
        ret, out, err = self.mgr.mon_command({
            'prefix': 'auth rm',
            'entity': entity,
        })


def next_action_for_mgmt_stack_service(
    scheduled_action: utils.Action,
    daemon_type: Optional[str],
    spec: Optional[ServiceSpec],
    curr_deps: List[str],
    last_deps: List[str],
) -> utils.NextDaemonStep:
    """This function exists to help refactor existing code to use
    choose_next_action instead of if-blocks inside serve.py.
    It avoids the need to muck around with common base classes at the
    cost of some duplication.
    Call this from choose_next_action.
    """
    if curr_deps == last_deps:
        return utils.NextDaemonStep(scheduled_action)
    sym_diff = set(curr_deps).symmetric_difference(last_deps)
    logger.info(
        'Reconfigure wanted %s: deps %r -> %r (diff %r)',
        spec.service_name() if spec else daemon_type,
        last_deps,
        curr_deps,
        sym_diff,
    )
    action = utils.Action.RECONFIG
    # we need only redeploy if secure_monitoring_stack or mgmt-gateway value has changed:
    # TODO(redo): check if we should just go always with redeploy (it's fast enough)
    # TODO(jjm) remove this assert when refactoring process WRT
    # choose_next_action is done.
    assert daemon_type in [
        'prometheus',
        'node-exporter',
        'alertmanager',
        'ceph-exporter',
        'pushgateway',
    ]
    REDEPLOY_TRIGGERS = ['secure_monitoring_stack', 'mgmt-gateway']
    # [from: JJM, to: Redo] in different commits you added calls to
    # symmetric_difference  for the same variables. I have removed this
    # duplication but please let me know if I overlooked something WRT
    # to that in case it was intentional.
    # Also what is this line below trying to check? I struggle with nested
    # inline comprehensions but its seems to me you just want to know if
    # the service newly depends on one of these mgmt stack deps?
    # If so we ought to be able to vastly simplify this...
    if any(svc in e for e in sym_diff for svc in REDEPLOY_TRIGGERS):
        action = utils.Action.REDEPLOY
    return utils.NextDaemonStep(action)
