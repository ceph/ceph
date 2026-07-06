from typing import TYPE_CHECKING, Tuple, Union, List, Dict, Optional, cast, Any, Callable
import logging
from fnmatch import fnmatch
from enum import Enum

from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from mgr_util import verify_tls, certificate_days_to_expire, ServerConfigException
from cephadm.ssl_cert_utils import get_certificate_info, get_private_key_info
from cephadm.tlsobject_types import (Cert,
                                     PrivKey,
                                     TLSObjectScope,
                                     TLSObjectException,
                                     TLSObjectProtocol,
                                     TLSCredentials,
                                     TLSObjectManager)
from cephadm.tlsobject_store import TLSObjectStore
from cephadm.vault import VaultIssuerConfig, VaultPKIClient
from cephadm.cert_metadata import VaultCertificateMetadata, VaultCertificateMetadataStore
from cephadm.cert_issuer import CertificateIssuer, build_certificate_issuers

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class CertFilterOption(str, Enum):
    NAME = 'name'
    STATUS = 'status'
    SIGNED_BY = 'signed-by'
    MANAGED_BY = 'managed-by'
    SCOPE = 'scope'
    SERVICE = 'service'

    def __str__(self) -> str:
        return self.value


class CertStatus(str, Enum):
    VALID = 'valid'
    INVALID = 'invalid'
    EXPIRED = 'expired'
    EXPIRING = 'expiring'

    def __str__(self) -> str:
        return self.value


class CertInfo:
    """
      - managed_by: owner of the certificate lifecycle.
      - is_valid: True if the certificate is valid.
      - is_close_to_expiration: True if the certificate is close to expiration.
      - days_to_expiration: Number of days until expiration.
      - error_info: Details of any exception encountered during validation.
    """
    def __init__(self, cert_name: str,
                 target: Optional[str],
                 user_made: Optional[bool] = None,
                 is_valid: bool = False,
                 is_close_to_expiration: bool = False,
                 days_to_expiration: int = 0,
                 error_info: str = '',
                 managed_by: Optional[Union[TLSObjectManager, str]] = None):
        if managed_by is not None:
            self.managed_by = TLSObjectManager(managed_by)
        elif user_made is not None:
            self.managed_by = TLSObjectManager.USER if user_made else TLSObjectManager.CEPHADM
        else:
            self.managed_by = TLSObjectManager.CEPHADM
        self.cert_name = cert_name
        self.target = target or ''
        self.is_valid = is_valid
        self.is_close_to_expiration = is_close_to_expiration
        self.days_to_expiration = days_to_expiration
        self.error_info = error_info

    @property
    def user_made(self) -> bool:
        return self.managed_by == TLSObjectManager.USER

    @property
    def signed_by(self) -> str:
        return self.managed_by.value

    @property
    def status(self) -> CertStatus:
        """Return certificate status as a CertStatus enum."""
        if not self.is_valid:
            return CertStatus.EXPIRED if 'expired' in self.error_info.lower() else CertStatus.INVALID
        if self.is_close_to_expiration:
            return CertStatus.EXPIRING
        return CertStatus.VALID

    def __str__(self) -> str:
        return f'{self.cert_name} ({self.target})' if self.target else f'{self.cert_name}'

    def is_operationally_valid(self) -> bool:
        return self.is_valid and not self.is_close_to_expiration

    def get_status_description(self) -> str:
        cert_source = {
            TLSObjectManager.USER: 'user-made',
            TLSObjectManager.CEPHADM: 'cephadm-signed',
            TLSObjectManager.VAULT: 'vault-managed',
            TLSObjectManager.ACME: 'acme-managed',
        }[self.managed_by]
        cert_target = f' ({self.target})' if self.target else ''
        cert_details = f"'{self.cert_name}{cert_target}' ({cert_source})"
        if not self.is_valid:
            if 'expired' in self.error_info.lower():
                return f'Certificate {cert_details} has expired'
            else:
                return f'Certificate {cert_details} is not valid (error: {self.error_info})'
        elif self.is_close_to_expiration:
            return f'Certificate {cert_details} is about to expire (remaining days: {self.days_to_expiration})'

        return 'Certificate is valid'


class CertMgr:
    """
    Cephadm Certificate Manager plays a crucial role in maintaining a secure and automated certificate
    lifecycle within Cephadm deployments. CertMgr manages SSL/TLS certificates for all services
    handled by cephadm, acting as the root Certificate Authority (CA) for all certificates.
    This class provides mechanisms for storing, validating, renewing, and monitoring certificate status.

    It tracks known certificates and private keys, associates them with services, and ensures
    their validity. If certificates are close to expiration or invalid, depending on the configuration
    (governed by the mgr/cephadm/certificate_automated_rotation_enabled parameter), CertMgr generates
    warnings or attempts renewal for cephadm-signed certificates.

    Additionally, CertMgr provides methods for certificate management, including retrieving, saving,
    and removing certificates and keys, as well as reporting certificate health status in case of issues.

    This class holds the following important mappings:
      - known_certs
      - known_keys
      - consumers_by_scope   (maps *consumers* to the certificate/key names bound at each scope)

    The first two mappings hold all the known certificate/key *names* (logical store identifiers such
    as "rgw_ssl_cert", "rgw_ssl_key"). These names are not X.509 subjects but certs in PEM (or similar)
    format in the internal TLS object store.

    Each certificate/key name has a pre-defined scope: Global, Host, or Service.

       - Global: The same certificate is used cluster-wide for the consumer (e.g., mgmt-gateway).
       - Host: Certificates specific to individual hosts where the consumer runs (e.g., grafana, oauth2-proxy).
       - Service: Certificates tied to a specific service type (e.g., RGW) and shared by all its daemons.

    In addition to the scope, every scoped certificate/key object may have an associated *target*.
    A target is the concrete identifier within a given scope:

       - For HOST-scoped objects: the target is the host FQDN or inventory-ip name where the certificate is bound.
       - For SERVICE-scoped objects: the target is the service instance name (e.g. "rgw.myzone").
       - For GLOBAL objects, the target is always None because the object applies cluster-wide.

    Scopes define *where* in the system an object conceptually lives, while targets specify the
    *which one* within that scope. The TLSObjectStore uses both pieces of information when saving,
    retrieving, or removing certificates/keys, ensuring that a single logical object name can have
    multiple per-target instances when required.

    Examples:

      - cephadm_root_ca_cert --> scope=GLOBAL, target=None
      - rgw_ssl_cert for rgw.myzone --> scope=SERVICE, target="rgw.myzone"
      - cephadm-signed_grafana_cert on host node12 --> scope=HOST, target="node12"

    The consumers_by_scope mapping associates each scoped consumer (service type or subsystem/integration such as
    "rgw", "nfs", "nvmeof", "grafana", "oauth2-proxy") with the certificate/key *names* it uses.
    This information is needed to trigger the corresponding service reconfiguration when updating some
    certificate and also when setting the cert/key pair from CLI.

    Notes and invariants:
      - The cephadm root CA certificate/key are GLOBAL and unique per cluster (include fsid).
      - Consumers are always service types or subsystems; hosts are never consumers. At HOST scope,
        the host is the target under the consumer. It doesn't own the certificate.
      - cephadm-signed names use a fixed prefix and are treated as HOST-scoped, e.g.:
            cephadm-signed_<service_name>__<label>_cert
            cephadm-signed_<service_name>__<label>_key
        (the label is optional; separator is defined by LABEL_SEPARATOR).
      - Each cert_name/key_name belongs to exactly one scope; SERVICE/HOST scoped names are stored
        per target (service name or host), while GLOBAL scoped names store a single object.
    """

    CEPHADM_ROOT_CA_CERT = 'cephadm_root_ca_cert'
    CEPHADM_ROOT_CA_KEY = 'cephadm_root_ca_key'

    CEPHADM_CERT_ERROR = 'CEPHADM_CERT_ERROR'
    CEPHADM_CERT_WARNING = 'CEPHADM_CERT_WARNING'
    VAULT_TOKEN_STORE_KEY = 'certmgr/vault/token'

    CEPHADM_SIGNED = 'cephadm-signed'
    LABEL_SEPARATOR = "__lbl__"

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.certificates_health_report: List[CertInfo] = []
        self.known_certs: Dict[TLSObjectScope, List[str]] = {
            TLSObjectScope.SERVICE: [],
            TLSObjectScope.HOST: [],
            TLSObjectScope.GLOBAL: [self.CEPHADM_ROOT_CA_CERT],
        }
        self.known_keys: Dict[TLSObjectScope, List[str]] = {
            TLSObjectScope.SERVICE: [],
            TLSObjectScope.HOST: [],
            TLSObjectScope.GLOBAL: [self.CEPHADM_ROOT_CA_KEY],
        }
        self.consumers_by_scope: Dict[TLSObjectScope, Dict[str, Dict[str, List[str]]]] = {
            TLSObjectScope.SERVICE: {},
            TLSObjectScope.HOST: {},
            TLSObjectScope.GLOBAL: {},
        }
        self.certificate_issuers: Dict[TLSObjectManager, CertificateIssuer] = build_certificate_issuers()
        self.vault_cert_metadata_store = VaultCertificateMetadataStore(self.mgr)

    def is_cephadm_signed_object(self, object_name: str) -> bool:
        return object_name.startswith(self.CEPHADM_SIGNED)

    def self_signed_cert(self, service_name: str, label: Optional[str] = None) -> str:
        if label:
            return f'{self.CEPHADM_SIGNED}_{service_name}{self.LABEL_SEPARATOR}{label}_cert'
        else:
            return f'{self.CEPHADM_SIGNED}_{service_name}_cert'

    def self_signed_key(self, service_name: str, label: Optional[str] = None) -> str:
        if label:
            return f'{self.CEPHADM_SIGNED}_{service_name}{self.LABEL_SEPARATOR}{label}_key'
        else:
            return f'{self.CEPHADM_SIGNED}_{service_name}_key'

    def service_name_from_cert(self, cert_name: str) -> str:
        prefix = f'{self.CEPHADM_SIGNED}_'
        suffix = '_cert'
        if cert_name.startswith(prefix) and cert_name.endswith(suffix):
            middle = cert_name[len(prefix):-len(suffix)]
            if self.LABEL_SEPARATOR in middle:
                service_name, _ = middle.split(self.LABEL_SEPARATOR, 1)
            else:
                service_name, _ = middle, None
            return service_name
        return 'unknown-service'

    def init_tlsobject_store(self) -> None:
        self.cert_store = TLSObjectStore(self.mgr, Cert, self.known_certs, self.is_cephadm_signed_object)
        self.cert_store.load()
        self.key_store = TLSObjectStore(self.mgr, PrivKey, self.known_keys, self.is_cephadm_signed_object)
        self.key_store.load()
        self.vault_cert_metadata_store.load()
        self._initialize_root_ca(self.mgr.get_mgr_ip())

    def load(self) -> None:
        self.init_tlsobject_store()

    def _initialize_root_ca(self, ip: str) -> None:
        self.ssl_certs: SSLCerts = SSLCerts(self.mgr._cluster_fsid, self.mgr.certificate_duration_days)
        old_cert = cast(Cert, self.cert_store.get_tlsobject(self.CEPHADM_ROOT_CA_CERT))
        old_key = cast(PrivKey, self.key_store.get_tlsobject(self.CEPHADM_ROOT_CA_KEY))
        if old_key and old_cert:
            try:
                self.ssl_certs.load_root_credentials(old_cert.cert, old_key.key)
            except SSLConfigException as e:
                raise SSLConfigException("Cannot load cephadm root CA certificates.") from e
        else:
            self.ssl_certs.generate_root_cert(addr=ip)
            self.cert_store.save_tlsobject(
                self.CEPHADM_ROOT_CA_CERT,
                self.ssl_certs.get_root_cert(),
                managed_by=TLSObjectManager.CEPHADM,
            )
            self.key_store.save_tlsobject(
                self.CEPHADM_ROOT_CA_KEY,
                self.ssl_certs.get_root_key(),
                managed_by=TLSObjectManager.CEPHADM,
            )

    def get_root_ca(self) -> str:
        return self.ssl_certs.get_root_cert()

    def get_vault_issuer_config(self) -> VaultIssuerConfig:
        return VaultIssuerConfig.from_mgr(self.mgr)

    def get_vault_token(self) -> Optional[str]:
        token = self.mgr.get_store(self.VAULT_TOKEN_STORE_KEY)
        return token.strip() if token else None

    def set_vault_token(self, token: Optional[str]) -> None:
        self.mgr.set_store(self.VAULT_TOKEN_STORE_KEY, token.strip() if token else None)

    def rm_vault_token(self) -> None:
        self.set_vault_token(None)

    def _vault_metadata_target(self, cert_name: str, service_name: Optional[str] = None,
                               host: Optional[str] = None) -> Optional[str]:
        scope = self.get_cert_scope(cert_name)
        if scope == TLSObjectScope.SERVICE:
            return service_name
        if scope == TLSObjectScope.HOST:
            return host
        return None

    def save_vault_certificate_metadata(self, metadata: VaultCertificateMetadata) -> None:
        self.vault_cert_metadata_store.save(metadata)

    def get_vault_certificate_metadata(self, cert_name: str, service_name: Optional[str] = None,
                                       host: Optional[str] = None) -> Optional[VaultCertificateMetadata]:
        target = self._vault_metadata_target(cert_name, service_name, host)
        return self.vault_cert_metadata_store.get(cert_name, target)

    def rm_vault_certificate_metadata(self, cert_name: str, service_name: Optional[str] = None,
                                      host: Optional[str] = None) -> bool:
        target = self._vault_metadata_target(cert_name, service_name, host)
        return self.vault_cert_metadata_store.remove(cert_name, target)

    def _vault_metadata_matches_target(self, metadata: VaultCertificateMetadata,
                                       service_name: Optional[str] = None,
                                       host: Optional[str] = None) -> bool:
        if metadata.scope == TLSObjectScope.SERVICE:
            return service_name is not None and metadata.target == service_name
        if metadata.scope == TLSObjectScope.HOST:
            return host is not None and metadata.target == host
        return service_name is None and host is None

    def rm_vault_certificate_metadata_by_key(self, key_name: str, service_name: Optional[str] = None,
                                             host: Optional[str] = None) -> None:
        for metadata in list(self.vault_cert_metadata_store.list()):
            if metadata.key_name == key_name and self._vault_metadata_matches_target(metadata, service_name, host):
                self.vault_cert_metadata_store.remove(metadata.cert_name, metadata.target)

    def _ensure_vault_issue_target(self, cert_name: str, key_name: str,
                                   service_name: Optional[str] = None,
                                   host: Optional[str] = None) -> Tuple[TLSObjectScope, Optional[str]]:
        cert_scope = self.get_cert_scope(cert_name)
        key_scope = self.get_key_scope(key_name)
        if cert_scope == TLSObjectScope.UNKNOWN:
            raise TLSObjectException(f"Unknown certificate '{cert_name}'. Cannot issue it from Vault.")
        if key_scope == TLSObjectScope.UNKNOWN:
            raise TLSObjectException(f"Unknown key '{key_name}'. Cannot issue certificate '{cert_name}' from Vault.")
        if cert_scope != key_scope:
            raise TLSObjectException(
                f"Certificate '{cert_name}' and key '{key_name}' must have the same scope to be issued from Vault. "
                f"Got cert scope '{cert_scope.value}' and key scope '{key_scope.value}'."
            )
        if cert_scope == TLSObjectScope.SERVICE:
            if not service_name:
                raise TLSObjectException(f"Vault-managed certificate '{cert_name}' is service-scoped. Please specify service_name.")
            return cert_scope, service_name
        if cert_scope == TLSObjectScope.HOST:
            if not host:
                raise TLSObjectException(f"Vault-managed certificate '{cert_name}' is host-scoped. Please specify host.")
            return cert_scope, host
        return cert_scope, None

    def _ensure_vault_ca_scope(self, ca_cert_name: Optional[str], scope: TLSObjectScope) -> None:
        if not ca_cert_name:
            return
        ca_scope = self.get_cert_scope(ca_cert_name)
        if ca_scope == TLSObjectScope.UNKNOWN:
            raise TLSObjectException(f"Unknown CA certificate '{ca_cert_name}'. Cannot save Vault issuing CA.")
        if ca_scope != scope:
            raise TLSObjectException(
                f"CA certificate '{ca_cert_name}' must have the same scope as the Vault-managed certificate. "
                f"Got CA scope '{ca_scope.value}' and certificate scope '{scope.value}'."
            )

    def _ensure_vault_overwrite_allowed(self, cert_name: str, key_name: str,
                                        service_name: Optional[str] = None,
                                        host: Optional[str] = None,
                                        ca_cert_name: Optional[str] = None) -> None:
        objects = [
            ('certificate', cert_name, self.cert_store.get_tlsobject_if_exists(cert_name, service_name, host)),
            ('key', key_name, self.key_store.get_tlsobject_if_exists(key_name, service_name, host)),
        ]
        if ca_cert_name:
            objects.append(
                ('CA certificate', ca_cert_name, self.cert_store.get_tlsobject_if_exists(ca_cert_name, service_name, host))
            )
        for obj_type, obj_name, obj in objects:
            if not obj:
                continue
            if not getattr(obj, 'editable', True) and getattr(obj, 'managed_by', TLSObjectManager.CEPHADM) != TLSObjectManager.VAULT:
                raise TLSObjectException(
                    f"Cannot overwrite non-editable {obj_type} '{obj_name}' with Vault-managed material."
                )

    def issue_vault_certificate(self,
                                cert_name: str,
                                key_name: str,
                                common_name: str,
                                service_name: Optional[str] = None,
                                host: Optional[str] = None,
                                ca_cert_name: Optional[str] = None,
                                alt_names: Optional[List[str]] = None,
                                ip_sans: Optional[List[str]] = None,
                                pki_mount: Optional[str] = None,
                                role: Optional[str] = None,
                                ttl: Optional[str] = None) -> TLSCredentials:
        """Issue a certificate from Vault PKI and store it in certmgr.

        This stores the resulting cert/key as non-editable and
        managed_by=vault. It is used by both the manual Vault enrollment path
        and services using certificate_source=vault. The persisted metadata is
        consumed by the periodic Vault renewal path.
        """
        scope, target = self._ensure_vault_issue_target(cert_name, key_name, service_name, host)
        self._ensure_vault_ca_scope(ca_cert_name, scope)
        self._ensure_vault_overwrite_allowed(cert_name, key_name, service_name, host, ca_cert_name)

        config = self.get_vault_issuer_config()
        client = VaultPKIClient(config, self.get_vault_token())
        creds = client.issue_certificate(
            common_name=common_name,
            alt_names=alt_names or [],
            ip_sans=ip_sans or [],
            ttl=ttl,
            mount=pki_mount,
            role=role,
        )

        self.save_cert(
            cert_name,
            creds.cert,
            service_name=service_name,
            host=host,
            managed_by=TLSObjectManager.VAULT,
            editable=False,
        )
        self.save_key(
            key_name,
            creds.key,
            service_name=service_name,
            host=host,
            managed_by=TLSObjectManager.VAULT,
            editable=False,
        )
        if ca_cert_name and creds.ca_cert:
            self.save_cert(
                ca_cert_name,
                creds.ca_cert,
                service_name=service_name,
                host=host,
                managed_by=TLSObjectManager.VAULT,
                editable=False,
            )

        metadata = VaultCertificateMetadata(
            cert_name=cert_name,
            key_name=key_name,
            ca_cert_name=ca_cert_name if creds.ca_cert else None,
            scope=scope,
            target=target,
            pki_mount=pki_mount or config.pki_mount,
            role=role or config.role,
            ttl=ttl or config.ttl,
            common_name=common_name,
            alt_names=alt_names or [],
            ip_sans=ip_sans or [],
        )
        self.save_vault_certificate_metadata(metadata)
        return creds

    def register_self_signed_cert_key_pair(self, service_name: str, label: Optional[str] = None) -> None:
        """
        Registers a self-signed certificate/key for a given service under host scope.

        :param service_name: The name of the service.
        """
        self.cert_store.register_object_name(self.self_signed_cert(service_name, label), TLSObjectScope.HOST)
        self.key_store.register_object_name(self.self_signed_key(service_name, label), TLSObjectScope.HOST)

    def register_cert_key_pair(
        self,
        consumer: str,
        cert_name: str,
        key_name: str,
        scope: TLSObjectScope,
        ca_cert_name: Optional[str] = None
    ) -> None:
        """
        Registers a certificate/key for a given consumer under a specific scope.

        :param consumer: The consumer of the certificate (e.g., service-type, other gobal consumer).
        :param cert_name: The name of the certificate.
        :param key_name: The name of the key.
        :param scope: The TLSObjectScope (SERVICE, HOST, GLOBAL).
        """
        self.register_cert(consumer, cert_name, scope)
        self.register_key(consumer, key_name, scope)
        if ca_cert_name:
            self.register_cert(consumer, ca_cert_name, scope)

    def register_cert(self, consumer: str, cert_name: str, scope: TLSObjectScope) -> None:
        self._register_tls_object(consumer, cert_name, scope, "certs")

    def register_key(self, consumer: str, key_name: str, scope: TLSObjectScope) -> None:
        self._register_tls_object(consumer, key_name, scope, "keys")

    def _register_tls_object(self, consumer: str, obj_name: str, scope: TLSObjectScope, obj_type: str) -> None:
        """
        Registers a TLS-related object (certificate or key) for a given consumer under a specific scope.

        :param consumer: The consumer of the TLS object.
        :param obj_name: The name of the certificate or key.
        :param scope: The TLSObjectScope (SERVICE, HOST, GLOBAL).
        :param obj_type: either "certs" or "keys".
        """
        storage = self.known_certs if obj_type == "certs" else self.known_keys

        if obj_name and obj_name not in storage[scope]:
            storage[scope].append(obj_name)

        if consumer not in self.consumers_by_scope[scope]:
            self.consumers_by_scope[scope][consumer] = {"certs": [], "keys": []}

        if obj_name not in self.consumers_by_scope[scope][consumer][obj_type]:
            self.consumers_by_scope[scope][consumer][obj_type].append(obj_name)

    def get_associated_service(self, cert_info: CertInfo) -> Optional[str]:
        """
        Retrieves the service associeted to the certificate
        """
        if self.is_cephadm_signed_object(cert_info.cert_name):
            return self.service_name_from_cert(cert_info.cert_name)
        for scoped_consumers in self.consumers_by_scope.values():
            for consumer, bundles in scoped_consumers.items():
                if cert_info.cert_name in bundles.get('certs', []):
                    cert_scope = self.get_cert_scope(cert_info.cert_name)
                    if cert_scope == TLSObjectScope.SERVICE:
                        return cert_info.target
                    else:
                        return consumer
        return None

    def generate_cert(
        self,
        host_fqdn: Union[str, List[str]],
        node_ip: Union[str, List[str]],
        custom_san_list: Optional[List[str]] = None,
        duration_in_days: Optional[int] = None,
    ) -> TLSCredentials:
        cert, key = self.ssl_certs.generate_cert(
            host_fqdn,
            node_ip,
            custom_san_list=custom_san_list,
            duration_in_days=duration_in_days
        )
        ca_cert = self.mgr.cert_mgr.get_root_ca()
        return TLSCredentials(cert=cert, key=key, ca_cert=ca_cert)

    def cert_exists(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        cert_obj = self.cert_store.get_tlsobject(cert_name, service_name, host)
        return cert_obj is not None

    def is_cert_editable(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        # By definition a certificate which doesn't not exist it's considered
        # editable so user can populate its content by using a custom value.
        if self.cert_store.tlsobject_exists(cert_name):
            cert_obj = cast(Cert, self.cert_store.get_tlsobject(cert_name, service_name, host))
            return cert_obj.editable if cert_obj else True
        else:
            return True

    def get_cert(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        cert_obj = cast(Cert, self.cert_store.get_tlsobject(cert_name, service_name, host))
        return cert_obj.cert if cert_obj else None

    def get_key(self, key_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        key_obj = cast(PrivKey, self.key_store.get_tlsobject(key_name, service_name, host))
        return key_obj.key if key_obj else None

    def get_self_signed_tls_credentials(self, service_name: str, hostname: str, label: Optional[str] = None) -> TLSCredentials:
        cert_obj = cast(Cert, self.cert_store.get_tlsobject(self.self_signed_cert(service_name, label), host=hostname))
        key_obj = cast(PrivKey, self.key_store.get_tlsobject(self.self_signed_key(service_name, label), host=hostname))
        cert = cert_obj.cert if cert_obj else ''
        key = key_obj.key if key_obj else ''
        ca_cert = self.mgr.cert_mgr.get_root_ca()
        return TLSCredentials(cert=cert, key=key, ca_cert=ca_cert)

    def save_cert(self, cert_name: str, cert: str, service_name: Optional[str] = None, host: Optional[str] = None,
                  user_made: Optional[bool] = None, editable: bool = False,
                  managed_by: Optional[Union[TLSObjectManager, str]] = None) -> None:
        self.cert_store.save_tlsobject(
            cert_name,
            cert,
            service_name=service_name,
            host=host,
            user_made=user_made,
            editable=editable,
            managed_by=managed_by,
        )

    def save_key(self, key_name: str, key: str, service_name: Optional[str] = None, host: Optional[str] = None,
                 user_made: Optional[bool] = None, editable: bool = False,
                 managed_by: Optional[Union[TLSObjectManager, str]] = None) -> None:
        self.key_store.save_tlsobject(
            key_name,
            key,
            service_name=service_name,
            host=host,
            user_made=user_made,
            editable=editable,
            managed_by=managed_by,
        )

    def save_self_signed_cert_key_pair(self, service_name: str, tls_creds: TLSCredentials, host: str,
                                       label: Optional[str] = None) -> None:
        ss_cert_name = self.self_signed_cert(service_name, label)
        ss_key_name = self.self_signed_key(service_name, label)
        self.cert_store.save_tlsobject(
            ss_cert_name,
            tls_creds.cert,
            host=host,
            managed_by=TLSObjectManager.CEPHADM,
        )
        self.key_store.save_tlsobject(
            ss_key_name,
            tls_creds.key,
            host=host,
            managed_by=TLSObjectManager.CEPHADM,
        )

    def _is_inline_saved_tlsobject(self, obj: Optional[TLSObjectProtocol]) -> bool:
        # Inline-saved credentials are persisted as managed_by=user but editable=False.
        return bool(
            obj
            and getattr(obj, 'managed_by', TLSObjectManager.CEPHADM) == TLSObjectManager.USER
            and not getattr(obj, 'editable', True)
        )

    def rm_inline_saved_cert_key_pair(
        self,
        cert_name: str,
        key_name: str,
        service_name: Optional[str] = None,
        host: Optional[str] = None,
        ca_cert_name: Optional[str] = None,
    ) -> None:
        """Remove inline-saved (non-editable) TLS objects for a given service/host.
        This intentionally does *not* remove user-provisioned certmgr entries
        (editable=True), which are considered reusable references.
        """
        context = f"service={service_name!r}, host={host!r}" if host else f"service={service_name!r}"

        cert_obj = cast(Cert, self.cert_store.get_tlsobject_if_exists(cert_name, service_name, host))
        if self._is_inline_saved_tlsobject(cert_obj):
            logger.info("Removing inline-saved cert %r (%s)", cert_name, context)
            self.rm_cert_if_present(cert_name, service_name, host)
        elif cert_obj:
            logger.info("Skipping cert removal for %r — exists but is not inline-saved (editable=True) (%s)", cert_name, context)

        key_obj = cast(PrivKey, self.key_store.get_tlsobject_if_exists(key_name, service_name, host))
        if self._is_inline_saved_tlsobject(key_obj):
            logger.info("Removing inline-saved private key %r (%s)", key_name, context)
            self.rm_key_if_present(key_name, service_name, host)
        elif key_obj:
            logger.info("Skipping key removal for %r — exists but is not inline-saved (editable=True) (%s)", key_name, context)

        if ca_cert_name:
            ca_obj = cast(Cert, self.cert_store.get_tlsobject_if_exists(ca_cert_name, service_name, host))
            if self._is_inline_saved_tlsobject(ca_obj):
                logger.info("Removing inline-saved CA cert %r (%s)", ca_cert_name, context)
                self.rm_cert_if_present(ca_cert_name, service_name, host)
            elif ca_obj:
                logger.info("Skipping CA cert removal for %r — exists but is not inline-saved (editable=True) (%s)", ca_cert_name, context)

    def rm_cert(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        removed = self.cert_store.rm_tlsobject(cert_name, service_name, host)
        if removed:
            self.rm_vault_certificate_metadata(cert_name, service_name, host)
        return removed

    def rm_key(self, key_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        removed = self.key_store.rm_tlsobject(key_name, service_name, host)
        if removed:
            self.rm_vault_certificate_metadata_by_key(key_name, service_name, host)
        return removed

    def rm_self_signed_cert_key_pair(self, service_name: str, host: str, label: Optional[str] = None) -> None:
        self.rm_cert(self.self_signed_cert(service_name, label), service_name, host)
        self.rm_key(self.self_signed_key(service_name, label), service_name, host)

    def rm_cert_if_present(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        try:
            return self.rm_cert(cert_name, service_name, host)
        except TLSObjectException:
            return False

    def rm_key_if_present(self, key_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        try:
            return self.rm_key(key_name, service_name, host)
        except TLSObjectException:
            return False

    def try_rm_self_signed_cert_key_pair(self, service_name: str, host: str, label: Optional[str] = None) -> None:
        cert_removed = self.rm_cert_if_present(self.self_signed_cert(service_name, label), service_name, host)
        key_removed = self.rm_key_if_present(self.self_signed_key(service_name, label), service_name, host)
        if cert_removed or key_removed:
            logger.info(
                f"Removing cephadm-signed cert/key for service: {service_name}, host: {host}: "
                f"key/cert removed: {key_removed}/{cert_removed}"
            )

    def cert_ls(self, filter_by: str = '',
                include_details: bool = False,
                include_cephadm_signed: bool = False) -> Dict:
        """
        lifecycle-owner filtering behavior in `cert_ls`:

        Defaults:
        - If `include_cephadm_signed` is False and no explicit `signed-by=` or
          `managed-by=` selector is provided, we auto-filter to show only
          user-made certs (and always include the root CA).
        - If the caller explicitly filters by `signed-by=...` or
          `managed-by=...`, that explicit filter wins.

        `signed-by` is kept as a compatibility alias for the lifecycle owner;
        new callers should prefer `managed-by`.
        """

        def _lhs(expr: str) -> str:
            return expr.partition('=')[0].strip().lower()

        def _build_cert_context(cert_info: CertInfo) -> Dict[CertFilterOption, Any]:
            scope = self.get_cert_scope(cert_info.cert_name)
            svc = self.get_associated_service(cert_info) or ''
            return {
                CertFilterOption.NAME: cert_info.cert_name,
                CertFilterOption.STATUS: cert_info.status,
                CertFilterOption.SIGNED_BY: cert_info.signed_by,
                CertFilterOption.MANAGED_BY: cert_info.managed_by.value,
                CertFilterOption.SCOPE: scope,
                CertFilterOption.SERVICE: svc,
            }

        def _field_filter(expr: str) -> Callable[[Dict[CertFilterOption, Any]], bool]:
            key_str, _, value = expr.partition('=')
            key_str = key_str.strip().lower()
            value = value.strip()

            try:
                key = CertFilterOption(key_str)
            except ValueError:
                return lambda cert_ctx: True

            if key in (CertFilterOption.NAME, CertFilterOption.SERVICE):
                return lambda cert_ctx: fnmatch(cert_ctx.get(key, ''), value)

            if key in (
                CertFilterOption.SCOPE,
                CertFilterOption.STATUS,
                CertFilterOption.SIGNED_BY,
                CertFilterOption.MANAGED_BY,
            ):
                return lambda cert_ctx: cert_ctx.get(key) == value

            # Default: unknown field selector -> nop filter (don't exclude)
            return lambda cert_ctx: True

        def build_filters() -> List[Callable[[Dict[CertFilterOption, Any]], bool]]:
            filter_exprs = [e.strip() for e in filter_by.split(',') if e.strip()]
            cert_filters = [_field_filter(expr) for expr in filter_exprs]
            # By default: filter out cephadm-signed certs unless explicitly included
            # with the exception of the cephadm root CA cert (CEPHADM_ROOT_CA_CERT) as
            # technically the user may be interested in adding it to his CA trust chain.
            # An explicit signed-by= or managed-by= selector disables this auto-filter.
            explicit_manager_filter = any(
                _lhs(e) in (str(CertFilterOption.SIGNED_BY), str(CertFilterOption.MANAGED_BY))
                for e in filter_exprs
            )
            if not include_cephadm_signed and not explicit_manager_filter:
                cert_filters.append(
                    lambda cert_ctx:
                    cert_ctx.get(CertFilterOption.MANAGED_BY) == TLSObjectManager.USER.value
                    or cert_ctx[CertFilterOption.NAME] == self.CEPHADM_ROOT_CA_CERT
                )
            return cert_filters

        filters = build_filters()
        cert_objects: List = self.cert_store.list_tlsobjects()
        ls: Dict = {}
        for cert_name, cert_obj, target in cert_objects:

            cert_info = self._check_certificate_state(cert_name, target, cert_obj)
            ctx = _build_cert_context(cert_info)
            if not all(f(ctx) for f in filters):
                continue

            cert_extended_info = get_certificate_info(cert_obj.cert, include_details)
            cert_scope = self.get_cert_scope(cert_name)
            if cert_name not in ls:
                ls[cert_name] = {'scope': cert_scope.value, 'certificates': {}}
            if cert_scope == TLSObjectScope.GLOBAL:
                ls[cert_name]['certificates'] = cert_extended_info
            else:
                ls[cert_name]['certificates'][target] = cert_extended_info

        return ls

    def key_ls(self, include_cephadm_generated_keys: bool = False) -> Dict:
        key_objects: List = self.key_store.list_tlsobjects()
        ls: Dict = {}
        for key_name, key_obj, target in key_objects:
            if not include_cephadm_generated_keys and self.is_cephadm_signed_object(key_name):
                continue
            priv_key_info = get_private_key_info(key_obj.key)
            key_scope = self.get_key_scope(key_name)
            if key_name not in ls:
                ls[key_name] = {'scope': key_scope.value, 'keys': {}}
            if key_scope == TLSObjectScope.GLOBAL:
                ls[key_name]['keys'] = priv_key_info
            else:
                ls[key_name]['keys'].update({target: priv_key_info})

        # we don't want this key to be leaked
        ls.pop(self.CEPHADM_ROOT_CA_KEY, None)

        return ls

    def list_consumer_known_certificates(self, consumer: str) -> List[str]:
        """
        Retrieves all certificates associated with a given consumer.

        :param consumer: The consumer name.
        :return: A list of certificate names, or None if the consumer is not found.
        """
        for scope, scoped_consumers in self.consumers_by_scope.items():
            if consumer in scoped_consumers:
                return scoped_consumers[consumer]['certs']  # Return certs for the consumer
        return []

    def get_consumers(self, get_scope: bool = False) -> Dict[str, Any]:
        return {scope.value: consumers for scope, consumers in self.consumers_by_scope.items()}

    def list_consumers(self) -> List[str]:
        """
        Retrieves a list of all registered consumers across all scopes.
        :return: A list of consumer names.
        """
        consumers: List[str] = []
        for scoped_consumers in self.consumers_by_scope.values():
            consumers.extend(scoped_consumers.keys())
        return consumers

    def get_cert_scope(self, cert_name: str) -> TLSObjectScope:
        if self.is_cephadm_signed_object(cert_name):
            return TLSObjectScope.HOST
        for scope, certificates in self.known_certs.items():
            if cert_name in certificates:
                return scope
        return TLSObjectScope.UNKNOWN

    def get_key_scope(self, key_name: str) -> TLSObjectScope:
        for scope, keys in self.known_keys.items():
            if key_name in keys:
                return scope
        return TLSObjectScope.UNKNOWN

    def _notify_certificates_health_status(self, problematic_certificates: List[CertInfo]) -> None:

        previously_reported_issues = [(c.cert_name, c.target) for c in self.certificates_health_report]
        for cert_info in problematic_certificates:
            if (cert_info.cert_name, cert_info.target) not in previously_reported_issues:
                self.certificates_health_report.append(cert_info)

        # Nothing to report => clear both checks
        if not self.certificates_health_report:
            self.mgr.remove_health_warning(self.CEPHADM_CERT_ERROR)
            self.mgr.remove_health_warning(self.CEPHADM_CERT_WARNING)
            return

        # Split issues by severity
        error_certs: List[CertInfo] = []
        warn_certs: List[CertInfo] = []
        for cert_info in self.certificates_health_report:
            if cert_info.status in (CertStatus.INVALID, CertStatus.EXPIRED):
                error_certs.append(cert_info)
            elif cert_info.status == CertStatus.EXPIRING:
                warn_certs.append(cert_info)

        def _emit_health_issue(check_id: str, is_error: bool, certs: List[CertInfo]) -> None:
            invalid_count = sum(1 for c in certs if c.status == CertStatus.INVALID)
            expired_count = sum(1 for c in certs if c.status == CertStatus.EXPIRED)
            expiring_count = sum(1 for c in certs if c.status == CertStatus.EXPIRING)
            issues = [
                f'{invalid_count} {CertStatus.INVALID}' if invalid_count else '',
                f'{expired_count} {CertStatus.EXPIRED}' if expired_count else '',
                f'{expiring_count} {CertStatus.EXPIRING}' if expiring_count else '',
            ]
            total = len(certs)
            issues_description = ', '.join(filter(None, issues))
            short_msg = f'Detected {total} cephadm certificate(s) issues: {issues_description}'

            detailed_msgs = [c.get_status_description() for c in certs]
            if is_error:
                logger.error(short_msg)
                self.mgr.set_health_error(check_id, short_msg, total, detailed_msgs)
            else:
                logger.warning(short_msg)
                self.mgr.set_health_warning(check_id, short_msg, total, detailed_msgs)

        # Emit/clear error check
        if error_certs:
            _emit_health_issue(self.CEPHADM_CERT_ERROR, True, error_certs)
        else:
            self.mgr.remove_health_warning(self.CEPHADM_CERT_ERROR)

        # Emit/clear warning check
        if warn_certs:
            _emit_health_issue(self.CEPHADM_CERT_WARNING, False, warn_certs)
        else:
            self.mgr.remove_health_warning(self.CEPHADM_CERT_WARNING)

    def check_certificate_state(self, cert_name: str, target: str, cert: str, key: Optional[str] = None) -> CertInfo:
        """
        Checks if a certificate is valid and close to expiration.

        Returns:
            - is_valid: True if the certificate is valid.
            - is_close_to_expiration: True if the certificate is close to expiration.
            - days_to_expiration: Number of days until expiration.
            - exception_info: Details of any exception encountered during validation.
        """
        cert_obj = Cert(cert, True)
        key_obj = PrivKey(key, True) if key else None
        return self._check_certificate_state(cert_name, target, cert_obj, key_obj)

    def _check_certificate_state(self, cert_name: str, target: Optional[str], cert: Cert, key: Optional[PrivKey] = None) -> CertInfo:
        """
        Checks if a certificate is valid and close to expiration.

        Returns: CertInfo
        """
        try:
            days_to_expiration = verify_tls(cert.cert, key.key) if key else certificate_days_to_expire(cert.cert)
            is_close_to_expiration = days_to_expiration < self.mgr.certificate_renewal_threshold_days
            return CertInfo(cert_name, target, is_valid=True, is_close_to_expiration=is_close_to_expiration, days_to_expiration=days_to_expiration, error_info="", managed_by=cert.managed_by)
        except ServerConfigException as e:
            return CertInfo(cert_name, target, is_valid=False, is_close_to_expiration=False, days_to_expiration=0, error_info=str(e), managed_by=cert.managed_by)

    def get_problematic_certificates(self) -> List[Tuple[CertInfo, Cert]]:

        def get_key(cert_name: str, key_name: str, target: Optional[str]) -> Optional[PrivKey]:
            try:
                tlsobj_target = self.cert_store.determine_tlsobject_target(cert_name, target)
                key = cast(
                    PrivKey,
                    self.key_store.get_tlsobject(
                        key_name,
                        service_name=tlsobj_target.service,
                        host=tlsobj_target.host
                    )
                )
                return key
            except TLSObjectException:
                return None

        # Filter non-empty entries skipping cephadm root CA cetificate
        certs_tlsobjs = [c for c in self.cert_store.list_tlsobjects() if c[1] and c[0] != self.CEPHADM_ROOT_CA_CERT]
        problematics_certs: List[Tuple[CertInfo, Cert]] = []
        for cert_name, cert_tlsobj, target in certs_tlsobjs:
            cert_obj = cast(Cert, cert_tlsobj)
            if not cert_obj:
                logger.error(f'Cannot find certificate {cert_name} in the TLSObjectStore')
                continue

            key_name = cert_name.replace('_cert', '_key')
            key_obj = get_key(cert_name, key_name, target)
            if key_obj:
                # certificate has a key, let's check the cert/key pair
                cert_info = self._check_certificate_state(cert_name, target, cert_obj, key_obj)
            elif any(key_name in ks for ks in self.known_keys.values()) or self.is_cephadm_signed_object(key_name):
                # certificate is supposed to have a key but it's missing
                logger.error(f"Key '{key_name}' is missing for certificate '{cert_name}'.")
                cert_info = CertInfo(cert_name, target, is_valid=False, is_close_to_expiration=False, days_to_expiration=0, error_info="missing key", managed_by=cert_obj.managed_by)
            else:
                # certificate has no associated key
                cert_info = self._check_certificate_state(cert_name, target, cert_obj)

            if not cert_info.is_operationally_valid():
                problematics_certs.append((cert_info, cert_obj))
            else:
                target_info = f" ({target})" if target else ""
                logger.info(f'Certificate for "{cert_name}{target_info}" is still valid for {cert_info.days_to_expiration} days.')

        return problematics_certs

    def _get_certificate_issuer(self, managed_by: TLSObjectManager) -> CertificateIssuer:
        return self.certificate_issuers[managed_by]

    def _supports_auto_fix(self, cert_obj: Cert) -> bool:
        """Return whether certmgr currently knows how to fix this manager type."""
        return self._get_certificate_issuer(cert_obj.managed_by).supports_auto_fix()

    def _renew_certificate(self, cert_info: CertInfo, cert_obj: Cert) -> bool:
        issuer = self._get_certificate_issuer(cert_obj.managed_by)
        if not issuer.supports_auto_fix():
            return False
        return issuer.renew(self, cert_info, cert_obj)

    def _renew_self_signed_certificate(self, cert_info: CertInfo, cert_obj: Cert) -> bool:
        """Compatibility wrapper for cephadm-managed self-signed renewal."""
        return self._get_certificate_issuer(TLSObjectManager.CEPHADM).renew(self, cert_info, cert_obj)

    def check_services_certificates(self, fix_issues: bool = False) -> Tuple[List[str], List[CertInfo]]:
        """
        Checks services' certificates and optionally attempts to fix issues if fix_issues is True.

        :param fix_issues: Whether to attempt fixing issues automatically.
        :return: A tuple with:
            - List of services requiring reconfiguration.
            - List of certificates that require manual intervention.
        """

        def requires_user_intervention(cert_info: CertInfo, cert_obj: Cert) -> bool:
            """Determines if a certificate requires manual user intervention."""
            if cert_info.is_operationally_valid():
                return False
            if not self.mgr.certificate_automated_rotation_enabled:
                return True
            return not self._supports_auto_fix(cert_obj)

        def trigger_auto_fix(cert_info: CertInfo, cert_obj: Cert) -> bool:
            """Attempts to automatically fix certificate issues if possible."""
            if not self.mgr.certificate_automated_rotation_enabled:
                return False
            if not self._supports_auto_fix(cert_obj):
                return False

            # Cephadm-managed invalid certs are removed to force regeneration by
            # the existing service certificate path. External issuers such as
            # Vault must never remove old material before a replacement has been
            # issued successfully, so they renew in-place instead.
            if not cert_info.is_valid and cert_obj.managed_by == TLSObjectManager.CEPHADM:
                tlsobj_target = self.cert_store.determine_tlsobject_target(cert_info.cert_name, cert_info.target)
                logger.info(
                    f'Removing invalid certificate for {cert_info.cert_name} to trigger regeneration '
                    f'(service: {tlsobj_target.service}, host: {tlsobj_target.host}).'
                )
                self.cert_store.rm_tlsobject(cert_info.cert_name, tlsobj_target.service, tlsobj_target.host)
                return True
            elif not cert_info.is_valid or cert_info.is_close_to_expiration:
                return self._renew_certificate(cert_info, cert_obj)
            else:
                return False

        def log_issue(cert_info: CertInfo) -> None:
            msg = cert_info.get_status_description()
            if cert_info.status in (CertStatus.INVALID, CertStatus.EXPIRED):
                logger.error(msg)
            elif cert_info.status == CertStatus.EXPIRING:
                logger.warning(msg)
            else:
                logger.info(msg)

        # Process all problematic certificates and try to fix them in case automated certs renewal
        # is enabled. Successfully fixed ones are collected to trigger a service reconfiguration.
        certs_with_issues: List[CertInfo] = []
        services_to_reconfig = set()
        for cert_info, cert_obj in self.get_problematic_certificates():

            log_issue(cert_info)

            if requires_user_intervention(cert_info, cert_obj):
                certs_with_issues.append(cert_info)
                continue

            if fix_issues:
                if trigger_auto_fix(cert_info, cert_obj):
                    svc = self.get_associated_service(cert_info)
                    if svc:
                        services_to_reconfig.add(svc)
                    else:
                        logger.error(f'Cannot find the service associated with the certificate {cert_info.cert_name}')
                elif not cert_info.is_operationally_valid():
                    # Auto-fix was possible in principle but failed (for example,
                    # Vault was unreachable or renewal metadata is missing). Keep
                    # the old material and report the problem so the user is warned
                    # and certmgr retries on the next periodic check.
                    certs_with_issues.append(cert_info)

        # Clear previously reported issues as we are newly checking all the certificates
        self.certificates_health_report = []

        # All problematic certificates have been processed. certs_with_issues now only
        # contains certificates that couldn't be fixed either because they are not
        # auto-fixable, because issuer renewal failed, or automated rotation is disabled.
        # In these cases, health warning or error is raised to notify the user.
        self._notify_certificates_health_status(certs_with_issues)

        return list(services_to_reconfig), certs_with_issues
