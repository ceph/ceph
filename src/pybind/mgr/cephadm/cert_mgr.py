from typing import TYPE_CHECKING, Tuple, Union, List, Dict, Optional, cast, Any, Callable
import logging
from fnmatch import fnmatch
from enum import Enum

from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from mgr_util import verify_tls, certificate_days_to_expire, ServerConfigException
from cephadm.ssl_cert_utils import get_certificate_info, get_private_key_info
from cephadm.tlsobject_types import Cert, PrivKey, TLSObjectScope, TLSObjectException, CertKeyPair
from cephadm.tlsobject_store import TLSObjectStore

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class CertFilterOption(str, Enum):
    NAME = 'name'
    STATUS = 'status'
    SIGNED_BY = 'signed-by'
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
      - is_valid: True if the certificate is valid.
      - is_close_to_expiration: True if the certificate is close to expiration.
      - days_to_expiration: Number of days until expiration.
      - error_info: Details of any exception encountered during validation.
    """
    def __init__(self, cert_name: str,
                 target: Optional[str],
                 user_made: bool = False,
                 is_valid: bool = False,
                 is_close_to_expiration: bool = False,
                 days_to_expiration: int = 0,
                 error_info: str = ''):
        self.user_made = user_made
        self.cert_name = cert_name
        self.target = target or ''
        self.is_valid = is_valid
        self.is_close_to_expiration = is_close_to_expiration
        self.days_to_expiration = days_to_expiration
        self.error_info = error_info

    @property
    def signed_by(self) -> str:
        return "user" if self.user_made else "cephadm"

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
        cert_source = 'user-made' if self.user_made else 'cephadm-signed'
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
    CEPHADM_CERTMGR_HEALTH_ERR = 'CEPHADM_CERT_ERROR'
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
            self.cert_store.save_tlsobject(self.CEPHADM_ROOT_CA_CERT, self.ssl_certs.get_root_cert())
            self.key_store.save_tlsobject(self.CEPHADM_ROOT_CA_KEY, self.ssl_certs.get_root_key())

    def get_root_ca(self) -> str:
        return self.ssl_certs.get_root_cert()

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
        ca_cert_name: Optional[str] = None,
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
    ) -> CertKeyPair:
        cert, key = self.ssl_certs.generate_cert(host_fqdn, node_ip, custom_san_list=custom_san_list, duration_in_days=duration_in_days)
        ca_cert = self.mgr.cert_mgr.get_root_ca()
        return CertKeyPair(cert=cert, key=key, ca_cert=ca_cert)

    def cert_exists(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        cert_obj = self.cert_store.get_tlsobject(cert_name, service_name, host)
        return cert_obj is not None

    def is_cert_editable(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        cert_obj = cast(Cert, self.cert_store.get_tlsobject(cert_name, service_name, host))
        return cert_obj.editable if cert_obj else True

    def get_cert(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        cert_obj = cast(Cert, self.cert_store.get_tlsobject(cert_name, service_name, host))
        return cert_obj.cert if cert_obj else None

    def get_key(self, key_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        key_obj = cast(PrivKey, self.key_store.get_tlsobject(key_name, service_name, host))
        return key_obj.key if key_obj else None

    def get_self_signed_cert_key_pair(self, service_name: str, hostname: str, label: Optional[str] = None) -> CertKeyPair:
        cert_obj = cast(Cert, self.cert_store.get_tlsobject(self.self_signed_cert(service_name, label), host=hostname))
        key_obj = cast(PrivKey, self.key_store.get_tlsobject(self.self_signed_key(service_name, label), host=hostname))
        cert = cert_obj.cert if cert_obj else ''
        key = key_obj.key if key_obj else ''
        ca_cert = self.mgr.cert_mgr.get_root_ca()
        return CertKeyPair(cert=cert, key=key, ca_cert=ca_cert)

    def save_cert(self, cert_name: str, cert: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False, editable: bool = False) -> None:
        self.cert_store.save_tlsobject(cert_name, cert, service_name, host, user_made, editable)

    def save_key(self, key_name: str, key: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False, editable: bool = False) -> None:
        self.key_store.save_tlsobject(key_name, key, service_name, host, user_made, editable)

    def save_self_signed_cert_key_pair(self, service_name: str, tls_pair: CertKeyPair, host: str, label: Optional[str] = None) -> None:
        ss_cert_name = self.self_signed_cert(service_name, label)
        ss_key_name = self.self_signed_key(service_name, label)
        self.cert_store.save_tlsobject(ss_cert_name, tls_pair.cert, host=host, user_made=False)
        self.key_store.save_tlsobject(ss_key_name, tls_pair.key, host=host, user_made=False)

    def rm_cert(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        return self.cert_store.rm_tlsobject(cert_name, service_name, host)

    def rm_key(self, key_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> bool:
        return self.key_store.rm_tlsobject(key_name, service_name, host)

    def rm_self_signed_cert_key_pair(self, service_name: str, host: str, label: Optional[str] = None) -> None:
        self.rm_cert(self.self_signed_cert(service_name, label), service_name, host)
        self.rm_key(self.self_signed_key(service_name, label), service_name, host)

    def cert_ls(self, filter_by: str = '',
                include_details: bool = False,
                include_cephadm_signed: bool = False) -> Dict:
        """
        signed-by filtering behavior in `cert_ls`:

        Defaults:
        - If `include_cephadm_signed` is False and no explicit `signed-by=` is provided,
          we auto-filter to show only user-made certs (and always include the root CA).
        - If the caller explicitly filters by `signed-by=...`, that explicit filter wins.

        Behavior matrix:
          +------------------------+-----------------------------+----------------------------------------------+
          | include_cephadm_signed | 'signed-by=' in filter_by?  | Effective behavior on signed-by               |
          +------------------------+-----------------------------+----------------------------------------------+
          | False                  | No                          | Auto-filter: signed-by=user  + root CA        |
          | False                  | Yes                         | Use user's explicit selector                  |
          | True                   | No                          | No auto filter (include user + cephadm)       |
          | True                   | Yes                         | Use user's explicit selector                  |
          +------------------------+-----------------------------+----------------------------------------------+
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

            if key in (CertFilterOption.SCOPE, CertFilterOption.STATUS, CertFilterOption.SIGNED_BY):
                return lambda cert_ctx: cert_ctx.get(key) == value

            # Default: unknown field selector -> nop filter (don't exclude)
            return lambda cert_ctx: True

        def build_filters() -> List[Callable[[Dict[CertFilterOption, Any]], bool]]:
            filter_exprs = [e.strip() for e in filter_by.split(',') if e.strip()]
            cert_filters = [_field_filter(expr) for expr in filter_exprs]
            # By default: filter out cephadm-signed certs unless explicitly included
            # with the exception of the cephadm root CA cert (CEPHADM_ROOT_CA_CERT) as
            # technically the user may be interested in adding it to his CA trust chain
            explicit_signed_by = any(_lhs(e) == str(CertFilterOption.SIGNED_BY) for e in filter_exprs)
            if not include_cephadm_signed and not explicit_signed_by:
                cert_filters.append(
                    lambda cert_ctx:
                    cert_ctx.get(CertFilterOption.SIGNED_BY) == 'user'
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

        if not self.certificates_health_report:
            self.mgr.remove_health_warning(CertMgr.CEPHADM_CERTMGR_HEALTH_ERR)
            return

        detailed_error_msgs = []
        invalid_count = 0
        expired_count = 0
        expiring_count = 0
        for cert_info in self.certificates_health_report:
            cert_status = cert_info.get_status_description()
            detailed_error_msgs.append(cert_status)
            if not cert_info.is_valid:
                if 'expired' in cert_info.error_info:
                    expired_count += 1
                else:
                    invalid_count += 1
            elif cert_info.is_close_to_expiration:
                expiring_count += 1

        # Generate a short description with a summary of all the detected issues
        issues = [
            f'{invalid_count} {CertStatus.INVALID}' if invalid_count > 0 else '',
            f'{expired_count} {CertStatus.EXPIRED}' if expired_count > 0 else '',
            f'{expiring_count} {CertStatus.EXPIRING}' if expiring_count > 0 else ''
        ]
        issues_description = ', '.join(filter(None, issues))  # collect only non-empty issues
        total_issues = invalid_count + expired_count + expiring_count
        short_error_msg = (f'Detected {total_issues} cephadm certificate(s) issues: {issues_description}')

        if invalid_count > 0 or expired_count > 0:
            logger.error(short_error_msg)
            self.mgr.set_health_error(CertMgr.CEPHADM_CERTMGR_HEALTH_ERR, short_error_msg, total_issues, detailed_error_msgs)
        else:
            logger.warning(short_error_msg)
            self.mgr.set_health_warning(CertMgr.CEPHADM_CERTMGR_HEALTH_ERR, short_error_msg, total_issues, detailed_error_msgs)

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
            return CertInfo(cert_name, target, cert.user_made, True, is_close_to_expiration, days_to_expiration, "")
        except ServerConfigException as e:
            return CertInfo(cert_name, target, cert.user_made, False, False, 0, str(e))

    def get_problematic_certificates(self) -> List[Tuple[CertInfo, Cert]]:

        def get_key(cert_name: str, key_name: str, target: Optional[str]) -> Optional[PrivKey]:
            try:
                tlsobj_target = self.cert_store.determine_tlsobject_target(cert_name, target)
                key = cast(PrivKey, self.key_store.get_tlsobject(key_name,
                                                                 service_name=tlsobj_target.service,
                                                                 host=tlsobj_target.host))
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
                cert_info = CertInfo(cert_name, target, cert_obj.user_made, False, False, 0, "missing key")
            else:
                # certificate has no associated key
                cert_info = self._check_certificate_state(cert_name, target, cert_obj)

            if not cert_info.is_operationally_valid():
                problematics_certs.append((cert_info, cert_obj))
            else:
                target_info = f" ({target})" if target else ""
                logger.info(f'Certificate for "{cert_name}{target_info}" is still valid for {cert_info.days_to_expiration} days.')

        return problematics_certs

    def _renew_self_signed_certificate(self, cert_info: CertInfo, cert_obj: Cert) -> bool:
        try:
            logger.info(f'Renewing cephadm-signed certificate for {cert_info.cert_name}')
            new_cert, new_key = self.ssl_certs.renew_cert(cert_obj.cert, self.mgr.certificate_duration_days)
            tlsobj_target = self.cert_store.determine_tlsobject_target(cert_info.cert_name, cert_info.target)
            self.cert_store.save_tlsobject(cert_info.cert_name, new_cert, service_name=tlsobj_target.service, host=tlsobj_target.host)
            key_name = cert_info.cert_name.replace('_cert', '_key')
            self.key_store.save_tlsobject(key_name, new_key, service_name=tlsobj_target.service, host=tlsobj_target.host)
            return True
        except SSLConfigException as e:
            logger.error(f'Error while trying to renew cephadm-signed certificate for {cert_info.cert_name}: {e}')
            return False

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
            close_to_expiry = (not cert_info.is_operationally_valid() and not self.mgr.certificate_automated_rotation_enabled)
            user_made_and_invalid = cert_obj.user_made and not cert_info.is_operationally_valid()
            return close_to_expiry or user_made_and_invalid

        def trigger_auto_fix(cert_info: CertInfo, cert_obj: Cert) -> bool:
            """Attempts to automatically fix certificate issues if possible."""
            if not self.mgr.certificate_automated_rotation_enabled or cert_obj.user_made:
                return False

            # This is a cephadm-signed certificate, let's try to fix it
            if not cert_info.is_valid:
                # Remove the invalid certificate to force regeneration
                tlsobj_target = self.cert_store.determine_tlsobject_target(cert_info.cert_name, cert_info.target)
                logger.info(
                    f'Removing invalid certificate for {cert_info.cert_name} to trigger regeneration '
                    f'(service: {tlsobj_target.service}, host: {tlsobj_target.host}).'
                )
                self.cert_store.rm_tlsobject(cert_info.cert_name, tlsobj_target.service, tlsobj_target.host)
                return True
            elif cert_info.is_close_to_expiration:
                return self._renew_self_signed_certificate(cert_info, cert_obj)
            else:
                return False

        # Process all problematic certificates and try to fix them in case automated certs renewal
        # is enabled. Successfully fixed ones are collected to trigger a service reconfiguration.
        certs_with_issues = []
        services_to_reconfig = set()
        for cert_info, cert_obj in self.get_problematic_certificates():

            logger.warning(cert_info.get_status_description())

            if requires_user_intervention(cert_info, cert_obj):
                certs_with_issues.append(cert_info)
                continue

            if fix_issues and trigger_auto_fix(cert_info, cert_obj):
                svc = self.get_associated_service(cert_info)
                if svc:
                    services_to_reconfig.add(svc)
                else:
                    logger.error(f'Cannot find the service associated with the certificate {cert_info.cert_name}')

        # Clear previously reported issues as we are newly checking all the certificates
        self.certificates_health_report = []

        # All problematic certificates have been processed. certs_with_issues now only
        # contains certificates that couldn't be fixed either because they are user-made
        # or automated rotation is disabled. In these cases, health warning or error
        # is raised to notify the user.
        self._notify_certificates_health_status(certs_with_issues)

        return list(services_to_reconfig), certs_with_issues
