from typing import TYPE_CHECKING, Tuple, Union, List, Dict, Optional, cast, Any
import logging

from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from mgr_util import verify_tls, verify_cacrt_content, ServerConfigException
from cephadm.ssl_cert_utils import get_certificate_info, get_private_key_info
from cephadm.tlsobject_types import Cert, PrivKey
from cephadm.tlsobject_store import TLSObjectStore, TLSObjectScope, TLSObjectException

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


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
      - entities

    First ones holds all the known certificates and keys managed by cephadm. Each certificate/key has a
    pre-defined scope: Global, Host, or Service.

       - Global: The same certificates is used for all the service daemons (e.g mgmt-gateway).
       - Host: Certificates specific to individual hosts within the cluster (e.g Grafana).
       - Service: Certificates tied to specific service (e.g RGW).

    The entities mapping associates each scoped entity with its certificates. This information is needed
    to trigger the corresponding service reconfiguration when updating some certificate and also when
    setting the cert/key pair from CLI.
    """

    CEPHADM_ROOT_CA_CERT = 'cephadm_root_ca_cert'
    CEPHADM_ROOT_CA_KEY = 'cephadm_root_ca_key'
    CEPHADM_CERTMGR_HEALTH_ERR = 'CEPHADM_CERT_ERROR'

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
        self.entities: Dict[TLSObjectScope, Dict[str, Dict[str, List[str]]]] = {
            TLSObjectScope.SERVICE: {},
            TLSObjectScope.HOST: {},
            TLSObjectScope.GLOBAL: {},
        }

    def init_tlsobject_store(self) -> None:
        self.cert_store = TLSObjectStore(self.mgr, Cert, self.known_certs)
        self.cert_store.load()
        self.key_store = TLSObjectStore(self.mgr, PrivKey, self.known_keys)
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

    def register_cert_key_pair(self, entity: str, cert_name: str, key_name: str, scope: TLSObjectScope) -> None:
        """
        Registers a certificate/key for a given entity under a specific scope.

        :param entity: The entity (e.g., service, host) owning the certificate.
        :param cert_name: The name of the certificate.
        :param key_name: The name of the key.
        :param scope: The TLSObjectScope (SERVICE, HOST, GLOBAL).
        """
        self.register_cert(entity, cert_name, scope)
        self.register_key(entity, key_name, scope)

    def register_cert(self, entity: str, cert_name: str, scope: TLSObjectScope) -> None:
        self._register_tls_object(entity, cert_name, scope, "certs")

    def register_key(self, entity: str, key_name: str, scope: TLSObjectScope) -> None:
        self._register_tls_object(entity, key_name, scope, "keys")

    def _register_tls_object(self, entity: str, obj_name: str, scope: TLSObjectScope, obj_type: str) -> None:
        """
        Registers a TLS-related object (certificate or key) for a given entity under a specific scope.

        :param entity: The entity (service name) owning the TLS object.
        :param obj_name: The name of the certificate or key.
        :param scope: The TLSObjectScope (SERVICE, HOST, GLOBAL).
        :param obj_type: either "certs" or "keys".
        """
        storage = self.known_certs if obj_type == "certs" else self.known_keys

        if obj_name and obj_name not in storage[scope]:
            storage[scope].append(obj_name)

        if entity not in self.entities[scope]:
            self.entities[scope][entity] = {"certs": [], "keys": []}

        self.entities[scope][entity][obj_type].append(obj_name)

    def cert_to_entity(self, cert_name: str) -> str:
        """
        Retrieves the entity that owns a given certificate or key name.

        :param cert_name: The certificate or key name.
        :return: The entity name if found, otherwise None.
        """
        for scope_entities in self.entities.values():
            for entity, certs in scope_entities.items():
                if cert_name in certs:
                    return entity
        return 'unkown'

    def generate_cert(
        self,
        host_fqdn: Union[str, List[str]],
        node_ip: Union[str, List[str]],
        custom_san_list: Optional[List[str]] = None,
    ) -> Tuple[str, str]:
        return self.ssl_certs.generate_cert(host_fqdn, node_ip, custom_san_list=custom_san_list)

    def get_cert(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        cert_obj = cast(Cert, self.cert_store.get_tlsobject(cert_name, service_name, host))
        return cert_obj.cert if cert_obj else None

    def get_key(self, key_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        key_obj = cast(PrivKey, self.key_store.get_tlsobject(key_name, service_name, host))
        return key_obj.key if key_obj else None

    def save_cert(self, cert_name: str, cert: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self.cert_store.save_tlsobject(cert_name, cert, service_name, host, user_made)

    def save_key(self, key_name: str, key: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self.key_store.save_tlsobject(key_name, key, service_name, host, user_made)

    def rm_cert(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        self.cert_store.rm_tlsobject(cert_name, service_name, host)

    def rm_key(self, key_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        self.key_store.rm_tlsobject(key_name, service_name, host)

    def cert_ls(self, include_datails: bool = False) -> Dict:
        cert_objects: List = self.cert_store.list_tlsobjects()
        ls: Dict = {}
        for cert_name, cert_obj, target in cert_objects:
            cert_extended_info = get_certificate_info(cert_obj.cert, include_datails)
            cert_scope = self.get_cert_scope(cert_name)
            if cert_name not in ls:
                ls[cert_name] = {'scope': str(cert_scope), 'certificates': {}}
            if cert_scope == TLSObjectScope.GLOBAL:
                ls[cert_name]['certificates'] = cert_extended_info
            else:
                ls[cert_name]['certificates'][target] = cert_extended_info

        return ls

    def key_ls(self) -> Dict:
        key_objects: List = self.key_store.list_tlsobjects()
        ls: Dict = {}
        for key_name, key_obj, target in key_objects:
            priv_key_info = get_private_key_info(key_obj.key)
            key_scope = self.get_key_scope(key_name)
            if key_name not in ls:
                ls[key_name] = {'scope': str(key_scope), 'keys': {}}
            if key_scope == TLSObjectScope.GLOBAL:
                ls[key_name]['keys'] = priv_key_info
            else:
                ls[key_name]['keys'].update({target: priv_key_info})

        # we don't want this key to be leaked
        del ls[self.CEPHADM_ROOT_CA_KEY]

        return ls

    def list_entity_known_certificates(self, entity: str) -> List[str]:
        """
        Retrieves all certificates associated with a given entity.

        :param entity: The entity name.
        :return: A list of certificate names, or None if the entity is not found.
        """
        for scope, entities in self.entities.items():
            if entity in entities:
                return entities[entity]['certs']  # Return certs for the entity
        return []

    def get_entities(self, get_scope: bool = False) -> Dict[str, Any]:
        return {f'{scope}': entities for scope, entities in self.entities.items()}

    def list_entities(self) -> List[str]:
        """
        Retrieves a list of all registered entities across all scopes.
        :return: A list of entity names.
        """
        entities: List[str] = []
        for scope_entities in self.entities.values():
            entities.extend(scope_entities.keys())
        return entities

    def get_cert_scope(self, cert_name: str) -> TLSObjectScope:
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
                if "expired" in cert_info.error_info:
                    expired_count += 1
                else:
                    invalid_count += 1
            elif cert_info.is_close_to_expiration:
                expiring_count += 1

        # Generate a short description with a summery of all the detected issues
        issues = [
            f'{invalid_count} invalid' if invalid_count > 0 else '',
            f'{expired_count} expired' if expired_count > 0 else '',
            f'{expiring_count} expiring' if expiring_count > 0 else ''
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
            days_to_expiration = verify_tls(cert.cert, key.key) if key else verify_cacrt_content(cert.cert)
            is_close_to_expiration = days_to_expiration < self.mgr.certificate_renewal_threshold_days
            return CertInfo(cert_name, target, cert.user_made, True, is_close_to_expiration, days_to_expiration, "")
        except ServerConfigException as e:
            return CertInfo(cert_name, target, cert.user_made, False, False, 0, str(e))

    def prepare_certificate(self,
                            cert_name: str,
                            key_name: str,
                            host_fqdns: Union[str, List[str]],
                            host_ips: Union[str, List[str]],
                            target_host: str = '',
                            target_service: str = '',
                            ) -> Tuple[Optional[str], Optional[str]]:

        if not cert_name or not key_name:
            logger.error("Certificate name and key name must be provided when calling prepare_certificates.")
            return None, None

        cert_obj = cast(Cert, self.cert_store.get_tlsobject(cert_name, target_service, target_host))
        key_obj = cast(PrivKey, self.key_store.get_tlsobject(key_name, target_service, target_host))
        if cert_obj and key_obj:
            target = target_host or target_service
            cert_info = self._check_certificate_state(cert_name, target, cert_obj, key_obj)
            if cert_info.is_operationally_valid():
                return cert_obj.cert, key_obj.key
            elif cert_obj.user_made:
                self._notify_certificates_health_status([cert_info])
                return None, None
            else:
                logger.warning(f'Found invalid cephadm certificate/key pair {cert_name}/{key_name}, '
                               f'status: {cert_info.get_status_description()}, '
                               f'error: {cert_info.error_info}')

        # Reaching this point means either certificates are not present or they are
        # invalid cephadm-signed certificates. Either way, we will just generate new ones.
        logger.info(f'Generating cephadm-signed certificates for {cert_name}/{key_name}')
        cert, pkey = self.generate_cert(host_fqdns, host_ips)
        self.mgr.cert_mgr.save_cert(cert_name, cert, host=target_host, service_name=target_service)
        self.mgr.cert_mgr.save_key(key_name, pkey, host=target_host, service_name=target_service)
        return cert, pkey

    def get_problematic_certificates(self) -> List[Tuple[CertInfo, Cert]]:

        def get_key(cert_name: str, key_name: str, target: Optional[str]) -> Optional[PrivKey]:
            try:
                service_name, host = self.cert_store.determine_tlsobject_target(cert_name, target)
                key = cast(PrivKey, self.key_store.get_tlsobject(key_name, service_name=service_name, host=host))
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
            elif key_name in self.known_keys:
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
            service_name, host = self.cert_store.determine_tlsobject_target(cert_info.cert_name, cert_info.target)
            self.cert_store.save_tlsobject(cert_info.cert_name, new_cert, service_name=service_name, host=host)
            key_name = cert_info.cert_name.replace('_cert', '_key')
            self.key_store.save_tlsobject(key_name, new_key, service_name=service_name, host=host)
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
                service_name, host = self.cert_store.determine_tlsobject_target(cert_info.cert_name, cert_info.target)
                logger.info(
                    f'Removing invalid certificate for {cert_info.cert_name} to trigger regeneration '
                    f'(service: {service_name}, host: {host}).'
                )
                self.cert_store.rm_tlsobject(cert_info.cert_name, service_name, host)
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
                services_to_reconfig.add(self.cert_to_entity(cert_info.cert_name))

        # Clear previously reported issues as we are newly checking all the certifiactes
        self.certificates_health_report = []

        # All problematic certificates have been processed. certs_with_issues now only
        # contains certificates that couldn't be fixed either because they are user-made
        # or automated rotation is disabled. In these cases, health warning or error
        # is raised to notify the user.
        self._notify_certificates_health_status(certs_with_issues)

        return list(services_to_reconfig), certs_with_issues
