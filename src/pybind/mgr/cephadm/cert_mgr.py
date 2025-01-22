from typing import TYPE_CHECKING, Tuple, Union, List, Dict, Optional, cast, Any
import logging
import copy

from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from mgr_util import verify_tls, ServerConfigException, get_cert_issuer_info, verify_cacrt_content
from cephadm.tlsobject_types import Cert, PrivKey
from cephadm.tlsobject_store import TLSObjectStore

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
    def __init__(self, entity: str,
                 target: str,
                 is_valid: bool = False,
                 is_close_to_expiration: bool = False,
                 days_to_expiration: int = 0,
                 error_info: str = ''):
        self.entity = entity
        self.target = target
        self.is_valid = is_valid
        self.is_close_to_expiration = is_close_to_expiration
        self.days_to_expiration = days_to_expiration
        self.error_info = error_info


class CertMgr:

    CEPHADM_ROOT_CA_CERT = 'cephadm_root_ca_cert'
    CEPHADM_ROOT_CA_KEY = 'cephadm_root_ca_key'

    ####################################################
    #  cephadm certmgr known Certificates section
    service_name_cert = [
        'iscsi_ssl_cert',
        'rgw_frontend_ssl_cert',
        'ingress_ssl_cert',
        'nvmeof_server_cert',
        'nvmeof_client_cert',
        'nvmeof_root_ca_cert',
    ]

    host_cert = [
        'grafana_cert',
    ]

    general_cert = [
        'mgmt_gw_cert',
        'oauth2_proxy_cert',
        CEPHADM_ROOT_CA_CERT,
    ]

    ####################################################
    #  cephadm certmgr known Keys section
    service_name_key = [
        'iscsi_ssl_key',
        'ingress_ssl_key',
        'nvmeof_server_key',
        'nvmeof_client_key',
        'nvmeof_encryption_key',
    ]

    host_key = [
        'grafana_key',
    ]

    general_key = [
        'mgmt_gw_key',
        'oauth2_proxy_key',
        CEPHADM_ROOT_CA_KEY,
    ]

    # Entries in known_certs that don't have a key here are probably
    # certs in PEM format so there is no need to store a separate key
    known_certs = service_name_cert + host_cert + general_cert
    known_keys = service_name_key + host_key + general_key

    # In an effort to try and track all the certs we manage in cephadm
    # we're being explicit here and listing them out.
    cert_to_service = {
        'rgw_frontend_ssl_cert': 'rgw',
        'iscsi_ssl_cert': 'iscsi',
        'ingress_ssl_cert': 'ingress',
        'nvmeof_server_cert': 'nvmeof',
        'nvmeof_client_cert': 'nvmeof',
        'nvmeof_root_ca_cert': 'nvmeof',
        'mgmt_gw_cert': 'mgmt-gateway',
        'oauth2_proxy_cert': 'oauth2-proxy',
        'grafana_cert': 'grafana',
    }

    def __init__(self,
                 mgr: "CephadmOrchestrator",
                 certificate_automated_rotation_enabled: bool,
                 certificate_duration_days: int,
                 renewal_threshold_days: int,
                 mgr_ip: str) -> None:
        self.mgr = mgr
        self.mgr_ip = mgr_ip
        self.certificate_automated_rotation_enabled = certificate_automated_rotation_enabled
        self.certificate_duration_days = certificate_duration_days
        self.renewal_threshold_days = renewal_threshold_days
        self._init_tlsobject_store()
        self._initialize_root_ca(mgr_ip)

    def _init_tlsobject_store(self) -> None:
        self.cert_store = TLSObjectStore(self.mgr, Cert, self.known_certs, self.service_name_cert, self.host_cert, self.general_cert)
        self.cert_store.load()
        self.key_store = TLSObjectStore(self.mgr, PrivKey, self.known_keys, self.service_name_key, self.host_key, self.general_key)
        self.key_store.load()

    def get_cert(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        cert_obj = cast(Cert, self.cert_store.get_tlsobject(entity, service_name, host))
        return cert_obj.cert if cert_obj else None

    def get_key(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        key_obj = cast(PrivKey, self.key_store.get_tlsobject(entity, service_name, host))
        return key_obj.key if key_obj else None

    def save_cert(self, entity: str, cert: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self.cert_store.save_tlsobject(entity, cert, service_name, host, user_made)

    def save_key(self, entity: str, key: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self.key_store.save_tlsobject(entity, key, service_name, host, user_made)

    def rm_cert(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        self.cert_store.rm_tlsobject(entity, service_name, host)

    def rm_key(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        self.key_store.rm_tlsobject(entity, service_name, host)

    def cert_ls(self) -> Dict[str, Union[bool, Dict[str, Dict[str, bool]]]]:

        def get_cert_info(cert: Cert) -> Dict:
            try:
                org, cn = get_cert_issuer_info(cert.cert)
                days_to_expiration = verify_cacrt_content(cert.cert)
                return {
                    'user_made': cert.user_made,
                    'org': org,
                    'cn': cn,
                    'days_to_expiration': days_to_expiration,
                }
            except ServerConfigException as e:
                return {
                    'user_made': cert.user_made,
                    'invalid_certificate': f'{e}'
                }

        ls: Dict = copy.deepcopy(self.cert_store.tlsobject_ls())
        for k, v in ls.items():
            if isinstance(v, dict):
                tmp: Dict[str, Any] = {key: get_cert_info(v[key]) for key in v if isinstance(v[key], Cert)}
                ls[k] = tmp if tmp else False
            elif isinstance(v, Cert):
                ls[k] = get_cert_info(v) if bool(v) else False

        return ls

    def key_ls(self) -> Dict[str, Union[bool, Dict[str, bool]]]:
        ls: Dict = copy.deepcopy(self.key_store.tlsobject_ls())
        if self.CEPHADM_ROOT_CA_KEY in ls:
            del ls[self.CEPHADM_ROOT_CA_KEY]
        for k, v in ls.items():
            if isinstance(v, dict):
                tmp: Dict[str, Any] = {key: True for key in v if v[key]}
                ls[k] = tmp if tmp else False
            elif isinstance(v, PrivKey):
                ls[k] = bool(v)
        return ls

    def load(self) -> None:
        self.cert_store.load()
        self.key_store.load()

    def _initialize_root_ca(self, ip: str) -> None:
        self.ssl_certs: SSLCerts = SSLCerts(self.certificate_duration_days)
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

    def generate_cert(
        self,
        host_fqdn: Union[str, List[str]],
        node_ip: Union[str, List[str]],
        custom_san_list: Optional[List[str]] = None,
    ) -> Tuple[str, str]:
        return self.ssl_certs.generate_cert(host_fqdn, node_ip, custom_san_list=custom_san_list)

    def _raise_certificate_health_warning(self, cert_info: CertInfo, cert_obj: Cert) -> None:
        target = f'{cert_info.target}' if cert_info.target else ''
        cert_details = f'service: {cert_info.entity}{target}, remaining days: {cert_info.days_to_expiration}'
        short_err_msg = ''
        detailed_err_msg = ''
        if not cert_info.is_valid:
            short_err_msg = f'Invalid certificate for {cert_details}: {cert_info.error_info}'
            detailed_err_msg = (
                f'Detected invalid certificate for {cert_details}. '
                'Please use appropriate commands to set a valid key and certificate or reset them to an empty string for cephadm to generate self-signed certificates. '
                'Reconfigure affected daemons as needed.'
            )
        elif cert_info.is_close_to_expiration:
            short_err_msg = f'Certificate for {cert_details} is close to expiration.'
            detailed_err_msg = (
                f'The certificate for {cert_details} is close to expiration. '
                'Please replace it with a valid certificate and reconfigure the affected service(s) or daemon(s) as necessary.'
            )
        else:
            short_err_msg = f'Certificate for {cert_details}: has expired'
            detailed_err_msg = (
                f'Detected an expired certificate for {cert_details}. '
                'Please use appropriate commands to set a valid key and certificate or reset them to an empty string for cephadm to generate self-signed certificates. '
                'Reconfigure affected daemons as needed.'
            )

        self.mgr.set_health_warning('CEPHADM_CERT_ERROR',
                                    short_err_msg,
                                    1,
                                    [detailed_err_msg]
                                    )

    def is_valid_certificate(self, entity: str, target: str, cert: str, key: str) -> CertInfo:
        """
        Checks if a certificate is valid and close to expiration.

        Returns:
            - is_valid: True if the certificate is valid.
            - is_close_to_expiration: True if the certificate is close to expiration.
            - days_to_expiration: Number of days until expiration.
            - exception_info: Details of any exception encountered during validation.
        """
        cert_obj = Cert(cert, True)
        key_obj = PrivKey(key, True)
        return self._is_valid_certificate(entity, target, cert_obj, key_obj)

    def _is_valid_certificate(self, entity: str, target: str, cert: Cert, key: PrivKey) -> CertInfo:
        """
        Checks if a certificate is valid and close to expiration.

        Returns: CertInfo
        """
        try:
            days_to_expiration = verify_tls(cert.cert, key.key)
            is_close_to_expiration = days_to_expiration < self.renewal_threshold_days
            return CertInfo(entity, target, True, is_close_to_expiration, days_to_expiration, "")
        except ServerConfigException as e:
            return CertInfo(entity, target, False, False, 0, str(e))

    def _validate_and_manage_certificate(self, cert_entity: str, cert_obj: Cert, key_obj: PrivKey, target: str = '') -> CertInfo:
        """Helper method to validate a cert/key pair and handle errors."""

        cert_info = self._is_valid_certificate(cert_entity, target, cert_obj, key_obj)
        cert_source = 'user-made' if cert_obj.user_made else 'self-signed'

        if cert_info.is_close_to_expiration:
            logger.warning(f'Detected a {cert_source} certificate close to its expiration, {cert_info}')
            if self.certificate_automated_rotation_enabled:
                self._renew_certificate(cert_info, cert_obj)
            else:
                self._raise_certificate_health_warning(cert_info, cert_obj)
        elif not cert_info.is_valid:
            logger.warning(f'Detected a {cert_source} invalid certificate, {cert_info}')
            if cert_obj.user_made:
                # TODO(redo): should we proceed in this case once ACME is setup?
                self._raise_certificate_health_warning(cert_info, cert_obj)
            else:
                # self-signed invalid certificate.. shouldn't happen but let's try to renew it
                service_name, host = self.get_cert_target(cert_entity, target)
                logger.info(f'Removing invalid certificate for {cert_entity} to trigger regeneration (service: {service_name}, host: {host}).')
                self.cert_store.rm_tlsobject(cert_entity, service_name, host)
        else:
            target_info = f" ({target})" if target else ""
            logger.info(f'Certificate for "{cert_entity}{target_info}" is still valid for {cert_info.days_to_expiration} days.')
            self.mgr.remove_health_warning('CEPHADM_CERT_ERROR')

        return cert_info

    def _renew_certificate(self, cert_info: CertInfo, cert_obj: Cert) -> None:
        """Renew a self-signed or user-made certificate."""
        if cert_obj.user_made:
            # By now we just trigger a health warning since we don't have ACME support yet
            self._raise_certificate_health_warning(cert_info, cert_obj)
        else:
            try:
                logger.info(f'Renewing self-signed certificate for {cert_info.entity}')
                new_cert, new_key = self.ssl_certs.renew_cert(cert_obj.cert, self.certificate_duration_days)
                self.cert_store.save_tlsobject(cert_info.entity, new_cert)
                self.key_store.save_tlsobject(cert_info.entity, new_key)
            except SSLConfigException as e:
                logger.error(f'Error while trying to renew self-signed certificate for {cert_info.entity}: {e}')

    def get_cert_target(self, cert_entity: str, entity: str) -> Tuple[Optional[str], Optional[str]]:
        """Determine the service name or host based on the cert_entity."""
        service_name = entity if cert_entity in self.service_name_cert else None
        host = entity if cert_entity in self.host_cert else None
        return service_name, host

    def check_certificates(self) -> List[str]:

        services_to_reconfig = set()

        def get_cert_and_key(cert_entity: str, entity: str = '') -> Tuple[Optional[Cert], Optional[PrivKey], str]:
            """Retrieve certificate and key, translating names as necessary."""
            service_name, host = self.get_cert_target(cert_entity, entity) if entity else (None, None)
            cert = cast(Cert, self.cert_store.get_tlsobject(cert_entity, service_name=service_name, host=host))
            key_entity = cert_entity.replace("_cert", "_key")
            key = cast(PrivKey, self.key_store.get_tlsobject(key_entity, service_name=service_name, host=host))
            return cert, key, entity

        def process_certificate(cert_entity: str, cert_obj: Optional[Cert], key_obj: Optional[PrivKey], target: str = '') -> None:
            nonlocal services_to_reconfig
            if cert_obj and key_obj:
                cert_state = self._validate_and_manage_certificate(cert_entity, cert_obj, key_obj, target)
                if (not cert_state.is_valid or cert_state.is_close_to_expiration) and not cert_obj.user_made:
                    services_to_reconfig.add(self.cert_to_service[cert_entity])
            elif cert_obj:
                # Edge case where cert is present but key is None
                # this could only happen if somebody has put manually a bad key!
                logger.warning(f"Key is missing for certificate '{cert_entity}'. Attempting renewal.")
                cert_info = CertInfo(cert_entity, target)
                self._renew_certificate(cert_info, cert_obj)

        for cert_entity, cert_entries in self.cert_store.tlsobject_ls().items():
            if not cert_entries:
                continue

            if isinstance(cert_entries, dict):
                # Process only valid instances
                for target in [entry for entry, exists in cert_entries.items() if exists]:
                    cert, key, target = get_cert_and_key(cert_entity, target)
                    process_certificate(cert_entity, cert, key, target)
            else:
                # Global cert case
                cert, key, _ = get_cert_and_key(cert_entity)
                process_certificate(cert_entity, cert, key)

        logger.info(f'redo: services to reconfigure {services_to_reconfig}')

        # return the list of services that need reconfiguration
        return list(services_to_reconfig)
