import json
from typing import TYPE_CHECKING, Tuple, Union, List, Any, Dict, Optional
import logging

from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from orchestrator import OrchestratorError
from mgr_util import verify_tls, get_cert_issuer_info, ServerConfigException

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

CERT_STORE_CERT_PREFIX = 'cert_store.cert.'
CERT_STORE_KEY_PREFIX = 'cert_store.key.'

logger = logging.getLogger(__name__)


class Cert():
    def __init__(self, cert: str = '', user_made: bool = False) -> None:
        self.cert = cert
        self.user_made = user_made

    def __bool__(self) -> bool:
        return bool(self.cert)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Cert):
            return self.cert == other.cert and self.user_made == other.user_made
        return NotImplemented

    def to_json(self) -> Dict[str, Union[str, bool]]:
        return {
            'cert': self.cert,
            'user_made': self.user_made
        }

    @classmethod
    def from_json(cls, data: Dict[str, Union[str, bool]]) -> 'Cert':
        if 'cert' not in data:
            return cls()
        cert = data['cert']
        if not isinstance(cert, str):
            raise OrchestratorError('Tried to make Cert object with non-string cert')
        if any(k not in ['cert', 'user_made'] for k in data.keys()):
            raise OrchestratorError(f'Got unknown field for Cert object. Fields: {data.keys()}')
        user_made: Union[str, bool] = data.get('user_made', False)
        if not isinstance(user_made, bool):
            if isinstance(user_made, str):
                if user_made.lower() == 'true':
                    user_made = True
                elif user_made.lower() == 'false':
                    user_made = False
            try:
                user_made = bool(user_made)
            except Exception:
                raise OrchestratorError(f'Expected user_made field in Cert object to be bool but got {type(user_made)}')
        return cls(cert=cert, user_made=user_made)


class PrivKey():
    def __init__(self, key: str = '', user_made: bool = False) -> None:
        self.key = key
        self.user_made = user_made

    def __bool__(self) -> bool:
        return bool(self.key)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PrivKey):
            return self.key == other.key and self.user_made == other.user_made
        return NotImplemented

    def to_json(self) -> Dict[str, Union[str, bool]]:
        return {
            'key': self.key,
            'user_made': self.user_made
        }

    @classmethod
    def from_json(cls, data: Dict[str, str]) -> 'PrivKey':
        if 'key' not in data:
            return cls()
        key = data['key']
        if not isinstance(key, str):
            raise OrchestratorError('Tried to make PrivKey object with non-string key')
        if any(k not in ['key', 'user_made'] for k in data.keys()):
            raise OrchestratorError(f'Got unknown field for PrivKey object. Fields: {data.keys()}')
        user_made: Union[str, bool] = data.get('user_made', False)
        if not isinstance(user_made, bool):
            if isinstance(user_made, str):
                if user_made.lower() == 'true':
                    user_made = True
                elif user_made.lower() == 'false':
                    user_made = False
            try:
                user_made = bool(user_made)
            except Exception:
                raise OrchestratorError(f'Expected user_made field in PrivKey object to be bool but got {type(user_made)}')
        return cls(key=key, user_made=user_made)


class CertKeyStore():
    service_name_cert = [
        'rgw_frontend_ssl_cert',
        'iscsi_ssl_cert',
        'ingress_ssl_cert',
        'nvmeof_server_cert',
        'nvmeof_client_cert',
        'nvmeof_root_ca_cert',
    ]

    host_cert = [
        'grafana_cert',
    ]

    host_key = [
        'grafana_key',
    ]

    service_name_key = [
        'iscsi_ssl_key',
        'ingress_ssl_key',
        'nvmeof_server_key',
        'nvmeof_client_key',
    ]

    known_certs: Dict[str, Any] = {}
    known_keys: Dict[str, Any] = {}

    def __init__(self, mgr: 'CephadmOrchestrator') -> None:
        self.mgr: CephadmOrchestrator = mgr
        self._init_known_cert_key_dicts()

    def _init_known_cert_key_dicts(self) -> None:
        # In an effort to try and track all the certs we manage in cephadm
        # we're being explicit here and listing them out.
        self.cert_to_service = {
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

        self.known_certs = {
            'rgw_frontend_ssl_cert': {},  # service-name -> cert
            'iscsi_ssl_cert': {},  # service-name -> cert
            'ingress_ssl_cert': {},  # service-name -> cert
            'nvmeof_server_cert': {},  # service-name -> cert
            'nvmeof_client_cert': {},  # service-name -> cert
            'nvmeof_root_ca_cert': {},  # service-name -> cert
            'mgmt_gw_cert': Cert(),  # cert
            'oauth2_proxy_cert': Cert(),  # cert
            'cephadm_root_ca_cert': Cert(),  # cert
            'grafana_cert': {},  # host -> cert
        }
        # Similar to certs but for priv keys. Entries in known_certs
        # that don't have a key here are probably certs in PEM format
        # so there is no need to store a separate key
        self.known_keys = {
            'mgmt_gw_key': PrivKey(),  # cert
            'oauth2_proxy_key': PrivKey(),  # cert
            'cephadm_root_ca_key': PrivKey(),  # cert
            'grafana_key': {},  # host -> key
            'iscsi_ssl_key': {},  # service-name -> key
            'ingress_ssl_key': {},  # service-name -> key
            'nvmeof_server_key': {},  # service-name -> key
            'nvmeof_client_key': {},  # service-name -> key
        }

    def get_key_name_from_cert(self, cert_ref: str) -> str:
        """Translate a certificate reference name to its corresponding key reference name."""
        return cert_ref.replace("_cert", "_key")

    def get_cert(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[Cert]:
        self._validate_cert_entity(entity, service_name, host)

        cert = Cert()
        if entity in self.service_name_cert or entity in self.host_cert:
            var = service_name if entity in self.service_name_cert else host
            if var not in self.known_certs[entity]:
                return None
            cert = self.known_certs[entity][var]
        else:
            cert = self.known_certs[entity]
        if not cert or not isinstance(cert, Cert):
            return None
        return cert

    def save_cert(self, entity: str, cert: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self._validate_cert_entity(entity, service_name, host)

        cert_obj = Cert(cert, user_made)

        j: Union[str, Dict[Any, Any], None] = None
        if entity in self.service_name_cert or entity in self.host_cert:
            var = service_name if entity in self.service_name_cert else host
            j = {}
            self.known_certs[entity][var] = cert_obj
            for service_name in self.known_certs[entity].keys():
                j[var] = Cert.to_json(self.known_certs[entity][var])
        else:
            self.known_certs[entity] = cert_obj
            j = Cert.to_json(cert_obj)
        self.mgr.set_store(CERT_STORE_CERT_PREFIX + entity, json.dumps(j))

    def rm_cert(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        self.save_cert(entity, cert='', service_name=service_name, host=host)

    def _validate_cert_entity(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        if entity not in self.known_certs.keys():
            raise OrchestratorError(f'Attempted to access cert for unknown entity {entity}')

        if entity in self.host_cert and not host:
            raise OrchestratorError(f'Need host to access cert for entity {entity}')

        if entity in self.service_name_cert and not service_name:
            raise OrchestratorError(f'Need service name to access cert for entity {entity}')

    def cert_ls(self) -> Dict[str, Union[bool, Dict[str, bool]]]:
        ls: Dict[str, Any] = {}
        for k, v in self.known_certs.items():
            if k in self.service_name_cert or k in self.host_cert:
                tmp: Dict[str, Any] = {key: True for key in v if v[key]}
                ls[k] = tmp if tmp else False
            else:
                ls[k] = bool(v)
        return ls

    def get_key(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[PrivKey]:
        self._validate_key_entity(entity, host)

        key = PrivKey()
        if entity in self.host_key or entity in self.service_name_key:
            var = service_name if entity in self.service_name_key else host
            if var not in self.known_keys[entity]:
                return None
            key = self.known_keys[entity][var]
        else:
            key = self.known_keys[entity]
        if not key or not isinstance(key, PrivKey):
            return None
        return key

    def save_key(self, entity: str, key: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self._validate_key_entity(entity, host)

        pkey = PrivKey(key, user_made)

        j: Union[str, Dict[Any, Any], None] = None
        if entity in self.host_key or entity in self.service_name_key:
            var = service_name if entity in self.service_name_key else host
            j = {}
            self.known_keys[entity][var] = pkey
            for k in self.known_keys[entity]:
                j[k] = PrivKey.to_json(self.known_keys[entity][k])
        else:
            self.known_keys[entity] = pkey
            j = PrivKey.to_json(pkey)
        self.mgr.set_store(CERT_STORE_KEY_PREFIX + entity, json.dumps(j))

    def rm_key(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        self.save_key(entity, key='', service_name=service_name, host=host)

    def _validate_key_entity(self, entity: str, host: Optional[str] = None) -> None:
        if entity not in self.known_keys.keys():
            raise OrchestratorError(f'Attempted to access priv key for unknown entity {entity}')

        if entity in self.host_key and not host:
            raise OrchestratorError(f'Need host to access priv key for entity {entity}')

    def key_ls(self) -> Dict[str, Union[bool, Dict[str, bool]]]:
        ls: Dict[str, Any] = {}
        for k, v in self.known_keys.items():
            if k in self.host_key or k in self.service_name_key:
                tmp: Dict[str, Any] = {key: True for key in v if v[key]}
                ls[k] = tmp if tmp else False
            else:
                ls[k] = bool(v)
        return ls

    def load(self) -> None:
        for k, v in self.mgr.get_store_prefix(CERT_STORE_CERT_PREFIX).items():
            entity = k[len(CERT_STORE_CERT_PREFIX):]
            self.known_certs[entity] = json.loads(v)
            if entity in self.service_name_cert or entity in self.host_cert:
                for k in self.known_certs[entity]:
                    cert_obj = Cert.from_json(self.known_certs[entity][k])
                    self.known_certs[entity][k] = cert_obj
            else:
                cert_obj = Cert.from_json(self.known_certs[entity])
                self.known_certs[entity] = cert_obj

        for k, v in self.mgr.get_store_prefix(CERT_STORE_KEY_PREFIX).items():
            entity = k[len(CERT_STORE_KEY_PREFIX):]
            self.known_keys[entity] = json.loads(v)
            if entity in self.host_key or entity in self.service_name_key:
                for k in self.known_keys[entity]:
                    priv_key_obj = PrivKey.from_json(self.known_keys[entity][k])
                    self.known_keys[entity][k] = priv_key_obj
            else:
                priv_key_obj = PrivKey.from_json(self.known_keys[entity])
                self.known_keys[entity] = priv_key_obj


class CertMgr:

    CEPHADM_ROOT_CA_CERT = 'cephadm_root_ca_cert'
    CEPHADM_ROOT_CA_KEY = 'cephadm_root_ca_key'

    def __init__(self,
                 mgr: "CephadmOrchestrator",
                 certificate_duration_days: int,
                 renewal_threshold_days: int,
                 ip: str) -> None:
        self.mgr = mgr
        self.certificate_duration_days = certificate_duration_days
        self.renewal_threshold_days = renewal_threshold_days
        self.cert_key_store = CertKeyStore(mgr)
        self.cert_key_store.load()
        self._initialize_root_ca(ip)

    # Delegate the method calls to `cert_key_store` if `name` matches one of its methods
    def __getattr__(self, name: str) -> Any:
        if hasattr(self.cert_key_store, name):
            return getattr(self.cert_key_store, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def get_cert(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        cert_obj = self.cert_key_store.get_cert(entity, service_name, host)
        return cert_obj.cert if cert_obj else None

    def get_key(self, entity: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        key_obj = self.cert_key_store.get_key(entity, service_name, host)
        return key_obj.key if key_obj else None

    def _initialize_root_ca(self, ip: str) -> None:
        self.ssl_certs: SSLCerts = SSLCerts(self.certificate_duration_days)
        old_cert = self.cert_key_store.get_cert(self.CEPHADM_ROOT_CA_CERT)
        old_key = self.cert_key_store.get_key(self.CEPHADM_ROOT_CA_KEY)
        if old_key and old_cert:
            try:
                self.ssl_certs.load_root_credentials(old_cert.cert, old_key.key)
            except SSLConfigException as e:
                raise SSLConfigException("Cannot load cephadm root CA certificates.") from e
        else:
            self.ssl_certs.generate_root_cert(addr=ip)
            self.cert_key_store.save_cert(self.CEPHADM_ROOT_CA_CERT, self.ssl_certs.get_root_cert())
            self.cert_key_store.save_key(self.CEPHADM_ROOT_CA_KEY, self.ssl_certs.get_root_key())

    def get_root_ca(self) -> str:
        return self.ssl_certs.get_root_cert()

    def generate_cert(
        self,
        host_fqdn: Union[str, List[str]],
        node_ip: Union[str, List[str]],
        custom_san_list: Optional[List[str]] = None,
    ) -> Tuple[str, str]:
        return self.ssl_certs.generate_cert(host_fqdn, node_ip, custom_san_list=custom_san_list)

    def is_valid_certificate(self, cert_ref: str, cert_obj: Cert, key_obj: PrivKey, entity: str = '') -> (bool, bool):
        """Helper method to validate a cert/key pair and handle errors."""
        key = key_obj.key
        cert = cert_obj.cert
        is_valid = True
        is_close_to_expiration = False
        exception_info = ''
        try:
            (org, cn) = get_cert_issuer_info(cert)
            days_to_expiration = verify_tls(cert, key)
            is_close_to_expiration = (days_to_expiration < self.renewal_threshold_days)
            if is_close_to_expiration:
                logger.info(f'Certificate for "{cert_ref}" must be renewed as only {days_to_expiration} are left for expiration.')
        except ServerConfigException as e:
            is_valid = False
            exception_info = f'{e}'

        if is_close_to_expiration or not is_valid:
            if cert_obj.user_made:
                # TODO(redo): contact ACME to get a new certificate
                # logger.info(f'Triggering ACME certificate renewal for {cert_ref}')
                # self.enqueue_acme_certificate_renewal(cert_ref)
                entity_info = f" ({entity})" if entity else ""
                err_msg = f"""
                Detected invalid certificate for {cert_ref}{entity_info}. Please, use the appropriate commands to set a valid
                key and certificate or reset their value to an empty string if you want cephadm to generate self-signed certificates.
                Once done, reconfigure the affected daemons as needed.
                """
                logger.error(f'Detected invalid certificate for {cert_ref}{entity_info}: {exception_info}')
                self.mgr.set_health_warning(
                    'CEPHADM_CERT_ERROR',
                    f'Invalid certificate for {cert_ref}{entity_info}: {exception_info}',
                    1,
                    [err_msg]
                )
            else:
                # Trigger automated certificate renewal using local self-signed certificates
                logger.info(f'Triggering local certificate renewal for {cert_ref}')
                if is_close_to_expiration:
                    logger.info(f'Certificate {cert_ref} is valid, trying to renew self-signed certificates')
                    new_cert, new_key = self.ssl_certs.renew_cert(cert, self.certificate_duration_days)
                    self.cert_key_store.save_cert(cert_ref, new_cert)
                    self.cert_key_store.save_key(cert_ref, new_key)
                else:
                    service_name, host = self.get_service_or_host(cert_ref, entity)
                    logger.info(f'Certificate {cert_ref} is not valid, removing to force a new certificate generation service_name: {service_name}, host: {host}')
                    self.cert_key_store.rm_cert(cert_ref, service_name, host)

        else:
            logger.info(f'Certificate for "{cert_ref}" is still valid for {days_to_expiration} days.')
            self.mgr.remove_health_warning('CEPHADM_CERT_ERROR')

        return is_valid

    def get_service_or_host(self, cert_ref: str, entity: str) -> Tuple[Optional[str], Optional[str]]:
        """Determine the service name or host based on the cert_ref."""
        service_name = entity if cert_ref in self.cert_key_store.service_name_cert else None
        host = entity if cert_ref in self.cert_key_store.host_cert else None
        return service_name, host

    def check_certificates(self) -> List[str]:

        def get_cert_and_key(cert_ref: str, entity: str = '') -> Tuple[Optional[Cert], Optional[PrivKey], str]:
            """Retrieve certificate and key, translating names as necessary."""
            service_name, host = self.get_service_or_host(cert_ref, entity) if entity else (None, None)
            cert = self.cert_key_store.get_cert(cert_ref, service_name=service_name, host=host)
            key_ref = self.cert_key_store.get_key_name_from_cert(cert_ref)
            key = self.cert_key_store.get_key(key_ref, service_name=service_name, host=host)
            return cert, key, entity

        # services_to_reconfig = self.get_acme_ready_certificates()
        services_to_reconfig = []
        for cert_ref, cert_entries in self.cert_key_store.cert_ls().items():
            if not cert_entries:
                # No certificates to check for this reference
                continue

            if isinstance(cert_entries, dict):
                # Process only valid instances
                for entity in [entry for entry, exists in cert_entries.items() if exists]:
                    cert, key, entity = get_cert_and_key(cert_ref, entity)
                    if (cert and key) and not self.is_valid_certificate(cert_ref, cert, key, entity):
                        # TODO(redo): get srv name from the entity info
                        services_to_reconfig.append(self.cert_key_store.cert_to_service[cert_ref])
            else:
                # Global cert case
                cert, key, _ = get_cert_and_key(cert_ref)
                if (cert and key) and not self.is_valid_certificate(cert_ref, cert, key):
                    services_to_reconfig.append(self.cert_key_store.cert_to_service[cert_ref])

        logger.info(f'redo: services to reconfigure {services_to_reconfig}')

        # return the list of services that need reconfiguration
        return services_to_reconfig
