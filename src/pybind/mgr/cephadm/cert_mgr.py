
import json
from typing import TYPE_CHECKING, Tuple, Union, List, Any, Dict, Optional
from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

CERT_STORE_CERT_PREFIX = 'cert_store.cert.'
CERT_STORE_KEY_PREFIX = 'cert_store.key.'

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

    def get_cert(self, entity: str, service_name: str = '', host: str = '') -> str:
        self._validate_cert_entity(entity, service_name, host)

        cert = Cert()
        if entity in self.service_name_cert or entity in self.host_cert:
            var = service_name if entity in self.service_name_cert else host
            if var not in self.known_certs[entity]:
                return ''
            cert = self.known_certs[entity][var]
        else:
            cert = self.known_certs[entity]
        if not cert or not isinstance(cert, Cert):
            return ''
        return cert.cert

    def save_cert(self, entity: str, cert: str, service_name: str = '', host: str = '', user_made: bool = False) -> None:
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

    def rm_cert(self, entity: str, service_name: str = '', host: str = '') -> None:
        self.save_cert(entity, cert='', service_name=service_name, host=host)

    def _validate_cert_entity(self, entity: str, service_name: str = '', host: str = '') -> None:
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

    def get_key(self, entity: str, service_name: str = '', host: str = '') -> str:
        self._validate_key_entity(entity, host)

        key = PrivKey()
        if entity in self.host_key or entity in self.service_name_key:
            var = service_name if entity in self.service_name_key else host
            if var not in self.known_keys[entity]:
                return ''
            key = self.known_keys[entity][var]
        else:
            key = self.known_keys[entity]
        if not key or not isinstance(key, PrivKey):
            return ''
        return key.key

    def save_key(self, entity: str, key: str, service_name: str = '', host: str = '', user_made: bool = False) -> None:
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

    def rm_key(self, entity: str, service_name: str = '', host: str = '') -> None:
        self.save_key(entity, key='', service_name=service_name, host=host)

    def _validate_key_entity(self, entity: str, host: str = '') -> None:
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

    def __init__(self, mgr: "CephadmOrchestrator", ip: str) -> None:
        self.cert_key_store = CertKeyStore(mgr)
        self.cert_key_store.load()
        self._initialize_root_ca(ip)

    def _initialize_root_ca(self, ip: str) -> None:
        self.ssl_certs: SSLCerts = SSLCerts()
        old_cert = self.cert_key_store.get_cert(self.CEPHADM_ROOT_CA_CERT)
        old_key = self.cert_key_store.get_key(self.CEPHADM_ROOT_CA_KEY)
        if old_key and old_cert:
            try:
                self.ssl_certs.load_root_credentials(old_cert, old_key)
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

    def __getattr__(self, name: str) -> Any:
        # Delegate the method calls to `cert_key_store` if `name` matches one of its methods
        if hasattr(self.cert_key_store, name):
            return getattr(self.cert_key_store, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
