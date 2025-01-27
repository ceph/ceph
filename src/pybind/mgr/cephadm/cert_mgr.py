from typing import TYPE_CHECKING, Tuple, Union, List, Dict, Optional, cast, Any
import logging
import copy

from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from typing import TYPE_CHECKING, Tuple, Union, List, Optional
from cephadm.tlsobject_types import Cert, PrivKey
from cephadm.tlsobject_store import TLSObjectStore, TLSObjectScope

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class CertMgr:

    CEPHADM_ROOT_CA_CERT = 'cephadm_root_ca_cert'
    CEPHADM_ROOT_CA_KEY = 'cephadm_root_ca_key'

    # In an effort to try and track all the certs we manage in cephadm
    # we're being explicit here and listing them out.

    ####################################################
    #  cephadm certmgr known Certificates section
    known_certs = {
        TLSObjectScope.SERVICE: [
            'iscsi_ssl_cert',
            'rgw_frontend_ssl_cert',
            'ingress_ssl_cert',
            'nvmeof_server_cert',
            'nvmeof_client_cert',
            'nvmeof_root_ca_cert',
        ],
        TLSObjectScope.HOST: [
            'grafana_cert',
        ],
        TLSObjectScope.GLOBAL: [
            'mgmt_gw_cert',
            'oauth2_proxy_cert',
            CEPHADM_ROOT_CA_CERT,
        ],
    }

    ####################################################
    #  cephadm certmgr known Keys section
    known_keys = {
        TLSObjectScope.SERVICE: [
            'iscsi_ssl_key',
            'ingress_ssl_key',
            'nvmeof_server_key',
            'nvmeof_client_key',
            'nvmeof_encryption_key',
        ],
        TLSObjectScope.HOST: [
            'grafana_key',
        ],
        TLSObjectScope.GLOBAL: [
            'mgmt_gw_key',
            'oauth2_proxy_key',
            CEPHADM_ROOT_CA_KEY,
        ],
    }

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
        self.cert_store = TLSObjectStore(self.mgr, Cert, self.known_certs)
        self.cert_store.load()
        self.key_store = TLSObjectStore(self.mgr, PrivKey, self.known_keys)
        self.key_store.load()

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

    def cert_ls(self) -> Dict[str, Union[bool, Dict[str, Dict[str, bool]]]]:
        ls: Dict = copy.deepcopy(self.cert_store.get_tlsobjects())
        for k, v in ls.items():
            if isinstance(v, dict):
                tmp: Dict[str, Any] = {key: get_certificate_info(cast(Cert, v[key]).cert) for key in v if isinstance(v[key], Cert)}
                ls[k] = tmp if tmp else {}
            elif isinstance(v, Cert):
                ls[k] = get_certificate_info(cast(Cert, v).cert) if bool(v) else False
        return ls

    def key_ls(self) -> Dict[str, Union[bool, Dict[str, bool]]]:
        ls: Dict = copy.deepcopy(self.key_store.get_tlsobjects())
        if self.CEPHADM_ROOT_CA_KEY in ls:
            del ls[self.CEPHADM_ROOT_CA_KEY]
        for k, v in ls.items():
            if isinstance(v, dict) and v:
                tmp: Dict[str, Any] = {key: get_private_key_info(cast(PrivKey, v[key]).key) for key in v if v[key]}
                ls[k] = tmp if tmp else {}
            elif isinstance(v, PrivKey):
                ls[k] = get_private_key_info(cast(PrivKey, v).key)
        return ls

    def list_entity_known_certificates(self, entity: str) -> List[str]:
        return [cert_name for cert_name, service in self.cert_to_service.items() if service == entity]

    def entity_ls(self, get_scope: bool = False) -> List[Union[str, Tuple[str, str]]]:
        if get_scope:
            return [(entity, self.determine_scope(entity)) for entity in set(self.cert_to_service.values())]
        else:
            return list(self.cert_to_service.values())

    def determine_scope(self, entity: str) -> str:
        for cert, service in self.cert_to_service.items():
            if service == entity:
                if cert in self.known_certs[TLSObjectScope.SERVICE]:
                    return TLSObjectScope.SERVICE.value
                elif cert in self.known_certs[TLSObjectScope.HOST]:
                    return TLSObjectScope.HOST.value
                elif cert in self.known_certs[TLSObjectScope.GLOBAL]:
                    return TLSObjectScope.GLOBAL.value
        return TLSObjectScope.UNKNOWN.value
