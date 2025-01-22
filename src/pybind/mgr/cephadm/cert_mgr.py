from typing import TYPE_CHECKING, Tuple, Union, List, Dict, Optional, cast, Any

from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from cephadm.tlsobject_types import Cert, PrivKey
from cephadm.tlsobject_store import TLSObjectStore

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


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
