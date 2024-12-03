
from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from typing import TYPE_CHECKING, Tuple, Union, List, Optional

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


class CertMgr:

    CEPHADM_ROOT_CA_CERT = 'cephadm_root_ca_cert'
    CEPHADM_ROOT_CA_KEY = 'cephadm_root_ca_key'

    def __init__(self, mgr: "CephadmOrchestrator", ip: str) -> None:
        self.ssl_certs: SSLCerts = SSLCerts()
        old_cert = mgr.cert_key_store.get_cert(self.CEPHADM_ROOT_CA_CERT)
        old_key = mgr.cert_key_store.get_key(self.CEPHADM_ROOT_CA_KEY)
        if old_key and old_cert:
            try:
                self.ssl_certs.load_root_credentials(old_cert, old_key)
            except SSLConfigException:
                raise Exception("Cannot load cephadm root CA certificates.")
        else:
            self.ssl_certs.generate_root_cert(addr=ip)
            mgr.cert_key_store.save_cert(self.CEPHADM_ROOT_CA_CERT, self.ssl_certs.get_root_cert())
            mgr.cert_key_store.save_key(self.CEPHADM_ROOT_CA_KEY, self.ssl_certs.get_root_key())

    def get_root_ca(self) -> str:
        return self.ssl_certs.get_root_cert()

    def generate_cert(
        self,
        host_fqdn: Union[str, List[str]],
        node_ip: Union[str, List[str]],
        custom_san_list: Optional[List[str]] = None,
    ) -> Tuple[str, str]:
        return self.ssl_certs.generate_cert(host_fqdn, node_ip, custom_san_list=custom_san_list)
