
from cephadm.ssl_cert_utils import SSLCerts
from threading import Lock
from typing import TYPE_CHECKING, Tuple, Union, List

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


class CertMgr:

    CEPHADM_ROOT_CA_CERT = 'cephadm_root_ca_cert'
    CEPHADM_ROOT_CA_KEY = 'cephadm_root_ca_key'

    def __init__(self, mgr: "CephadmOrchestrator", ip: str) -> None:
        self.lock = Lock()
        self.initialized = False
        with self.lock:
            if self.initialized:
                return
            self.initialized = True
            self.mgr = mgr
            self.ssl_certs: SSLCerts = SSLCerts()
            old_cert = self.mgr.cert_key_store.get_cert(self.CEPHADM_ROOT_CA_CERT)
            old_key = self.mgr.cert_key_store.get_key(self.CEPHADM_ROOT_CA_KEY)
            if old_key and old_cert:
                self.ssl_certs.load_root_credentials(old_cert, old_key)
            else:
                self.ssl_certs.generate_root_cert(ip)
                self.mgr.cert_key_store.save_cert(self.CEPHADM_ROOT_CA_CERT, self.ssl_certs.get_root_cert())
                self.mgr.cert_key_store.save_key(self.CEPHADM_ROOT_CA_KEY, self.ssl_certs.get_root_key())

    def get_root_ca(self) -> str:
        with self.lock:
            if self.initialized:
                return self.ssl_certs.get_root_cert()
        raise Exception("Not initialized")

    def generate_cert(self, host_fqdn: Union[str, List[str]], node_ip: str) -> Tuple[str, str]:
        with self.lock:
            if self.initialized:
                return self.ssl_certs.generate_cert(host_fqdn, node_ip)
        raise Exception("Not initialized")
