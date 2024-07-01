
from cephadm.ssl_cert_utils import SSLCerts
from threading import Lock
from typing import TYPE_CHECKING, Tuple, Union, List

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


class CertMgr:

    KV_STORE_ROOT_CERT = 'cert_mgr/root/cert'
    KV_STORE_ROOT_KEY = 'cert_mgr/root/key'

    def __init__(self, mgr: "CephadmOrchestrator", ip: str) -> None:
        self.lock = Lock()
        self.initialized = False
        with self.lock:
            if self.initialized:
                return
            self.initialized = True
            self.mgr = mgr
            self.ssl_certs: SSLCerts = SSLCerts()
            #  TODO(redo): use cert store instead of the following
            old_cert = self.mgr.get_store(self.KV_STORE_ROOT_CERT)
            old_key = self.mgr.get_store(self.KV_STORE_ROOT_KEY)
            if old_key and old_cert:
                self.ssl_certs.load_root_credentials(old_cert, old_key)
            else:
                self.ssl_certs.generate_root_cert(ip)
                self.mgr.set_store(self.KV_STORE_ROOT_CERT, self.ssl_certs.get_root_cert())
                self.mgr.set_store(self.KV_STORE_ROOT_KEY, self.ssl_certs.get_root_key())

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
