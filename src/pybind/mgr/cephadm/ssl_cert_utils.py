
from typing import Any, Tuple, IO, List, Union, Optional
import ipaddress

from datetime import datetime, timedelta
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.backends import default_backend


class SSLConfigException(Exception):
    pass


class SSLCerts:
    def __init__(self) -> None:
        self.root_cert: Any
        self.root_key: Any
        self.key_file: IO[bytes]
        self.cert_file: IO[bytes]

    def generate_root_cert(
        self,
        addr: Optional[str] = None,
        custom_san_list: Optional[List[str]] = None
    ) -> Tuple[str, str]:
        self.root_key = rsa.generate_private_key(
            public_exponent=65537, key_size=4096, backend=default_backend())
        root_public_key = self.root_key.public_key()
        root_builder = x509.CertificateBuilder()
        root_builder = root_builder.subject_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u'cephadm-root'),
        ]))
        root_builder = root_builder.issuer_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u'cephadm-root'),
        ]))
        root_builder = root_builder.not_valid_before(datetime.now())
        root_builder = root_builder.not_valid_after(datetime.now() + timedelta(days=(365 * 10 + 3)))
        root_builder = root_builder.serial_number(x509.random_serial_number())
        root_builder = root_builder.public_key(root_public_key)

        san_list: List[x509.GeneralName] = []
        if addr:
            san_list.extend([x509.IPAddress(ipaddress.ip_address(addr))])
        if custom_san_list:
            san_list.extend([x509.DNSName(n) for n in custom_san_list])
        root_builder = root_builder.add_extension(
            x509.SubjectAlternativeName(
                san_list
            ),
            critical=False
        )

        root_builder = root_builder.add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=True,
        )

        self.root_cert = root_builder.sign(
            private_key=self.root_key, algorithm=hashes.SHA256(), backend=default_backend()
        )

        cert_str = self.root_cert.public_bytes(encoding=serialization.Encoding.PEM).decode('utf-8')
        key_str = self.root_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ).decode('utf-8')

        return (cert_str, key_str)

    def generate_cert(
        self,
        _hosts: Union[str, List[str]],
        _addrs: Union[str, List[str]],
        custom_san_list: Optional[List[str]] = None,
    ) -> Tuple[str, str]:

        addrs = [_addrs] if isinstance(_addrs, str) else _addrs
        hosts = [_hosts] if isinstance(_hosts, str) else _hosts

        valid_ips = True
        try:
            ips = [x509.IPAddress(ipaddress.ip_address(addr)) for addr in addrs]
        except Exception:
            valid_ips = False

        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=4096, backend=default_backend())
        public_key = private_key.public_key()

        builder = x509.CertificateBuilder()
        builder = builder.subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, addrs[0]), ]))
        builder = builder.issuer_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u'cephadm-root'), ]))
        builder = builder.not_valid_before(datetime.now())
        builder = builder.not_valid_after(datetime.now() + timedelta(days=(365 * 10 + 3)))
        builder = builder.serial_number(x509.random_serial_number())
        builder = builder.public_key(public_key)

        san_list: List[x509.GeneralName] = [x509.DNSName(host) for host in hosts]
        if valid_ips:
            san_list.extend(ips)
        if custom_san_list:
            san_list.extend([x509.DNSName(n) for n in custom_san_list])

        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                san_list
            ),
            critical=False
        )
        builder = builder.add_extension(x509.BasicConstraints(
            ca=False, path_length=None), critical=True,)

        cert = builder.sign(private_key=self.root_key,
                            algorithm=hashes.SHA256(), backend=default_backend())
        cert_str = cert.public_bytes(encoding=serialization.Encoding.PEM).decode('utf-8')
        key_str = private_key.private_bytes(encoding=serialization.Encoding.PEM,
                                            format=serialization.PrivateFormat.TraditionalOpenSSL,
                                            encryption_algorithm=serialization.NoEncryption()
                                            ).decode('utf-8')

        return (cert_str, key_str)

    def get_root_cert(self) -> str:
        try:
            return self.root_cert.public_bytes(encoding=serialization.Encoding.PEM).decode('utf-8')
        except AttributeError:
            return ''

    def get_root_key(self) -> str:
        try:
            return self.root_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ).decode('utf-8')
        except AttributeError:
            return ''

    def load_root_credentials(self, cert: str, priv_key: str) -> None:
        given_cert = x509.load_pem_x509_certificate(cert.encode('utf-8'), backend=default_backend())
        tz = given_cert.not_valid_after.tzinfo
        if datetime.now(tz) >= given_cert.not_valid_after:
            raise SSLConfigException('Given cert is expired')
        self.root_cert = given_cert
        self.root_key = serialization.load_pem_private_key(
            data=priv_key.encode('utf-8'), backend=default_backend(), password=None)
