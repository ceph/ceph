
from typing import Any, Tuple, IO
import ipaddress
import tempfile
import logging

from datetime import datetime, timedelta
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.backends import default_backend
from mgr_util import verify_tls_files

from orchestrator import OrchestratorError


logger = logging.getLogger(__name__)


class SSLConfigException(Exception):
    pass


class SSLCerts:
    def __init__(self) -> None:
        self.root_cert: Any
        self.root_key: Any
        self.key_file: IO[bytes]
        self.cert_file: IO[bytes]

    def generate_root_cert(self, addr: str) -> Tuple[str, str]:
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
        root_builder = root_builder.add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address(addr))]
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

    def generate_cert(self, host: str, addr: str) -> Tuple[str, str]:
        have_ip = True
        try:
            ip = x509.IPAddress(ipaddress.ip_address(addr))
        except Exception:
            have_ip = False

        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=4096, backend=default_backend())
        public_key = private_key.public_key()

        builder = x509.CertificateBuilder()
        builder = builder.subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, addr), ]))
        builder = builder.issuer_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u'cephadm-root'), ]))
        builder = builder.not_valid_before(datetime.now())
        builder = builder.not_valid_after(datetime.now() + timedelta(days=(365 * 10 + 3)))
        builder = builder.serial_number(x509.random_serial_number())
        builder = builder.public_key(public_key)
        if have_ip:
            builder = builder.add_extension(
                x509.SubjectAlternativeName(
                    [ip, x509.DNSName(host)]
                ),
                critical=False
            )
        else:
            builder = builder.add_extension(
                x509.SubjectAlternativeName(
                    [x509.DNSName(host)]
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

    def generate_cert_files(self, host: str, addr: str) -> Tuple[str, str]:
        cert, key = self.generate_cert(host, addr)

        self.cert_file = tempfile.NamedTemporaryFile()
        self.cert_file.write(cert.encode('utf-8'))
        self.cert_file.flush()  # cert_tmp must not be gc'ed

        self.key_file = tempfile.NamedTemporaryFile()
        self.key_file.write(key.encode('utf-8'))
        self.key_file.flush()  # pkey_tmp must not be gc'ed

        verify_tls_files(self.cert_file.name, self.key_file.name)
        return self.cert_file.name, self.key_file.name

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
            raise OrchestratorError('Given cert is expired')
        self.root_cert = given_cert
        self.root_key = serialization.load_pem_private_key(
            data=priv_key.encode('utf-8'), backend=default_backend(), password=None)
