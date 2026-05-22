from typing import Any, Tuple, IO, List, Union, Optional, Dict
import ipaddress
import re

from datetime import datetime, timedelta
from cryptography import x509
from cryptography.x509 import Certificate
from cryptography.x509.oid import NameOID, ExtensionOID
from cryptography.hazmat.primitives.asymmetric import rsa, ec
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.backends import default_backend


class SSLConfigException(Exception):
    pass


# ---------------------------------------------------------------------------
# Fullchain PEM helpers
# ---------------------------------------------------------------------------

# PEM block headers for every private-key format we accept
_KEY_HEADERS = (
    '-----BEGIN RSA PRIVATE KEY-----',    # PKCS#1 RSA
    '-----BEGIN PRIVATE KEY-----',         # PKCS#8 (unencrypted)
    '-----BEGIN EC PRIVATE KEY-----',      # SEC1 EC
)
_CERT_HEADER = '-----BEGIN CERTIFICATE-----'

# Regex that matches a single, complete PEM block (header ... footer).
# The capture group is the label between BEGIN/END so the two markers are
# guaranteed to match (e.g. "RSA PRIVATE KEY" on both sides).
_PEM_BLOCK_RE = re.compile(
    r'-----BEGIN ([^-]+)-----[\s\S]*?-----END \1-----',
    re.MULTILINE,
)


def split_fullchain_pem(pem_data: str) -> Tuple[str, str]:
    """Parse a PEM blob that may contain a private key and one or more
    certificate blocks (a *fullchain PEM* as output by many enterprise CAs).

    Supported key block types:

    * ``-----BEGIN RSA PRIVATE KEY-----``  (PKCS#1)
    * ``-----BEGIN PRIVATE KEY-----``       (PKCS#8 unencrypted)
    * ``-----BEGIN EC PRIVATE KEY-----``    (SEC1 EC)

    The function normalises the input so that downstream consumers
    (``TLSObjectStore``, the certificate-check loop, …) remain completely
    unaware of multi-block input:

    * The **private key** is extracted and returned separately.
    * The **certificate chain** (leaf cert first, then any intermediate CA
      certs) is returned as a single concatenated PEM string — the format
      expected by nginx, HAProxy and most TLS consumers.

    The leaf certificate is defined as the *first* ``CERTIFICATE`` block in
    the input (the convention followed by every CA tool we are aware of).
    Chain ordering is *not* enforced as a hard error — real-world CA tooling
    sometimes emits chains in non-standard order — but the key **must** match
    the leaf certificate public key.

    Args:
        pem_data: Raw PEM string containing a private key and 1–N certificates.

    Returns:
        ``(cert_chain, private_key)`` where *cert_chain* is all CERTIFICATE
        blocks joined by newlines (leaf first) and *private_key* is the single
        key block.  If no key block is present *private_key* is an empty string.

    Raises:
        SSLConfigException: if more than one private key block is present, if
            no certificate block is found, or if the key does not match the
            leaf certificate public key.
    """
    raw_blocks = [m.group(0) for m in _PEM_BLOCK_RE.finditer(pem_data)]
    if not raw_blocks:
        raise SSLConfigException('No PEM blocks found in the provided data')

    key_blocks = [b for b in raw_blocks if any(b.strip().startswith(h) for h in _KEY_HEADERS)]
    cert_blocks = [b for b in raw_blocks if b.strip().startswith(_CERT_HEADER)]

    if len(key_blocks) > 1:
        raise SSLConfigException(
            f'Fullchain PEM contains {len(key_blocks)} private key blocks; expected at most 1'
        )
    if not cert_blocks:
        raise SSLConfigException('Fullchain PEM contains no CERTIFICATE blocks')

    private_key_pem = (key_blocks[0].strip() + '\n') if key_blocks else ''
    cert_chain_pem = '\n'.join(b.strip() for b in cert_blocks) + '\n'

    # Validate that the key actually matches the leaf certificate.
    if private_key_pem:
        try:
            leaf_cert = x509.load_pem_x509_certificate(
                cert_blocks[0].encode('utf-8'), backend=default_backend()
            )
            priv_key = serialization.load_pem_private_key(
                private_key_pem.encode('utf-8'), password=None, backend=default_backend()
            )
            leaf_pub = leaf_cert.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            derived_pub = priv_key.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            if leaf_pub != derived_pub:
                raise SSLConfigException(
                    'Private key does not match the leaf certificate public key'
                )
        except SSLConfigException:
            raise
        except Exception as exc:
            raise SSLConfigException(
                f'Failed to validate private key against leaf certificate: {exc}'
            ) from exc

    return cert_chain_pem, private_key_pem


def is_fullchain_pem(pem_data: str) -> bool:
    """Return *True* if *pem_data* contains more than one PEM block.

    A "fullchain" PEM is any blob that bundles multiple PEM objects —
    for example a private key followed by a leaf cert and intermediate CA certs.
    """
    matches = list(_PEM_BLOCK_RE.finditer(pem_data))
    return len(matches) > 1


def contains_private_key(pem_data: str) -> bool:
    """Return *True* if *pem_data* contains at least one private key PEM block."""
    return any(
        m.group(0).strip().startswith(h)
        for m in _PEM_BLOCK_RE.finditer(pem_data)
        for h in _KEY_HEADERS
    )


def extract_ips_and_fqdns_from_cert(cert_pem: str) -> Tuple[List[str], List[str]]:
    """
    Extracts lists of IP addresses and FQDNs (DNS names) from the SAN (Subject Alternative Name) extension of a certificate.

    :param cert_pem: The certificate in PEM format.
    :return: A tuple containing two lists:
             - List of IP addresses as strings.
             - List of FQDNs (DNS names) as strings.
    """
    try:
        # Load the certificate
        certificate = x509.load_pem_x509_certificate(cert_pem.encode('utf-8'), backend=default_backend())

        try:
            san_extension = certificate.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
            san = san_extension.value
            # Extract IP addresses and FQDNs (DNS Names)
            ip_addresses = [str(ip) for ip in san.get_values_for_type(x509.IPAddress)]
            fqdns = [str(dns).lower() for dns in san.get_values_for_type(x509.DNSName)]
            return sorted(ip_addresses), sorted(fqdns)
        except x509.ExtensionNotFound:
            # SAN extension not found, return empty lists
            return [], []

    except Exception as e:
        raise ValueError(f"Failed to extract IPs and FQDNs from certificate: {e}")


def parse_extensions(cert: Certificate) -> Dict:
    """Parse extensions into a readable format."""
    parsed_extensions = {}
    for ext in cert.extensions:
        try:
            if ext.oid == ExtensionOID.SUBJECT_ALTERNATIVE_NAME:
                san = ext.value
                parsed_extensions["subjectAltName"] = {
                    "DNSNames": san.get_values_for_type(x509.DNSName),
                    "IPAddresses": [str(ip) for ip in san.get_values_for_type(x509.IPAddress)],
                }
            elif ext.oid == ExtensionOID.BASIC_CONSTRAINTS:
                basic_constraints = ext.value
                parsed_extensions["basicConstraints"] = {
                    "ca": basic_constraints.ca,
                    "path_length": basic_constraints.path_length,
                }
            elif ext.oid == ExtensionOID.SUBJECT_KEY_IDENTIFIER:
                parsed_extensions["subjectKeyIdentifier"] = {"present": True}
            elif ext.oid == ExtensionOID.AUTHORITY_KEY_IDENTIFIER:
                parsed_extensions["authorityKeyIdentifier"] = {"present": True}
            else:
                parsed_extensions[ext.oid.dotted_string] = {"value": "present"}
        except Exception as e:
            parsed_extensions[ext.oid.dotted_string] = {"error": str(e)}

    return parsed_extensions


def get_certificate_info(cert_data: str, include_details: bool = False) -> Dict:
    """Return detailed information about a certificate as a dictionary.

    When *cert_data* is a fullchain PEM (multiple CERTIFICATE blocks), only the
    leaf certificate (first block) is inspected — which is correct behaviour for
    expiry checks and subject/issuer display.
    """

    def get_oid_name(oid: Any) -> str:
        """Return a human-readable name for an OID."""
        oid_mapping = {
            NameOID.COMMON_NAME: 'commonName',
            NameOID.COUNTRY_NAME: 'countryName',
            NameOID.LOCALITY_NAME: 'localityName',
            NameOID.STATE_OR_PROVINCE_NAME: 'stateOrProvinceName',
            NameOID.ORGANIZATION_NAME: 'organizationName',
            NameOID.ORGANIZATIONAL_UNIT_NAME: 'organizationalUnitName',
        }
        return oid_mapping.get(oid, oid.dotted_string)

    try:
        # Extract only the first CERTIFICATE block so that fullchain PEMs
        # (leaf + intermediates in a single string) are handled gracefully.
        first_cert_pem = cert_data
        m = _PEM_BLOCK_RE.search(cert_data)
        if m and m.group(0).strip().startswith(_CERT_HEADER):
            first_cert_pem = m.group(0)

        cert = x509.load_pem_x509_certificate(first_cert_pem.encode('utf-8'), default_backend())
        remaining_days = (cert.not_valid_after - datetime.utcnow()).days
        info = {
            'subject': {get_oid_name(attr.oid): attr.value for attr in cert.subject},
            'validity': {
                'remaining_days': remaining_days,
            }
        }

        if include_details:
            info['issuer'] = {get_oid_name(attr.oid): attr.value for attr in cert.issuer}
            info['validity'] = {
                'not_before': cert.not_valid_before.isoformat(),
                'not_after': cert.not_valid_after.isoformat(),
                'remaining_days': remaining_days,
            }
            info['extensions'] = parse_extensions(cert)
            info['public_key'] = {}
            public_key = cert.public_key()
            if isinstance(public_key, rsa.RSAPublicKey):
                info['public_key'] = {
                    'key_type': 'RSA',
                    'key_size': public_key.key_size,
                }
            else:
                info['public_key'] = {
                    'key_type': 'Unknown',
                }

        return info
    except Exception as e:
        return {'Error': f'Error parsing certificate: {e}'}


def get_private_key_info(private_data: str) -> Dict:
    """Return detailed information about a private key as a dictionary."""
    try:
        private_key = serialization.load_pem_private_key(
            private_data.encode('utf-8'),
            password=None,
            backend=default_backend())

        info = {}
        if isinstance(private_key, rsa.RSAPrivateKey):
            info = {
                'key_type': 'RSA',
                'key_size': private_key.key_size,
            }
        else:
            info = {
                'key_type': 'Unknown',
            }
        return info
    except Exception as e:
        return {'Error': f'Error parsing key: {e}'}


class SSLCerts:
    def __init__(self, fsid: str, _certificate_duration_days: int = (365 * 10 + 3)) -> None:
        self.root_certificate_duration_days = (365 * 10 + 3)
        self.certificate_duration_days = _certificate_duration_days
        self.root_cert: Any
        self.root_key: Any
        self.key_file: IO[bytes]
        self.cert_file: IO[bytes]
        self.cluster_fsid: str = fsid

    def generate_root_cert(
        self,
        addr: Optional[str] = None,
        custom_san_list: Optional[List[str]] = None
    ) -> Tuple[str, str]:
        self.root_key = rsa.generate_private_key(
            public_exponent=65537, key_size=4096, backend=default_backend())
        root_public_key = self.root_key.public_key()
        root_builder = x509.CertificateBuilder()
        root_ca_name = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, f'cephadm-root-{self.cluster_fsid}'),
        ])
        root_builder = root_builder.subject_name(root_ca_name)
        root_builder = root_builder.issuer_name(root_ca_name)
        root_builder = root_builder.not_valid_before(datetime.now())
        root_builder = root_builder.not_valid_after(datetime.now() + timedelta(days=self.root_certificate_duration_days))
        root_builder = root_builder.serial_number(x509.random_serial_number())
        root_builder = root_builder.public_key(root_public_key)

        san_list: List[x509.GeneralName] = []
        san_list.append(x509.DNSName(f'fsid-{self.cluster_fsid}'))
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
        duration_in_days: Optional[int] = None,
    ) -> Tuple[str, str]:

        cert_duration_in_days = duration_in_days or self.certificate_duration_days
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
        builder = builder.issuer_name(self.get_root_issuer_name())
        builder = builder.not_valid_before(datetime.now())
        builder = builder.not_valid_after(datetime.now() + timedelta(days=cert_duration_in_days))
        builder = builder.serial_number(x509.random_serial_number())
        builder = builder.public_key(public_key)

        san_list: List[x509.GeneralName] = [x509.DNSName(host.lower()) for host in hosts]
        if valid_ips:
            san_list.extend(ips)
        if custom_san_list:
            san_list.extend([x509.DNSName(n.lower()) for n in custom_san_list])

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

    def renew_cert(
        self,
        old_cert: str,
        new_duration_days: Optional[int] = None
    ) -> Tuple[str, str]:
        """
        Renews a certificate, generating a new private key and extending its duration.

        :param old_cert: The existing certificate (PEM format) to be renewed.
        :param new_duration_days: The new validity duration for the certificate in days.
                                  If not provided, it defaults to `self.certificate_duration_days`.
        :return: A tuple containing the renewed certificate and the new private key (PEM format).
        """
        try:
            # Load the old certificate
            old_certificate = x509.load_pem_x509_certificate(old_cert.encode('utf-8'), backend=default_backend())

            # Generate a new private key
            new_private_key = rsa.generate_private_key(
                public_exponent=65537, key_size=4096, backend=default_backend()
            )

            # Extract existing SANs
            san_extension = old_certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName)
            san_list = san_extension.value

            # Build a new certificate with the same attributes
            builder = x509.CertificateBuilder()
            builder = builder.subject_name(old_certificate.subject)
            builder = builder.issuer_name(old_certificate.issuer)
            builder = builder.not_valid_before(datetime.now())
            builder = builder.not_valid_after(
                datetime.now() + timedelta(days=new_duration_days or self.certificate_duration_days)
            )
            builder = builder.serial_number(x509.random_serial_number())
            builder = builder.public_key(new_private_key.public_key())

            # Reuse SANs
            builder = builder.add_extension(san_list, critical=False)

            # Retain the original basic constraints
            basic_constraints = old_certificate.extensions.get_extension_for_class(x509.BasicConstraints)
            builder = builder.add_extension(basic_constraints.value, critical=basic_constraints.critical)

            # Sign the new certificate
            renewed_cert = builder.sign(private_key=self.root_key, algorithm=hashes.SHA256(), backend=default_backend())

            # Convert certificate and key to PEM format
            cert_str = renewed_cert.public_bytes(encoding=serialization.Encoding.PEM).decode('utf-8')
            key_str = new_private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ).decode('utf-8')

            return cert_str, key_str

        except Exception as e:
            raise SSLConfigException(f"Failed to renew certificate: {e}")

    def get_root_cert(self) -> str:
        try:
            return self.root_cert.public_bytes(encoding=serialization.Encoding.PEM).decode('utf-8')
        except AttributeError:
            return ''

    def get_root_issuer_name(self) -> x509.Name:
        if not self.root_cert:
            raise SSLConfigException("Root certificate not initialized.")
        return self.root_cert.subject

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
