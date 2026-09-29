from typing import Any, Tuple, List, Dict, cast
import re

from datetime import datetime
from cryptography import x509
from cryptography.x509 import Certificate
from cryptography.x509.oid import NameOID, ExtensionOID
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


class SSLConfigException(Exception):
    pass


# ---------------------------------------------------------------------------
# TLS PEM bundle parsing
# ---------------------------------------------------------------------------

# PEM block headers for every private-key format we accept.
_KEY_HEADERS = (
    '-----BEGIN RSA PRIVATE KEY-----',    # PKCS#1 RSA
    '-----BEGIN PRIVATE KEY-----',         # PKCS#8 (unencrypted)
    '-----BEGIN EC PRIVATE KEY-----',      # SEC1 EC
)


_ENCRYPTED_KEY_HEADER = '-----BEGIN ENCRYPTED PRIVATE KEY-----'
_CERT_HEADER = '-----BEGIN CERTIFICATE-----'

# Regex that matches a single, complete PEM block (header ... footer).
# The capture group is the label between BEGIN/END so the two markers are
# guaranteed to match (e.g. "RSA PRIVATE KEY" on both sides).
_PEM_BLOCK_RE = re.compile(
    r'-----BEGIN ([^-]+)-----[\s\S]*?-----END \1-----',
    re.MULTILINE,
)


def _pem_block_label(pem_block: str) -> str:
    m = _PEM_BLOCK_RE.match(pem_block.strip())
    return m.group(1) if m else ''


def _is_private_key_block(pem_block: str) -> bool:
    return _pem_block_label(pem_block).endswith('PRIVATE KEY')


def parse_tls_pem_bundle(pem_data: str) -> Tuple[str, str]:
    """Canonical parser for any TLS PEM bundle: a plain certificate, a
    certificate chain, a combined cert+key blob, or a full *fullchain PEM*
    (private key + leaf cert + intermediate certs) as output by many
    enterprise CAs. This is the single parsing function that should be used
    everywhere TLS PEM material needs to be parsed.

    Supported key block types:

    * ``-----BEGIN RSA PRIVATE KEY-----``   (PKCS#1)
    * ``-----BEGIN PRIVATE KEY-----``       (PKCS#8 unencrypted)
    * ``-----BEGIN EC PRIVATE KEY-----``    (SEC1 EC)

    * The **private key** is extracted and returned separately, if present.
    * The **certificate chain** (leaf cert first, then any intermediate CA
      certs) is returned as a single concatenated PEM string — the format
      expected by nginx, HAProxy and most TLS consumers.

    The leaf certificate is defined as the *first* ``CERTIFICATE`` block in
    the input (the convention followed by every CA tool we are aware of).
    Chain ordering is *not* enforced as a hard error — real-world CA tooling
    sometimes emits chains in non-standard order — but the key **must** match
    the leaf certificate public key.

    Args:
        pem_data: Raw PEM string containing 1-N certificates and, optionally,
            a single private key.

    Returns:
        ``(cert_chain, private_key)`` where *cert_chain* is all CERTIFICATE
        blocks joined by newlines (leaf first) and *private_key* is the single
        key block.  If no key block is present *private_key* is an empty string.

    Raises:
        SSLConfigException:
              - if more than one private key block is present
              - if no certificate block is found
              - if the key type is unsupported or encrypted, or if the key does
                 not match the leaf certificate public key.
    """
    raw_blocks = [m.group(0) for m in _PEM_BLOCK_RE.finditer(pem_data)]
    if not raw_blocks:
        raise SSLConfigException('No PEM blocks found in the provided data')

    # Fail fast on encrypted keys as they are not supported currently
    encrypted_key_blocks = [
        b for b in raw_blocks
        if b.strip().startswith(_ENCRYPTED_KEY_HEADER)
    ]
    if encrypted_key_blocks:
        raise SSLConfigException(
            'Encrypted private keys are not supported. '
            'Please provide an unencrypted private key PEM.'
        )

    unsupported_key_blocks = [
        b for b in raw_blocks
        if _is_private_key_block(b)
        and not any(b.strip().startswith(h) for h in _KEY_HEADERS)
    ]
    if unsupported_key_blocks:
        key_types = ', '.join(
            sorted({_pem_block_label(b) for b in unsupported_key_blocks})
        )
        supported_key_types = ', '.join(
            h.removeprefix('-----BEGIN ').removesuffix('-----')
            for h in _KEY_HEADERS
        )
        raise SSLConfigException(
            f'Unsupported private key PEM type(s): {key_types}. '
            f'Supported private key PEM types are: {supported_key_types}'
        )

    key_blocks = [
        b for b in raw_blocks
        if any(b.strip().startswith(h) for h in _KEY_HEADERS)
    ]
    cert_blocks = [b for b in raw_blocks if b.strip().startswith(_CERT_HEADER)]

    if len(key_blocks) > 1:
        raise SSLConfigException(
            f'PEM bundle contains {len(key_blocks)} private key blocks; expected at most 1'
        )
    if not cert_blocks:
        raise SSLConfigException('PEM bundle contains no CERTIFICATE blocks')

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


def contains_multiple_pem_blocks(pem_data: str) -> bool:
    """Return *True* if *pem_data* contains more than one PEM block.

    A "fullchain" PEM is any blob that bundles multiple PEM objects —
    for example a private key followed by a leaf cert and intermediate CA certs.
    """
    matches = list(_PEM_BLOCK_RE.finditer(pem_data))
    return len(matches) > 1


def contains_private_key(pem_data: str) -> bool:
    """Return *True* if *pem_data* contains at least one private key PEM block."""
    return any(_is_private_key_block(m.group(0)) for m in _PEM_BLOCK_RE.finditer(pem_data))


def extract_ips_and_fqdns_from_cert(cert_pem: str) -> Tuple[List[str], List[str]]:
    """
    Extracts IP addresses and FQDNs from the SAN extension of a certificate.

    :param cert_pem: The certificate in PEM format.
    :return: A tuple containing two lists:
             - List of IP addresses as strings.
             - List of FQDNs (DNS names) as strings.
    """
    try:
        # Load the certificate
        certificate = x509.load_pem_x509_certificate(
            cert_pem.encode('utf-8'), backend=default_backend()
        )

        try:
            san_extension = certificate.extensions.get_extension_for_oid(
                ExtensionOID.SUBJECT_ALTERNATIVE_NAME
            )
            san = cast(x509.SubjectAlternativeName, san_extension.value)
            # Extract IP addresses and FQDNs (DNS Names)
            ip_addresses = [
                str(ip) for ip in san.get_values_for_type(x509.IPAddress)
            ]
            fqdns = [
                str(dns).lower() for dns in san.get_values_for_type(x509.DNSName)
            ]
            return sorted(ip_addresses), sorted(fqdns)
        except x509.ExtensionNotFound:
            # SAN extension not found, return empty lists
            return [], []

    except Exception as e:
        raise ValueError(f"Failed to extract IPs and FQDNs from certificate: {e}")


def parse_extensions(cert: Certificate) -> Dict[str, Any]:
    """Parse extensions into a readable format."""
    parsed_extensions: Dict[str, Any] = {}
    for ext in cert.extensions:
        try:
            if ext.oid == ExtensionOID.SUBJECT_ALTERNATIVE_NAME:
                san = cast(x509.SubjectAlternativeName, ext.value)
                parsed_extensions["subjectAltName"] = {
                    "DNSNames": san.get_values_for_type(x509.DNSName),
                    "IPAddresses": [
                        str(ip) for ip in san.get_values_for_type(x509.IPAddress)
                    ],
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


def get_certificate_info(cert_data: str, include_details: bool = False) -> Dict[str, Any]:
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

        cert = x509.load_pem_x509_certificate(
            first_cert_pem.encode('utf-8'), default_backend()
        )
        remaining_days = (cert.not_valid_after - datetime.utcnow()).days
        info = {
            'subject': {get_oid_name(attr.oid): attr.value for attr in cert.subject},
            'contains_chain': contains_multiple_pem_blocks(cert_data),
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


def get_private_key_info(private_data: str) -> Dict[str, Any]:
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
