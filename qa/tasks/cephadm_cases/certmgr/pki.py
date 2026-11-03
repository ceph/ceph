#!/usr/bin/env python3
"""
Small PKI toolbox used by the certmgr teuthology tests.

The test module copies this file to the cluster host and runs it inside
``cephadm shell``, so it only depends on what the Ceph container image already
ships (python3 + python3-cryptography). It is not imported by teuthology.

Sub-commands (every command prints a single JSON document on stdout):

  ca DIR --cn CN [--parent DIR]
      Create a CA (self-signed root, or an intermediate signed by --parent).
      Writes DIR/ca.crt, DIR/ca.key and DIR/chain.pem (the intermediates a
      server must send for this CA, empty for a root).

  leaf PREFIX --ca DIR --cn CN [--dns N]... [--ip IP]... [--not-before D]
       [--days N] [--key-type rsa|rsa-pkcs1|ec] [--passphrase P]
      Issue a server certificate. --not-before is an offset in days from now
      (negative = in the past), --days the validity counted from not-before,
      so expired certificates can be built without faking the clock.
      Writes PREFIX.crt (leaf only), PREFIX.chain.crt (leaf + intermediates),
      PREFIX.key and PREFIX.full.pem (key + leaf + intermediates).

  key PATH [--key-type ...]
      Write a standalone private key (used to build mismatched pairs).

  info PATH
      Describe the first certificate found in PATH.

  verify PATH --ca CAFILE
      Check that the first certificate in PATH is signed by CAFILE's key.

  fetch HOST PORT [--cafile CAFILE]
      TLS handshake against HOST:PORT. Without --cafile the peer certificate
      is returned unverified. With --cafile the handshake must validate the
      chain up to CAFILE (hostname checking disabled).
"""

import argparse
import datetime
import ipaddress
import json
import os
import socket
import ssl
import sys
from typing import Any, Dict, List, Optional, Tuple

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

UTC = datetime.timezone.utc


def _now() -> datetime.datetime:
    return datetime.datetime.now(UTC)


def _not_after(cert: x509.Certificate) -> datetime.datetime:
    value = getattr(cert, 'not_valid_after_utc', None)
    return value if value is not None else cert.not_valid_after.replace(tzinfo=UTC)


def _not_before(cert: x509.Certificate) -> datetime.datetime:
    value = getattr(cert, 'not_valid_before_utc', None)
    return value if value is not None else cert.not_valid_before.replace(tzinfo=UTC)


def _new_key(key_type: str) -> Any:
    if key_type == 'ec':
        return ec.generate_private_key(ec.SECP256R1(), default_backend())
    return rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())


def _key_pem(key: Any, key_type: str, passphrase: Optional[str] = None) -> bytes:
    if passphrase:
        encryption: Any = serialization.BestAvailableEncryption(passphrase.encode())
        fmt = serialization.PrivateFormat.PKCS8
    else:
        encryption = serialization.NoEncryption()
        # rsa-pkcs1 -> "BEGIN RSA PRIVATE KEY", ec -> "BEGIN EC PRIVATE KEY",
        # rsa -> "BEGIN PRIVATE KEY" (PKCS#8)
        if key_type in ('rsa-pkcs1', 'ec'):
            fmt = serialization.PrivateFormat.TraditionalOpenSSL
        else:
            fmt = serialization.PrivateFormat.PKCS8
    return key.private_bytes(serialization.Encoding.PEM, fmt, encryption)


def _cert_pem(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.PEM)


def _load_first_cert(path: str) -> x509.Certificate:
    with open(path, 'rb') as f:
        return x509.load_pem_x509_certificate(f.read(), default_backend())


def _load_key(path: str) -> Any:
    with open(path, 'rb') as f:
        return serialization.load_pem_private_key(f.read(), password=None,
                                                  backend=default_backend())


def _write(path: str, data: bytes) -> None:
    with open(path, 'wb') as f:
        f.write(data)
    # private keys (and bundles holding one) are not world-readable
    os.chmod(path, 0o600 if b'PRIVATE KEY' in data else 0o644)


def _name(cn: str) -> x509.Name:
    return x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, 'Ceph QA certmgr'),
        x509.NameAttribute(NameOID.COMMON_NAME, cn),
    ])


def _fingerprint(cert: x509.Certificate) -> str:
    return cert.fingerprint(hashes.SHA256()).hex()


def describe(cert: x509.Certificate) -> Dict[str, Any]:
    dns: List[str] = []
    ips: List[str] = []
    try:
        san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        dns = list(san.get_values_for_type(x509.DNSName))
        ips = [str(i) for i in san.get_values_for_type(x509.IPAddress)]
    except x509.ExtensionNotFound:
        pass
    try:
        is_ca = cert.extensions.get_extension_for_class(x509.BasicConstraints).value.ca
    except x509.ExtensionNotFound:
        is_ca = False
    not_after = _not_after(cert)
    not_before = _not_before(cert)
    return {
        'subject': cert.subject.rfc4514_string(),
        'issuer': cert.issuer.rfc4514_string(),
        'serial': format(cert.serial_number, 'x'),
        'fingerprint': _fingerprint(cert),
        'not_before': not_before.isoformat(),
        'not_after': not_after.isoformat(),
        'validity_days': (not_after - not_before).days,
        'remaining_days': (not_after - _now()).days,
        'dns_names': dns,
        'ip_addresses': ips,
        'is_ca': is_ca,
    }


def _issue(subject_cn: str,
           public_key: Any,
           issuer_cert: Optional[x509.Certificate],
           issuer_key: Any,
           not_before: datetime.datetime,
           not_after: datetime.datetime,
           is_ca: bool,
           path_len: Optional[int] = None,
           dns: Optional[List[str]] = None,
           ips: Optional[List[str]] = None) -> x509.Certificate:
    subject = _name(subject_cn)
    issuer = issuer_cert.subject if issuer_cert is not None else subject
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=path_len), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(public_key), critical=False)
    )
    if issuer_cert is not None:
        builder = builder.add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(issuer_cert.public_key()),
            critical=False)
    if is_ca:
        builder = builder.add_extension(
            x509.KeyUsage(digital_signature=True, content_commitment=False,
                          key_encipherment=False, data_encipherment=False,
                          key_agreement=False, key_cert_sign=True, crl_sign=True,
                          encipher_only=False, decipher_only=False),
            critical=True)
    else:
        sans: List[x509.GeneralName] = [x509.DNSName(d) for d in (dns or [])]
        sans += [x509.IPAddress(ipaddress.ip_address(i)) for i in (ips or [])]
        if sans:
            builder = builder.add_extension(x509.SubjectAlternativeName(sans), critical=False)
        builder = builder.add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH,
                                   ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=False)
    return builder.sign(issuer_key, hashes.SHA256(), default_backend())


def cmd_ca(args: argparse.Namespace) -> Dict[str, Any]:
    os.makedirs(args.dir, exist_ok=True)
    key = _new_key('rsa')
    now = _now()
    if args.parent:
        parent_cert = _load_first_cert(os.path.join(args.parent, 'ca.crt'))
        parent_key = _load_key(os.path.join(args.parent, 'ca.key'))
        with open(os.path.join(args.parent, 'chain.pem'), 'rb') as f:
            parent_chain = f.read()
        cert = _issue(args.cn, key.public_key(), parent_cert, parent_key,
                      now - datetime.timedelta(days=1), now + datetime.timedelta(days=3650),
                      is_ca=True, path_len=0)
        chain = _cert_pem(cert) + parent_chain
    else:
        cert = _issue(args.cn, key.public_key(), None, key,
                      now - datetime.timedelta(days=1), now + datetime.timedelta(days=3650),
                      is_ca=True, path_len=1)
        chain = b''
    _write(os.path.join(args.dir, 'ca.key'), _key_pem(key, 'rsa'))
    _write(os.path.join(args.dir, 'ca.crt'), _cert_pem(cert))
    _write(os.path.join(args.dir, 'chain.pem'), chain)
    return describe(cert)


def cmd_leaf(args: argparse.Namespace) -> Dict[str, Any]:
    ca_cert = _load_first_cert(os.path.join(args.ca, 'ca.crt'))
    ca_key = _load_key(os.path.join(args.ca, 'ca.key'))
    with open(os.path.join(args.ca, 'chain.pem'), 'rb') as f:
        intermediates = f.read()
    key = _new_key(args.key_type)
    not_before = _now() + datetime.timedelta(days=args.not_before)
    not_after = not_before + datetime.timedelta(days=args.days)
    cert = _issue(args.cn, key.public_key(), ca_cert, ca_key, not_before, not_after,
                  is_ca=False, dns=args.dns, ips=args.ip)
    key_pem = _key_pem(key, args.key_type, args.passphrase)
    leaf_pem = _cert_pem(cert)
    _write(args.prefix + '.key', key_pem)
    _write(args.prefix + '.crt', leaf_pem)
    _write(args.prefix + '.chain.crt', leaf_pem + intermediates)
    _write(args.prefix + '.full.pem', key_pem + leaf_pem + intermediates)
    return describe(cert)


def cmd_key(args: argparse.Namespace) -> Dict[str, Any]:
    _write(args.path, _key_pem(_new_key(args.key_type), args.key_type))
    return {'path': args.path}


def cmd_info(args: argparse.Namespace) -> Dict[str, Any]:
    return describe(_load_first_cert(args.path))


def _verify_signature(cert: x509.Certificate, issuer: x509.Certificate) -> None:
    pub = issuer.public_key()
    if isinstance(pub, rsa.RSAPublicKey):
        pub.verify(cert.signature, cert.tbs_certificate_bytes,
                   padding.PKCS1v15(), cert.signature_hash_algorithm)
    elif isinstance(pub, ec.EllipticCurvePublicKey):
        pub.verify(cert.signature, cert.tbs_certificate_bytes,
                   ec.ECDSA(cert.signature_hash_algorithm))
    else:
        raise ValueError(f'unsupported issuer key type {type(pub)}')


def cmd_verify(args: argparse.Namespace) -> Dict[str, Any]:
    cert = _load_first_cert(args.path)
    ca = _load_first_cert(args.ca)
    try:
        _verify_signature(cert, ca)
        ok, error = True, ''
    except Exception as e:  # noqa: B902 - report any verification failure
        ok, error = False, f'{type(e).__name__}: {e}'
    return {'signed_by_ca': ok, 'issuer_matches': cert.issuer == ca.subject, 'error': error}


def _handshake(host: str, port: int, cafile: Optional[str]) -> Tuple[bytes, bool]:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    if cafile:
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=cafile)
    else:
        ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=10) as sock:
        with ctx.wrap_socket(sock) as tls:
            der = tls.getpeercert(binary_form=True)
            assert der is not None
            return der, bool(cafile)


def cmd_fetch(args: argparse.Namespace) -> Dict[str, Any]:
    try:
        der, verified = _handshake(args.host, args.port, args.cafile)
    except (OSError, ssl.SSLError) as e:
        return {'ok': False, 'error': f'{type(e).__name__}: {e}'}
    cert = x509.load_der_x509_certificate(der, default_backend())
    out = describe(cert)
    out.update({'ok': True, 'verified': verified,
                'pem': _cert_pem(cert).decode()})
    return out


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser(prog='certmgr_pki')
    sub = p.add_subparsers(dest='cmd')
    sub.required = True

    s = sub.add_parser('ca')
    s.add_argument('dir')
    s.add_argument('--cn', required=True)
    s.add_argument('--parent')
    s.set_defaults(func=cmd_ca)

    s = sub.add_parser('leaf')
    s.add_argument('prefix')
    s.add_argument('--ca', required=True)
    s.add_argument('--cn', required=True)
    s.add_argument('--dns', action='append', default=[])
    s.add_argument('--ip', action='append', default=[])
    s.add_argument('--not-before', type=int, default=-1)
    s.add_argument('--days', type=int, default=365)
    s.add_argument('--key-type', choices=['rsa', 'rsa-pkcs1', 'ec'], default='rsa')
    s.add_argument('--passphrase')
    s.set_defaults(func=cmd_leaf)

    s = sub.add_parser('key')
    s.add_argument('path')
    s.add_argument('--key-type', choices=['rsa', 'rsa-pkcs1', 'ec'], default='rsa')
    s.set_defaults(func=cmd_key)

    s = sub.add_parser('info')
    s.add_argument('path')
    s.set_defaults(func=cmd_info)

    s = sub.add_parser('verify')
    s.add_argument('path')
    s.add_argument('--ca', required=True)
    s.set_defaults(func=cmd_verify)

    s = sub.add_parser('fetch')
    s.add_argument('host')
    s.add_argument('port', type=int)
    s.add_argument('--cafile')
    s.set_defaults(func=cmd_fetch)

    args = p.parse_args(argv)
    print(json.dumps(args.func(args)))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
