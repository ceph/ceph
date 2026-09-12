"""Internal execution of cryptographic functions for the ceph mgr"""

from typing import Any, Dict, NoReturn, Tuple, Union

from uuid import uuid4
import datetime
import warnings

import bcrypt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.x509.oid import NameOID


from .caller import CryptoCaller, CryptoCallError


# the relative distinguished names accepted by create_self_signed_cert(),
# mapped to their x509 object identifiers. mgr_util validates the dname
# fields against the same set before we get here.
_RDN_OIDS = {
    'C': NameOID.COUNTRY_NAME,
    'ST': NameOID.STATE_OR_PROVINCE_NAME,
    'L': NameOID.LOCALITY_NAME,
    'O': NameOID.ORGANIZATION_NAME,
    'OU': NameOID.ORGANIZATIONAL_UNIT_NAME,
    'CN': NameOID.COMMON_NAME,
    'emailAddress': NameOID.EMAIL_ADDRESS,
}


class InternalError(CryptoCallError):
    pass


class InternalCryptoCaller(CryptoCaller):
    def fail(self, msg: str) -> NoReturn:
        raise InternalError(msg)

    def password_hash(self, password: str, salt_password: str) -> str:
        salt = salt_password.encode() if salt_password else bcrypt.gensalt()
        return bcrypt.hashpw(password.encode(), salt).decode()

    def verify_password(self, password: str, hashed_password: str) -> bool:
        _password = password.encode()
        _hashed_password = hashed_password.encode()
        try:
            ok = bcrypt.checkpw(_password, _hashed_password)
        except ValueError as err:
            self.fail(str(err))
        return ok

    def create_private_key(self) -> str:
        pkey = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        # PKCS8 PEM, the format pyOpenSSL's dump_privatekey() produced
        return pkey.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()

    def create_self_signed_cert(
        self, dname: Dict[str, str], pkey: str
    ) -> str:
        _pkey: Any = load_pem_private_key(pkey.encode(), password=None)

        # build the subject, which is also the issuer as the cert is
        # self-signed, from the dname settings
        attrs = []
        for k, v in dname.items():
            oid = _RDN_OIDS.get(k)
            if oid is None:
                self.fail('unsupported certificate subject field: %s' % k)
            attrs.append(x509.NameAttribute(oid, v))
        name = x509.Name(attrs)

        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(_pkey.public_key())
            .serial_number(int(uuid4()))
            .not_valid_before(now)
            .not_valid_after(now + datetime.timedelta(days=10 * 365))
            .sign(_pkey, hashes.SHA512())
        )
        return cert.public_bytes(serialization.Encoding.PEM).decode()

    def _load_cert(self, crt: Union[str, bytes]) -> x509.Certificate:
        crt_buffer = crt.encode() if isinstance(crt, str) else crt
        try:
            cert = x509.load_pem_x509_certificate(crt_buffer)
        except ValueError as e:
            self.fail('Invalid certificate: %s' % str(e))
        return cert

    def _name_value(self, name: x509.Name, oid: x509.ObjectIdentifier) -> str:
        attrs = name.get_attributes_for_oid(oid)
        if not attrs:
            return ''
        # cryptography types NameAttribute.value as str in some releases and
        # str | bytes in others; keep it Any so the bytes path type-checks
        # either way (like the other cryptography objects in this file).
        value: Any = attrs[0].value
        return value if isinstance(value, str) else value.decode('utf-8')

    def _issuer_info(self, cert: x509.Certificate) -> Tuple[str, str]:
        org_name = self._name_value(cert.issuer, NameOID.ORGANIZATION_NAME)
        cn = self._name_value(cert.issuer, NameOID.COMMON_NAME)
        return (org_name, cn)

    def certificate_days_to_expire(self, crt: str) -> int:
        cert = self._load_cert(crt)
        # not_valid_after is naive UTC; not_valid_after_utc only exists since
        # cryptography 42, so stick with the portable accessor and silence
        # its deprecation warning, same as for utcnow().
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            not_after = cert.not_valid_after
            now = datetime.datetime.utcnow()

        if now > not_after:
            org, cn = self._issuer_info(cert)
            self.fail(
                'Certificate issued by "%s/%s" expired on %s'
                % (org, cn, not_after)
            )

        return int((not_after - now).days)

    def get_cert_issuer_info(self, crt: str) -> Tuple[str, str]:
        return self._issuer_info(self._load_cert(crt))

    def verify_tls(self, crt: str, key: str) -> None:
        try:
            _key: Any = load_pem_private_key(key.encode(), password=None)
        except (ValueError, TypeError) as e:
            self.fail('Invalid private key: %s' % str(e))
        _crt: Any = self._load_cert(crt)
        # the cert and key match iff they share the same public key
        pem = serialization.Encoding.PEM
        spki = serialization.PublicFormat.SubjectPublicKeyInfo
        key_pub = _key.public_key().public_bytes(pem, spki)
        crt_pub = _crt.public_key().public_bytes(pem, spki)
        if key_pub != crt_pub:
            self.fail(
                'Invalid cert/key pair: '
                'private key and certificate do not match up'
            )
