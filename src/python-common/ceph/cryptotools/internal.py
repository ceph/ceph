"""Internal execution of cryptographic functions for the ceph mgr
"""

from typing import Dict, Any, Tuple, Union

from uuid import uuid4
import datetime
import warnings

from OpenSSL import crypto, SSL
import bcrypt


from .caller import CryptoCaller, CryptoCallError


class InternalError(CryptoCallError):
    pass


class InternalCryptoCaller(CryptoCaller):
    def fail(self, msg: str) -> None:
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
        pkey = crypto.PKey()
        pkey.generate_key(crypto.TYPE_RSA, 2048)
        return crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey).decode()

    def create_self_signed_cert(
        self, dname: Dict[str, str], pkey: str
    ) -> str:
        _pkey = crypto.load_privatekey(crypto.FILETYPE_PEM, pkey)

        # Create a "subject" object
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            req = crypto.X509Req()
        subj = req.get_subject()

        # populate the subject with the dname settings
        for k, v in dname.items():
            setattr(subj, k, v)

        # create a self-signed cert
        cert = crypto.X509()
        cert.set_subject(req.get_subject())
        cert.set_serial_number(int(uuid4()))
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(10 * 365 * 24 * 60 * 60)  # 10 years
        cert.set_issuer(cert.get_subject())
        cert.set_pubkey(_pkey)
        cert.sign(_pkey, 'sha512')
        return crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode()

    def _load_cert(self, crt: Union[str, bytes]) -> Any:
        crt_buffer = crt.encode() if isinstance(crt, str) else crt
        cert = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
        return cert

    def _issuer_info(self, cert: Any) -> Tuple[str, str]:
        components = cert.get_issuer().get_components()
        org_name = cn = ''
        for c in components:
            if c[0].decode() == 'O':  # org comp
                org_name = c[1].decode()
            elif c[0].decode() == 'CN':  # common name comp
                cn = c[1].decode()
        return (org_name, cn)

    def certificate_days_to_expire(self, crt: str) -> int:
        x509 = self._load_cert(crt)
        no_after = x509.get_notAfter()
        if not no_after:
            self.fail("Certificate does not have an expiration date.")

        end_date = datetime.datetime.strptime(
            no_after.decode(), '%Y%m%d%H%M%SZ'
        )

        if x509.has_expired():
            org, cn = self._issuer_info(x509)
            msg = 'Certificate issued by "%s/%s" expired on %s' % (
                org,
                cn,
                end_date,
            )
            self.fail(msg)

        # Certificate still valid, calculate and return days until expiration
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            days_until_exp = (end_date - datetime.datetime.utcnow()).days
        return int(days_until_exp)

    def get_cert_issuer_info(self, crt: str) -> Tuple[str, str]:
        return self._issuer_info(self._load_cert(crt))

    def verify_tls(self, crt: str, key: str) -> None:
        try:
            _key = crypto.load_privatekey(crypto.FILETYPE_PEM, key)
            _key.check()
        except (ValueError, crypto.Error) as e:
            self.fail('Invalid private key: %s' % str(e))
        try:
            _crt = self._load_cert(crt)
        except ValueError as e:
            self.fail('Invalid certificate key: %s' % str(e))

        try:
            context = SSL.Context(SSL.TLSv1_METHOD)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                context.use_certificate(_crt)
                context.use_privatekey(_key)
            context.check_privatekey()
        except crypto.Error as e:
            self.fail(
                'Private key and certificate do not match up: %s' % str(e)
            )
        except SSL.Error as e:
            self.fail(f'Invalid cert/key pair: {e}')
