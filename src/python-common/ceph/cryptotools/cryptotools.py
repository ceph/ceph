"""
This file has been isolated into a module so that it can be run
in a subprocess therefore sidestepping the
`PyO3 modules may only be initialized once per interpreter process` problem.
"""

import argparse
import bcrypt
import datetime
import json
import sys
import warnings

from argparse import Namespace
from OpenSSL import crypto, SSL
from uuid import uuid4
from typing import Tuple, Any, Dict, Union


class InternalError(ValueError):
    pass


class InternalCryptoCaller:
    def fail(self, msg: str) -> None:
        raise ValueError(msg)

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


def _read() -> str:
    return sys.stdin.read()


def _load() -> Dict[str, Any]:
    return json.loads(_read())


def _respond(data: Dict[str, Any]) -> None:
    json.dump(data, sys.stdout)


def _write(content: str) -> None:
    sys.stdout.write(content)
    sys.stdout.flush()


def _fail(msg: str, code: int = 0) -> Any:
    json.dump({'error': msg}, sys.stdout)
    sys.exit(code)


def password_hash(args: Namespace) -> None:
    data = _load()
    password = data['password']
    salt_password = data['salt_password']
    hash_str = args.crypto.password_hash(password, salt_password)
    _respond({'hash': hash_str})


def verify_password(args: Namespace) -> None:
    data = _load()
    password = data.get('password', '')
    hashed_password = data.get('hashed_password', '')
    try:
        ok = args.crypto.verify_password(password, hashed_password)
    except ValueError as err:
        _fail(str(err))
    _respond({'ok': ok})


def create_private_key(args: Namespace) -> None:
    _write(args.crypto.create_private_key())


def create_self_signed_cert(args: Namespace) -> None:
    data = _load()
    dname = data['dname']
    private_key = data['private_key']
    _write(args.crypto.create_self_signed_cert(dname, private_key))


def certificate_days_to_expire(args: Namespace) -> None:
    crt = _read()
    try:
        days_until_exp = args.crypto.certificate_days_to_expire(crt)
    except InternalError as err:
        _fail(str(err))
    _respond({'days_until_expiration': days_until_exp})


def get_cert_issuer_info(args: Namespace) -> None:
    crt = _read()
    org_name, cn = args.crypto.get_cert_issuer_info(crt)
    _respond({'org_name': org_name, 'cn': cn})


def verify_tls(args: Namespace) -> None:
    data = _load()
    crt = data['crt']
    key = data['key']
    try:
        args.crypto.verify_tls(crt, key)
    except ValueError as err:
        _fail(str(err))
    _respond({'ok': True})  # need to emit something on success


def main() -> None:
    # create the top-level parser
    parser = argparse.ArgumentParser(prog='cryptotools.py')
    parser.set_defaults(crypto=InternalCryptoCaller())
    subparsers = parser.add_subparsers(required=True)

    # create the parser for the "password_hash" command
    parser_password_hash = subparsers.add_parser('password_hash')
    parser_password_hash.set_defaults(func=password_hash)

    # create the parser for the "create_self_signed_cert" command
    parser_cssc = subparsers.add_parser('create_self_signed_cert')
    parser_cssc.set_defaults(func=create_self_signed_cert)

    parser_cpk = subparsers.add_parser('create_private_key')
    parser_cpk.set_defaults(func=create_private_key)

    # create the parser for the "certificate_days_to_expire" command
    parser_dte = subparsers.add_parser('certificate_days_to_expire')
    parser_dte.set_defaults(func=certificate_days_to_expire)

    # create the parser for the "get_cert_issuer_info" command
    parser_gcii = subparsers.add_parser('get_cert_issuer_info')
    parser_gcii.set_defaults(func=get_cert_issuer_info)

    # create the parser for the "verify_tls" command
    parser_verify_tls = subparsers.add_parser('verify_tls')
    parser_verify_tls.set_defaults(func=verify_tls)

    # password verification
    parser_verify_password = subparsers.add_parser('verify_password')
    parser_verify_password.set_defaults(func=verify_password)

    # parse the args and call whatever function was selected
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
