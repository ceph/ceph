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
from typing import Tuple, Optional


# subcommand functions
def password_hash(args: Namespace) -> None:
    data = json.loads(sys.stdin.read())

    password = data['password']
    salt_password = data['salt_password']

    if not salt_password:
        salt = bcrypt.gensalt()
    else:
        salt = salt_password.encode()

    hash_str = bcrypt.hashpw(password.encode(), salt).decode()
    json.dump({'hash': hash_str}, sys.stdout)


def create_self_signed_cert(args: Namespace) -> None:

    # Generate private key
    if args.private_key:
        # create a key pair
        pkey = crypto.PKey()
        pkey.generate_key(crypto.TYPE_RSA, 2048)
        print(crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey).decode())
        return

    data = json.loads(sys.stdin.read())

    dname = data['dname']
    pkey = crypto.load_privatekey(crypto.FILETYPE_PEM, data['private_key'])

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
    cert.set_pubkey(pkey)
    cert.sign(pkey, 'sha512')

    print(crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode())


def _get_cert_issuer_info(crt: str) -> Tuple[Optional[str], Optional[str]]:
    """Basic validation of a CA cert
    """

    crt_buffer = crt.encode() if isinstance(crt, str) else crt
    (org_name, cn) = (None, None)
    cert = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
    components = cert.get_issuer().get_components()
    for c in components:
        if c[0].decode() == 'O':  # org comp
            org_name = c[1].decode()
        elif c[0].decode() == 'CN':  # common name comp
            cn = c[1].decode()

    return (org_name, cn)


def verify_cacrt_content(args: Namespace) -> None:
    crt = sys.stdin.read()

    crt_buffer = crt.encode() if isinstance(crt, str) else crt
    x509 = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
    no_after = x509.get_notAfter()
    if not no_after:
        print("Certificate does not have an expiration date.", file=sys.stderr)
        sys.exit(1)

    end_date = datetime.datetime.strptime(no_after.decode(), '%Y%m%d%H%M%SZ')

    if x509.has_expired():
        org, cn = _get_cert_issuer_info(crt)
        msg = 'Certificate issued by "%s/%s" expired on %s' % (org, cn, end_date)
        print(msg, file=sys.stderr)
        sys.exit(1)

    # Certificate still valid, calculate and return days until expiration
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        days_until_exp = (end_date - datetime.datetime.utcnow()).days
        json.dump({'days_until_expiration': int(days_until_exp)}, sys.stdout)


def get_cert_issuer_info(args: Namespace) -> None:
    crt = sys.stdin.read()

    crt_buffer = crt.encode() if isinstance(crt, str) else crt
    (org_name, cn) = (None, None)
    cert = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
    components = cert.get_issuer().get_components()
    for c in components:
        if c[0].decode() == 'O':  # org comp
            org_name = c[1].decode()
        elif c[0].decode() == 'CN':  # common name comp
            cn = c[1].decode()
    json.dump({'org_name': org_name, 'cn': cn}, sys.stdout)


def _fail_message(msg: str) -> None:
    json.dump({'error': msg}, sys.stdout)
    sys.exit(0)


def verify_tls(args: Namespace) -> None:
    data = json.loads(sys.stdin.read())

    crt = data['crt']
    key = data['key']

    try:
        _key = crypto.load_privatekey(crypto.FILETYPE_PEM, key)
        _key.check()
    except (ValueError, crypto.Error) as e:
        _fail_message('Invalid private key: %s' % str(e))
    try:
        crt_buffer = crt.encode() if isinstance(crt, str) else crt
        _crt = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
    except ValueError as e:
        _fail_message('Invalid certificate key: %s' % str(e))

    try:
        context = SSL.Context(SSL.TLSv1_METHOD)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            context.use_certificate(_crt)
            context.use_privatekey(_key)

        context.check_privatekey()
    except crypto.Error as e:
        _fail_message('Private key and certificate do not match up: %s' % str(e))
    except SSL.Error as e:
        _fail_message(f'Invalid cert/key pair: {e}')
    json.dump({'ok': True}, sys.stdout)  # need to emit something on success


if __name__ == "__main__":
    # create the top-level parser
    parser = argparse.ArgumentParser(prog='cryptotools.py')
    subparsers = parser.add_subparsers(required=True)

    # create the parser for the "password_hash" command
    parser_foo = subparsers.add_parser('password_hash')
    parser_foo.set_defaults(func=password_hash)

    # create the parser for the "create_self_signed_cert" command
    parser_bar = subparsers.add_parser('create_self_signed_cert')
    parser_bar.add_argument('--private_key', required=False, action='store_true')
    parser_bar.add_argument('--certificate', required=False, action='store_true')
    parser_bar.set_defaults(func=create_self_signed_cert)

    # create the parser for the "verify_cacrt_content" command
    parser_bar = subparsers.add_parser('verify_cacrt_content')
    parser_bar.set_defaults(func=verify_cacrt_content)

    # create the parser for the "get_cert_issuer_info" command
    parser_bar = subparsers.add_parser('get_cert_issuer_info')
    parser_bar.add_argument('--org_name', required=False, action='store_true')
    parser_bar.add_argument('--cn', required=False, action='store_true')
    parser_bar.set_defaults(func=get_cert_issuer_info)

    # create the parser for the "verify_tls" command
    parser_bar = subparsers.add_parser('verify_tls')
    parser_bar.set_defaults(func=verify_tls)

    # parse the args and call whatever function was selected
    args = parser.parse_args()
    args.func(args)
