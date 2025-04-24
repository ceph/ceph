"""
This file has been isolated into a module so that it can be run
in a subprocess therefore sidestepping the
`PyO3 modules may only be initialized once per interpreter process` problem.
"""

from typing import Any, Dict

import argparse
import json
import sys

from argparse import Namespace

from .internal import InternalCryptoCaller, InternalError


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
