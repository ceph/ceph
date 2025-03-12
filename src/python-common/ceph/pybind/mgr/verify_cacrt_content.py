"""
This file has been isolated into a module so that it can be run
in a subprocess therefore sidestepping the
`PyO3 modules may only be initialized once per interpreter process` problem.
"""

import base64
import datetime
import sys

from OpenSSL import crypto
from typing import Tuple, Optional


def get_cert_issuer_info(crt: str) -> Tuple[Optional[str], Optional[str]]:
    """Basic validation of a ca cert

    Extracted from mgr_util.py and also duplicated in get_cert_issuer_info.py
    """

    crt_buffer = crt.encode("ascii") if isinstance(crt, str) else crt
    (org_name, cn) = (None, None)
    cert = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
    components = cert.get_issuer().get_components()
    for c in components:
        if c[0].decode() == 'O':  # org comp
            org_name = c[1].decode()
        elif c[0].decode() == 'CN':  # common name comp
            cn = c[1].decode()

    return (org_name, cn)


def main() -> None:
    crt = base64.b64decode(sys.argv[1]).decode("ascii")
    crt_buffer = crt.encode("ascii") if isinstance(crt, str) else crt
    x509 = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
    if x509.has_expired():
        org, cn = get_cert_issuer_info(crt)
        no_after = x509.get_notAfter()
        end_date = None
        if no_after is not None:
            end_date = datetime.datetime.strptime(no_after.decode('ascii'), '%Y%m%d%H%M%SZ')
            msg = 'Certificate issued by "%s/%s" expired on %s' % (org, cn, end_date)
            print(msg)


if __name__ == "__main__":
    main()
