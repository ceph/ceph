"""
This file has been isolated into a module so that it can be run
in a subprocess therefore sidestepping the
`PyO3 modules may only be initialized once per interpreter process` problem.
"""

import base64
import sys

from OpenSSL import crypto


def main() -> None:
    crt = base64.b64decode(sys.argv[1]).decode("ascii")

    crt_buffer = crt.encode("ascii") if isinstance(crt, str) else crt
    (org_name, cn) = (None, None)
    cert = crypto.load_certificate(crypto.FILETYPE_PEM, crt_buffer)
    components = cert.get_issuer().get_components()
    for c in components:
        if c[0].decode() == 'O':  # org comp
            org_name = c[1].decode()
        elif c[0].decode() == 'CN':  # common name comp
            cn = c[1].decode()

    print("%s#######%s" % (org_name, cn))


if __name__ == "__main__":
    main()
