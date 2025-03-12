"""
This file has been isolated into a module so that it can be run
in a subprocess therefore sidestepping the
`PyO3 modules may only be initialized once per interpreter process` problem.
"""

import base64
import json
import sys
import warnings

from OpenSSL import crypto
from uuid import uuid4


def main() -> None:
    dname = json.loads(base64.b64decode(sys.argv[1]).decode("utf-8"))

    # create a key pair
    pkey = crypto.PKey()
    pkey.generate_key(crypto.TYPE_RSA, 2048)

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

    print(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    print("#######")
    print(crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))


if __name__ == "__main__":
    main()
