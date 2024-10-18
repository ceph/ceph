from mgr_util import create_self_signed_cert, verify_tls, ServerConfigException
from OpenSSL import crypto, SSL

import unittest


class TLSchecks(unittest.TestCase):

    def test_defaults(self):
        crt, key = create_self_signed_cert()
        verify_tls(crt, key)

    def test_specific_dname(self):
        crt, key = create_self_signed_cert(dname={'O': 'Ceph', 'OU': 'testsuite'})
        verify_tls(crt, key)

    def test_invalid_RDN(self):
        self.assertRaises(ValueError, create_self_signed_cert,
                          dname={'O': 'Ceph', 'Bogus': 'testsuite'})

    def test_invalid_key(self):
        crt, key = create_self_signed_cert()

        # fudge the key, to force an error to be detected during verify_tls
        fudged = f"{key[:-35]}c0ffee==\n{key[-25:]}".encode('utf-8')
        self.assertRaises(ServerConfigException, verify_tls, crt, fudged)

    def test_mismatched_tls(self):
        crt, _ = create_self_signed_cert()

        # generate another key
        new_key = crypto.PKey()
        new_key.generate_key(crypto.TYPE_RSA, 2048)
        new_key = crypto.dump_privatekey(crypto.FILETYPE_PEM, new_key).decode('utf-8')

        self.assertRaises(SSL.Error, verify_tls, crt, new_key)
