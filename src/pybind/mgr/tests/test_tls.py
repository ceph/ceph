from mgr_util import create_self_signed_cert, verify_tls, ServerConfigException, get_cert_issuer_info, certificate_days_to_expire
from OpenSSL import crypto, SSL

import unittest


valid_ceph_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQCpHIQuSYhCII1J0SVGYnT1MA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzA2MTE1MjUyWhcN\nMzIwNzAzMTE1MjUyWjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAn2ApFna2CVYE7RDtjJVk\ncJTcJQrjzDOlCoZtxb1QMCQZMXjx/7d6bseQP+dkkeA0hZxnjJZWeu6c/YnQ1JiT\n2aDuDpWoJAaiinHRJyZuY5tqG+ggn95RdToZVbeC+0uALzYi4UFacC3sfpkyIKBR\nic43+2fQNz0PZ+8INSTtm75Y53gbWuGF7Dv95200AmAN2/u8LKWZIvdhbRborxOF\nlK2T40qbj9eH3ewIN/6Eibxrvg4va3pIoOaq0XdJHAL/MjDGJAtahPIenwcjuega\n4PSlB0h3qiyFXz7BG8P0QsPP6slyD58ZJtCGtJiWPOhlq47DlnWlJzRGDEFLLryf\n8wIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQBixd7RZawlYiTZaCmv3Vy7X/hhabac\nE/YiuFt1YMe0C9+D8IcCQN/IRww/Bi7Af6tm+ncHT9GsOGWX6hahXDKTw3b9nSDi\nETvjkUTYOayZGfhYpRA6m6e/2ypcUYsiXRDY9zneDKCdPREIA1D6L2fROHetFX9r\nX9rSry01xrYwNlYA1e6GLMXm2NaGsLT3JJlRBtT3P7f1jtRGXcwkc7ns0AtW0uNj\nGqRLHfJazdgWJFsj8vBdMs7Ci0C/b5/f7J/DLpPCvUA3Fqwn9MzHl01UwlDsKy1a\nROi4cfQNOLbWX8g3PfIlqtdGYNA77UPxvy1SUimmtdopZaEVWKkqeWYK\n-----END CERTIFICATE-----\n
"""

invalid_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQCpHIQuSYhCII1J0SVGYnT1MA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzA2MTE1MjUyWhcN\nMzIwNzAzMTE1MjUyWjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEBn2ApFna2CVYE7RDtjJVk\ncJTcJQrjzDOlCoZtxb1QMCQZMXjx/7d6bseQP+dkkeA0hZxnjJZWeu6c/YnQ1JiT\n2aDuDpWoJAaiinHRJyZuY5tqG+ggn95RdToZVbeC+0uALzYi4UFacC3sfpkyIKBR\nic43+2fQNz0PZ+8INSTtm75Y53gbWuGF7Dv95200AmAN2/u8LKWZIvdhbRborxOF\nlK2T40qbj9eH3ewIN/6Eibxrvg4va3pIoOaq0XdJHAL/MjDGJAtahPIenwcjuega\n4PSlB0h3qiyFXz7BG8P0QsPP6slyD58ZJtCGtJiWPOhlq47DlnWlJzRGDEFLLryf\n8wIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQBixd7RZawlYiTZaCmv3Vy7X/hhabac\nE/YiuFt1YMe0C9+D8IcCQN/IRww/Bi7Af6tm+ncHT9GsOGWX6hahXDKTw3b9nSDi\nETvjkUTYOayZGfhYpRA6m6e/2ypcUYsiXRDY9zneDKCdPREIA1D6L2fROHetFX9r\nX9rSry01xrYwNlYA1e6GLMXm2NaGsLT3JJlRBtT3P7f1jtRGXcwkc7ns0AtW0uNj\nGqRLHfJazdgWJFsj8vBdMs7Ci0C/b5/f7J/DLpPCvUA3Fqwn9MzHl01UwlDsKy1a\nROi4cfQNOLbWX8g3PfIlqtdGYNA77UPxvy1SUimmtdopZa\n-----END CERTIFICATE-----\n
"""

expired_cert = """-----BEGIN CERTIFICATE-----\nMIICtjCCAZ4CAQAwDQYJKoZIhvcNAQENBQAwITEQMA4GA1UEAwwHY2VwaGFkbTEN\nMAsGA1UECgwEQ2VwaDAeFw0xNTAyMTYxOTQ4MTdaFw0yMDAyMTUxOTQ4MTdaMCEx\nEDAOBgNVBAMMB2NlcGhhZG0xDTALBgNVBAoMBENlcGgwggEiMA0GCSqGSIb3DQEB\nAQUAA4IBDwAwggEKAoIBAQCxYHJ6RlPeuhZJyAMR1ru01BEGbwhI7vMga8pwyTX8\nNn1ow2asbj7lad+jO5j5Gon8GFwsrKM0S8vmITxd5QkshnHPQRQF8hz4aieNOQiL\nnVRBTHgLihEBJCpyuTmHLn1G374ZObuFqyHcnIrKNdeKb0JxNKbx26/2NrWwFGAe\nAj5KuizMHJMZYVLfYelP4g2hSRPe2JJWI4429LeLWuBQBL9t/IPY0IlmFDP4eL+S\nB2Py8Ig2XY5oyaaxpwI8H/cAY92lsoHPI3ldDn1JEiH5Gwzxf+9fF29cesp8BYcm\naav1jT8ONvsfn7AxKDKcfZIpRNKlOqFIC03gG5R3O1iHAgMBAAEwDQYJKoZIhvcN\nAQENBQADggEBADh9bAMR7RIK3M3u6LoTQQrcoxJ0pEXBrFQGQk2uz2krlDTKRS+2\nubwD8bLNd3dl5RzvVJ1hui8y9JMnqYwgMrjR9B0EDUM/ihYU2zO3ZN9nhhnTN2qT\n+UtFtyilg3U4nQdWGw2jFPu08JPoF/g+7iBH+/o5WOfzOovjLg4BsVlKUP4ND8Dv\nXr8gxZTlaoZvZlhMCdhiT2aKstCA9R3RYBbEo/FtcsHOkO0EFuxCLiVd/eo3F56C\njfVWnvqyz3r2f1G1VafvhhdlMJ4p35Hw1ms6nFTLx5dKwJW+Xve+qBU3Q5I5iV02\nAIXXBaqId/YqKXZd+Ge/XBmluXH929PtUOk=\n-----END CERTIFICATE-----\n
"""

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
        fudged = f"{key[:-35]}c0ffee==\n{key[-25:]}"
        self.assertRaises(ServerConfigException, verify_tls, crt, fudged)

    def test_mismatched_tls(self):
        crt, _ = create_self_signed_cert()

        # generate another key
        new_key = crypto.PKey()
        new_key.generate_key(crypto.TYPE_RSA, 2048)
        new_key = crypto.dump_privatekey(crypto.FILETYPE_PEM, new_key).decode('utf-8')
        self.assertRaises(ServerConfigException, verify_tls, crt, new_key)

    def test_get_cert_issuer_info(self):

        # valid certificate
        org, cn = get_cert_issuer_info(valid_ceph_cert)
        assert org == 'Ceph'
        assert cn == 'cephadm'

        # empty certificate
        self.assertRaises(ServerConfigException, get_cert_issuer_info, '')

        # invalid certificate
        self.assertRaises(ServerConfigException, get_cert_issuer_info, invalid_cert)

        # expired certificate
        self.assertRaisesRegex(ServerConfigException,
                               'Certificate issued by "Ceph/cephadm" expired',
                               certificate_days_to_expire, expired_cert)
