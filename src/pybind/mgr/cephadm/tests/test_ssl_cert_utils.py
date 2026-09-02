import unittest

from cephadm.ssl_cert_utils import SSLCerts, COMMON_NAME_MAX_LENGTH, extract_ips_and_fqdns_from_cert


class TestSSLCertsCommonNameLength(unittest.TestCase):
    """
    Regression tests for the X.509 Common Name (CN) length limit.

    RFC 5280 limits the CN field to 64 characters. generate_cert()
    previously passed the raw address/hostname directly as the CN with
    no length check, which fails certificate signing with an opaque
    low-level error when the hostname exceeds 64 characters (commonly
    seen with cloud providers, e.g. Google Cloud Platform, that embed
    project IDs into auto-assigned VM FQDNs).
    """

    def setUp(self):
        self.certs = SSLCerts(fsid="test-fsid-0000")
        self.certs.generate_root_cert(addr="10.0.0.1")

    def test_long_fqdn_does_not_raise(self):
        """A CN over 64 characters must not raise during certificate generation."""
        long_fqdn = "ceph-node1.europe-west3-a.c.project-12850071-31c6-4077-a2f.internal"
        self.assertGreater(len(long_fqdn), COMMON_NAME_MAX_LENGTH)

        # Should not raise ValueError: Attribute's length must be >= 1 and <= 64
        cert_pem, key_pem = self.certs.generate_cert(
            _hosts=[long_fqdn],
            _addrs=[long_fqdn],
        )
        self.assertIn("BEGIN CERTIFICATE", cert_pem)

    def test_long_fqdn_is_preserved_in_san(self):
        """
        Truncating the CN must not affect the SAN: the full, untruncated
        hostname must still be present for TLS hostname verification.
        """
        long_fqdn = "ceph-node1.europe-west3-a.c.project-12850071-31c6-4077-a2f.internal"
        cert_pem, _ = self.certs.generate_cert(
            _hosts=[long_fqdn],
            _addrs=[long_fqdn],
        )
        _, fqdns = extract_ips_and_fqdns_from_cert(cert_pem)
        self.assertIn(long_fqdn.lower(), fqdns)

    def test_short_hostname_is_not_truncated(self):
        """A hostname within the 64-char limit must be used as-is for the CN, unmodified."""
        short_host = "ceph-node1"
        cert_pem, _ = self.certs.generate_cert(
            _hosts=[short_host],
            _addrs=[short_host],
        )
        info_cn = self._extract_cn(cert_pem)
        self.assertEqual(info_cn, short_host)

    def test_cn_is_truncated_to_exactly_max_length(self):
        """When truncation does happen, the resulting CN must be exactly 64 characters."""
        long_fqdn = "a" * 100  # deliberately far over the limit
        cert_pem, _ = self.certs.generate_cert(
            _hosts=[long_fqdn],
            _addrs=[long_fqdn],
        )
        cn = self._extract_cn(cert_pem)
        self.assertEqual(len(cn), COMMON_NAME_MAX_LENGTH)
        self.assertEqual(cn, long_fqdn[:COMMON_NAME_MAX_LENGTH])

    @staticmethod
    def _extract_cn(cert_pem: str) -> str:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.backends import default_backend

        cert = x509.load_pem_x509_certificate(cert_pem.encode('utf-8'), backend=default_backend())
        cn_attrs = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        return cn_attrs[0].value


if __name__ == '__main__':
    unittest.main()
