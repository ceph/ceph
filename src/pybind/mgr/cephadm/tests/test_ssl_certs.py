import unittest

from cephadm.ssl_certs import SSLCerts, COMMON_NAME_MAX_LENGTH
from ceph.deployment.tls_utils import extract_ips_and_fqdns_from_cert


class TestSSLCertsCommonNameLength(unittest.TestCase):
    """Regression tests for the X.509 CN length limit (RFC 5280, 64 chars)."""

    def setUp(self):
        self.certs = SSLCerts(fsid="test-fsid-0000")
        self.certs.generate_root_cert(addr="10.0.0.1")

    def test_long_fqdn_does_not_raise(self):
        """A CN over 64 characters must not raise during certificate generation."""
        long_fqdn = "ceph-node1.europe-west3-a.c.project-12850071-31c6-4077-a2f.internal"
        self.assertGreater(len(long_fqdn), COMMON_NAME_MAX_LENGTH)

        # Should not raise ValueError: Attribute's length must be >= 1 and <= 64
        cert_pem, key_pem = self.certs.generate_cert(
            _hosts=[long_fqdn, 'grafana_servers'],
            _addrs=[long_fqdn],
        )
        self.assertIn("BEGIN CERTIFICATE", cert_pem)

    def test_long_fqdn_is_preserved_in_san(self):
        """
        Truncating the CN must not affect the SAN: the full, untruncated
        hostname (and any other host entries) must still be present for
        TLS hostname verification. Uses distinct _hosts/_addrs values,
        matching how real callers (e.g. Grafana cert generation) invoke
        this, so the test actually exercises the hosts/addrs split
        rather than masking it by passing the same value for both.
        """
        long_fqdn = "ceph-node1.europe-west3-a.c.project-12850071-31c6-4077-a2f.internal"
        cert_pem, _ = self.certs.generate_cert(
            _hosts=[long_fqdn, 'grafana_servers'],
            _addrs=[long_fqdn],
        )
        _, fqdns = extract_ips_and_fqdns_from_cert(cert_pem)
        self.assertIn(long_fqdn.lower(), fqdns)
        self.assertIn('grafana_servers', fqdns)

    def test_short_hostname_is_not_truncated(self):
        """A hostname within the 64-char limit must be used as-is for the CN, unmodified."""
        short_host = "ceph-node1"
        cert_pem, _ = self.certs.generate_cert(
            _hosts=[short_host, 'grafana_servers'],
            _addrs=[short_host],
        )
        info_cn = self._extract_cn(cert_pem)
        self.assertEqual(info_cn, short_host)

    def test_cn_is_truncated_at_first_label(self):
        """
        When truncation does happen, the CN is cut at the first DNS
        label (split on '.'), not at a blind character offset. A DNS
        label maxes at 63 characters (RFC 1035) and IPs never exceed
        64 characters, so the first label always fits under
        COMMON_NAME_MAX_LENGTH.
        """
        long_fqdn = "ceph-node1.europe-west3-a.c.project-12850071-31c6-4077-a2f.internal"
        cert_pem, _ = self.certs.generate_cert(
            _hosts=[long_fqdn, 'grafana_servers'],
            _addrs=[long_fqdn],
        )
        cn = self._extract_cn(cert_pem)
        self.assertEqual(cn, 'ceph-node1')
        self.assertLessEqual(len(cn), COMMON_NAME_MAX_LENGTH)

    def test_dotless_string_over_limit_is_still_truncated(self):
        """
        Defensive edge case: a dot-less string over 64 characters has
        no label to split on. split('.')[0] alone would return the
        whole string unchanged (still over the limit), so this must
        fall back to a character slice as well.
        """
        long_fqdn = "a" * 100
        cert_pem, _ = self.certs.generate_cert(
            _hosts=[long_fqdn, 'grafana_servers'],
            _addrs=[long_fqdn],
        )
        cn = self._extract_cn(cert_pem)
        self.assertLessEqual(len(cn), COMMON_NAME_MAX_LENGTH)

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
