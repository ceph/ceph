import os
import time
import unittest
from datetime import datetime, timedelta, timezone

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.backends import default_backend

from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException


def _build_self_signed_cert(not_valid_after):
    """Build a minimal self-signed cert/key pair with a specific expiry."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-root")])
    builder = x509.CertificateBuilder().subject_name(name).issuer_name(name)
    builder = builder.public_key(key.public_key()).serial_number(x509.random_serial_number())
    builder = builder.not_valid_before(datetime.now(timezone.utc) - timedelta(days=1))
    builder = builder.not_valid_after(not_valid_after)
    cert = builder.sign(private_key=key, algorithm=hashes.SHA256(), backend=default_backend())

    cert_pem = cert.public_bytes(encoding=serialization.Encoding.PEM).decode('utf-8')
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode('utf-8')
    return cert_pem, key_pem


class TestLoadRootCredentialsExpiryCheck(unittest.TestCase):
    """
    Regression tests for the UTC-vs-local timezone bug in
    load_root_credentials(). `not_valid_after` returns a timezone-naive
    datetime that represents UTC; using its (always-None) tzinfo for the
    expiry comparison silently degrades to a naive-local-vs-naive-UTC
    comparison, which is wrong on any mgr host not running in UTC.
    """

    def setUp(self):
        self.certs = SSLCerts(fsid="test-fsid-0000")

    def test_valid_cert_is_accepted(self):
        """A certificate with time remaining must not be rejected as expired."""
        not_after = datetime.now(timezone.utc) + timedelta(hours=1)
        cert_pem, key_pem = _build_self_signed_cert(not_after)
        # Should not raise
        self.certs.load_root_credentials(cert_pem, key_pem)

    def test_expired_cert_is_rejected(self):
        """A certificate whose expiry has passed must raise SSLConfigException."""
        not_after = datetime.now(timezone.utc) - timedelta(minutes=5)
        cert_pem, key_pem = _build_self_signed_cert(not_after)
        with self.assertRaises(SSLConfigException):
            self.certs.load_root_credentials(cert_pem, key_pem)

    def test_expiry_check_is_timezone_independent(self):
        """
        The comparison must be based on absolute UTC time, not on the
        mgr host's local timezone. This directly exercises the bug: the
        original implementation used `datetime.now(tz)` where `tz` was
        always None (since `not_valid_after` is naive), which returns
        naive *local* time and compares it against a naive *UTC* value.

        This test genuinely changes the process's local timezone (via
        TZ env var + time.tzset(), Unix-only) to IST (UTC+5:30) and
        confirms a certificate expiring in 1 hour is still correctly
        accepted. Against the original buggy implementation, this case
        fails: local IST "now" is ~5.5 hours ahead of the naive-UTC
        `not_valid_after` value, so the (broken) comparison treats a
        still-valid certificate as already expired.
        """
        original_tz = os.environ.get('TZ')
        try:
            os.environ['TZ'] = 'Asia/Kolkata'  # UTC+5:30
            time.tzset()

            not_after = datetime.now(timezone.utc) + timedelta(hours=1)
            cert_pem, key_pem = _build_self_signed_cert(not_after)

            # Should not raise, regardless of the process's local timezone
            self.certs.load_root_credentials(cert_pem, key_pem)
        finally:
            if original_tz is not None:
                os.environ['TZ'] = original_tz
            else:
                os.environ.pop('TZ', None)
            time.tzset()


if __name__ == '__main__':
    unittest.main()
