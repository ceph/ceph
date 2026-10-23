"""
certmgr integration tests: automated rotation of cephadm-signed certificates.

Run by the orch/cephadm/certmgr suite; shared helpers live in base.py.
"""

from typing import Any, Dict

from tasks.cephadm_cases.certmgr.base import CERT_HEALTH_CODES, CertMgrTestCase, WORKDIR


class TestCertMgrRenewal(CertMgrTestCase):
    """Automated rotation of cephadm-signed certificates, observed on grafana
    (a non-Ceph daemon, so a reconfig restarts it and reloads the cert)."""

    PORT = 3000

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.host = cls.host_names[0]
        cls._ceph_cls('orch', 'apply', 'grafana', '--placement', cls.host)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._ceph_cls('orch', 'rm', 'grafana', check=False)
        super().tearDownClass()

    def setUp(self) -> None:
        """Every test starts from the same settled state, whatever ran before:
        default policy, grafana up, the served cert is the stored one and no
        certificate health check is raised."""
        super().setUp()
        self.reset_policy()
        self.wait_service('grafana')
        self.poll('grafana serves the stored cephadm-signed cert',
                  lambda: bool(self.stored())
                  and self.served_fp() == self.fingerprint_of(self.stored()),
                  timeout=600, kick=True)
        for code in CERT_HEALTH_CODES:
            self.wait_health(code, present=False, timeout=300)

    def tearDown(self) -> None:
        self.reset_policy()
        super().tearDown()

    def reset_policy(self) -> None:
        # threshold before rotation: re-enabling rotation while a test's raised
        # threshold is still set would renew the cert on the way out
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.set_opt('certificate_check_debug_mode', True)
        self.set_opt('certificate_automated_rotation_enabled', True)
        self.ceph('config', 'rm', 'mgr', 'mgr/cephadm/certificate_duration_days')
        self.orch('certmgr', 'reload')

    def stored(self) -> str:
        return self.get_cert('cephadm-signed_grafana_cert', host=self.host)

    def served_fp(self) -> str:
        res = self.served(self.host, self.PORT)
        return str(res['fingerprint']) if res.get('ok') else ''

    def short_lived_cert(self) -> Dict[str, Any]:
        """Replace grafana's cert with a fresh 90-day one (the minimum duration)."""
        self.set_opt('certificate_duration_days', 90)
        # newly generated certs pick up the duration after a store reload
        # (setting the option alone does not change newly generated certs)
        self.orch('certmgr', 'reload')
        # The cert being replaced may itself be a 90-day one (left by an
        # earlier test), so wait for a *different* cert, not just a short one.
        previous = self.fingerprint_of(self.stored())
        # Not rm_cert_key(): cephadm may regenerate a cephadm-signed cert
        # right after removal, so reading back '' would race. The poll below
        # checks the outcome that matters: a different cert is stored.
        # Both objects exist here (the line above read the cert), so a failed
        # rm is a real error: report it now rather than as a poll timeout.
        self.ceph('orch', 'certmgr', 'cert', 'rm', 'cephadm-signed_grafana_cert',
                  '--hostname', self.host)
        self.ceph('orch', 'certmgr', 'key', 'rm', 'cephadm-signed_grafana_key',
                  '--hostname', self.host)
        self.orch('redeploy', 'grafana')
        self.poll('a new cert in the store',
                  lambda: self.stored() != ''
                  and self.fingerprint_of(self.stored()) != previous,
                  timeout=600, kick=True)
        new = self.fingerprint_of(self.stored())
        served = self.wait_served(self.host, self.PORT,
                                  lambda r: r['fingerprint'] == new, 'new grafana cert')
        self.assertLessEqual(served['validity_days'], 91, served)
        return served

    def test_short_lived_cert_is_issued(self) -> None:
        """certificate_duration_days applies to newly issued certs."""
        served = self.short_lived_cert()
        self.assertTrue(self.issued_by_cephadm_root_ca(served['pem']))
        self.assertEqual(self.fingerprint_of(self.stored()), served['fingerprint'])

    def test_expiring_cert_is_renewed_and_served(self) -> None:
        """With rotation on, an expiring cert is renewed with the same SANs, served,
        and raises no warning; renewals stop once outside the threshold."""
        before = self.short_lived_cert()
        # 89 days left < 90. A renewed cert is again 90 days long, so it is
        # renewed (and grafana reconfigured) on every check until the
        # threshold is lowered again below; the churn is expected.
        self.set_opt('certificate_renewal_threshold_days', 90)
        renewed = self.wait_served(self.host, self.PORT,
                                   lambda r: r['fingerprint'] != before['fingerprint'],
                                   'renewed grafana certificate')
        self.assertTrue(self.issued_by_cephadm_root_ca(renewed['pem']))
        self.assertEqual(renewed['dns_names'], before['dns_names'])
        self.assertEqual(renewed['ip_addresses'], before['ip_addresses'])
        self.assertNotIn('CEPHADM_CERT_WARNING', self.health_checks(),
                         'auto-renewed certificates must not raise a warning')
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.poll('served cert matches the store once renewals stop',
                  lambda: self.served_fp() == self.fingerprint_of(self.stored()),
                  timeout=300, kick=True)
        settled = self.served_fp()
        self.hold('no renewal once the cert is outside the threshold',
                  lambda: self.served_fp() == settled, seconds=90)

    def test_rotation_disabled_warns_instead(self) -> None:
        """With rotation off, an expiring cert is only reported; it is renewed once
        rotation is back on."""
        before = self.short_lived_cert()
        self.set_opt('certificate_automated_rotation_enabled', False)
        self.set_opt('certificate_renewal_threshold_days', 90)
        self.wait_health('CEPHADM_CERT_WARNING', detail='cephadm-signed_grafana_cert')
        # several check iterations (debug mode, serve loop kicked): neither
        # the store nor the daemon may change. The store is checked as well
        # because a renewal reaches the daemon only after a reconfig.
        self.hold('rotation stays disabled',
                  lambda: self.fingerprint_of(self.stored()) == before['fingerprint']
                  and self.served_fp() == before['fingerprint'],
                  seconds=60)
        self.assertIn('about to expire',
                      '\n'.join(self.ceph_json('orch', 'certmgr', 'cert', 'check')))
        self.set_opt('certificate_automated_rotation_enabled', True)
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] != before['fingerprint'],
                         'renewal once rotation is re-enabled')
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.wait_health('CEPHADM_CERT_WARNING', present=False)

    def test_invalid_cert_is_regenerated(self) -> None:
        """A cephadm-signed cert whose key no longer matches is replaced."""
        before = self.fingerprint_of(self.stored())  # == served, see setUp
        bad_key = f'{WORKDIR}/renewal-bad.key'
        self.pki('key', bad_key)
        self.ceph('orch', 'certmgr', 'key', 'set', 'cephadm-signed_grafana_key',
                  '--hostname', self.host, '-i', bad_key)
        self.poll('store holds a regenerated cert',
                  lambda: self.stored() != ''
                  and self.fingerprint_of(self.stored()) != before,
                  timeout=600, kick=True)
        regenerated = self.fingerprint_of(self.stored())
        after = self.wait_served(self.host, self.PORT,
                                 lambda r: r['fingerprint'] == regenerated,
                                 'regenerated grafana certificate')
        self.assertTrue(self.issued_by_cephadm_root_ca(after['pem']))
        self.assertIn('All certificates are valid',
                      '\n'.join(self.ceph_json('orch', 'certmgr', 'cert', 'check')))
        self.wait_health('CEPHADM_CERT_ERROR', present=False)
