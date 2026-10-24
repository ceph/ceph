"""
certmgr integration tests: CEPHADM_CERT_WARNING / CEPHADM_CERT_ERROR reporting
for user certificates.

Run by the orch/cephadm/certmgr suite; shared helpers live in base.py.
"""

from typing import Dict, Optional

from tasks.cephadm_cases.certmgr.base import CERT_HEALTH_CODES, CertMgrTestCase, WORKDIR


class TestCertMgrHealth(CertMgrTestCase):
    """Health reporting for user-provided certificates. No TLS daemons."""

    SVC = 'rgw.health-qa'

    def setUp(self) -> None:
        super().setUp()
        # debug mode: check on every serve loop and let --force store bad certs
        self.set_opt('certificate_check_debug_mode', True)
        self.set_opt('certificate_check_period', 1)
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        for code in CERT_HEALTH_CODES:
            self.wait_health(code, present=False, timeout=300)

    def tearDown(self) -> None:
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        self.set_opt('certificate_check_period', 1)
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.set_opt('certificate_check_debug_mode', True)
        for code in CERT_HEALTH_CODES:
            self.wait_health(code, present=False, timeout=300)
        super().tearDown()

    def cert_check(self) -> str:
        return '\n'.join(self.ceph_json('orch', 'certmgr', 'cert', 'check'))

    def wait_health_summary(self, code: str, summary: Optional[str],
                            services: Optional[Dict[str, bool]] = None,
                            timeout: int = 600) -> None:
        """Wait until health check `code` has exactly this summary message
        (None: the check is absent), a count and as many detail lines as the
        number of certificates it reports, and, for each service in
        `services`, a detail line for it (True) or none (False)."""
        def ok() -> bool:
            checks = self.health_checks()
            if summary is None:
                return code not in checks
            check = checks.get(code)
            if not check or check['summary']['message'] != summary:
                return False
            details = [d['message'] for d in check.get('detail', [])]
            # summary: 'Detected <N> cephadm certificate(s) issues: ...'
            total = int(summary.split()[1])
            self.assertEqual(check['summary']['count'], total, check)
            self.assertEqual(len(details), total, check)
            for svc, listed in (services or {}).items():
                self.assertEqual(any(f'({svc})' in d for d in details), listed,
                                 f'{svc} listed={listed} in {code}: {details}')
            return True
        self.poll(f'health {code} summary={summary!r}', ok, timeout=timeout, kick=True)

    def test_expiring_user_cert_raises_warning(self) -> None:
        """15 days left < 30-day threshold: WARNING. User certs are only reported,
        never renewed."""
        leaf = self.leaf('health-expiring', days=15)
        self.set_pair('rgw', leaf, service=self.SVC, force=True)
        self.wait_health('CEPHADM_CERT_WARNING', detail='rgw_ssl_cert')
        self.assertIn('about to expire', self.cert_check())
        self.assertIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'status=expiring'))
        # user certificates are never renewed by cephadm
        self.hold('user cert untouched',
                  lambda: self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC))
                  == leaf['fingerprint'], seconds=60)
        good = self.leaf('health-fixed')
        self.set_pair('rgw', good, service=self.SVC)
        self.wait_health('CEPHADM_CERT_WARNING', present=False)

    def test_expired_user_cert_raises_error(self) -> None:
        leaf = self.leaf('health-expired', not_before=-30, days=10)
        self.set_pair('rgw', leaf, service=self.SVC, force=True)
        self.wait_health('CEPHADM_CERT_ERROR', detail='rgw_ssl_cert')
        self.assertIn('has expired', self.cert_check())
        self.assertIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'status=expired'))
        self.set_pair('rgw', self.leaf('health-renewed'), service=self.SVC)
        self.wait_health('CEPHADM_CERT_ERROR', present=False)

    def test_mismatched_key_raises_error(self) -> None:
        leaf = self.leaf('health-mismatch')
        self.set_pair('rgw', leaf, service=self.SVC)
        other_key = f'{WORKDIR}/health-other.key'
        self.pki('key', other_key)
        self.ceph('orch', 'certmgr', 'key', 'set', 'rgw_ssl_key',
                  '--service-name', self.SVC, '-i', other_key)
        self.wait_health('CEPHADM_CERT_ERROR', detail='rgw_ssl_cert')
        self.assertIn('not valid', self.cert_check())
        # 'cert ls' status describes the certificate alone and stays 'valid'
        # here; the cert/key pair check is what the health check reports
        self.ceph('orch', 'certmgr', 'key', 'set', 'rgw_ssl_key',
                  '--service-name', self.SVC, '-i', leaf['key'])
        self.wait_health('CEPHADM_CERT_ERROR', present=False)

    def test_missing_key_raises_error(self) -> None:
        leaf = self.leaf('health-nokey')
        self.set_pair('rgw', leaf, service=self.SVC)
        self.ceph('orch', 'certmgr', 'key', 'rm', 'rgw_ssl_key', '--service-name', self.SVC)
        self.wait_health('CEPHADM_CERT_ERROR', detail='rgw_ssl_cert')
        self.assertIn('missing key', self.cert_check())

    def test_renewal_threshold_applies_to_user_certs(self) -> None:
        """certificate_renewal_threshold_days also decides when a user cert
        is 'about to expire', and a change takes effect without a reload.
        The cert itself never changes: only the threshold moves around its
        45 days. (cephadm never renews user certs, it only warns.)"""
        leaf = self.leaf('health-threshold', days=45)
        self.set_pair('rgw', leaf, service=self.SVC)
        # 45 days left > 30-day threshold: healthy over several checks
        self.hold('45-day cert is fine with a 30-day threshold',
                  lambda: 'CEPHADM_CERT_WARNING' not in self.health_checks(), seconds=60)
        # 45 days left < 60-day threshold: the same cert is now expiring
        self.set_opt('certificate_renewal_threshold_days', 60)
        self.wait_health('CEPHADM_CERT_WARNING', detail='rgw_ssl_cert')
        # back to 30: healthy again
        self.set_opt('certificate_renewal_threshold_days', 30)
        self.wait_health('CEPHADM_CERT_WARNING', present=False)

    def test_check_period_zero_disables_checks(self) -> None:
        """Period 0 disables the check even in debug mode; re-enabling it reports
        the pending issue."""
        self.set_opt('certificate_check_period', 0)
        leaf = self.leaf('health-disabled', not_before=-30, days=10)
        self.set_pair('rgw', leaf, service=self.SVC, force=True)
        # note: 'certmgr cert check' refreshes health itself, so it is not used here
        self.hold('no certificate health reported while checks are disabled',
                  lambda: 'CEPHADM_CERT_ERROR' not in self.health_checks(), seconds=120)
        self.set_opt('certificate_check_period', 1)
        self.wait_health('CEPHADM_CERT_ERROR', detail='rgw_ssl_cert')

    def test_mixed_issues_are_counted_per_severity(self) -> None:
        """Several certificates in different bad states at once: 2 expiring,
        1 expired and 3 invalid (2 mismatched keys, 1 missing key).

        Invalid and expired certs are reported in CEPHADM_CERT_ERROR and
        expiring ones in CEPHADM_CERT_WARNING, both at the same time. Each
        summary counts its certs per status ('3 invalid, 1 expired'), and
        each check has one detail line per cert. Fixing certs one group at a
        time updates the counts, and clearing one check leaves the other."""
        svcs = {k: f'rgw.health-mix-{k}' for k in
                ('exp1', 'exp2', 'expired', 'mism1', 'mism2', 'nokey')}
        error, warning = 'CEPHADM_CERT_ERROR', 'CEPHADM_CERT_WARNING'

        def summary(n: int, issues: str) -> str:
            return f'Detected {n} cephadm certificate(s) issues: {issues}'

        try:
            # 2 expiring: 15 days left < 30-day threshold (--force: debug mode)
            for k in ('exp1', 'exp2'):
                self.set_pair('rgw', self.leaf(f'health-mix-{k}', days=15),
                              service=svcs[k], force=True)
            # 1 expired
            self.set_pair('rgw', self.leaf('health-mix-expired', not_before=-30, days=10),
                          service=svcs['expired'], force=True)
            # 3 invalid: a valid pair, then a key that does not match the cert
            # (twice) or no key at all (once)
            good = {}
            other_key = f'{WORKDIR}/health-mix-other.key'
            self.pki('key', other_key)
            for k in ('mism1', 'mism2', 'nokey'):
                good[k] = self.leaf(f'health-mix-{k}')
                self.set_pair('rgw', good[k], service=svcs[k])
            for k in ('mism1', 'mism2'):
                self.ceph('orch', 'certmgr', 'key', 'set', 'rgw_ssl_key',
                          '--service-name', svcs[k], '-i', other_key)
            self.ceph('orch', 'certmgr', 'key', 'rm', 'rgw_ssl_key',
                      '--service-name', svcs['nokey'])

            # both checks at once, each with its own certs only
            errors = {svcs[k]: True for k in ('expired', 'mism1', 'mism2', 'nokey')}
            expiring = {svcs[k]: True for k in ('exp1', 'exp2')}
            self.wait_health_summary(error, summary(4, '3 invalid, 1 expired'),
                                     {**errors, **{s: False for s in expiring}})
            self.wait_health_summary(warning, summary(2, '2 expiring'),
                                     {**expiring, **{s: False for s in errors}})

            # fix the expired cert: ERROR loses it, WARNING is unchanged
            self.set_pair('rgw', self.leaf('health-mix-expired-fixed'),
                          service=svcs['expired'])
            self.wait_health_summary(error, summary(3, '3 invalid'),
                                     {**errors, svcs['expired']: False})
            self.wait_health_summary(warning, summary(2, '2 expiring'), expiring)

            # fix the invalid ones: ERROR clears, WARNING stays
            for k in ('mism1', 'mism2', 'nokey'):
                self.ceph('orch', 'certmgr', 'key', 'set', 'rgw_ssl_key',
                          '--service-name', svcs[k], '-i', good[k]['key'])
            self.wait_health_summary(error, None)
            self.wait_health_summary(warning, summary(2, '2 expiring'), expiring)

            # fix one expiring cert, then the other: WARNING counts down, then clears
            self.set_pair('rgw', self.leaf('health-mix-exp1-fixed'), service=svcs['exp1'])
            self.wait_health_summary(warning, summary(1, '1 expiring'),
                                     {svcs['exp1']: False, svcs['exp2']: True})
            self.set_pair('rgw', self.leaf('health-mix-exp2-fixed'), service=svcs['exp2'])
            self.wait_health_summary(warning, None)
            self.wait_health_summary(error, None)
        finally:
            for svc in svcs.values():
                self.rm_cert_key('rgw_ssl_cert', service=svc)

    def test_health_detail_lists_every_bad_cert(self) -> None:
        """One detail line per bad cert, each naming its own service."""
        svc2 = 'rgw.health-qa2'
        try:
            self.set_pair('rgw', self.leaf('health-a', not_before=-30, days=10),
                          service=self.SVC, force=True)
            self.set_pair('rgw', self.leaf('health-b', not_before=-30, days=10),
                          service=svc2, force=True)
            # match '(<service>)': SVC is a prefix of svc2, so a bare
            # substring match would be satisfied by svc2's entry alone
            self.wait_health('CEPHADM_CERT_ERROR', detail=f'({self.SVC})')
            self.wait_health('CEPHADM_CERT_ERROR', detail=f'({svc2})')
        finally:
            self.rm_cert_key('rgw_ssl_cert', service=svc2)
