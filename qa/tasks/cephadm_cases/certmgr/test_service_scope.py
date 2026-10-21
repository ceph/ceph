"""
certmgr integration tests: RGW (service scope) through every certificate
source: cephadm-signed, inline and reference.

Run by the orch/cephadm/certmgr suite; shared helpers live in base.py.
"""

from typing import Any, Dict

from tasks.cephadm_cases.certmgr.base import CertMgrTestCase, WORKDIR


class TestCertMgrServiceScope(CertMgrTestCase):
    """RGW end to end: what clients see for each certificate source."""

    SVC_ID = 'certqa'
    SVC = f'rgw.{SVC_ID}'
    PORT = 8443

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.host = cls.host_names[0]

    def tearDown(self) -> None:
        self.rm_service(self.SVC)
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        super().tearDown()

    def spec(self, source: str, **extra: Any) -> Dict[str, Any]:
        spec: Dict[str, Any] = {'ssl': True, 'certificate_source': source,
                                'rgw_frontend_port': self.PORT}
        spec.update(extra)
        return {'service_type': 'rgw', 'service_id': self.SVC_ID,
                'placement': {'hosts': [self.host]}, 'spec': spec}

    def deploy(self, source: str, **extra: Any) -> None:
        self.apply(self.SVC, self.spec(source, **extra))
        self.wait_service(self.SVC)

    def change_tls(self, source: str, **extra: Any) -> None:
        """Change the TLS config of the running RGW service.

        Workaround: when an RGW TLS dependency changes (cert hash, source)
        cephadm only *reconfigures* the daemon (RgwService has no
        choose_next_action override). Reconfig rewrites rgw/cert/<svc>, but
        RGW reads it only at startup, so the old cert stays in service.
        Redeploy explicitly until that is fixed (known open issue: RGW does
        not pick up a rotated certificate on its own)."""
        self.apply(self.SVC, self.spec(source, **extra))
        self.orch('redeploy', self.SVC)
        self.wait_service(self.SVC)

    def cephadm_signed_name(self) -> str:
        return f'cephadm-signed_{self.SVC}_cert'

    def assert_verified_chain(self) -> None:
        res = self.served(self.host, self.PORT, cafile=f'{WORKDIR}/ca/root/ca.crt')
        self.assertTrue(res['ok'], f'handshake trusting only the QA root failed: {res}')

    def test_cephadm_signed(self) -> None:
        """Signed by the cephadm root CA with host IP and custom SANs, stored per host,
        hidden from the default 'cert ls', and not editable through certmgr."""
        self.deploy('cephadm-signed', custom_sans=['s3.certqa.test'])
        served = self.wait_served(self.host, self.PORT, lambda r: True, 'rgw TLS')
        self.assertTrue(self.issued_by_cephadm_root_ca(served['pem']))
        self.assertIn(self.hosts[self.host], served['ip_addresses'])
        self.assertIn('s3.certqa.test', served['dns_names'])
        verified = self.served(self.host, self.PORT, cafile=f'{WORKDIR}/cephadm-root-ca.crt')
        self.assertTrue(verified['ok'], verified)
        # stored as a host-scoped cephadm-signed object, hidden by default
        name = self.cephadm_signed_name()
        self.assertEqual(self.fingerprint_of(self.get_cert(name, host=self.host)),
                         served['fingerprint'])
        self.assertNotIn(name, self.cert_ls())
        ls = self.cert_ls('--include-cephadm-signed')
        self.assertIn(self.host, ls[name]['certificates'])
        self.assertIn(name, self.cert_ls('--filter-by', 'signed-by=cephadm'))
        # cephadm-signed objects are host-scoped and owned by cephadm
        pem = self.pem_file('rgw-cephadm-signed',
                            self.get_key(f'cephadm-signed_{self.SVC}_key', host=self.host),
                            self.get_cert(name, host=self.host))
        self.assert_fails(self.cert_key_set_args('rgw', pem)
                          + ['--hostname', self.host, '--cert-name', name], 'not editable')

    def test_inline(self) -> None:
        """Served with its chain, owned by the spec (not editable through certmgr),
        and replaced when the spec carries a new one."""
        leaf = self.leaf('rgw-inline', dns=['s3.certqa.test'], ips=[self.hosts[self.host]])
        self.deploy('inline', ssl_cert=self.read(leaf['chain']), ssl_key=self.read(leaf['key']))
        served = self.wait_served(self.host, self.PORT,
                                  lambda r: r['fingerprint'] == leaf['fingerprint'],
                                  'inline certificate')
        self.assertEqual(served['subject'], leaf['subject'])
        self.assert_verified_chain()
        ls = self.cert_ls('--filter-by', 'signed-by=user')
        self.assertIn(self.SVC, ls['rgw_ssl_cert']['certificates'])
        # inline certificates belong to the spec, not to certmgr
        self.assert_fails(['orch', 'certmgr', 'cert-key', 'set', 'rgw', '--service-name',
                           self.SVC, '-i', self.leaf('rgw-inline-2')['full']], 'not editable')
        # a new inline cert in the spec replaces the stored and the served one
        leaf2 = self.leaf('rgw-inline-3', ips=[self.hosts[self.host]])
        self.change_tls('inline', ssl_cert=self.read(leaf2['chain']),
                        ssl_key=self.read(leaf2['key']))
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] == leaf2['fingerprint'],
                         'updated inline certificate')
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf2['fingerprint'])

    def test_reference_requires_certificate_first(self) -> None:
        """A 'reference' spec is refused while certmgr has no cert for the service:
        non-zero exit (an OrchestratorError, unlike spec validation errors), a
        hint to run 'cert-key set', and nothing deployed."""
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        res = self.apply(self.SVC, self.spec('reference'), check=False)
        self.assertNotEqual(res.rc, 0, res)
        self.assertIn('cert-key set', res.err + res.out)
        self.assertFalse(self.daemons(self.SVC))

    def test_reference(self) -> None:
        """A cert set through certmgr is served; removing the service keeps it
        (reference certs are user data)."""
        leaf = self.leaf('rgw-ref', ips=[self.hosts[self.host]])
        self.set_pair('rgw', leaf, service=self.SVC)
        self.deploy('reference')
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] == leaf['fingerprint'],
                         'referenced certificate')
        self.assert_verified_chain()
        # removing the service keeps user-provided reference objects
        self.rm_service(self.SVC)
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.assertTrue(self.get_key('rgw_ssl_key', service=self.SVC))

    def test_source_switches_garbage_collect(self) -> None:
        """Switching source drops the objects owned by the old one (cephadm-signed,
        inline) and keeps reference objects."""
        name = self.cephadm_signed_name()
        self.deploy('cephadm-signed')
        self.poll('cephadm-signed cert stored',
                  lambda: bool(self.get_cert(name, host=self.host)), timeout=300)

        leaf = self.leaf('rgw-gc-inline', ips=[self.hosts[self.host]])
        self.change_tls('inline', ssl_cert=self.read(leaf['chain']),
                        ssl_key=self.read(leaf['key']))
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] == leaf['fingerprint'], 'inline after switch')
        self.poll('cephadm-signed cert removed after switching to inline',
                  lambda: self.get_cert(name, host=self.host) == '', timeout=300, kick=True)

        # inline -> reference without a certmgr entry: the apply is refused
        # until a certificate is provided. (The refused apply also drops the
        # running service's inline objects; that side effect is questionable
        # and racy to observe, since the still-inline spec re-saves them on
        # the next config generation, so it is not asserted here.)
        res = self.apply(self.SVC, self.spec('reference'), check=False)
        self.assertNotEqual(res.rc, 0, res)
        self.assertIn('cert-key set', res.err + res.out)

        ref = self.leaf('rgw-gc-ref', ips=[self.hosts[self.host]])
        self.set_pair('rgw', ref, service=self.SVC)
        self.change_tls('reference')
        self.wait_served(self.host, self.PORT,
                         lambda r: r['fingerprint'] == ref['fingerprint'], 'reference after switch')

        self.change_tls('cephadm-signed')
        self.wait_served(self.host, self.PORT,
                         lambda r: self.issued_by_cephadm_root_ca(r['pem']),
                         'cephadm-signed after switch')
        # reference objects are user data and survive the switch
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         ref['fingerprint'])

    def test_service_removal_cleans_cephadm_signed_and_inline(self) -> None:
        """Removing the service removes its cephadm-signed and inline certs."""
        name = self.cephadm_signed_name()
        self.deploy('cephadm-signed')
        self.poll('cephadm-signed cert stored',
                  lambda: bool(self.get_cert(name, host=self.host)), timeout=300)
        self.rm_service(self.SVC)
        self.poll('cephadm-signed cert removed with the service',
                  lambda: self.get_cert(name, host=self.host) == '', timeout=300, kick=True)

        leaf = self.leaf('rgw-rm-inline', ips=[self.hosts[self.host]])
        self.deploy('inline', ssl_cert=self.read(leaf['chain']), ssl_key=self.read(leaf['key']))
        self.rm_service(self.SVC)
        self.poll('inline cert removed with the service',
                  lambda: self.get_cert('rgw_ssl_cert', service=self.SVC) == '',
                  timeout=300, kick=True)
