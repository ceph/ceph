"""
certmgr integration tests: certmgr store semantics through the CLI (bindings,
root CA, scope targets, PEM layouts, rejected inputs, filters, rm/get,
persistence).

Run by the orch/cephadm/certmgr suite; shared helpers live in base.py.
"""

import json
import unittest
from typing import List, Tuple

from tasks.cephadm_cases.certmgr.base import CertMgrTestCase, ROOT_CA, ROOT_CA_KEY, WORKDIR


class TestCertMgrCLI(CertMgrTestCase):
    """certmgr store semantics through the CLI. No TLS daemons are deployed."""

    MGRS_REQUIRED = 2
    SVC = 'rgw.cli-qa'

    def tearDown(self) -> None:
        self.rm_cert_key('rgw_ssl_cert', service=self.SVC)
        self.rm_cert_key('grafana_ssl_cert', host=self.host_names[0])
        self.rm_cert_key('mgmt_gateway_ssl_cert')
        super().tearDown()

    def test_bindings_expose_every_consumer_with_its_scope(self) -> None:
        bindings = self.ceph_json('orch', 'certmgr', 'bindings', 'ls')
        expected = {
            'service': {'rgw': 'rgw_ssl_cert', 'ingress': 'ingress_ssl_cert',
                        'iscsi': 'iscsi_ssl_cert', 'nvmeof': 'nvmeof_ssl_cert',
                        'nfs': 'nfs_ssl_cert', 'smb': 'smb_ssl_cert'},
            'host': {'grafana': 'grafana_ssl_cert', 'oauth2-proxy': 'oauth2_proxy_ssl_cert'},
            'global': {'mgmt-gateway': 'mgmt_gateway_ssl_cert'},
        }
        for scope, consumers in expected.items():
            self.assertIn(scope, bindings)
            for consumer, cert_name in consumers.items():
                self.assertIn(consumer, bindings[scope], f'{consumer} missing in {scope} scope')
                self.assertIn(cert_name, bindings[scope][consumer]['certs'])
                self.assertIn(cert_name.replace('_cert', '_key'),
                              bindings[scope][consumer]['keys'])
        # services that need a CA bundle register it next to the leaf
        self.assertIn('nfs_ssl_ca_cert', bindings['service']['nfs']['certs'])
        self.assertIn('smb_ssl_ca_cert', bindings['service']['smb']['certs'])
        self.assertIn('nvmeof_root_ca_cert', bindings['service']['nvmeof']['certs'])

    def test_root_ca_is_listed_and_is_a_ca(self) -> None:
        ls = self.cert_ls()
        self.assertIn(ROOT_CA, ls, 'root CA must be listed even without --include-cephadm-signed')
        self.assertEqual(ls[ROOT_CA]['scope'], 'global')
        info = self.cert_info_pem(self.get_cert(ROOT_CA))
        self.assertTrue(info['is_ca'])
        self.assertGreater(info['remaining_days'], 9 * 365)
        self.assertNotIn(ROOT_CA_KEY, self.ceph_json('orch', 'certmgr', 'key', 'ls'))
        self.assertNotIn(ROOT_CA_KEY, self.ceph_json('orch', 'certmgr', 'key', 'ls',
                                                     '--include-cephadm-generated-keys'))

    def test_root_ca_is_not_editable(self) -> None:
        root_fp = self.fingerprint_of(self.get_cert(ROOT_CA))
        other = self.leaf('not-a-root', ca='root')
        res = self.ceph('orch', 'certmgr', 'cert', 'set', ROOT_CA, '-i', other['crt'],
                        check=False)
        self.assertNotEqual(res.rc, 0, res)
        self.assertEqual(self.fingerprint_of(self.get_cert(ROOT_CA)), root_fp,
                         'root CA must never be replaced')

    # Known issue: _raise_non_editable_cert_error() only assigns its context
    # for a service, host or consumer, so 'cert set' on a global certificate
    # fails with an UnboundLocalError instead of the 'not editable' error.
    # Drop the decorator once that is fixed (an unexpected success fails).
    @unittest.expectedFailure
    def test_root_ca_edit_reports_not_editable(self) -> None:
        other = self.leaf('not-a-root-2', ca='root')
        self.assert_fails(['orch', 'certmgr', 'cert', 'set', ROOT_CA, '-i', other['crt']],
                          'not editable')

    def test_set_get_roundtrip_service_scope(self) -> None:
        leaf = self.leaf('cli-rt', dns=['s3.cli-qa.test'])
        self.set_pair('rgw', leaf, service=self.SVC)
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.assertEqual(self.get_key('rgw_ssl_key', service=self.SVC).strip(),
                         self.read(leaf['key']).strip())
        ls = self.cert_ls()
        self.assertEqual(ls['rgw_ssl_cert']['scope'], 'service')
        entry = ls['rgw_ssl_cert']['certificates'][self.SVC]
        self.assertTrue(entry['contains_chain'])
        self.assertIn('rgw_ssl_key', self.ceph_json('orch', 'certmgr', 'key', 'ls'))

    def test_cert_ls_filters(self) -> None:
        """'cert ls --filter-by' with one valid, user-provided, service-scoped
        cert (rgw_ssl_cert for SVC). Each filter is a 'key=value' match on
        the cert's name (glob), status, scope, service (glob) or signer;
        several filters separated by ',' must all match."""
        leaf = self.leaf('cli-filter')
        self.set_pair('rgw', leaf, service=self.SVC)
        # name glob: matches rgw_ssl_cert, and also drops the root CA that
        # 'cert ls' otherwise always lists
        by_name = self.cert_ls('--filter-by', 'name=rgw*')
        self.assertIn('rgw_ssl_cert', by_name)
        self.assertNotIn(ROOT_CA, by_name)
        # two filters (AND), both true for this cert: positive control for
        # the status filter used just below
        self.assertIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'scope=service,status=valid'))
        # the cert is valid (365 days), so a status=expired filter excludes it.
        # The positive case (an expired cert is listed) is in the health facet.
        self.assertNotIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'status=expired'))
        # wrong scope excludes it, its own service includes it
        self.assertNotIn('rgw_ssl_cert', self.cert_ls('--filter-by', 'scope=host'))
        self.assertIn('rgw_ssl_cert', self.cert_ls('--filter-by', f'service={self.SVC}'))
        # an explicit signed-by filter replaces the default user-only view:
        # the user cert goes, the cephadm-signed root CA stays
        signed_by_cephadm = self.cert_ls('--filter-by', 'signed-by=cephadm')
        self.assertNotIn('rgw_ssl_cert', signed_by_cephadm)
        self.assertIn(ROOT_CA, signed_by_cephadm)

    def test_scope_targets_are_enforced(self) -> None:
        """'cert-key set' needs the target that matches the consumer's scope:
        --service-name for service scope, --hostname for host scope, none for
        global scope. The leaf is valid, so each rejection below must come
        from the missing target (the expected message checks that), not from
        the certificate itself."""
        leaf = self.leaf('cli-scope')
        # rgw is service-scoped: rejected without --service-name
        self.assert_fails(self.cert_key_set_args('rgw', leaf['full']), '--service-name')
        # grafana is host-scoped: rejected without --hostname
        self.assert_fails(self.cert_key_set_args('grafana', leaf['full']), '--hostname')
        # unknown consumer: rejected, pointing at 'certmgr bindings ls'
        self.assert_fails(self.cert_key_set_args('no-such-service', leaf['full']),
                          'bindings ls')
        # host and global scope succeed with the right target
        self.set_pair('grafana', leaf, host=self.host_names[0])
        self.set_pair('mgmt-gateway', leaf)
        self.assertTrue(self.get_cert('grafana_ssl_cert', host=self.host_names[0]))
        self.assertTrue(self.get_cert('mgmt_gateway_ssl_cert'))

    def test_consumer_with_several_certs_needs_cert_name(self) -> None:
        """nvmeof is bound to more than one certificate (nvmeof_ssl_cert,
        nvmeof_root_ca_cert, ...), so 'cert-key set nvmeof' cannot tell which
        one to set and must ask for --cert-name. The target (--service-name)
        is given, so the rejection comes from the ambiguity alone."""
        leaf = self.leaf('cli-multi')
        self.assert_fails(self.cert_key_set_args('nvmeof', leaf['full'])
                          + ['--service-name', 'nvmeof.cli-qa'], '--cert-name')

    def test_pem_inputs_accepted(self) -> None:
        """Every key format (PKCS#8, PKCS#1, SEC1) in every block order is accepted:
        the key is split out and the chain is kept as given."""
        layouts = ('key+leaf', 'key+leaf+intermediate', 'leaf+intermediate+key')
        for key_type in ('rsa', 'rsa-pkcs1', 'ec'):
            for idx, layout in enumerate(layouts):
                with self.subTest(key_type=key_type, layout=layout):
                    name = f'cli-{key_type}-layout{idx}'
                    leaf = self.leaf(name, key_type=key_type)
                    parts = {'key+leaf': (leaf['key'], leaf['crt']),
                             'key+leaf+intermediate': (leaf['key'], leaf['chain']),
                             'leaf+intermediate+key': (leaf['chain'], leaf['key'])}[layout]
                    pem = self.pem_file(f'{name}-in', *parts)
                    self.set_pair('rgw', leaf, service=self.SVC, pem=pem)
                    stored = self.get_cert('rgw_ssl_cert', service=self.SVC)
                    self.assertEqual(self.fingerprint_of(stored), leaf['fingerprint'])
                    self.assertNotIn('PRIVATE KEY', stored,
                                     'key material must be split out of the certificate')
                    self.assertEqual(self.get_key('rgw_ssl_key', service=self.SVC).strip(),
                                     self.read(leaf['key']).strip())
                    # the stored chain is leaf first, then the intermediate
                    # when one was given (it must not be dropped on parsing)
                    expected = leaf['chain'] if 'intermediate' in layout else leaf['crt']
                    self.assertEqual(stored.strip(), self.read(expected).strip(),
                                     'stored chain differs from the input chain')

    def test_invalid_inputs_rejected(self) -> None:
        """Each bad input is refused with its own reason, and nothing is stored."""
        good = self.leaf('cli-good')
        other_key = f'{WORKDIR}/cli-other.key'
        self.pki('key', other_key)
        expired = self.leaf('cli-expired', not_before=-30, days=10)
        expiring = self.leaf('cli-expiring', days=5)
        encrypted = self.leaf('cli-encrypted', passphrase='secret')
        cases: List[Tuple[str, str, Tuple[str, ...]]] = [
            ('mismatched key', self.pem_file('cli-mismatch', other_key, good['crt']),
             ('does not match',)),
            ('expired', expired['full'], ('expired',)),
            ('expiring', expiring['full'], ('about to expire',)),
            ('encrypted key', encrypted['full'], ('encrypted',)),
            ('two private keys', self.pem_file('cli-two-keys', good['key'], other_key,
                                               good['crt']), ('private key',)),
            ('key without cert', good['key'], ('no CERTIFICATE blocks',)),
            ('cert without key', good['chain'], ('private key are both required',)),
            ('corrupt certificate block', self.pem_file(
                'cli-corrupt', good['key'],
                '-----BEGIN CERTIFICATE-----\n' + ('A' * 64 + '\n') * 5
                + '-----END CERTIFICATE-----\n'), ('failed to parse',)),
        ]
        for label, pem, needles in cases:
            with self.subTest(case=label):
                self.assert_fails(self.cert_key_set_args('rgw', pem)
                                  + ['--service-name', self.SVC], *needles)
                self.assertEqual(self.get_cert('rgw_ssl_cert', service=self.SVC), '',
                                 f'{label}: nothing may be stored after a rejected input')
        self.assert_fails(['orch', 'certmgr', 'cert', 'set', 'rgw_ssl_cert',
                           '--service-name', self.SVC, '-i', good['full']], 'cert-key set')

    def test_force_without_debug_mode_does_not_bypass_validation(self) -> None:
        # The positive case (--force with debug mode on stores the cert) is
        # exercised by the health facet; doing it here would raise
        # CEPHADM_CERT_ERROR, which this facet does not ignore.
        expired = self.leaf('cli-force', not_before=-30, days=10)
        self.set_opt('certificate_check_debug_mode', False)
        self.assert_fails(self.cert_key_set_args('rgw', expired['full'])
                          + ['--service-name', self.SVC, '--force'], 'expired')
        self.assertEqual(self.get_cert('rgw_ssl_cert', service=self.SVC), '',
                         'a rejected certificate must not be stored')

    def test_separate_cert_and_key_set(self) -> None:
        leaf = self.leaf('cli-separate')
        self.ceph('orch', 'certmgr', 'cert', 'set', 'rgw_ssl_cert',
                  '--service-name', self.SVC, '-i', leaf['chain'])
        self.ceph('orch', 'certmgr', 'key', 'set', 'rgw_ssl_key',
                  '--service-name', self.SVC, '-i', leaf['key'])
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.assertIn('All certificates are valid',
                      '\n'.join(self.ceph_json('orch', 'certmgr', 'cert', 'check')))

    def test_rm_and_missing_objects(self) -> None:
        """rm removes cert and key; get/rm on a missing object, or get without the
        required --service-name, fail with a clear error."""
        leaf = self.leaf('cli-rm')
        self.set_pair('rgw', leaf, service=self.SVC)
        self.ceph('orch', 'certmgr', 'cert', 'rm', 'rgw_ssl_cert', '--service-name', self.SVC)
        self.ceph('orch', 'certmgr', 'key', 'rm', 'rgw_ssl_key', '--service-name', self.SVC)
        self.assertEqual(self.get_cert('rgw_ssl_cert', service=self.SVC), '')
        self.assertEqual(self.get_key('rgw_ssl_key', service=self.SVC), '')
        self.assert_fails(['orch', 'certmgr', 'cert', 'get', 'rgw_ssl_cert',
                           '--service-name', self.SVC], 'no secret found')
        self.assert_fails(['orch', 'certmgr', 'cert', 'rm', 'rgw_ssl_cert',
                           '--service-name', self.SVC], 'cannot delete')
        self.assert_fails(['orch', 'certmgr', 'cert', 'rm', 'no_such_cert'], 'cannot delete')
        self.assert_fails(['orch', 'certmgr', 'cert', 'get', 'rgw_ssl_cert'],
                          'need service name')

    def test_generate_certificates(self) -> None:
        """Certificates for the dashboard and prometheus mgr modules are issued by
        the cephadm root CA; other modules are refused."""
        for module in ('dashboard', 'prometheus'):
            with self.subTest(module=module):
                out = json.loads(self.orch('certmgr', 'generate-certificates', module).out)
                self.assertIn('PRIVATE KEY', out['key'])
                self.assertTrue(self.issued_by_cephadm_root_ca(out['cert']))
                info = self.cert_info_pem(out['cert'])
                self.assertFalse(info['is_ca'])
                if module == 'dashboard':
                    self.assertIn('dashboard_servers', info['dns_names'])
        self.assert_fails(['orch', 'certmgr', 'generate-certificates', 'nope'], 'nope')

    def test_store_survives_reload_and_mgr_failover(self) -> None:
        leaf = self.leaf('cli-persist')
        self.set_pair('rgw', leaf, service=self.SVC)
        root_fp = self.fingerprint_of(self.get_cert(ROOT_CA))
        self.assertEqual(self.orch('certmgr', 'reload').out.strip(), 'OK')
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.fail_mgr_and_wait()
        self.assertEqual(self.fingerprint_of(self.get_cert('rgw_ssl_cert', service=self.SVC)),
                         leaf['fingerprint'])
        self.assertEqual(self.fingerprint_of(self.get_cert(ROOT_CA)), root_fp,
                         'root CA must not be regenerated on failover')
