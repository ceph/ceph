"""
certmgr integration tests: grafana (host scope).

Run by the orch/cephadm/certmgr suite; shared helpers live in base.py.
"""

import unittest
from typing import Any, Dict, List

from tasks.cephadm_cases.certmgr.base import CertMgrTestCase, WORKDIR


class GrafanaTestCase(CertMgrTestCase):
    """grafana helpers shared by the host-scope classes. No test_ methods."""

    GRAFANA_PORT = 3000

    def tearDown(self) -> None:
        self.rm_service('grafana')
        super().tearDown()

    def grafana_spec(self, hosts: List[str], **spec: Any) -> Dict[str, Any]:
        return {'service_type': 'grafana', 'placement': {'hosts': hosts}, 'spec': spec}


class TestCertMgrHostScope(GrafanaTestCase):
    """grafana (host scope)."""

    def test_grafana_cephadm_signed_per_host(self) -> None:
        """Host scope: each grafana daemon gets its own cert, bound to its host,
        and the certs go away with the daemons. (grafana takes cephadm-signed
        or reference certs only; host-scoped reference lookup is a known open
        issue and not covered here.)"""
        hosts = self.host_names[:2]
        name = 'cephadm-signed_grafana_cert'
        self.apply('grafana', self.grafana_spec(hosts))
        self.wait_service('grafana')
        fingerprints = set()
        for host in hosts:
            with self.subTest(host=host):
                served = self.wait_served(host, self.GRAFANA_PORT, lambda r: True,
                                          'grafana TLS')
                self.assertTrue(self.issued_by_cephadm_root_ca(served['pem']))
                self.assertIn(self.hosts[host], served['ip_addresses'])
                for other in set(hosts) - {host}:
                    self.assertNotIn(self.hosts[other], served['ip_addresses'])
                self.assertIn('grafana_servers', served['dns_names'])
                self.assertEqual(self.fingerprint_of(self.get_cert(name, host=host)),
                                 served['fingerprint'])
                fingerprints.add(served['fingerprint'])
        self.assertEqual(len(fingerprints), len(hosts), 'hosts must not share a certificate')
        ls = self.cert_ls('--include-cephadm-signed', '--filter-by', 'name=cephadm-signed_grafana*')
        self.assertEqual(sorted(ls[name]['certificates']), sorted(hosts))
        self.rm_service('grafana')
        for host in hosts:
            self.poll(f'{name} removed for {host}',
                      lambda: self.get_cert(name, host=host) == '', timeout=300, kick=True)

    def test_grafana_inline_is_rejected(self) -> None:
        """grafana has no ssl_cert/ssl_key spec fields: a host-scoped consumer
        cannot share one inline certificate across the hosts of its spec, so
        user certificates come in through 'reference' (one certmgr entry per
        host). A spec asking for 'inline' must be refused: not stored, and
        nothing deployed.

        The exit code is not checked: a spec rejected by validation currently
        exits 0 (see apply()). What matters is that the spec is not stored."""
        res = self.apply('grafana', self.grafana_spec(self.host_names[:1],
                                                      certificate_source='inline'),
                         check=False)
        self.assertIn('ssl_cert', res.err + res.out, res)
        self.assertEqual(self.orch_ls('grafana'), [], 'an inline grafana spec must not be stored')
        self.assertFalse(self.daemons('grafana'))


class TestCertMgrHostScopeReference(GrafanaTestCase):
    """grafana with user-provided certificates ('reference'), one per host.

    Run by its own facet (host-scope-reference) because of a known issue:
    cephadm looks the per-host certificate up under the host's FQDN
    (get_fqdn()), while 'cert-key set --hostname' stores it under the host
    name from 'orch host ls'. grafana is then deployed without a certificate
    and fails, which raises CEPHADM_FAILED_DAEMON; that facet ignores it so
    the expected failure below does not fail the job.

    Once the lookup is fixed: drop the expectedFailure (an unexpected
    success fails the job), drop the facet's ignorelist, and move the test
    into TestCertMgrHostScope.
    """

    @unittest.expectedFailure
    def test_grafana_reference_per_host(self) -> None:
        """Each grafana daemon serves the user certificate stored for its own
        host, with the chain sent: a client trusting only the QA root CA
        validates it."""
        hosts = self.host_names[:2]
        leaves = {}
        try:
            for host in hosts:
                leaves[host] = self.leaf(f'grafana-ref-{host}', ips=[self.hosts[host]],
                                         dns=['grafana_servers'])
                self.set_pair('grafana', leaves[host], host=host)
            self.apply('grafana', self.grafana_spec(hosts, certificate_source='reference'))
            for host in hosts:
                with self.subTest(host=host):
                    expected = leaves[host]['fingerprint']
                    # bounded: with the known issue grafana never serves it
                    self.wait_served(host, self.GRAFANA_PORT,
                                     lambda r: r['fingerprint'] == expected,
                                     'grafana reference certificate', timeout=300)
                    res = self.served(host, self.GRAFANA_PORT,
                                      cafile=f'{WORKDIR}/ca/root/ca.crt')
                    self.assertTrue(res['ok'], res)
        finally:
            self.rm_service('grafana')
            for host in hosts:
                self.rm_cert_key('grafana_ssl_cert', host=host)
