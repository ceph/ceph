"""
certmgr integration tests: grafana (host scope).

Run by the orch/cephadm/certmgr suite; shared helpers live in base.py.
"""

import json
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

    def host_fqdn(self, host: str) -> str:
        """The FQDN cephadm reports for a host (cephadm's get_fqdn(), run on it)."""
        for remote in self.ctx.cluster.remotes.keys():
            if remote.shortname == host:
                script = 'import socket; print(socket.getfqdn() or socket.gethostname())'
                return remote.sh(['python3', '-c', script]).strip().lower()
        raise AssertionError(f'no remote for host {host}')


class TestCertMgrHostScope(GrafanaTestCase):
    """grafana (host scope)."""

    def test_grafana_cephadm_signed_per_host(self) -> None:
        """Host scope: each grafana daemon gets its own cert, bound to its host,
        and the certs go away with the daemons."""
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

    def test_grafana_reference_per_host(self) -> None:
        """Each grafana daemon serves the user certificate stored for its own
        host (registered with the host name from 'orch host ls'), with the
        chain sent: a client trusting only the QA root CA validates it."""
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
                    self.wait_served(host, self.GRAFANA_PORT,
                                     lambda r: r['fingerprint'] == expected,
                                     'grafana reference certificate')
                    res = self.served(host, self.GRAFANA_PORT,
                                      cafile=f'{WORKDIR}/ca/root/ca.crt')
                    self.assertTrue(res['ok'], res)
        finally:
            self.rm_service('grafana')
            for host in hosts:
                self.rm_cert_key('grafana_ssl_cert', host=host)

    def test_grafana_reference_separate_cert_and_key_set(self) -> None:
        """A per-host reference certificate provisioned with 'cert set' and
        'key set' is served, and both the certificate and the key set by the
        user are still in the store once grafana has been deployed (deploying
        reconciles the service's certificates)."""
        host = self.host_names[0]
        leaf = self.leaf(f'grafana-sep-{host}', ips=[self.hosts[host]], dns=['grafana_servers'])
        try:
            self.ceph('orch', 'certmgr', 'cert', 'set', 'grafana_ssl_cert',
                      '--hostname', host, '-i', leaf['chain'])
            self.ceph('orch', 'certmgr', 'key', 'set', 'grafana_ssl_key',
                      '--hostname', host, '-i', leaf['key'])
            self.apply('grafana', self.grafana_spec([host], certificate_source='reference'))
            expected = leaf['fingerprint']
            self.wait_served(host, self.GRAFANA_PORT, lambda r: r['fingerprint'] == expected,
                             'grafana reference certificate')
            self.assertEqual(self.fingerprint_of(self.get_cert('grafana_ssl_cert', host=host)),
                             expected, 'the certificate set with "cert set" was not kept')
            self.assertEqual(self.get_key('grafana_ssl_key', host=host),
                             self.read(leaf['key']).strip(),
                             'the key set with "key set" was not kept')
            self.assertIn('All certificates are valid',
                          '\n'.join(self.ceph_json('orch', 'certmgr', 'cert', 'check')))
        finally:
            self.rm_service('grafana')
            self.rm_cert_key('grafana_ssl_cert', host=host)

    def test_grafana_reference_registered_under_fqdn(self) -> None:
        """A per-host reference certificate registered under the host FQDN,
        as the documentation used to recommend, is still served."""
        host = self.host_names[0]
        fqdn = self.host_fqdn(host)
        if fqdn == host:
            self.skipTest(f'the FQDN of {host} is its host name')
        leaf = self.leaf(f'grafana-fqdn-{host}', ips=[self.hosts[host]], dns=['grafana_servers'])
        try:
            self.set_pair('grafana', leaf, host=fqdn)
            self.apply('grafana', self.grafana_spec([host], certificate_source='reference'))
            expected = leaf['fingerprint']
            self.wait_served(host, self.GRAFANA_PORT, lambda r: r['fingerprint'] == expected,
                             'grafana reference certificate registered under the FQDN')
        finally:
            self.rm_service('grafana')
            self.rm_cert_key('grafana_ssl_cert', host=fqdn)


class TestCertMgrHostScopeMissingCert(GrafanaTestCase):
    """grafana with certificate_source 'reference' and no certificate,
    either before the first deployment or removed while grafana runs.

    Run by its own facet (host-scope-missing-cert), which ignores
    CEPHADM_DAEMON_PLACE_FAIL: the first test raises it on purpose.
    """

    def test_grafana_reference_without_certificate_is_not_deployed(self) -> None:
        """Without a certificate for its host, grafana is not deployed and
        CEPHADM_DAEMON_PLACE_FAIL says why. Once the certificate is provided,
        grafana is deployed with it and the health check clears."""
        host = self.host_names[0]
        leaf = self.leaf(f'grafana-missing-{host}', ips=[self.hosts[host]],
                         dns=['grafana_servers'])
        try:
            self.apply('grafana', self.grafana_spec([host], certificate_source='reference'))
            self.wait_health('CEPHADM_DAEMON_PLACE_FAIL',
                             detail='Cannot get the TLS certificate/key needed to deploy Grafana')
            self.assertFalse(self.daemons('grafana'), 'grafana deployed without a certificate')

            self.set_pair('grafana', leaf, host=host)
            expected = leaf['fingerprint']
            self.wait_served(host, self.GRAFANA_PORT, lambda r: r['fingerprint'] == expected,
                             'grafana reference certificate')
            self.wait_health('CEPHADM_DAEMON_PLACE_FAIL', present=False)
        finally:
            self.rm_service('grafana')
            self.rm_cert_key('grafana_ssl_cert', host=host)

    def test_grafana_reconfig_without_certificate_keeps_running_daemon(self) -> None:
        """Once its certificate and key are removed, a reconfig of a running
        grafana fails and is reported on the daemon, while grafana keeps
        running and serving the certificate it was deployed with."""
        host = self.host_names[0]
        leaf = self.leaf(f'grafana-reconfig-{host}', ips=[self.hosts[host]],
                         dns=['grafana_servers'])
        expected = leaf['fingerprint']
        error = 'Cannot get the TLS certificate/key needed to deploy Grafana'

        def reconfig_failure_reported() -> bool:
            return any(error in json.dumps(event)
                       for d in self.daemons('grafana') for event in d.get('events', []))

        def still_serving() -> bool:
            served = self.served(host, self.GRAFANA_PORT)
            running = [d for d in self.daemons('grafana') if d.get('status_desc') == 'running']
            return bool(served.get('ok')) and served.get('fingerprint') == expected \
                and len(running) == 1

        try:
            self.set_pair('grafana', leaf, host=host)
            self.apply('grafana', self.grafana_spec([host], certificate_source='reference'))
            self.wait_served(host, self.GRAFANA_PORT, lambda r: r['fingerprint'] == expected,
                             'grafana reference certificate')

            self.rm_cert_key('grafana_ssl_cert', host=host)
            self.orch('reconfig', 'grafana')
            self.poll('grafana reconfig failure reported on the daemon',
                      reconfig_failure_reported, timeout=300, kick=True)
            self.hold('grafana running with its original certificate', still_serving)
        finally:
            self.rm_service('grafana')
            self.rm_cert_key('grafana_ssl_cert', host=host)
