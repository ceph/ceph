"""
certmgr integration tests: mgmt-gateway (global scope).

Run by the orch/cephadm/certmgr suite; shared helpers live in base.py.
"""

from typing import Any, Dict

from tasks.cephadm_cases.certmgr.base import CertMgrTestCase, WORKDIR


class TestCertMgrGlobalScope(CertMgrTestCase):
    """mgmt-gateway (global scope)."""

    GW_PORT = 9876
    GW_INTERNAL_PORT = 29443

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.host = cls.host_names[0]

    def tearDown(self) -> None:
        self.rm_service('mgmt-gateway')
        self.rm_cert_key('mgmt_gateway_ssl_cert')
        super().tearDown()

    def gateway_spec(self, **spec: Any) -> Dict[str, Any]:
        base = {'port': self.GW_PORT, 'enable_health_check_endpoint': True}
        base.update(spec)
        return {'service_type': 'mgmt-gateway', 'placement': {'hosts': [self.host]},
                'spec': base}

    def test_mgmt_gateway_cephadm_signed(self) -> None:
        """Both the external port and the internal one (used by other daemons)
        serve certs signed by the cephadm root CA."""
        self.apply('mgmt-gateway', self.gateway_spec())
        self.wait_service('mgmt-gateway')
        external = self.wait_served(self.host, self.GW_PORT, lambda r: True, 'gateway TLS')
        self.assertTrue(self.issued_by_cephadm_root_ca(external['pem']))
        internal = self.wait_served(self.host, self.GW_INTERNAL_PORT, lambda r: True,
                                    'gateway internal TLS')
        self.assertTrue(self.issued_by_cephadm_root_ca(internal['pem']))

    def test_mgmt_gateway_reference_global(self) -> None:
        """A global user cert (set with no target) is served, chain included."""
        leaf = self.leaf('gw-ref', ips=[self.hosts[self.host]])
        self.set_pair('mgmt-gateway', leaf)
        ls = self.cert_ls()
        self.assertEqual(ls['mgmt_gateway_ssl_cert']['scope'], 'global')
        self.apply('mgmt-gateway', self.gateway_spec(certificate_source='reference'))
        self.wait_service('mgmt-gateway')
        self.wait_served(self.host, self.GW_PORT,
                         lambda r: r['fingerprint'] == leaf['fingerprint'], 'gateway reference')
        res = self.served(self.host, self.GW_PORT, cafile=f'{WORKDIR}/ca/root/ca.crt')
        self.assertTrue(res['ok'], res)

    def test_mgmt_gateway_inline(self) -> None:
        leaf = self.leaf('gw-inline', ips=[self.hosts[self.host]])
        self.apply('mgmt-gateway', self.gateway_spec(certificate_source='inline',
                                                     ssl_cert=self.read(leaf['chain']),
                                                     ssl_key=self.read(leaf['key'])))
        self.wait_service('mgmt-gateway')
        self.wait_served(self.host, self.GW_PORT,
                         lambda r: r['fingerprint'] == leaf['fingerprint'],
                         'gateway inline cert')
        res = self.served(self.host, self.GW_PORT, cafile=f'{WORKDIR}/ca/root/ca.crt')
        self.assertTrue(res['ok'], f'gateway must send the intermediate: {res}')
