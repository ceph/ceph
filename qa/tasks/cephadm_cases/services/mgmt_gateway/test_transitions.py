"""
mgmt-gateway integration tests: moving between security modes.

secure_monitoring_stack, the gateway and oauth2-proxy each change how every
monitoring daemon is configured (TLS, mTLS, basic auth, URL prefixes). These
tests walk the transitions in both directions and check, after each one,
what a client gets from the daemons, and that prometheus still scrapes.

Run by the orch/cephadm/services/mgmt-gateway suite (transitions facet);
shared helpers live in common.py.
"""

import logging
from typing import Dict

from tasks.cephadm_cases.services.mgmt_gateway.common import (
    GatewayTestCase, GW, OAUTH2,
)

log = logging.getLogger(__name__)


class TestSecurityTransitions(GatewayTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.deploy_idp()

    def setUp(self) -> None:
        super().setUp()
        self._reset()

    def tearDown(self) -> None:
        self._reset()
        super().tearDown()

    def _reset(self) -> None:
        self.rm_service(OAUTH2)
        self.rm_service(GW)
        self.set_secure_monitoring(False)

    def assert_security(self, security: bool, gateway: bool) -> None:
        self.assertEqual(self.security_config(),
                         {'security_enabled': security, 'mgmt_gw_enabled': gateway})

    def test_full_cycle(self) -> None:
        """plain -> secure -> gateway -> gateway+SSO -> gateway -> secure -> plain"""
        self.wait_mode('plain')
        self.assert_security(False, False)

        self.set_secure_monitoring(True)
        self.wait_mode('secure')
        self.assert_security(True, False)

        self.deploy_gateway()
        self.wait_mode('gateway')
        self.assert_security(True, True)

        self.deploy_oauth2()
        self.wait_mode('oauth2')

        self.rm_service(OAUTH2)
        self.wait_mode('gateway')

        # the flag is still on: removing the gateway keeps TLS + basic auth
        self.rm_service(GW)
        self.wait_mode('secure')
        self.assert_security(True, False)

        self.set_secure_monitoring(False)
        self.wait_mode('plain')
        self.assert_security(False, False)

    def test_gateway_straight_from_plain(self) -> None:
        """Deploying the gateway (and SSO) on a plain stack secures it without
        touching secure_monitoring_stack; removing them goes back to plain."""
        self.wait_mode('plain')
        self.deploy_gateway()
        self.wait_mode('gateway')
        self.deploy_oauth2()
        self.wait_mode('oauth2')
        # removing the gateway first, with oauth2-proxy still deployed
        self.rm_service(GW)
        self.rm_service(OAUTH2)
        self.wait_mode('plain')
        self.assert_security(False, False)

    def test_secure_flag_is_ignored_with_gateway(self) -> None:
        """With a gateway the stack is secured whatever the flag says:
        toggling it neither weakens nor breaks anything."""
        self.deploy_gateway()
        self.wait_mode('gateway')
        for value in (True, False):
            before = self.last_configured()
            self.set_secure_monitoring(value)
            self.assert_security(True, True)
            # the flag is one of the daemons' dependencies, so cephadm may
            # reconfigure them; let that settle before holding the checks
            try:
                self.poll('monitoring daemons reconfigured',
                          lambda: self.last_configured() != before,
                          timeout=300, period=15, kick=True)
            except AssertionError:
                log.info('secure_monitoring_stack=%s caused no reconfig', value)
            self.wait_mode('gateway')
            self.hold_mode('gateway', seconds=120)

    def last_configured(self) -> Dict[str, str]:
        return {d['daemon_name']: str(d.get('last_configured'))
                for svc in ('prometheus', 'alertmanager', 'node-exporter', 'grafana')
                for d in self.daemons(svc)}
