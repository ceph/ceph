"""
mgmt-gateway reproducers for bugs cephadm does not fix yet.

Each test states the expected behaviour and is marked
@unittest.expectedFailure: it keeps the job green while the bug is there,
and turns it red once a fix lands (the log reports it as an "unexpected
success"), as a reminder to drop the marker and move the test to its facet.

  test_internal_urls_follow_a_live_gateway
      without a virtual_ip, every internal URL (dashboard API hosts,
      grafana datasource, prometheus discovery) points at the first
      gateway daemon, alive or not.
  test_login_returns_to_requested_page[_custom_port]
      oauth2-proxy's allowlist has the gateway short host name and address,
      without port, while the callback uses the FQDN (and the port), so
      after login users land on '/' instead of the page they asked for.

Run by the orch/cephadm/services/mgmt-gateway suite (open-issues facet);
shared helpers live in common.py.
"""

import unittest
import urllib.parse
from typing import Optional

from tasks.cephadm_cases.services.base import WORKDIR
from tasks.cephadm_cases.services.mgmt_gateway.common import (
    GatewayTestCase, GW, OAUTH2,
)

LABEL = 'mgmt'
JAR = f'{WORKDIR}/open-issues.cookies'


class TestMgmtGatewayOpenIssues(GatewayTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.deploy_idp()

    def tearDown(self) -> None:
        for svc in (OAUTH2, GW):
            self.rm_service(svc)
        self.unlabel_hosts(LABEL)
        self.gw_port = None
        super().tearDown()

    @unittest.expectedFailure  # open: internal URLs pinned to the first gateway
    def test_internal_urls_follow_a_live_gateway(self) -> None:
        """Two gateways without a VIP: when the one the internal URLs point
        at stops, cephadm points them at the one still running."""
        self.label_hosts(LABEL, self.host_names[:2])
        self.deploy_gateway(placement={'label': LABEL})
        self.wait_mode('gateway')
        api_host = self.ceph('dashboard', 'get-prometheus-api-host').out.strip()
        pinned = urllib.parse.urlsplit(api_host).hostname
        stopped = [d for d in self.daemons(GW)
                   if pinned in self.host_names_for(d['hostname'])]
        self.assertEqual(len(stopped), 1, f'{api_host} matches no gateway: {self.daemons(GW)}')
        self.orch('daemon', 'stop', stopped[0]['daemon_name'])
        try:
            self.poll('internal URLs point at the running gateway',
                      lambda: urllib.parse.urlsplit(
                          self.ceph('dashboard', 'get-prometheus-api-host').out.strip()
                      ).hostname not in self.host_names_for(stopped[0]['hostname']),
                      timeout=600, kick=True)
        finally:
            self.orch('daemon', 'start', stopped[0]['daemon_name'])

    def _login_lands_on(self, port: Optional[int]) -> None:
        self.gw_port = port
        self.deploy_gateway()
        self.deploy_oauth2()
        self.wait_mode('oauth2')
        path = '/grafana/api/user'
        res = self.login(path, JAR)
        self.assert_logged_in(res)
        landed = urllib.parse.urlsplit(res['final_url'])
        self.assertEqual(landed.path, path,
                         f'after login the user landed on {res["final_url"]}: {res["hops"]}')

    @unittest.expectedFailure  # open: oauth2-proxy allowlist misses the FQDN
    def test_login_returns_to_requested_page(self) -> None:
        """After logging in, the user is taken back to the page they asked for."""
        self._login_lands_on(None)

    @unittest.expectedFailure  # open: oauth2-proxy allowlist misses the FQDN/port
    def test_login_returns_to_requested_page_custom_port(self) -> None:
        """Same, with the gateway on a non-default port."""
        self._login_lands_on(9876)
