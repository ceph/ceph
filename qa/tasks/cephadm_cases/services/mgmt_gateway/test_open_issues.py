"""
mgmt-gateway reproducers for behaviour main does not implement yet.

Each test states the expected behaviour and is expected to FAIL on main
until the corresponding fix lands; it then moves to the matching facet.
Kept out of the regular facets so they do not turn the suite red.

  test_gateway_failure_moves_vip
      keepalived checks https://<host addr>:<port>/health, but with a
      virtual_ip nginx only listens on <vip>:<port>, so the check fails on
      every host and a dead gateway never loses the VIP.
  test_internal_urls_follow_a_live_gateway
      without a virtual_ip, every internal URL (dashboard API hosts,
      grafana datasource, prometheus discovery) points at the first
      gateway daemon, alive or not.
  test_advertised_port_with_ssl_disabled
      ssl: false with no port: nginx listens on 80 while cephadm records
      and advertises 443.
  test_login_returns_to_requested_page[_custom_port]
      oauth2-proxy's allowlist has the gateway short host name and address,
      without port, while the callback uses the FQDN (and the port), so
      after login users land on '/' instead of the page they asked for.

Run by the orch/cephadm/services/mgmt-gateway suite (open-issues facet);
shared helpers live in common.py.
"""

import urllib.parse
from typing import Optional

from tasks.cephadm_cases.services.base import ROOT_CA_FILE, WORKDIR
from tasks.cephadm_cases.services.mgmt_gateway.common import (
    GatewayTestCase, GW, OAUTH2,
)

LABEL = 'mgmt'
INGRESS = 'ingress.mgmt-gw'
JAR = f'{WORKDIR}/open-issues.cookies'


class TestMgmtGatewayOpenIssues(GatewayTestCase):

    vip: str = ''

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.vip = str(cls.job_ctx().vip['vips'][0])
        cls.deploy_idp()

    def tearDown(self) -> None:
        for svc in (OAUTH2, INGRESS, GW):
            self.rm_service(svc)
        self.unlabel_hosts(LABEL)
        self.gw_port = None
        self.gw_vip = None
        super().tearDown()

    def vip_holder(self) -> Optional[str]:
        holders = [h for h in self.host_names
                   if f' {self.vip}/' in self.remote_for(h).sh(['ip', '-o', 'addr', 'show'])]
        return holders[0] if len(holders) == 1 else None

    def test_gateway_failure_moves_vip(self) -> None:
        """Stopping the gateway on the VIP holder moves the VIP to the other
        gateway host."""
        prefix = self.job_ctx().vip['vnet'].prefixlen
        self.label_hosts(LABEL, self.host_names[:2])
        # Apply both before waiting: with a virtual_ip nginx only listens on
        # <vip>:<port>, which it can bind only once the ingress daemons have
        # set ip_nonlocal_bind on the host (or keepalived holds the VIP).
        self.apply(GW, self.gateway_spec({'label': LABEL}, virtual_ip=self.vip))
        self.apply('ingress', {'service_type': 'ingress', 'service_id': 'mgmt-gw',
                               'placement': {'label': LABEL},
                               'spec': {'backend_service': GW,
                                        'virtual_ip': f'{self.vip}/{prefix}',
                                        'keepalive_only': True}})
        self.wait_service(GW)
        self.wait_service(INGRESS)
        self.gw_vip = self.vip
        self.wait_mode('gateway')
        holder = self.vip_holder()
        self.assertIsNotNone(holder)
        gw = [d['daemon_name'] for d in self.daemons(GW) if d['hostname'] == holder]
        self.orch('daemon', 'stop', gw[0])
        try:
            self.poll('VIP moved away from the stopped gateway',
                      lambda: self.vip_holder() not in (None, holder), timeout=300)
            res = self.http(self.gw('/health'), cacert=ROOT_CA_FILE)
            self.assertEqual(res['status'], 200, res)
        finally:
            self.orch('daemon', 'start', gw[0])

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

    def test_advertised_port_with_ssl_disabled(self) -> None:
        """With ssl: false and no port, the port cephadm records for the
        daemon is the one nginx serves."""
        self.deploy_gateway(ssl=False)
        self.poll('gateway serving plain HTTP',
                  lambda: self.http(f'http://{self.addr(self.gw_host)}:80/')['status'] == 200
                  or self.http(f'http://{self.addr(self.gw_host)}:443/')['status'] == 200,
                  timeout=600, kick=True)
        ports = self.gateway_ports()
        self.assertEqual(len(ports), 1, ports)
        res = self.http(f'http://{self.addr(self.gw_host)}:{ports[0]}/')
        self.assertEqual(res['status'], 200,
                         f'cephadm records port {ports[0]} but nginx does not serve it: {res}')

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

    def test_login_returns_to_requested_page(self) -> None:
        """After logging in, the user is taken back to the page they asked for."""
        self._login_lands_on(None)

    def test_login_returns_to_requested_page_custom_port(self) -> None:
        """Same, with the gateway on a non-default port."""
        self._login_lands_on(9876)
