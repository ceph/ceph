"""
mgmt-gateway reproducers for behaviour main does not implement yet.

Each test states the expected behaviour. The ones for bugs cephadm does
not fix yet are marked @unittest.expectedFailure: they keep the job green
while the bug is there, and turn it red (unexpected success) once a fix
lands, as a reminder to drop the marker and move the test to its facet.

Fixed (pass, to be moved to the regular facets):
  test_gateway_failure_moves_vip
      keepalived checked https://<host addr>:<port>/health, but with a
      virtual_ip nginx only listens on <vip>:<port> (since the fix for
      tracker 74929), so the check failed on every host and a dead gateway
      never lost the VIP. Runs keepalived's own check on each host first.
  test_advertised_port_with_ssl_disabled
      ssl: false with no port: nginx listened on 80 while cephadm recorded
      and advertised 443.

Open (expected to fail):
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

import re
import unittest
import urllib.parse
from typing import Optional, Tuple

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

    def keepalived_check(self, host: str) -> Tuple[str, int]:
        """Run on 'host' the backend check keepalived runs there, as found in
        its deployed configuration. Returns the command and its exit code."""
        remote = self.remote_for(host)
        conf = remote.sh(['sudo', 'sh', '-c', 'cat /var/lib/ceph/*/keepalived.*/keepalived.conf'])
        m = re.search(r'script "([^"]+)"', conf)
        self.assertIsNotNone(m, f'no check script in keepalived.conf on {host}: {conf}')
        assert m is not None
        proc = remote.run(args=['sudo', 'sh', '-c', m.group(1)], check_status=False)
        return m.group(1), proc.exitstatus

    def test_gateway_failure_moves_vip(self) -> None:
        """keepalived's check sees each running gateway as healthy; stopping
        the gateway on the VIP holder moves the VIP to the other gateway host
        and the gateway stays reachable; once it is back, it is still
        reachable through the VIP (keepalived may move the VIP back)."""
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
        for host in self.host_names[:2]:
            self.poll(f'keepalived check passes on {host} (gateway running)',
                      lambda: self.keepalived_check(host)[1] == 0, timeout=120)
        holder = self.vip_holder()
        self.assertIsNotNone(holder)
        assert holder is not None
        gw = [d['daemon_name'] for d in self.daemons(GW) if d['hostname'] == holder]
        self.stop_daemon(gw[0])
        try:
            cmd, rc = self.keepalived_check(holder)
            self.assertNotEqual(rc, 0, f'keepalived check passes with the gateway down: {cmd}')
            self.poll('VIP moved away from the stopped gateway',
                      lambda: self.vip_holder() not in (None, holder), timeout=300)
            res = self.http(self.gw('/health'), cacert=ROOT_CA_FILE)
            self.assertEqual(res['status'], 200, res)
        finally:
            self.start_daemon(gw[0])
        self.wait_mode('gateway')
        self.hold_mode('gateway', seconds=60)

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

    @unittest.expectedFailure  # open: oauth2-proxy allowlist misses the FQDN
    def test_login_returns_to_requested_page(self) -> None:
        """After logging in, the user is taken back to the page they asked for."""
        self._login_lands_on(None)

    @unittest.expectedFailure  # open: oauth2-proxy allowlist misses the FQDN/port
    def test_login_returns_to_requested_page_custom_port(self) -> None:
        """Same, with the gateway on a non-default port."""
        self._login_lands_on(9876)
