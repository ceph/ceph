"""
mgmt-gateway integration tests: high availability.

The topology of cephadm's HA setup: two gateways (hosts labelled 'mgmt')
share a virtual IP held by a keepalive-only ingress, with two instances of
every service behind them: oauth2-proxy, prometheus, alertmanager, grafana,
and the dashboard in two mgrs. The suite yaml runs the vip task first; the
VIP comes from ctx.vip.

Each test_*_down test takes one instance of one layer away while a user is
logged in, checks the user does not notice, then brings it back.

Run by the orch/cephadm/services/mgmt-gateway suite (ha facet); shared
helpers live in common.py.
"""

import re
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

from tasks.cephadm_cases.services.base import ROOT_CA_FILE, WORKDIR
from tasks.cephadm_cases.services.mgmt_gateway.common import (
    GatewayTestCase, GW, OAUTH2, GW_INTERNAL_PORT,
)

LABEL = 'mgmt'
INGRESS = 'ingress.mgmt-gw'
JAR = f'{WORKDIR}/ha.cookies'


class TestMgmtGatewayHA(GatewayTestCase):

    vip: str = ''

    @classmethod
    def monitoring_hosts(cls) -> List[str]:
        # redundant backends: one instance of each on both gateway hosts
        return cls.host_names[:2]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.vip = str(cls.job_ctx().vip['vips'][0])
        prefix = cls.job_ctx().vip['vnet'].prefixlen
        gw_hosts = cls.host_names[:2]
        for host in gw_hosts:
            cls._ceph_cls('orch', 'host', 'label', 'add', host, LABEL)
        cls.deploy_idp()
        cls.apply(GW, {'service_type': GW, 'placement': {'label': LABEL},
                       'spec': {'virtual_ip': cls.vip}})
        cls.apply('ingress', {'service_type': 'ingress', 'service_id': 'mgmt-gw',
                              'placement': {'label': LABEL},
                              'spec': {'backend_service': GW,
                                       'virtual_ip': f'{cls.vip}/{prefix}',
                                       'keepalive_only': True}})
        cls.wait_service(GW)
        cls.wait_service(INGRESS)
        cls.deploy_oauth2(placement={'label': LABEL})
        cls.gw_vip = cls.vip

    def vip_holders(self) -> List[str]:
        """The hosts that currently have the VIP configured."""
        return [h for h in self.host_names
                if f' {self.vip}/' in self.remote_for(h).sh(['ip', '-o', 'addr', 'show'])]

    def vip_holder(self) -> Optional[str]:
        """The host that currently has the VIP configured, if any."""
        holders = self.vip_holders()
        self.assertLessEqual(len(holders), 1, f'VIP on several hosts: {holders}')
        return holders[0] if holders else None

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

    def test_access_through_vip(self) -> None:
        """Both gateways and both oauth2-proxies run; every route works
        through the VIP, with SSO."""
        self.assertEqual(len(self.daemons(GW)), 2)
        self.assertEqual(len(self.daemons(OAUTH2)), 2)
        self.poll('VIP configured on a gateway host',
                  lambda: self.vip_holder() in self.host_names[:2], timeout=300)
        self.wait_mode('oauth2')

    def test_services_use_the_vip(self) -> None:
        """Internal and external URLs cephadm hands out point at the VIP, so
        a gateway failure is transparent to the other services."""
        self.wait_mode('oauth2')
        internal = f'https://{self.vip}:{GW_INTERNAL_PORT}/internal'
        self.assertEqual(self.ceph('dashboard', 'get-prometheus-api-host').out.strip(),
                         f'{internal}/prometheus')
        self.assertEqual(self.ceph('dashboard', 'get-alertmanager-api-host').out.strip(),
                         f'{internal}/alertmanager')
        callback = urllib.parse.urlsplit(self.idp_redirect('/')['redirect_uri'])
        self.assertEqual(callback.hostname, self.vip)

    def test_keepalived_failover(self) -> None:
        """When keepalived stops on the VIP holder the VIP moves to the other
        gateway host and every route keeps working, scraping included."""
        self.wait_mode('oauth2')
        holder = self.vip_holder()
        self.assertIsNotNone(holder)
        keepalived = [d['daemon_name'] for d in self.daemons(INGRESS)
                      if d['daemon_type'] == 'keepalived' and d['hostname'] == holder]
        self.assertEqual(len(keepalived), 1, self.daemons(INGRESS))
        self.orch('daemon', 'stop', keepalived[0])
        try:
            self.poll('VIP moved to the other gateway host',
                      lambda: self.vip_holder() not in (None, holder), timeout=300)
            self.wait_mode('oauth2', timeout=600)
        finally:
            self.orch('daemon', 'start', keepalived[0])
        self.wait_service(INGRESS)

    def test_port_free_on_host_addresses(self) -> None:
        """With a virtual_ip the gateway binds the VIP (and the loopback, for
        keepalived's check) only: its port stays free on the hosts' own
        addresses for other services (tracker 74929)."""
        ports = self.gateway_ports()
        self.assertEqual(len(ports), 1, ports)
        for host in self.host_names[:2]:
            addr = self.hosts[host]
            out = self.remote_for(host).sh([
                'sudo', 'python3', '-c',
                'import socket, sys\n'
                's = socket.socket(socket.AF_INET6 if ":" in sys.argv[1] else socket.AF_INET)\n'
                'try:\n'
                '    s.bind((sys.argv[1], int(sys.argv[2]))); print("free")\n'
                'except OSError as e:\n'
                '    print("busy", e)\n',
                addr, str(ports[0])])
            self.assertEqual(out.strip(), 'free',
                             f'{addr}:{ports[0]} on {host} is taken: {out.strip()}')

    def test_session_spans_oauth2_instances(self) -> None:
        """nginx balances over both oauth2-proxy instances: a session opened
        through one is accepted by the other."""
        self.wait_mode('oauth2')
        res = self.login('/prometheus/api/v1/status/buildinfo', JAR)
        self.assert_logged_in(res)
        url = f"{res['origin']}/prometheus/api/v1/status/buildinfo"
        r = self.http_many([{'url': url, 'jar': JAR, 'cacert': ROOT_CA_FILE}] * 10)
        self.assertEqual([x['status'] for x in r], [200] * 10, r)

    # ------------------------------------------------------------------
    # one instance of a layer down
    # ------------------------------------------------------------------
    def one_of_two(self, service: str) -> Dict[str, Any]:
        daemons = self.daemons(service)
        self.assertEqual(len(daemons), 2, f'{service} must be redundant: {daemons}')
        return daemons[0]

    def assert_instance_down_unnoticed(self, service: str) -> None:
        """Stop one instance of a service while a user is logged in: every
        route keeps working through the VIP, with the session and with a
        bearer token, and scraping goes on. Then start it again."""
        self.wait_mode('oauth2')
        res = self.login('/', JAR)
        self.assert_logged_in(res)
        origin = res['origin']
        victim = self.one_of_two(service)['daemon_name']
        self.stop_daemon(victim)
        try:
            self.wait_checks(f'{victim} down: logged-in user unaffected',
                             lambda: self.session_checks(origin, JAR), timeout=300)
            self.wait_mode('oauth2', timeout=600)
        finally:
            self.start_daemon(victim)
        self.wait_mode('oauth2')

    def test_active_gateway_down(self) -> None:
        """keepalived's check sees each running gateway as healthy; stopping
        the gateway on the VIP holder fails the check there, moves the VIP to
        the other gateway host and the logged-in user does not notice. Once
        it is back, every route still works through the VIP (keepalived may
        move the VIP back)."""
        self.wait_mode('oauth2')
        for host in self.host_names[:2]:
            self.poll(f'keepalived check passes on {host} (gateway running)',
                      lambda: self.keepalived_check(host)[1] == 0, timeout=120)
        res = self.login('/', JAR)
        self.assert_logged_in(res)
        holder = self.vip_holder()
        self.assertIsNotNone(holder)
        assert holder is not None
        victim = [d['daemon_name'] for d in self.daemons(GW) if d['hostname'] == holder]
        self.assertEqual(len(victim), 1, self.daemons(GW))
        self.stop_daemon(victim[0])
        try:
            self.poll('keepalived check fails with the gateway down',
                      lambda: self.keepalived_check(holder)[1] != 0, timeout=120)

            # while VRRP hands over, both hosts may briefly have the VIP
            def moved() -> bool:
                holders = self.vip_holders()
                return bool(holders) and holder not in holders
            self.poll('VIP moved away from the stopped gateway', moved, timeout=300)
            self.wait_checks(f'{victim[0]} down: logged-in user unaffected',
                             lambda: self.session_checks(res['origin'], JAR), timeout=300)
        finally:
            self.start_daemon(victim[0])
        self.wait_mode('oauth2')
        self.hold_mode('oauth2', seconds=60)

    def test_oauth2_proxy_instance_down(self) -> None:
        self.assert_instance_down_unnoticed(OAUTH2)

    def test_prometheus_instance_down(self) -> None:
        self.assert_instance_down_unnoticed('prometheus')

    def test_alertmanager_instance_down(self) -> None:
        self.assert_instance_down_unnoticed('alertmanager')

    def test_grafana_instance_down(self) -> None:
        self.assert_instance_down_unnoticed('grafana')

    def test_active_mgr_down(self) -> None:
        """The active mgr (dashboard) fails over: the dashboard comes back
        through the VIP on the new active mgr, with the same session."""
        self.wait_mode('oauth2')
        res = self.login('/', JAR)
        self.assert_logged_in(res)
        self.fail_mgr_and_wait()
        self.wait_checks('logged-in user after mgr failover',
                         lambda: self.session_checks(res['origin'], JAR), timeout=600)
        self.wait_mode('oauth2')
