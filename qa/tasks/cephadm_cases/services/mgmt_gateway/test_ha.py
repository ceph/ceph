"""
mgmt-gateway integration tests: high availability.

Two gateways (hosts labelled 'mgmt') share a virtual IP held by a
keepalive-only ingress, and two oauth2-proxy instances sit behind them.
The suite yaml runs the vip task first; the VIP comes from ctx.vip.

Run by the orch/cephadm/services/mgmt-gateway suite (ha facet); shared
helpers live in common.py.
"""

import urllib.parse
from typing import Optional

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

    def vip_holder(self) -> Optional[str]:
        """The host that currently has the VIP configured, if any."""
        holders = [h for h in self.host_names
                   if f' {self.vip}/' in self.remote_for(h).sh(['ip', '-o', 'addr', 'show'])]
        self.assertLessEqual(len(holders), 1, f'VIP on several hosts: {holders}')
        return holders[0] if holders else None

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

    def test_session_spans_oauth2_instances(self) -> None:
        """nginx balances over both oauth2-proxy instances: a session opened
        through one is accepted by the other."""
        self.wait_mode('oauth2')
        res = self.login('/prometheus/api/v1/status/buildinfo', JAR)
        self.assert_logged_in(res)
        url = f"{res['origin']}/prometheus/api/v1/status/buildinfo"
        r = self.http_many([{'url': url, 'jar': JAR, 'cacert': ROOT_CA_FILE}] * 10)
        self.assertEqual([x['status'] for x in r], [200] * 10, r)
