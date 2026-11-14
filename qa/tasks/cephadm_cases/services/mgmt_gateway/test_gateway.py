"""
mgmt-gateway integration tests: a single gateway, no SSO.

Run by the orch/cephadm/services/mgmt-gateway suite (gateway facet); shared
helpers live in common.py.
"""

from typing import List

from tasks.cephadm_cases.services.base import ROOT_CA_FILE
from tasks.cephadm_cases.services.mgmt_gateway.common import (
    Check, GatewayTestCase, GW, ALERTMANAGER_PORT,
)


class TestMgmtGateway(GatewayTestCase):
    """One gateway on the first host, monitoring stack on the second."""

    def setUp(self) -> None:
        super().setUp()
        # every test starts from the default gateway, whatever the previous
        # one left behind
        self.gw_port = None
        self.deploy_gateway()

    def test_routes_and_backend_protection(self) -> None:
        """Backends need a client certificate; the gateway serves every
        route and passes basic auth through to prometheus/alertmanager."""
        self.wait_mode('gateway')

    def test_health_endpoints(self) -> None:
        ext, internal = self.http_many([
            {'url': self.gw('/health'), 'cacert': ROOT_CA_FILE},
            {'url': self.gw_internal('/health')},
        ])
        self.assertEqual(ext['status'], 200, ext)
        self.assertEqual(ext['body'].strip(), 'OK', ext)
        self.assertTrue(self.rejected_without_client_cert(internal),
                        f'internal /health must require a client certificate: {internal}')

    def test_dashboard_standby_handling(self) -> None:
        """Standby mgrs answer 503 so nginx moves to the active one: '/'
        works on every request, before and after a mgr failover."""
        self.assertEqual(self.mgr_option('dashboard', 'standby_behaviour'), 'error')
        self.assertEqual(self.mgr_option('dashboard', 'standby_error_status_code'), '503')

        def dashboard_up() -> bool:
            res = self.http_many([{'url': self.gw('/'), 'cacert': ROOT_CA_FILE}] * 6)
            return all(r['status'] == 200 for r in res)
        self.poll('dashboard through the gateway', dashboard_up, timeout=300)
        self.fail_mgr_and_wait()
        self.poll('dashboard through the gateway after mgr failover', dashboard_up,
                  timeout=600)
        self.hold('dashboard through the gateway', dashboard_up, seconds=60)

    def test_credentials_rotation(self) -> None:
        """New prometheus/alertmanager credentials reach the backends: the old
        ones stop working through the gateway, the new ones work."""
        old_p = self.prometheus_creds()
        old_a = self.alertmanager_creds()
        new_p = ('qa-prom', 'qa-prom-pass-1')
        new_a = ('qa-am', 'qa-am-pass-1')
        try:
            self.set_credentials('prometheus', *new_p)
            self.set_credentials('alertmanager', *new_a)
            self.assertEqual(self.prometheus_creds(), new_p)
            self.assertEqual(self.alertmanager_creds(), new_a)
            prom = self.gw('/prometheus/api/v1/status/buildinfo')
            am = self.gw('/alertmanager/api/v2/status')

            def rotated() -> bool:
                r = self.http_many([
                    dict(url=prom, user=new_p[0], password=new_p[1], cacert=ROOT_CA_FILE),
                    dict(url=prom, user=old_p[0], password=old_p[1], cacert=ROOT_CA_FILE),
                    dict(url=am, user=new_a[0], password=new_a[1], cacert=ROOT_CA_FILE),
                    dict(url=am, user=old_a[0], password=old_a[1], cacert=ROOT_CA_FILE),
                ])
                return [x['status'] for x in r] == [200, 401, 200, 401]
            self.poll('rotated credentials enforced', rotated, timeout=600, kick=True)
            # grafana keeps querying prometheus with the new credentials
            self.wait_mode('gateway')
        finally:
            self.set_credentials('prometheus', *old_p)
            self.set_credentials('alertmanager', *old_a)
        self.wait_mode('gateway')

    def test_ssl_disabled(self) -> None:
        """ssl: false serves plain HTTP on the gateway port; the internal
        endpoint stays mTLS."""
        self.gw_port = 8080
        self.deploy_gateway(ssl=False)
        self.poll('gateway listening on 8080', lambda: self.gateway_ports() == [8080],
                  timeout=600, kick=True)

        def plain_http() -> bool:
            r = self.http_many([
                {'url': self.gw('/', scheme='http')},
                {'url': self.gw('/', scheme='https')},
                {'url': self.gw_internal('/internal/prometheus/api/v1/status/buildinfo')},
            ])
            return (r[0]['status'] == 200 and r[1]['status'] != 200
                    and self.rejected_without_client_cert(r[2]))
        self.poll('gateway serving plain HTTP', plain_http, timeout=600, kick=True)

    def test_removal_restores_dashboard_and_monitoring(self) -> None:
        """Removing the gateway resets the dashboard standby behaviour and
        takes monitoring back to what secure_monitoring_stack says (off)."""
        self.wait_mode('gateway')
        self.rm_service(GW)
        self.poll('dashboard standby behaviour restored',
                  lambda: self.mgr_option('dashboard', 'standby_behaviour') == 'redirect'
                  and self.mgr_option('dashboard', 'standby_error_status_code') == '500',
                  timeout=300)
        self.wait_mode('plain')
        self.assertEqual(self.security_config(),
                         {'security_enabled': False, 'mgmt_gw_enabled': False})
        # alertmanager is reachable directly again
        res = self.http(self.direct(ALERTMANAGER_PORT, '/api/v2/status', 'http'))
        self.assertEqual(res['status'], 200, res)

    # ------------------------------------------------------------------
    # one component down (no redundancy in this facet)
    # ------------------------------------------------------------------
    def test_backend_down_is_isolated(self) -> None:
        """Prometheus down: its route reports the outage, every other route
        is still served. Once it is back, everything works again."""
        self.wait_mode('gateway')
        puser, ppass = self.prometheus_creds()
        auser, apass = self.alertmanager_creds()
        ca = {'cacert': ROOT_CA_FILE}

        def isolated() -> List[Check]:
            r = self.http_many([
                dict(url=self.gw('/'), **ca),
                dict(url=self.gw('/grafana/api/health'), **ca),
                dict(url=self.gw('/alertmanager/api/v2/status'), user=auser, password=apass, **ca),
                dict(url=self.gw('/prometheus/api/v1/status/buildinfo'),
                     user=puser, password=ppass, **ca),
            ])
            return [('dashboard still served', r[0]['status'] == 200),
                    ('grafana still served', r[1]['status'] == 200),
                    ('alertmanager still served', r[2]['status'] == 200),
                    ('/prometheus reports the outage', r[3]['status'] in (502, 503, 504))]
        victim = self.daemons('prometheus')[0]['daemon_name']
        self.stop_daemon(victim, force=True)  # the only instance
        try:
            self.wait_checks('prometheus down, other routes served', isolated, timeout=300)
        finally:
            self.start_daemon(victim)
        self.wait_mode('gateway')

    def test_gateway_down_and_recovery(self) -> None:
        """The gateway is the single entry point: when it is down nothing is
        reachable; once it is back, every route works again."""
        self.wait_mode('gateway')

        def unreachable() -> List[Check]:
            r = self.http_many([{'url': self.gw('/health'), 'cacert': ROOT_CA_FILE},
                                {'url': self.gw('/'), 'cacert': ROOT_CA_FILE}])
            return [('/health unreachable', r[0]['status'] == 0),
                    ('/ unreachable', r[1]['status'] == 0)]
        victim = self.daemons(GW)[0]['daemon_name']
        self.stop_daemon(victim, force=True)  # the only instance
        try:
            self.wait_checks('gateway down', unreachable, timeout=120)
        finally:
            self.start_daemon(victim)
        self.wait_mode('gateway')
