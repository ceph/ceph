"""
mgmt-gateway integration tests: SSO with oauth2-proxy and an OIDC IdP
(oidc_idp.py).

Run by the orch/cephadm/services/mgmt-gateway suite (oauth2 facet); shared
helpers live in common.py.
"""

import urllib.parse

from tasks.cephadm_cases.services.base import WORKDIR
from tasks.cephadm_cases.services.mgmt_gateway.common import (
    GatewayTestCase, GW, OAUTH2, IDP_CLIENT_ID, IDP_EMAIL, IDP_USER,
)

JAR = f'{WORKDIR}/oauth2.cookies'


class TestOAuth2Proxy(GatewayTestCase):
    """Gateway on the first host, oauth2-proxy on the second, the IdP on
    the third."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        # the IdP first: oauth2-proxy reads its discovery document at start-up
        cls.deploy_idp()
        cls.apply(GW, {'service_type': GW, 'placement': {'hosts': [cls.gw_host]},
                       'spec': {'enable_health_check_endpoint': True}})
        cls.wait_service(GW)
        cls.deploy_oauth2()

    def setUp(self) -> None:
        super().setUp()
        if not self.daemons(OAUTH2):
            self.deploy_oauth2()

    def test_every_route_goes_through_the_idp(self) -> None:
        """Without a session every route is sent to login; a bearer token
        from the IdP opens them all, with no basic auth on the backends."""
        self.wait_mode('oauth2')

    def test_callback_url(self) -> None:
        """The IdP is asked to call back the gateway: right client, right
        host, gateway port, /oauth2/callback."""
        self.wait_mode('oauth2')
        query = self.idp_redirect('/grafana/')
        self.assertEqual(query.get('client_id'), IDP_CLIENT_ID, query)
        callback = urllib.parse.urlsplit(query['redirect_uri'])
        self.assertEqual(callback.scheme, 'https', query)
        self.assertEqual(callback.path, '/oauth2/callback', query)
        self.assertIn(callback.hostname, self.host_names_for(self.gw_host), query)
        self.assertIn(callback.port, (None, 443), query)

    def test_browser_login(self) -> None:
        """A user logging in through the IdP gets a session cookie that opens
        every route, and grafana sees that user (auth proxy headers)."""
        self.wait_mode('oauth2')
        res = self.login('/grafana/api/user', JAR)
        self.assert_logged_in(res)
        origin = res['origin']
        r = self.http_many([
            {'url': f'{origin}/grafana/api/user', 'jar': JAR},
            {'url': f'{origin}/prometheus/api/v1/status/buildinfo', 'jar': JAR},
            {'url': f'{origin}/alertmanager/api/v2/status', 'jar': JAR},
            {'url': f'{origin}/', 'jar': JAR},
        ])
        self.assertEqual([x['status'] for x in r], [200] * 4, r)
        grafana_user = r[0]['json'] or {}
        claims = self.token_claims()
        self.assertIn(grafana_user.get('login'),
                      {IDP_USER, IDP_EMAIL, claims.get('sub'),
                       claims.get('preferred_username')},
                      f'grafana did not get the IdP user: {grafana_user}')

    def test_session_survives_redeploy_and_failover(self) -> None:
        """The cookie secret is part of the stored spec: a session opened
        before a mgr failover and an oauth2-proxy redeploy stays valid."""
        self.wait_mode('oauth2')
        res = self.login('/prometheus/api/v1/status/buildinfo', JAR)
        self.assert_logged_in(res)
        url = f"{res['origin']}/prometheus/api/v1/status/buildinfo"
        before = {d['daemon_name']: d.get('container_id') for d in self.daemons(OAUTH2)}
        self.fail_mgr_and_wait()
        self.orch('redeploy', OAUTH2)

        def redeployed() -> bool:
            now = {d['daemon_name']: d for d in self.daemons(OAUTH2)}
            return set(now) == set(before) and all(
                d.get('status_desc') == 'running'
                and d.get('container_id') not in (None, before[name])
                for name, d in now.items())
        self.poll('oauth2-proxy redeployed', redeployed, timeout=600, kick=True)
        self.poll('existing session accepted after redeploy',
                  lambda: self.http(url, jar=JAR)['status'] == 200,
                  timeout=300)

    def test_dashboard_sso_follows_oauth2_proxy(self) -> None:
        """Dashboard SSO can be enabled while oauth2-proxy runs, and cephadm
        turns it off when the last oauth2-proxy daemon goes away. The gateway
        then falls back to basic auth on the backends."""
        self.wait_mode('oauth2')
        self.ceph('dashboard', 'sso', 'enable', 'oauth2')
        self.assertEqual(self.mgr_option('dashboard', 'sso_oauth2'), 'true')
        self.rm_service(OAUTH2)
        self.poll('dashboard SSO disabled',
                  lambda: self.mgr_option('dashboard', 'sso_oauth2') == 'false',
                  timeout=300)
        self.wait_mode('gateway')
        res = self.ceph('dashboard', 'sso', 'enable', 'oauth2', check=False)
        self.assertNotEqual(res.rc, 0, f'SSO enabled without oauth2-proxy: {res!r}')
        self.assertIn('oauth2-proxy', res.err, res)
        # and back
        self.deploy_oauth2()
        self.wait_mode('oauth2')
