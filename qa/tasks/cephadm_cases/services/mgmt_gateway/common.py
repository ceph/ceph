"""
Shared setup and checks for the mgmt-gateway / oauth2-proxy tests.

Run by the orch/cephadm/services/mgmt-gateway suite. Each facet is a test
module in this package:

  test_gateway.py       TestMgmtGateway: one gateway, no SSO
  test_oauth2.py        TestOAuth2Proxy: gateway + oauth2-proxy + an OIDC IdP
  test_ha.py            TestMgmtGatewayHA: two gateways behind a VIP
                        (keepalive-only ingress), two oauth2-proxy instances
  test_transitions.py   TestSecurityTransitions: moving between plain,
                        secure_monitoring_stack, gateway and gateway + SSO
  test_open_issues.py   TestMgmtGatewayOpenIssues (DNM): reproducers for
                        behaviour main does not implement yet

What the monitoring stack looks like from a client depends on which of these
is in place, so the checks are grouped by "mode":

  plain     secure_monitoring_stack off, no gateway: HTTP, no auth
  secure    secure_monitoring_stack on, no gateway: HTTPS + basic auth
  gateway   gateway deployed: backends need a client certificate (mTLS),
            access goes through the gateway, basic auth on prometheus and
            alertmanager
  oauth2    gateway + oauth2-proxy: every route goes through the IdP, the
            backends drop basic auth

The identity provider is oidc_idp.py, a minimal OIDC provider run as a
cephadm custom container with the cluster's own Ceph image (python3 and
python3-cryptography are all it needs), so the suite pulls no image from
outside the Ceph registries. It has one test user and also accepts the
password grant, so tests can get a bearer token without a browser.
"""

import json
import logging
import os
import urllib.parse
from typing import Any, Dict, List, Optional, Sequence, Tuple

from tasks.cephadm_cases.services.base import (
    ServicesTestCase, WORKDIR, ROOT_CA_FILE,
)

log = logging.getLogger(__name__)

GW = 'mgmt-gateway'
OAUTH2 = 'oauth2-proxy'
IDP = 'container.qa-idp'

GW_INTERNAL_PORT = 29443
PROMETHEUS_PORT = 9095
ALERTMANAGER_PORT = 9093
NODE_EXPORTER_PORT = 9100

IDP_PORT = 5556
IDP_DIR = '/etc/qa-idp'  # where the IdP files are mounted in its container
IDP_CLIENT_ID = 'ceph-mgmt-gateway'
IDP_CLIENT_SECRET = 'cephadm-qa-client-secret'
IDP_USER = 'cephadm-qa'
IDP_EMAIL = 'cephadm-qa@example.com'
IDP_PASSWORD = 'cephadm-qa-password'

Check = Tuple[str, bool]

# (service behind the route, route) as a logged-in user reaches them
SSO_ROUTES = (
    ('dashboard', '/'),
    ('grafana', '/grafana/api/health'),
    ('prometheus', '/prometheus/api/v1/status/buildinfo'),
    ('alertmanager', '/alertmanager/api/v2/status'),
)


class GatewayTestCase(ServicesTestCase):
    """Monitoring stack + helpers for gateway, oauth2-proxy and the IdP."""

    MGRS_REQUIRED = 2  # dashboard standby handling and mgr failover

    # where things run; set in setUpClass, tests may change them
    gw_host: str = ''
    gw_port: Optional[int] = None  # None: the gateway default (443)
    gw_vip: Optional[str] = None
    mon_host: str = ''
    idp_host: str = ''
    _token: Optional[str] = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        # the gateway publishes the dashboard only if the module is up when
        # it is deployed (MgrTestCase disables every module but cephadm)
        cls.enable_mgr_module('dashboard')
        cls.gw_host = cls.host_names[0]
        cls.mon_host = cls.host_names[1]
        cls.idp_host = cls.host_names[2]
        cls.deploy_monitoring()

    # ------------------------------------------------------------------
    # monitoring stack
    # ------------------------------------------------------------------
    @classmethod
    def monitoring_hosts(cls) -> List[str]:
        """Where prometheus, alertmanager and grafana run (one instance per host)."""
        return [cls.mon_host]

    @classmethod
    def deploy_monitoring(cls) -> None:
        cls.apply('node-exporter', {'service_type': 'node-exporter',
                                    'placement': {'host_pattern': '*'}})
        for svc in ('prometheus', 'alertmanager', 'grafana'):
            cls.apply(svc, {'service_type': svc,
                            'placement': {'hosts': cls.monitoring_hosts()}})
        for svc in ('node-exporter', 'prometheus', 'alertmanager', 'grafana'):
            cls.wait_service(svc)

    def set_secure_monitoring(self, enabled: bool) -> None:
        self.set_opt('secure_monitoring_stack', enabled)

    def security_config(self) -> Dict[str, bool]:
        return self.ceph_json('orch', 'get-security-config') or {}

    def prometheus_creds(self) -> Tuple[str, str]:
        info = self.ceph_json('orch', 'prometheus', 'get-credentials') or {}
        return info.get('user', ''), info.get('password', '')

    def alertmanager_creds(self) -> Tuple[str, str]:
        info = self.ceph_json('orch', 'alertmanager', 'get-credentials') or {}
        return info.get('user', ''), info.get('password', '')

    def set_credentials(self, service: str, user: str, password: str) -> None:
        path = self.write(f'{WORKDIR}/{service}-creds.json',
                          json.dumps({'username': user, 'password': password}))
        self.orch(service, 'set-credentials', '-i', path)

    # ------------------------------------------------------------------
    # URLs
    # ------------------------------------------------------------------
    def addr(self, host: str) -> str:
        return self.hosts[host]

    def direct(self, port: int, path: str, scheme: str = 'https',
               host: Optional[str] = None) -> str:
        return f'{scheme}://{self.addr(host or self.mon_host)}:{port}{path}'

    def gw_origin(self, scheme: str = 'https') -> str:
        host = self.gw_vip or self.addr(self.gw_host)
        default = 443 if scheme == 'https' else 80
        port = self.gw_port or default
        return f'{scheme}://{host}' + ('' if port == default else f':{port}')

    def gw(self, path: str, scheme: str = 'https') -> str:
        return f'{self.gw_origin(scheme)}{path}'

    def gw_internal(self, path: str) -> str:
        host = self.gw_vip or self.addr(self.gw_host)
        return f'https://{host}:{GW_INTERNAL_PORT}{path}'

    # ------------------------------------------------------------------
    # gateway and oauth2-proxy
    # ------------------------------------------------------------------
    def gateway_spec(self, placement: Optional[Dict[str, Any]] = None,
                     **spec: Any) -> Dict[str, Any]:
        body: Dict[str, Any] = {'enable_health_check_endpoint': True}
        if self.gw_port:
            body['port'] = self.gw_port
        body.update(spec)
        return {'service_type': GW,
                'placement': placement or {'hosts': [self.gw_host]},
                'spec': body}

    def deploy_gateway(self, placement: Optional[Dict[str, Any]] = None,
                       **spec: Any) -> None:
        self.apply(GW, self.gateway_spec(placement, **spec))
        self.wait_service(GW)

    @classmethod
    def oauth2_spec(cls, placement: Dict[str, Any]) -> Dict[str, Any]:
        return {'service_type': OAUTH2,
                'placement': placement,
                'spec': {
                    'provider_display_name': 'cephadm QA IdP',
                    'client_id': IDP_CLIENT_ID,
                    'client_secret': IDP_CLIENT_SECRET,
                    'oidc_issuer_url': cls.idp_issuer(),
                    # the IdP serves a self-signed certificate
                    'ssl_insecure_skip_verify': True,
                }}

    @classmethod
    def deploy_oauth2(cls, placement: Optional[Dict[str, Any]] = None) -> None:
        cls.apply(OAUTH2, cls.oauth2_spec(placement or {'hosts': [cls.host_names[1]]}))
        cls.wait_service(OAUTH2)

    def gateway_ports(self) -> List[int]:
        return sorted({p for d in self.daemons(GW) for p in (d.get('ports') or [])})

    # ------------------------------------------------------------------
    # identity provider (oidc_idp.py)
    # ------------------------------------------------------------------
    @classmethod
    def idp_issuer(cls) -> str:
        return f'https://{cls.hosts[cls.idp_host]}:{IDP_PORT}'

    @classmethod
    def ceph_image(cls) -> str:
        """The image the cluster runs, already present on every host."""
        for d in cls.daemons_of_type('mgr'):
            if d.get('container_image_name'):
                return str(d['container_image_name'])
        return cls._ceph_cls('config', 'get', 'mgr', 'container_image').out.strip()

    @classmethod
    def deploy_idp(cls) -> None:
        addr = cls.hosts[cls.idp_host]
        cls._probe_cls('selfsigned', f'{WORKDIR}/idp', '--cn', 'idp.cephadm-qa',
                       '--ip', addr, '--dns', cls.idp_host)
        crt = cls.remote.sh(['sudo', 'cat', f'{WORKDIR}/idp.crt'])
        key = cls.remote.sh(['sudo', 'cat', f'{WORKDIR}/idp.key'])
        with open(os.path.join(os.path.dirname(os.path.dirname(__file__)),
                               'oidc_idp.py')) as f:
            server = f.read()
        config = {
            'issuer': cls.idp_issuer(),
            'listen': f'0.0.0.0:{IDP_PORT}',
            'tls_cert': f'{IDP_DIR}/tls.crt',
            'tls_key': f'{IDP_DIR}/tls.key',
            'clients': [{'id': IDP_CLIENT_ID, 'secret': IDP_CLIENT_SECRET,
                         'redirect_path': '/oauth2/callback'}],
            'users': [{'username': IDP_USER, 'email': IDP_EMAIL,
                       'password': IDP_PASSWORD, 'roles': ['administrator']}],
        }
        spec = {
            'service_type': 'container',
            'service_id': 'qa-idp',
            'placement': {'hosts': [cls.idp_host]},
            'spec': {
                'image': cls.ceph_image(),
                'entrypoint': '/usr/bin/python3',
                'ports': [IDP_PORT],
                'dirs': ['etc'],
                'files': {'etc/oidc_idp.py': server,
                          'etc/config.json': json.dumps(config, indent=1),
                          'etc/tls.crt': crt,
                          'etc/tls.key': key},
                'volume_mounts': {'etc': f'{IDP_DIR}:z'},
            },
            'extra_container_args': [
                # cephadm writes the files 0600, owned by the spec's uid/gid
                # (default nobody); run as that user rather than relying on
                # root's DAC override to read them
                '--user=65534:65534',
                # cephadm runs custom containers without host networking and
                # publishes no ports ('ports' only drives its port check and
                # the firewall), so the IdP would only listen inside its own
                # network namespace
                '--net=host',
            ],
            'extra_entrypoint_args': [f'{IDP_DIR}/oidc_idp.py', f'{IDP_DIR}/config.json'],
        }
        cls.apply('qa-idp', spec)
        cls.wait_service(IDP)
        discovery = f'{cls.idp_issuer()}/.well-known/openid-configuration'
        cls.poll('IdP discovery endpoint',
                 lambda: cls._http_many_cls([{'url': discovery}])[0]['status'] == 200,
                 timeout=300, period=10)

    def token(self) -> str:
        """An ID token for the IdP test user (password grant), cached per class."""
        if not type(self)._token:
            res = self.probe('token', self.idp_issuer(),
                             '--client-id', IDP_CLIENT_ID,
                             '--client-secret', IDP_CLIENT_SECRET,
                             '--user', IDP_EMAIL, '--password', IDP_PASSWORD)
            self.assertTrue(res['ok'], f'password grant failed: {res}')
            type(self)._token = res['id_token']
        return str(type(self)._token)

    def token_claims(self) -> Dict[str, Any]:
        res = self.probe('token', self.idp_issuer(),
                         '--client-id', IDP_CLIENT_ID,
                         '--client-secret', IDP_CLIENT_SECRET,
                         '--user', IDP_EMAIL, '--password', IDP_PASSWORD)
        self.assertTrue(res['ok'], f'password grant failed: {res}')
        return dict(res['claims'])

    def idp_redirect(self, path: str = '/') -> Dict[str, Any]:
        """Follow an unauthenticated request until it reaches the IdP and
        return the authorize URL's query parameters."""
        url = self.gw(path)
        idp = urllib.parse.urlsplit(self.idp_issuer())
        for _ in range(6):
            res = self.http(url, cacert=ROOT_CA_FILE)
            self.assertTrue(300 <= res['status'] < 400,
                            f'expected a redirect towards the IdP for {url}: {res}')
            url = urllib.parse.urljoin(url, res['location'])
            parts = urllib.parse.urlsplit(url)
            if parts.netloc == idp.netloc:
                query = urllib.parse.parse_qs(parts.query)
                return {k: v[0] for k, v in query.items()}
        raise AssertionError(f'never reached the IdP from {path}; last URL {url}')

    def login(self, path: str, jar: str) -> Dict[str, Any]:
        """Browser-style login through the gateway, starting at path.

        Starts from the origin cephadm put in the callback URL: the CSRF
        cookie oauth2-proxy sets must come back to the same host, and the
        session cookie is only sent to that host afterwards, so the result
        carries that 'origin' for follow-up requests with the jar."""
        callback = urllib.parse.urlsplit(self.idp_redirect(path)['redirect_uri'])
        # as a browser would: no explicit default port
        origin = f'{callback.scheme}://{callback.hostname}'
        if callback.port not in (None, 443):
            origin += f':{callback.port}'
        self.run_cmd(['rm', '-f', jar], check=False)
        res = self.probe('login', f'{origin}{path}', '--user', IDP_EMAIL,
                         '--password', IDP_PASSWORD, '--jar', jar)
        res['origin'] = origin
        return res

    def assert_logged_in(self, res: Dict[str, Any]) -> None:
        """The login walk went through the IdP and came back with a session."""
        hops = res['hops']
        self.assertTrue(any('/oauth2/callback' in h['url'] for h in hops),
                        f'login never reached the gateway callback: {hops}')
        self.assertEqual(res['status'], 200, f'login did not land on a page: {res}')

    # ------------------------------------------------------------------
    # failure injection
    # ------------------------------------------------------------------
    def daemon_state(self, daemon_name: str) -> str:
        for d in self.ceph_json('orch', 'ps') or []:
            if d.get('daemon_name') == daemon_name:
                return str(d.get('status_desc'))
        return 'absent'

    def stop_daemon(self, daemon_name: str, force: bool = False) -> None:
        """Stop a daemon. cephadm refuses to stop a service's last daemon
        unless forced: tests that take a non-redundant service down on
        purpose pass force=True."""
        self.orch('daemon', 'stop', daemon_name, *(['--force'] if force else []))
        self.poll(f'{daemon_name} stopped',
                  lambda: self.daemon_state(daemon_name) != 'running',
                  timeout=300, kick=True)

    def start_daemon(self, daemon_name: str) -> None:
        self.orch('daemon', 'start', daemon_name)
        self.poll(f'{daemon_name} running',
                  lambda: self.daemon_state(daemon_name) == 'running',
                  timeout=600, kick=True)

    @classmethod
    def fail_closed(cls, res: Dict[str, Any]) -> bool:
        """The request did not reach a backend: an error, no answer, or a
        redirect to login."""
        if 200 <= res['status'] < 300:
            return False
        if 300 <= res['status'] < 400:
            return cls._sent_to_login(res)
        return True

    def session_checks(self, origin: str, jar: str,
                       down: Sequence[str] = ()) -> List[Check]:
        """A logged-in user's view: every route is served, except the
        routes of the services in 'down', which must report the outage."""
        r = self.http_many([{'url': f'{origin}{path}', 'jar': jar} for _, path in SSO_ROUTES])
        out: List[Check] = []
        for (svc, path), res in zip(SSO_ROUTES, r):
            if svc in down:
                out.append((f'{path} reports {svc} is down', res['status'] in (502, 503, 504)))
            else:
                out.append((f'{path} served with the session', res['status'] == 200))
        return out

    # ------------------------------------------------------------------
    # mode checks
    # ------------------------------------------------------------------
    @staticmethod
    def node_targets_up(res: Dict[str, Any], expected: int) -> bool:
        """Prometheus scrapes every node-exporter successfully."""
        if res['status'] != 200 or not res['json']:
            return False
        targets = [t for t in res['json']['data']['activeTargets']
                   if t.get('labels', {}).get('job') == 'node-exporter']
        return len(targets) == expected and all(t['health'] == 'up' for t in targets)

    def checks(self, mode: str) -> List[Check]:
        return getattr(self, f'_checks_{mode}')()

    def _checks_plain(self) -> List[Check]:
        node = self.host_names[0]
        r = self.http_many([
            {'url': self.direct(PROMETHEUS_PORT, '/api/v1/status/buildinfo', 'http')},
            {'url': self.direct(ALERTMANAGER_PORT, '/api/v2/status', 'http')},
            {'url': self.direct(NODE_EXPORTER_PORT, '/metrics', 'http', host=node)},
            {'url': self.direct(PROMETHEUS_PORT, '/api/v1/targets?state=active', 'http')},
        ])
        return [
            ('prometheus answers plain HTTP without auth', r[0]['status'] == 200),
            ('alertmanager answers plain HTTP without auth', r[1]['status'] == 200),
            ('node-exporter answers plain HTTP', r[2]['status'] == 200),
            ('prometheus scrapes every node-exporter',
             self.node_targets_up(r[3], len(self.host_names))),
            ('get-credentials is empty (security off)',
             self.prometheus_creds() == ('', '')),
        ]

    def _checks_secure(self) -> List[Check]:
        node = self.host_names[0]
        puser, ppass = self.prometheus_creds()
        auser, apass = self.alertmanager_creds()
        ca = {'cacert': ROOT_CA_FILE}
        r = self.http_many([
            dict(url=self.direct(PROMETHEUS_PORT, '/api/v1/status/buildinfo'), **ca),
            dict(url=self.direct(PROMETHEUS_PORT, '/api/v1/status/buildinfo'),
                 user=puser, password=ppass, **ca),
            {'url': self.direct(PROMETHEUS_PORT, '/api/v1/status/buildinfo', 'http')},
            dict(url=self.direct(ALERTMANAGER_PORT, '/api/v2/status'), **ca),
            dict(url=self.direct(ALERTMANAGER_PORT, '/api/v2/status'),
                 user=auser, password=apass, **ca),
            dict(url=self.direct(NODE_EXPORTER_PORT, '/metrics', host=node), **ca),
            {'url': self.direct(NODE_EXPORTER_PORT, '/metrics', 'http', host=node)},
            dict(url=self.direct(PROMETHEUS_PORT, '/api/v1/targets?state=active'),
                 user=puser, password=ppass, **ca),
        ])
        return [
            ('get-credentials returns user and password', bool(puser and ppass)),
            ('prometheus refuses requests without credentials', r[0]['status'] == 401),
            ('prometheus accepts the credentials over verified TLS', r[1]['status'] == 200),
            ('prometheus no longer answers plain HTTP', r[2]['status'] != 200),
            ('alertmanager refuses requests without credentials', r[3]['status'] == 401),
            ('alertmanager accepts the credentials over verified TLS', r[4]['status'] == 200),
            ('node-exporter serves TLS signed by the cephadm root CA', r[5]['status'] == 200),
            ('node-exporter no longer answers plain HTTP', r[6]['status'] != 200),
            ('prometheus scrapes every node-exporter over TLS',
             self.node_targets_up(r[7], len(self.host_names))),
        ]

    def _backends_need_client_cert(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        node = self.host_names[0]
        reqs = [
            {'url': self.direct(PROMETHEUS_PORT, '/api/v1/status/buildinfo')},
            {'url': self.direct(ALERTMANAGER_PORT, '/api/v2/status')},
            {'url': self.direct(NODE_EXPORTER_PORT, '/metrics', host=node)},
            {'url': self.gw_internal('/internal/prometheus/api/v1/status/buildinfo')},
        ]
        names = [
            'prometheus refuses clients without a certificate',
            'alertmanager refuses clients without a certificate',
            'node-exporter refuses clients without a certificate',
            'gateway internal endpoint refuses clients without a certificate',
        ]
        return reqs, names

    def _checks_gateway(self) -> List[Check]:
        puser, ppass = self.prometheus_creds()
        auser, apass = self.alertmanager_creds()
        ca = {'cacert': ROOT_CA_FILE}
        direct, direct_names = self._backends_need_client_cert()
        r = self.http_many(direct + [
            dict(url=self.gw('/'), **ca),
            dict(url=self.gw('/grafana/api/health'), **ca),
            dict(url=self.gw('/prometheus/api/v1/status/buildinfo'), **ca),
            dict(url=self.gw('/prometheus/api/v1/status/buildinfo'),
                 user=puser, password=ppass, **ca),
            dict(url=self.gw('/alertmanager/api/v2/status'),
                 user=auser, password=apass, **ca),
            dict(url=self.gw('/prometheus/api/v1/targets?state=active'),
                 user=puser, password=ppass, **ca),
        ])
        n = len(direct)
        sec = self.security_config()
        return [(name, self.rejected_without_client_cert(res))
                for name, res in zip(direct_names, r[:n])] + [
            ('get-security-config reports security and gateway enabled',
             sec.get('security_enabled') is True and sec.get('mgmt_gw_enabled') is True),
            ('gateway serves the dashboard', r[n]['status'] == 200),
            ('gateway serves grafana', r[n + 1]['status'] == 200),
            ('prometheus through the gateway refuses requests without credentials',
             r[n + 2]['status'] == 401),
            ('prometheus through the gateway accepts the credentials',
             r[n + 3]['status'] == 200),
            ('alertmanager through the gateway accepts the credentials',
             r[n + 4]['status'] == 200),
            ('prometheus scrapes every node-exporter (mTLS, discovery via gateway)',
             self.node_targets_up(r[n + 5], len(self.host_names))),
        ]

    @staticmethod
    def _sent_to_login(res: Dict[str, Any]) -> bool:
        return 300 <= res['status'] < 400 and bool(res['location']) \
            and ('/oauth2/' in res['location'] or f':{IDP_PORT}/' in res['location'])

    def _checks_oauth2(self) -> List[Check]:
        puser, ppass = self.prometheus_creds()
        bearer = self.token()
        ca = {'cacert': ROOT_CA_FILE}
        routes = [path for _, path in SSO_ROUTES]
        direct, direct_names = self._backends_need_client_cert()
        reqs = list(direct)
        reqs += [dict(url=self.gw(p), **ca) for p in routes]
        reqs += [dict(url=self.gw(p), bearer=bearer, **ca) for p in routes]
        reqs += [
            # basic auth is not a way around the IdP
            dict(url=self.gw('/prometheus/api/v1/status/buildinfo'),
                 user=puser, password=ppass, **ca),
            dict(url=self.gw('/prometheus/api/v1/targets?state=active'), bearer=bearer, **ca),
        ]
        r = self.http_many(reqs)
        n, k = len(direct), len(routes)
        out: List[Check] = [(name, self.rejected_without_client_cert(res))
                            for name, res in zip(direct_names, r[:n])]
        out += [(f'{p} without a session is sent to login', self._sent_to_login(res))
                for p, res in zip(routes, r[n:n + k])]
        # with SSO the backends drop basic auth: the bearer token alone works
        out += [(f'{p} with a bearer token is served', res['status'] == 200)
                for p, res in zip(routes, r[n + k:n + 2 * k])]
        out += [
            ('prometheus basic auth credentials do not bypass the IdP',
             self._sent_to_login(r[n + 2 * k])),
            ('prometheus scrapes every node-exporter',
             self.node_targets_up(r[n + 2 * k + 1], len(self.host_names))),
        ]
        return out

    def wait_mode(self, mode: str, timeout: int = 900) -> None:
        self.wait_checks(f'{mode} mode', lambda: self.checks(mode), timeout=timeout)

    def hold_mode(self, mode: str, seconds: int = 120) -> None:
        self.hold_checks(f'{mode} mode', lambda: self.checks(mode), seconds=seconds)

    # ------------------------------------------------------------------
    # cleanup
    # ------------------------------------------------------------------
    def remove_all(self, services: Sequence[str]) -> None:
        for svc in services:
            self.rm_service(svc)
