"""
Integration tests for the cephadm monitoring stack (tracker #81323).

Shared helpers for ``test_monitoring_secure``. The workunit
``qa/suites/orch/cephadm/workunits/task/test_monitoring_stack_secure.yaml``
enables ``mgr/cephadm/secure_monitoring_stack`` and deploys prometheus,
alertmanager, grafana, and node-exporter.

Patterns (command wrappers, poll helpers) follow
``qa/tasks/cephadm_cases/certmgr/base.py`` without PKI machinery.
"""

from __future__ import annotations

import json
import logging
import os
import time
from io import StringIO
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest import SkipTest

from tasks.cephadm import _shell
from tasks.ceph_test_case import CephTestCase

log = logging.getLogger(__name__)

WORKDIR = '/tmp/monitoring-qa'
MONITORING_CA = f'{WORKDIR}/cephadm-root-ca.crt'

# Ports used by the secure-stack scrapes (HTTP / HTTPS).
SERVICE_PORTS = {
    'prometheus': 9095,
    'alertmanager': 9093,
    'grafana': 3000,
    'node-exporter': 9100,
}

# Fake remote-write sink. Runs on the Prometheus host: Teuthology may block
# host.b → admin:19291. set-remote-write uses that host's IP, not 127.0.0.1
# (Prometheus is in a container). Copy the script to WORKDIR there.
REMOTE_WRITE_PORT = 19291
REMOTE_WRITE_RECEIVER_SRC = os.path.abspath(os.path.join(
    os.path.dirname(__file__), 'mock_prometheus_remote_write.py'))
REMOTE_WRITE_RECEIVER = f'{WORKDIR}/mock_prometheus_remote_write.py'
REMOTE_WRITE_PID = f'{WORKDIR}/rw-receiver.pid'
REMOTE_WRITE_LOG = f'{WORKDIR}/rw-receiver.log'

SECURE_MONITORING_OPT = 'mgr/cephadm/secure_monitoring_stack'


class CommandFailed(Exception):
    """A ceph/cephadm-shell command exited non-zero when it was expected to succeed."""


class CommandResult:
    def __init__(self, args: List[str], rc: int, out: str, err: str) -> None:
        self.args = args
        self.rc = rc
        self.out = out
        self.err = err

    def __repr__(self) -> str:
        cmd = ' '.join(a if len(a) < 60 else a[:24] + '...' for a in self.args)
        return f'<rc={self.rc} cmd={cmd!r} err={self.err.strip()[-500:]!r}>'


class MonitoringTestCase(CephTestCase):
    """Shared helpers. Not collected on its own (no test_ methods).

    ``cephfs_test_runner`` injects ``ctx`` / ``mgr_cluster``. ``setUpClass``
    performs the usual mgr reset (``MgrTestCase.setup_mgrs``) when Teuthology
    registered mgr daemons. Commands go through ``tasks.cephadm._shell``.
    Copy/start of the remote-write receiver uses ``ctx.cluster.remotes``.
    """

    MGRS_REQUIRED = 1

    # populated by setUpClass
    remote: Any = None
    hosts: Dict[str, str] = {}
    _monitoring_ca: Optional[str] = None
    _rw_remote: Any = None
    _rw_addr: Optional[str] = None

    @classmethod
    def _mgr(cls) -> Any:
        assert cls.mgr_cluster is not None
        return cls.mgr_cluster

    # ------------------------------------------------------------------
    # class setup
    # ------------------------------------------------------------------
    @classmethod
    def setUpClass(cls) -> None:
        # Preserve MgrTestCase.setUpClass behaviour for pulpito/teuthology.
        assert cls.mgr_cluster is not None
        if len(cls.mgr_cluster.mgr_ids) < cls.MGRS_REQUIRED:
            raise SkipTest(
                'Only have {0} manager daemons, {1} are required'.format(
                    len(cls.mgr_cluster.mgr_ids), cls.MGRS_REQUIRED))
        cls.mgr_cluster.mon_manager.raw_cluster_cmd(
            'config', 'set', 'mds',
            'defer_client_eviction_on_laggy_osds', 'false')
        # setup_mgrs stops/restarts mgrs via DaemonGroup. Skip when none
        # are registered (no Teuthology daemon map).
        if cls.mgr_cluster.mgr_daemons:
            from tasks.mgr.mgr_test_case import MgrTestCase
            MgrTestCase.setup_mgrs.__func__(cls)  # type: ignore[attr-defined]

        cls.remote = cls._mgr().admin_remote
        cls.remote.run(args=['sudo', 'rm', '-rf', WORKDIR])
        cls.remote.run(args=['sudo', 'mkdir', '-p', WORKDIR])
        cls._monitoring_ca = None
        cls._wait_orch_ready()
        cls.hosts = cls._host_addrs()
        # Monitoring stack + secure_monitoring_stack come from the suite YAML
        # (roles + cluster-conf). Tests wait/assert; they do not redeploy.

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, 'remote', None) is not None:
            cls.remote.run(args=['sudo', 'rm', '-rf', WORKDIR], check_status=False)
        super().tearDownClass()

    # ------------------------------------------------------------------
    # command execution
    # ------------------------------------------------------------------
    @classmethod
    def _run_cls(cls, args: List[str], check: bool = True, timeout: int = 300) -> CommandResult:
        proc = _shell(cls.ctx, 'ceph', cls._mgr().admin_remote,
                      ['timeout', str(timeout)] + args,
                      extra_cephadm_args=['-m', f'{WORKDIR}:{WORKDIR}:z'],
                      stdout=StringIO(), stderr=StringIO(), check_status=False)
        res = CommandResult(args, proc.exitstatus, proc.stdout.getvalue(),
                            proc.stderr.getvalue())
        log.debug('%r', res)
        if check and res.rc != 0:
            raise CommandFailed(f'command failed: {res!r}')
        return res

    @classmethod
    def _ceph_cls(cls, *args: str, check: bool = True) -> CommandResult:
        return cls._run_cls(['ceph'] + list(args), check=check)

    def ceph(self, *args: str, check: bool = True) -> CommandResult:
        return self._ceph_cls(*args, check=check)

    def ceph_json(self, *args: str) -> Any:
        out = self.ceph(*args, '--format', 'json').out.strip()
        return json.loads(out) if out else None

    def orch(self, *args: str, check: bool = True) -> CommandResult:
        return self.ceph('orch', *args, check=check)

    # ------------------------------------------------------------------
    # cluster helpers
    # ------------------------------------------------------------------
    @classmethod
    def _wait_orch_ready(cls, timeout: int = 180) -> None:
        cls.wait_until_true(
            lambda: cls._ceph_cls('orch', 'status', check=False).rc == 0,
            timeout=timeout)

    @classmethod
    def _host_addrs(cls) -> Dict[str, str]:
        hosts = json.loads(cls._ceph_cls('orch', 'host', 'ls', '--format', 'json').out)
        return {h['hostname']: h['addr'] for h in hosts}

    @classmethod
    def _orch_ls_cls(cls, service_name: str, refresh: bool = False) -> List[Dict[str, Any]]:
        args = ['orch', 'ls', '--service_name', service_name, '--format', 'json']
        if refresh:
            args.append('--refresh')
        out = cls._ceph_cls(*args).out.strip()
        if not out or out == 'No services reported':
            return []
        return json.loads(out)

    @classmethod
    def _service_running_cls(cls, service_name: str) -> bool:
        ls = cls._orch_ls_cls(service_name, refresh=True)
        if not ls:
            return False
        status = ls[0].get('status', {})
        return status.get('size', 0) > 0 and status.get('running', 0) >= status['size']

    @classmethod
    def _ensure_service_cls(cls, service_name: str, timeout: int = 900) -> None:
        """Wait until ``service_name`` is running. Do not ``orch apply``.

        Roles (or the operator on a live cluster) must already have deployed
        it. Missing daemons fail this wait.
        """
        if cls._service_running_cls(service_name):
            return
        deadline = time.time() + timeout
        while time.time() < deadline:
            if cls._service_running_cls(service_name):
                return
            time.sleep(15)
        raise AssertionError(
            f'timed out waiting for {service_name} to become running '
            f'(not deployed; this test does not orch apply)')

    def kick(self) -> None:
        """Wake up the cephadm serve loop."""
        self.orch('ps', '--refresh', check=False)

    def poll(self, what: str, fn: Callable[[], bool], timeout: int = 600,
             period: int = 10, kick: bool = False) -> None:
        deadline = time.time() + timeout
        while True:
            if kick:
                self.kick()
            try:
                if fn():
                    return
            except (CommandFailed, KeyError, IndexError, ValueError) as e:
                log.info('poll %s: not yet (%s)', what, e)
            if time.time() > deadline:
                raise AssertionError(f'timed out after {timeout}s waiting for {what}')
            time.sleep(period)

    def daemons(self, service_name: Optional[str] = None,
                daemon_type: Optional[str] = None) -> List[Dict[str, Any]]:
        args = ['orch', 'ps']
        if service_name:
            args += ['--service_name', service_name]
        if daemon_type:
            args += ['--daemon-type', daemon_type]
        return self.ceph_json(*args) or []

    def daemon_addr(self, daemon_type: str) -> Tuple[str, str]:
        """Return (hostname, addr) for the first daemon of the given type."""
        daemons = self.daemons(daemon_type=daemon_type)
        self.assertTrue(daemons, f'no {daemon_type} daemon reported')
        hostname = daemons[0]['hostname']
        addr = self.hosts.get(hostname)
        self.assertTrue(addr, f'no addr for host {hostname}')
        return hostname, addr

    def service_url(self, daemon_type: str, path: str = '/',
                    secure: Optional[bool] = None) -> str:
        """Build http(s)://addr:port/path for a monitoring daemon."""
        if secure is None:
            secure = self.secure_monitoring_enabled()
        _, addr = self.daemon_addr(daemon_type)
        port = SERVICE_PORTS[daemon_type]
        scheme = 'https' if secure else 'http'
        if not path.startswith('/'):
            path = '/' + path
        return f'{scheme}://{addr}:{port}{path}'

    # ------------------------------------------------------------------
    # secure monitoring stack + credentials
    # ------------------------------------------------------------------
    def secure_monitoring_enabled(self) -> bool:
        out = self.ceph('config', 'get', 'mgr', SECURE_MONITORING_OPT).out.strip().lower()
        return out in ('true', '1', 'yes', 'on')

    def get_prometheus_credentials(self) -> Dict[str, str]:
        return self.ceph_json('orch', 'prometheus', 'get-credentials') or {}

    def get_alertmanager_credentials(self) -> Dict[str, str]:
        return self.ceph_json('orch', 'alertmanager', 'get-credentials') or {}

    def set_prometheus_credentials(self, username: str, password: str) -> CommandResult:
        return self.orch('prometheus', 'set-credentials', username, password)

    def set_alertmanager_credentials(self, username: str, password: str) -> CommandResult:
        return self.orch('alertmanager', 'set-credentials', username, password)

    def _ensure_monitoring_ca(self) -> str:
        """Write the cephadm root CA from get-credentials and return its path.

        Prometheus, Alertmanager, Grafana, and the mgr Prometheus module under
        ``secure_monitoring_stack`` are signed by this CA. Leaf certs include
        the host IP as a SAN, so scrapes of ``https://<addr>:port`` can verify
        without ``curl -k``.
        """
        cls = type(self)
        if cls._monitoring_ca:
            return cls._monitoring_ca
        creds = self.get_prometheus_credentials()
        pem = (creds.get('certificate') or '').strip()
        self.assertTrue(
            pem, 'prometheus get-credentials returned no certificate')
        if not pem.endswith('\n'):
            pem += '\n'
        self.remote.write_file(MONITORING_CA, pem, sudo=True, mode='0644')
        cls._monitoring_ca = MONITORING_CA
        return MONITORING_CA

    def _curl_tls_args(self, url: str) -> List[str]:
        if url.startswith('https://'):
            return ['--cacert', self._ensure_monitoring_ca()]
        return []

    # ------------------------------------------------------------------
    # HTTP scrape helpers
    # ------------------------------------------------------------------
    def http_status(self, url: str, *,
                    user: Optional[str] = None,
                    password: Optional[str] = None) -> Tuple[int, CommandResult]:
        """GET ``url`` and return ``(http_status_or_-1, CommandResult)``.

        ``curl -f`` is omitted so HTTP 401 is visible as a status code rather
        than only a non-zero exit. HTTPS uses ``--cacert`` with the cephadm
        root CA from ``get-credentials`` (no ``-k``).
        """
        args: List[str] = ['curl', '-sS'] + self._curl_tls_args(url)
        if user is not None:
            args += ['-u', f'{user}:{password or ""}']
        args += ['-o', '/dev/null', '-w', '%{http_code}', url]
        proc = self.remote.run(args=args, stdout=StringIO(), stderr=StringIO(),
                               check_status=False)
        res = CommandResult(args, proc.exitstatus, proc.stdout.getvalue(),
                            proc.stderr.getvalue())
        code = -1
        out = res.out.strip()
        if out.isdigit():
            code = int(out)
        return code, res

    def assert_mgr_prometheus_exports_health(self, *,
                                             secure: Optional[bool] = None) -> None:
        """Require a ``ceph_health_status`` sample from the mgr Prometheus module.

        ``ceph mgr services`` publishes the module URL (``.../``). This fetches
        ``<url>metrics`` from the admin host. ``secure=None`` follows
        ``secure_monitoring_stack``: HTTPS with the cephadm root CA when it is
        on, plain HTTP when it is off.
        """
        if secure is None:
            secure = self.secure_monitoring_enabled()
        services = self.ceph_json('mgr', 'services') or {}
        url = str(services.get('prometheus') or '').strip()
        self.assertTrue(
            url, f'mgr services did not publish a prometheus URL: {services!r}')
        scheme = 'https://' if secure else 'http://'
        self.assertTrue(
            url.startswith(scheme),
            f'expected a {scheme} mgr prometheus URL, got {url}')
        metrics_url = url + 'metrics' if url.endswith('/') else url + '/metrics'
        log.info('mgr prometheus metrics %s', metrics_url)
        args = ['curl', '-fsS'] + self._curl_tls_args(metrics_url) + [metrics_url]
        proc = self.remote.run(args=args, stdout=StringIO(), stderr=StringIO(),
                               check_status=False)
        res = CommandResult(args, proc.exitstatus, proc.stdout.getvalue(),
                            proc.stderr.getvalue())
        self.assertEqual(
            res.rc, 0,
            f'mgr prometheus metrics scrape failed for {metrics_url}: {res!r}')
        self.assertTrue(
            any(line.startswith('ceph_health_status') for line in res.out.splitlines()),
            f'ceph_health_status missing from {metrics_url}')

    def assert_grafana_api_health(self) -> None:
        """Require Grafana ``/api/health`` with ``database == "ok"``.

        Grafana speaks HTTPS with a cephadm-signed cert and does not use
        Prometheus/Alertmanager ``get-credentials``. Same check as
        ``test_monitoring_stack_basic.yaml`` (``https://<ip>:3000/api/health``),
        with ``--cacert`` instead of ``-k``. No ``-u``.
        """
        url = self.service_url('grafana', '/api/health', secure=True)
        self.assertTrue(
            url.startswith('https://'),
            f'expected an https Grafana health URL, got {url}')
        log.info('grafana health %s', url)

        def healthy() -> bool:
            args = ['curl', '-fsS'] + self._curl_tls_args(url) + [url]
            proc = self.remote.run(args=args, stdout=StringIO(), stderr=StringIO(),
                                   check_status=False)
            res = CommandResult(args, proc.exitstatus, proc.stdout.getvalue(),
                                proc.stderr.getvalue())
            if res.rc != 0:
                log.info('grafana health scrape failed: %r', res)
                return False
            try:
                body = json.loads(res.out)
            except json.JSONDecodeError as e:
                log.info('grafana health not JSON (%s): %r', e, res.out[:200])
                return False
            ok = isinstance(body, dict) and body.get('database') == 'ok'
            if not ok:
                log.info('grafana health unexpected body: %r', body)
            return ok

        self.poll('grafana /api/health database=ok', healthy, timeout=300, period=10)

    def monitoring_json(self, daemon_type: str, path: str, *,
                        secure: Optional[bool] = None) -> Any:
        """GET a monitoring API path and parse the JSON body.

        ``secure=None`` follows ``secure_monitoring_stack``: HTTPS with the
        cephadm root CA, and basic auth from ``get-credentials``, when it is
        on.
        """
        if secure is None:
            secure = self.secure_monitoring_enabled()
        user = password = None
        if secure:
            creds = (self.get_prometheus_credentials() if daemon_type == 'prometheus'
                     else self.get_alertmanager_credentials())
            user = creds.get('user') or ''
            password = creds.get('password') or ''
            self.assertTrue(
                user and password,
                f'{daemon_type} get-credentials returned no user/password')
        url = self.service_url(daemon_type, path, secure=secure)
        args = ['curl', '-fsS'] + self._curl_tls_args(url)
        if user is not None:
            args += ['-u', f'{user}:{password or ""}']
        args.append(url)
        proc = self.remote.run(args=args, stdout=StringIO(), stderr=StringIO(),
                               check_status=False)
        res = CommandResult(args, proc.exitstatus, proc.stdout.getvalue(),
                            proc.stderr.getvalue())
        if res.rc != 0:
            raise CommandFailed(f'GET {url} failed: {res!r}')
        return json.loads(res.out)

    def alertmanager_alert_active(self, alertname: str, *,
                                  secure: Optional[bool] = None) -> bool:
        """True when Alertmanager lists ``alertname`` in state ``active``."""
        body = self.monitoring_json('alertmanager', '/api/v2/alerts', secure=secure)
        if not isinstance(body, list):
            return False
        return any(
            a.get('labels', {}).get('alertname') == alertname
            and (a.get('status') or {}).get('state') == 'active'
            for a in body)

    def set_custom_alerts(self, yaml_text: str) -> CommandResult:
        """Store Prometheus rules and schedule a prometheus reconfig."""
        path = f'{WORKDIR}/custom_alerts.yml'
        self.remote.write_file(path, yaml_text, sudo=True)
        return self.orch('prometheus', 'set-custom-alerts', '-i', path)

    def assert_custom_alert_reaches_alertmanager(
            self, *, secure: Optional[bool] = None) -> None:
        """Install an always-true rule and require it in Alertmanager.

        ``expr: vector(1)`` fires on the next Prometheus evaluation after
        ``set-custom-alerts`` reconfigures the daemon. The rule is then
        replaced with an empty group list, and this waits until Alertmanager
        no longer lists the alert. That can take several minutes.
        ``secure=None`` follows ``secure_monitoring_stack``.
        """
        if secure is None:
            secure = self.secure_monitoring_enabled()
        alertname = 'QaCustomAlwaysOn'
        rule = (
            'groups:\n'
            '- name: qa-custom\n'
            '  rules:\n'
            '  - alert: QaCustomAlwaysOn\n'
            '    expr: vector(1)\n'
            '    for: 0s\n'
            '    labels:\n'
            '      severity: warning\n'
        )
        log.info('install custom alert %s', alertname)
        self.set_custom_alerts(rule)
        try:
            self.poll(
                f'{alertname} active in alertmanager',
                lambda: self.alertmanager_alert_active(alertname, secure=secure),
                timeout=300, period=15, kick=True)
        finally:
            log.info('clear custom alert %s', alertname)
            self.set_custom_alerts('groups: []\n')
            self.poll(
                f'{alertname} cleared from alertmanager',
                lambda: not self.alertmanager_alert_active(alertname, secure=secure),
                timeout=600, period=15, kick=True)

    def assert_ceph_mon_down_alert(self, *, secure: Optional[bool] = None,
                                   timeout: int = 180) -> None:
        """Stop one monitor and require a ``CephMonDown`` alert.

        Prometheus ``/api/v1/alerts`` must report the alert ``firing``.
        Alertmanager ``/api/v2/alerts`` must report it ``active``. The monitor
        is started again, and this waits until both endpoints have dropped
        the alert. ``secure=None`` follows ``secure_monitoring_stack``:
        HTTPS with basic auth when it is on, plain HTTP without auth when
        it is off.
        """
        if secure is None:
            secure = self.secure_monitoring_enabled()
        mons = self.daemons(daemon_type='mon')
        self.assertGreaterEqual(
            len(mons), 2, f'need two monitors so one can stop; got {mons!r}')
        mon_name = mons[-1].get('daemon_name') or ''
        self.assertTrue(mon_name, f'mon daemon has no name: {mons[-1]!r}')

        def firing() -> bool:
            body = self.monitoring_json('prometheus', '/api/v1/alerts', secure=secure)
            alerts = (body or {}).get('data', {}).get('alerts') or []
            prom_firing = any(
                a.get('labels', {}).get('alertname') == 'CephMonDown'
                and a.get('state') == 'firing'
                for a in alerts)
            am_active = self.alertmanager_alert_active('CephMonDown', secure=secure)
            log.info('CephMonDown prometheus_firing=%s alertmanager_active=%s',
                     prom_firing, am_active)
            return prom_firing and am_active

        def mon_restored() -> bool:
            up = any(
                d.get('daemon_name') == mon_name and d.get('status_desc') == 'running'
                for d in self.daemons(daemon_type='mon'))
            health = self.ceph_json('health', 'detail') or {}
            mon_down = 'MON_DOWN' in (health.get('checks') or {})
            log.info('%s running=%s MON_DOWN=%s', mon_name, up, mon_down)
            return up and not mon_down

        def alert_cleared() -> bool:
            body = self.monitoring_json('prometheus', '/api/v1/alerts', secure=secure)
            alerts = (body or {}).get('data', {}).get('alerts') or []
            prom_firing = any(
                a.get('labels', {}).get('alertname') == 'CephMonDown'
                and a.get('state') == 'firing'
                for a in alerts)
            am_active = self.alertmanager_alert_active('CephMonDown', secure=secure)
            log.info('CephMonDown cleared prometheus_firing=%s alertmanager_active=%s',
                     prom_firing, am_active)
            return not prom_firing and not am_active

        log.info('stop %s to raise CephMonDown', mon_name)
        self.orch('daemon', 'stop', mon_name)
        try:
            self.poll(f'CephMonDown after stopping {mon_name}', firing,
                      timeout=timeout, period=15)
        finally:
            self.orch('daemon', 'start', mon_name)
            # start returns before the mon is in quorum. Other health checks
            # may still be present, so wait for this mon and MON_DOWN only.
            self.poll(f'{mon_name} back and MON_DOWN cleared', mon_restored,
                      timeout=timeout, period=15)
            self.poll('CephMonDown cleared from prometheus and alertmanager',
                      alert_cleared, timeout=600, period=15)

    def prometheus_spec(self) -> Dict[str, Any]:
        """Return the stored prometheus spec from ``orch ls --export``."""
        specs = self.ceph_json('orch', 'ls', 'prometheus', '--export') or []
        if isinstance(specs, dict):
            specs = [specs]
        self.assertTrue(specs, 'prometheus spec missing from orch ls --export')
        return specs[0]

    def prometheus_spec_targets(self) -> List[str]:
        spec = self.prometheus_spec()
        inner = spec.get('spec') if isinstance(spec.get('spec'), dict) else {}
        return list(inner.get('targets') or spec.get('targets') or [])

    def prometheus_federate_targets(self, *,
                                    secure: Optional[bool] = None) -> List[Dict[str, Any]]:
        """Active scrape targets whose job name starts with ``federate``.

        Same filter as
        ``.data.activeTargets[] | select(.labels.job | startswith("federate"))``,
        parsed with Python ``json`` (no jq).
        """
        body = self.monitoring_json('prometheus', '/api/v1/targets', secure=secure)
        found: List[Dict[str, Any]] = []
        for t in (body or {}).get('data', {}).get('activeTargets', []):
            job = (t.get('labels') or {}).get('job') or ''
            if job.startswith('federate'):
                found.append(t)
        log.info('federate targets=%s', [
            {
                'job': (t.get('labels') or {}).get('job'),
                'instance': (t.get('labels') or {}).get('instance'),
                'health': t.get('health'),
                'scrapeUrl': t.get('scrapeUrl'),
            } for t in found
        ])
        return found

    def prometheus_federate_up(self, hostport: str, *,
                               secure: Optional[bool] = None) -> List[Dict[str, Any]]:
        """Federate jobs for ``hostport`` with scrape health ``up``."""
        return [
            t for t in self.prometheus_federate_targets(secure=secure)
            if (t.get('labels') or {}).get('instance') == hostport
            and t.get('health') == 'up'
        ]

    def prometheus_job_targets(self, job: str, *,
                               secure: Optional[bool] = None) -> List[Dict[str, Any]]:
        """Active scrape targets whose ``labels.job`` equals ``job``."""
        body = self.monitoring_json('prometheus', '/api/v1/targets', secure=secure)
        found: List[Dict[str, Any]] = []
        for t in (body or {}).get('data', {}).get('activeTargets', []):
            if ((t.get('labels') or {}).get('job') or '') == job:
                found.append(t)
        log.info('%s targets=%s', job, [
            {
                'instance': (t.get('labels') or {}).get('instance'),
                'health': t.get('health'),
                'scrapeUrl': t.get('scrapeUrl'),
            } for t in found
        ])
        return found

    def assert_node_exporter_scraped(self, *,
                                     secure: Optional[bool] = None) -> None:
        """Prometheus must scrape each node-exporter over TLS when secure.

        Roles deploy node-exporter. This checks ``/api/v1/targets``:
        ``job=node-exporter``, ``health=up``, ``scrapeUrl`` is
        ``https://…:9100/metrics`` (or http when the stack is not secure).
        """
        if secure is None:
            secure = self.secure_monitoring_enabled()
        ls = self._orch_ls_cls('node-exporter', refresh=True)
        want = int((ls[0].get('status') or {}).get('running') or 0) if ls else 0
        self.assertGreater(want, 0, 'no running node-exporter daemon')
        scheme = 'https://' if secure else 'http://'

        def ready() -> bool:
            rows = self.prometheus_job_targets('node-exporter', secure=secure)
            if len(rows) < want:
                return False
            return all(
                t.get('health') == 'up'
                and str(t.get('scrapeUrl') or '').startswith(scheme)
                and ':9100/metrics' in str(t.get('scrapeUrl') or '')
                for t in rows
            )

        self.poll(
            f'prometheus node-exporter targets up ({scheme}…:9100/metrics)',
            ready, timeout=300, period=10, kick=True)
        rows = self.prometheus_job_targets('node-exporter', secure=secure)
        self.assertGreaterEqual(
            len(rows), want,
            f'expected {want} node-exporter scrape targets, got {len(rows)}: '
            f'{rows!r}')
        for t in rows:
            url = str(t.get('scrapeUrl') or '')
            self.assertEqual(
                t.get('health'), 'up',
                f'node-exporter target not up: {t!r}')
            self.assertTrue(
                url.startswith(scheme) and ':9100/metrics' in url,
                f'expected {scheme}…:9100/metrics scrapeUrl, got {url!r}')
        log.info('node-exporter scrape check passed: %d target(s)', len(rows))

    def assert_node_exporter_metrics(self, *,
                                     secure: Optional[bool] = None) -> None:
        """Each running node-exporter must answer on ``:9100/metrics``.

        Same check as ``test_monitoring_stack_basic.yaml`` (curl each host
        ``:9100``), with ``https`` and ``--cacert`` when the stack is secure
        and ``http`` when it is not. Path is ``/metrics``. No ``-u``
        (node-exporter has no ``get-credentials``).
        """
        if secure is None:
            secure = self.secure_monitoring_enabled()
        scheme = 'https' if secure else 'http'
        port = SERVICE_PORTS['node-exporter']
        daemons = [
            d for d in self.daemons(daemon_type='node-exporter')
            if d.get('status_desc') == 'running'
        ]
        self.assertTrue(daemons, 'no running node-exporter daemon')
        for d in daemons:
            hostname = d.get('hostname') or ''
            addr = self.hosts.get(hostname)
            self.assertTrue(addr, f'no addr for node-exporter host {hostname}')
            url = f'{scheme}://{addr}:{port}/metrics'
            code, res = self.http_status(url)
            log.info('node-exporter metrics %s (%s) status=%s',
                     url, hostname, code)
            self.assertEqual(
                code, 200,
                f'node-exporter :9100/metrics failed on {hostname}: {res!r}')
        log.info('node-exporter metrics check passed: %d host(s)', len(daemons))

    def _dashboard_multicluster_config(self) -> str:
        return self.ceph('dashboard', 'get-multicluster-config').out.strip()

    def _set_dashboard_multicluster_config(self, value: str) -> None:
        """Store Dashboard ``MULTICLUSTER_CONFIG`` from a file (cert can be large).

        The payload is a JSON object of strings so
        ``Settings.cast`` can ``literal_eval`` it (JSON booleans would fail).
        """
        path = f'{WORKDIR}/multicluster-fake.json'
        self.remote.write_file(path, value, sudo=True)
        self._run_cls([
            'bash', '-c',
            f'ceph dashboard set-multicluster-config "$(cat {path})"',
        ])

    def assert_prometheus_set_target(self, *,
                                     secure: Optional[bool] = None) -> None:
        """``set-target`` must appear as a federate scrape, then clean up.

        Product split (not a test shortcut):

        * ``ceph orch prometheus set-target`` writes ``host:port`` onto
          the prometheus spec and redeploys.
        * ``prometheus.yml.j2`` builds ``federate_*`` jobs from
          ``clusters_credentials``, which cephadm asks Dashboard for
          (``get_cluster_credentials_files``). That dict is filled only
          when ``MULTICLUSTER_CONFIG`` has ``prometheus_access_info``
          whose URL matches the target. ``spec.targets`` alone does not
          render a federate job.

        A real second cluster would supply that Dashboard entry. This
        suite has one cluster, so we fake ``MULTICLUSTER_CONFIG`` and
        point ``prometheus_url`` at *this* Prometheus (self-federate).
        Same orch + Dashboard path as two clusters, without a second
        cluster.

        Then: ``set-target``, wait until ``/api/v1/targets`` (Python JSON)
        shows ``federate_*`` ``up``. ``finally``: ``remove-target``,
        restore Dashboard, require the federate job and
        ``orch ls --export`` target gone.
        ``secure=None`` follows ``secure_monitoring_stack``.
        """
        if secure is None:
            secure = self.secure_monitoring_enabled()
        _, addr = self.daemon_addr('prometheus')
        port = SERVICE_PORTS['prometheus']
        hostport = f'{addr}:{port}'
        scheme = 'https' if secure else 'http'
        prometheus_url = f'{scheme}://{hostport}'

        creds = self.get_prometheus_credentials() if secure else {}
        user = creds.get('user') or ''
        password = creds.get('password') or ''
        certificate = creds.get('certificate') or ''
        if secure:
            self.assertTrue(
                user and password,
                'prometheus get-credentials returned no user/password')

        # Fake a *second* cluster in Dashboard. We do not stand up another
        # Ceph cluster: prometheus_url is this cluster's Prometheus so
        # federate scrapes ourselves. orch set-target still needs this
        # store or prometheus.yml will have spec.targets but no federate_*
        # job (Dashboard owns credentials; orch owns the target list).
        fake = {
            'current_user': 'admin',
            'config': {
                'qa-fake-fsid': [{
                    'name': 'qa-fake-fsid',
                    'url': 'https://192.0.2.1:8443',
                    'cluster_alias': 'qa-fake',
                    'user': 'admin',
                    'token': 'unused',
                    'prometheus_url': prometheus_url,
                    'ssl_certificate': '',
                    'prometheus_access_info': {
                        'user': user,
                        'password': password,
                        'certificate': certificate,
                    },
                }]
            }
        }
        previous = self._dashboard_multicluster_config()
        already = hostport in self.prometheus_spec_targets()

        log.info(
            'seed fake Dashboard MULTICLUSTER_CONFIG for %s '
            '(one-cluster stand-in: no second Ceph cluster)',
            prometheus_url)

        self._set_dashboard_multicluster_config(json.dumps(fake))
        try:
            if already:
                log.info('%s already in prometheus spec; redeploy after seeding',
                         hostport)
                self.orch('redeploy', 'prometheus')
            else:
                log.info('set-target %s', prometheus_url)
                self.orch('prometheus', 'set-target', prometheus_url)
            self.poll(
                f'prometheus spec lists {hostport}',
                lambda: hostport in self.prometheus_spec_targets(),
                timeout=600, period=15, kick=True)
            self.poll(
                f'federate target {hostport} up',
                lambda: bool(self.prometheus_federate_up(hostport, secure=secure)),
                timeout=600, period=15, kick=True)
            up = self.prometheus_federate_up(hostport, secure=secure)
            self.assertTrue(
                up,
                f'expected federate_* health=up for {hostport}; '
                f'last targets={self.prometheus_federate_targets(secure=secure)!r}')
            log.info('set-target check passed: federate job for %s is up', hostport)
        finally:
            log.info('remove-target %s', hostport)
            self.orch('prometheus', 'remove-target', hostport, check=False)
            if previous in ('', '{}', 'None'):
                self.ceph('dashboard', 'reset-multicluster-config')
            else:
                self._set_dashboard_multicluster_config(previous)
            self.poll(
                f'prometheus spec dropped {hostport}',
                lambda: hostport not in self.prometheus_spec_targets(),
                timeout=600, period=15, kick=True)
            self.poll(
                f'federate target {hostport} gone',
                lambda: not self.prometheus_federate_targets(secure=secure),
                timeout=600, period=15, kick=True)
            log.info('cleanup passed: no federate jobs, spec dropped %s', hostport)

    def admin_host_addr(self) -> str:
        """Cluster IP of the admin / test-runner host."""
        raw = (getattr(self.remote, 'hostname', None)
               or getattr(self.remote, 'name', '') or '')
        short = raw.split('.')[0]
        for h, addr in self.hosts.items():
            if h == raw or h.split('.')[0] == short:
                self.assertTrue(addr, f'empty orch addr for host {h}')
                return addr
        proc = self.remote.run(
            args=['hostname', '-I'], stdout=StringIO(), stderr=StringIO(),
            check_status=False)
        for tok in (proc.stdout.getvalue() if proc.stdout else '').split():
            if '.' in tok and not tok.startswith('127.'):
                return tok
        self.fail(
            f'cannot resolve admin host addr from {raw!r} hosts={self.hosts!r}')

    def _hostname_matches_remote(self, remote: Any, hostname: str) -> bool:
        host_short = hostname.split('.')[0]
        names = [
            getattr(remote, 'hostname', None) or '',
            getattr(remote, 'shortname', None) or '',
            getattr(remote, 'name', None) or '',
        ]
        return any(
            n == hostname or (n and n.split('.')[0] == host_short)
            for n in names)

    def _is_admin_host(self, hostname: str, addr: str) -> bool:
        try:
            if addr and addr == self.admin_host_addr():
                return True
        except AssertionError:
            pass
        return self._hostname_matches_remote(self.remote, hostname)

    def _remote_for_host(self, hostname: str, addr: str) -> Any:
        """Remote for the Prometheus host (where the remote-write mock runs)."""
        cluster = getattr(getattr(self, 'ctx', None), 'cluster', None)
        remotes = getattr(cluster, 'remotes', None) if cluster is not None else None
        if remotes:
            for remote, roles in remotes.items():
                role_list = [str(r) for r in (roles or [])]
                if any(r.startswith('prometheus.') for r in role_list):
                    return remote
            for remote, _roles in remotes.items():
                if self._hostname_matches_remote(remote, hostname):
                    return remote
        if self._is_admin_host(hostname, addr):
            return self.remote
        self.fail(
            f'no ctx.cluster.remotes entry for prometheus host {hostname} '
            f'({addr})')

    def _prometheus_receiver_remote(self) -> Any:
        remote = getattr(self, '_rw_remote', None)
        self.assertIsNotNone(remote, 'prometheus receiver remote not bound')
        return remote

    def prometheus_remote_write_url(self) -> str:
        spec = self.prometheus_spec()
        inner = spec.get('spec') if isinstance(spec.get('spec'), dict) else {}
        return inner.get('remote_write_url') or spec.get('remote_write_url') or ''

    def prometheus_has_metric(self, metric: str, *,
                              secure: Optional[bool] = None) -> bool:
        path = f'/api/v1/query?query={metric}'
        body = self.monitoring_json('prometheus', path, secure=secure)
        result = (body or {}).get('data', {}).get('result') or []
        log.info('prometheus query %s results=%d', metric, len(result))
        return bool(result)

    def prometheus_config_yaml(self, *,
                               secure: Optional[bool] = None) -> str:
        body = self.monitoring_json(
            'prometheus', '/api/v1/status/config', secure=secure)
        return (body or {}).get('data', {}).get('yaml') or ''

    def _prom_host_curl(self, *curl_args: str) -> CommandResult:
        """curl on the Prometheus host (loopback /status; no admin→:19291)."""
        args = ['curl'] + list(curl_args)
        proc = self._prometheus_receiver_remote().run(
            args=args, stdout=StringIO(), stderr=StringIO(), check_status=False)
        return CommandResult(
            args, proc.exitstatus, proc.stdout.getvalue(), proc.stderr.getvalue())

    def remote_write_receiver_status(self) -> Dict[str, Any]:
        """Scoreboard of the fake remote Prometheus (loopback on the Prometheus host)."""
        res = self._prom_host_curl(
            '-fsS', '--max-time', '5',
            f'http://127.0.0.1:{REMOTE_WRITE_PORT}/status')
        if res.rc != 0 or not res.out.strip():
            raise CommandFailed(f'receiver /status failed: {res!r}')
        body = json.loads(res.out)
        log.info(
            'remote_write receiver status post_accepted=%s metric_hits=%s '
            'snappy_decoded=%s snappy_errors=%s',
            body.get('post_accepted'), body.get('metric_hits'),
            body.get('snappy_decoded'), body.get('snappy_errors'))
        return body

    def _start_remote_write_receiver(self) -> None:
        """Copy the receiver to WORKDIR on the Prometheus host and start it."""
        self.assertTrue(
            os.path.isfile(REMOTE_WRITE_RECEIVER_SRC),
            f'missing receiver script {REMOTE_WRITE_RECEIVER_SRC}')
        remote = self._prometheus_receiver_remote()
        remote.run(args=['sudo', 'mkdir', '-p', WORKDIR], check_status=True)
        with open(REMOTE_WRITE_RECEIVER_SRC, encoding='utf-8') as f:
            remote.write_file(REMOTE_WRITE_RECEIVER, f.read(), sudo=True)
        self._kill_remote_write_pid(remote)
        time.sleep(1)
        start = (
            f'nohup python3 {REMOTE_WRITE_RECEIVER} --bind 0.0.0.0 '
            f'--port {REMOTE_WRITE_PORT} --watch-metric ceph_health_status '
            f'>{REMOTE_WRITE_LOG} 2>&1 & '
            f'echo $! > {REMOTE_WRITE_PID}'
        )
        remote.run(args=['sudo', 'bash', '-c', start], check_status=True)
        self.poll(
            'remote_write receiver healthz',
            self._remote_write_receiver_ok,
            timeout=30, period=2)

    def _remote_write_receiver_ok(self) -> bool:
        res = self._prom_host_curl(
            '-fsS', '--max-time', '3',
            f'http://127.0.0.1:{REMOTE_WRITE_PORT}/healthz')
        return res.rc == 0 and res.out.strip() == 'ok'

    def _kill_remote_write_pid(self, remote: Any) -> None:
        """Stop the receiver using only the pid file (not ``pkill -f``)."""
        remote.run(
            args=['sudo', 'bash', '-c',
                  f'if [ -f {REMOTE_WRITE_PID} ]; then '
                  f'kill $(cat {REMOTE_WRITE_PID}) 2>/dev/null || true; '
                  f'rm -f {REMOTE_WRITE_PID}; fi'],
            check_status=False)

    def _stop_remote_write_receiver(self) -> None:
        remote = getattr(self, '_rw_remote', None)
        if remote is None:
            return
        self._kill_remote_write_pid(remote)

    def assert_prometheus_remote_write(self, *,
                                       secure: Optional[bool] = None) -> None:
        """``set-remote-write`` must push ``ceph_health_status`` samples.

        The receiver stands in for another cluster's Prometheus. It runs
        on the Prometheus host so Teuthology does not need host.b →
        admin:19291. Copy uses ``ctx.cluster.remotes``. ``set-remote-write``
        uses that host IP (not 127.0.0.1). ``GET /status`` must show
        ``post_accepted >= 1`` and ``metric_hits['ceph_health_status'] >= 1``.
        ``finally`` cleans up.
        """
        if secure is None:
            secure = self.secure_monitoring_enabled()
        metric = 'ceph_health_status'
        hostname, recv_addr = self.daemon_addr('prometheus')
        self._rw_remote = self._remote_for_host(hostname, recv_addr)
        self._rw_addr = recv_addr
        recv_base = f'http://{recv_addr}:{REMOTE_WRITE_PORT}'
        write_url = f'{recv_base}/api/v1/write'
        log.info('remote_write receiver on %s (%s)', hostname, recv_addr)

        self.poll(
            f'prometheus has {metric}',
            lambda: self.prometheus_has_metric(metric, secure=secure),
            timeout=120, period=10)

        try:
            self._start_remote_write_receiver()
            log.info('set-remote-write %s %s', recv_base, metric)
            self.orch('prometheus', 'set-remote-write', recv_base, metric)
            self.poll(
                f'prometheus spec remote_write_url={recv_base}',
                lambda: self.prometheus_remote_write_url() == recv_base,
                timeout=120, period=5, kick=True)
            self.poll(
                f'live prometheus config has {write_url}',
                lambda: write_url in self.prometheus_config_yaml(secure=secure),
                timeout=600, period=15, kick=True)
            self.poll(
                f'remote_write receiver saw {metric}',
                lambda: (
                    (self.remote_write_receiver_status()
                     .get('metric_hits') or {}).get(metric) or 0) >= 1,
                timeout=180, period=10)
            status = self.remote_write_receiver_status()
            self.assertGreaterEqual(
                status.get('post_accepted') or 0, 1,
                f'expected remote_write POSTs at {recv_base}; last {status!r}')
            hits = (status.get('metric_hits') or {}).get(metric) or 0
            self.assertGreaterEqual(
                hits, 1,
                f'expected {metric} in remote_write payload at {recv_base}; '
                f'last {status!r}')
            log.info('remote-write check passed: %s', status)
        finally:
            log.info('remove-remote-write %s', recv_base)
            self.orch('prometheus', 'remove-remote-write', recv_base, check=False)
            self.poll(
                'prometheus spec dropped remote_write_url',
                lambda: not self.prometheus_remote_write_url(),
                timeout=120, period=5, kick=True)
            self._stop_remote_write_receiver()
            self._rw_remote = None
            self._rw_addr = None
            log.info('cleanup passed: remote_write removed, receiver stopped')

    def assert_unauthenticated_scrape_fails(
            self,
            daemon_type: str,
            path: str = '/') -> None:
        """Require that a no-credentials scrape of ``daemon_type`` returns 401.

        The handshake is verified with the cephadm root CA, so a TLS error is
        a real failure (not a stand-in for missing basic auth).
        """
        url = self.service_url(daemon_type, path, secure=True)
        log.info('unauthenticated scrape %s', url)
        code, res = self.http_status(url)
        if code == 401:
            return
        self.fail(
            f'expected HTTP 401 for unauthenticated scrape of {daemon_type} '
            f'({url}); got http={code} {res!r}')
