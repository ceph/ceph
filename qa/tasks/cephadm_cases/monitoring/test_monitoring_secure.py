"""
Secure monitoring stack integration tests (tracker #81323).

``qa/suites/orch/cephadm/workunits/task/test_monitoring_stack_secure.yaml``
enables ``mgr/cephadm/secure_monitoring_stack`` and deploys prometheus,
alertmanager, grafana, and node-exporter. This module asserts behaviour
against that stack.
"""

from __future__ import annotations

import logging

from tasks.cephadm_cases.monitoring.base import MonitoringTestCase

log = logging.getLogger(__name__)


class TestMonitoringSecure(MonitoringTestCase):
    """Tracker #81323 checks against a secure monitoring stack."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        # Roles + cluster-conf bring the stack up; wait until scrapes are meaningful.
        for name in ('prometheus', 'alertmanager', 'grafana', 'node-exporter'):
            cls._ensure_service_cls(name)

    def test_unauthenticated_scrape_fails(self) -> None:
        """Unauthenticated scrapes of prometheus and alertmanager must fail.

        With ``secure_monitoring_stack`` the endpoints speak TLS and require
        basic auth. An unauthenticated HTTPS GET (verified with the cephadm
        root CA from ``get-credentials``) must return HTTP 401.
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')

        self.assert_unauthenticated_scrape_fails(
            'prometheus', '/api/v1/status/config')
        self.assert_unauthenticated_scrape_fails(
            'alertmanager', '/api/v2/status')

    def test_mgr_prometheus_exports_health_metric(self) -> None:
        """The mgr Prometheus module must export ceph_health_status over HTTPS.

        ``ceph mgr services`` publishes the module URL. With
        ``secure_monitoring_stack`` that URL is https, and ``<url>metrics``
        must include a ``ceph_health_status`` sample.
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')
        self.assert_mgr_prometheus_exports_health()

    def test_grafana_api_health(self) -> None:
        """Grafana ``/api/health`` must report ``database == "ok"`` over TLS.

        Grafana is always HTTPS with a cephadm-signed cert. ``/api/health``
        does not use Prometheus/Alertmanager basic auth. The scrape is
        verified with the CA from ``get-credentials`` (no ``curl -k``).
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')
        self.assert_grafana_api_health()

    def test_node_exporter_prometheus_target(self) -> None:
        """Prometheus must scrape node-exporter over TLS.

        Roles deploy node-exporter. ``/api/v1/targets`` must list
        ``job=node-exporter`` with ``health=up`` and
        ``scrapeUrl`` ``https://…:9100/metrics``. That is the secure-stack
        scrape, not a direct curl of ``:9100``.
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')
        self.assert_node_exporter_scraped()

    def test_node_exporter_metrics(self) -> None:
        """Each host node-exporter metrics endpoint must be responsive.

        Same check as ``test_monitoring_stack_basic.yaml`` (curl each
        host ``:9100``). With the secure stack that is
        ``https://<addr>:9100/metrics`` and ``--cacert`` (no ``-u``).
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')
        self.assert_node_exporter_metrics()

    def test_ceph_mon_down_alert(self) -> None:
        """Stopping one monitor must raise CephMonDown on the secured endpoints.

        Prometheus ``/api/v1/alerts`` must show the alert firing, and
        Alertmanager ``/api/v2/alerts`` must show it active. The monitor is
        started again, and both endpoints must drop the alert before this
        returns. Requests use HTTPS and the credentials from ``get-credentials``.
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')
        self.assert_ceph_mon_down_alert()

    def test_set_prometheus_target(self) -> None:
        """set-target must appear as a federate scrape on the secured endpoint.

        ``ceph orch prometheus set-target`` only appends ``host:port`` to
        the prometheus spec. Federate jobs in prometheus.yml are rendered
        from Dashboard ``MULTICLUSTER_CONFIG`` (``prometheus_access_info``
        for that target), not from ``spec.targets`` alone. Without that
        Dashboard store there is no ``federate_*`` job, so
        ``/api/v1/targets`` never lists it.

        This job has one cluster. A second real cluster is not required:
        we seed a fake Dashboard multi-cluster entry whose
        ``prometheus_url`` is *this* Prometheus (self-federate), then call
        set-target. That stands in for two clusters; it is not a Dashboard
        UI test.

        ``/api/v1/targets`` is parsed with Python ``json`` (no jq). Cleanup
        removes the target and restores Dashboard config.
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')
        self.assert_prometheus_set_target()

    def test_prometheus_remote_write(self) -> None:
        """set-remote-write must push samples to a fake remote Prometheus.

        A stdlib HTTP receiver stands in for another cluster. It runs on
        the Prometheus host (Teuthology may block host.b → admin:19291).
        After set-remote-write, ``GET /status`` must show
        ``post_accepted >= 1`` and ``metric_hits['ceph_health_status'] >= 1``.
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')
        self.assert_prometheus_remote_write()

    def test_custom_alert_reaches_alertmanager(self) -> None:
        """A set-custom-alerts rule must show up in Alertmanager.

        The rule expression is ``vector(1)``, so it fires once Prometheus
        reloads it. The request to Alertmanager uses HTTPS and the
        credentials from ``get-credentials``. The rule is removed afterward,
        and this waits until Alertmanager drops the alert.
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')
        self.assert_custom_alert_reaches_alertmanager()

    def test_authenticated_scrape_succeeds(self) -> None:
        """Credentials from get-credentials must succeed over TLS.

        ``ceph orch prometheus get-credentials`` and
        ``ceph orch alertmanager get-credentials`` supply the basic-auth
        user, password, and certificate. An HTTPS GET with those credentials
        (verified with that CA, no ``curl -k``) must return HTTP 200.
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')

        for daemon_type, path, creds in (
                ('prometheus', '/api/v1/status/config',
                 self.get_prometheus_credentials()),
                ('alertmanager', '/api/v2/status',
                 self.get_alertmanager_credentials())):
            user = creds.get('user') or ''
            password = creds.get('password') or ''
            self.assertTrue(
                user and password,
                f'{daemon_type} get-credentials returned no user/password')
            url = self.service_url(daemon_type, path, secure=True)
            self.assertTrue(
                url.startswith('https://'),
                f'expected an https URL for {daemon_type}, got {url}')
            log.info('authenticated scrape %s as %s', url, user)
            code, res = self.http_status(url, user=user, password=password)
            self.assertEqual(
                code, 200,
                f'expected HTTP 200 for authenticated HTTPS scrape of '
                f'{daemon_type} ({url}) as user {user!r}; '
                f'got http={code} rc={res.rc} err={res.err!r}')

    def test_set_credentials_rotates_access(self) -> None:
        """set-credentials replaces the previous basic-auth password.

        The new user and password must succeed over TLS. The previous pair
        must be rejected once the daemon has reconfigured. Original
        credentials are restored before the test returns.
        """
        self.assertTrue(
            self.secure_monitoring_enabled(),
            'suite start must enable mgr/cephadm/secure_monitoring_stack')

        targets = (
            ('prometheus', '/api/v1/status/config',
             self.get_prometheus_credentials, self.set_prometheus_credentials),
            ('alertmanager', '/api/v2/status',
             self.get_alertmanager_credentials, self.set_alertmanager_credentials),
        )
        for daemon_type, path, get_creds, set_creds in targets:
            old = get_creds()
            old_user = old.get('user') or ''
            old_password = old.get('password') or ''
            self.assertTrue(
                old_user and old_password,
                f'{daemon_type} get-credentials returned no user/password')
            new_user = 'qarotate' if old_user != 'qarotate' else 'qarotate2'
            new_password = ('qarotatepass' if old_password != 'qarotatepass'
                            else 'qarotatepass2')
            url = self.service_url(daemon_type, path, secure=True)
            log.info('rotate %s credentials %s -> %s', daemon_type, old_user, new_user)
            try:
                set_creds(new_user, new_password)
                # set-credentials updates the mgr store immediately; the
                # daemon serves the new hash only after cephadm reconfigures it.
                self.poll(
                    f'{daemon_type} accepts rotated credentials',
                    lambda url=url, user=new_user, password=new_password:
                        self.http_status(url, user=user, password=password)[0] == 200,
                    timeout=600, period=15, kick=True)
                code, res = self.http_status(
                    url, user=old_user, password=old_password)
                self.assertEqual(
                    code, 401,
                    f'old {daemon_type} credentials for {old_user!r} still '
                    f'worked ({url}); got http={code} rc={res.rc} err={res.err!r}')
                code, res = self.http_status(
                    url, user=new_user, password=new_password)
                self.assertEqual(
                    code, 200,
                    f'rotated {daemon_type} credentials for {new_user!r} failed '
                    f'({url}); got http={code} rc={res.rc} err={res.err!r}')
            finally:
                set_creds(old_user, old_password)
                self.poll(
                    f'{daemon_type} restored original credentials',
                    lambda url=url, user=old_user, password=old_password:
                        self.http_status(url, user=user, password=password)[0] == 200,
                    timeout=600, period=15, kick=True)
