import logging
import ssl

import requests
from requests.adapters import HTTPAdapter

from .mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestDashboard(MgrTestCase):
    MGRS_REQUIRED = 3

    def setUp(self):
        super(TestDashboard, self).setUp()

        self._assign_ports("dashboard", "ssl_server_port")
        self._load_module("dashboard")
        self.mgr_cluster.mon_manager.raw_cluster_cmd("dashboard",
                                                     "create-self-signed-cert")

    def tearDown(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("config", "set", "mgr",
                                                     "mgr/dashboard/standby_behaviour",
                                                     "redirect")
        self.mgr_cluster.mon_manager.raw_cluster_cmd("config", "set", "mgr",
                                                     "mgr/dashboard/standby_error_status_code",
                                                     "500")

    def wait_until_webserver_available(self, url):
        def _check_connection():
            try:
                requests.get(url, allow_redirects=False, verify=False)
                return True
            except requests.ConnectionError:
                pass
            return False
        self.wait_until_true(_check_connection, timeout=30)

    def test_standby(self):
        # skip this test if mgr_standby_modules=false
        if self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "config", "get", "mgr", "mgr_standby_modules").strip() == "false":
            log.info("Skipping test_standby since mgr_standby_modules=false")
            return

        original_active_id = self.mgr_cluster.get_active_id()
        original_uri = self._get_uri("dashboard")
        log.info("Originally running manager '{}' at {}".format(
            original_active_id, original_uri))

        # Force a failover and wait until the previously active manager
        # is listed as standby.
        self.mgr_cluster.mgr_fail(original_active_id)
        self.wait_until_true(
            lambda: original_active_id in self.mgr_cluster.get_standby_ids(),
            timeout=30)

        failed_active_id = self.mgr_cluster.get_active_id()
        failed_over_uri = self._get_uri("dashboard")
        log.info("After failover running manager '{}' at {}".format(
            failed_active_id, failed_over_uri))

        self.assertNotEqual(original_uri, failed_over_uri)

        # Wait until web server of the standby node is settled.
        self.wait_until_webserver_available(original_uri)

        # The original active daemon should have come back up as a standby
        # and be doing redirects to the new active daemon.
        r = requests.get(original_uri, allow_redirects=False, verify=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers['Location'], failed_over_uri)

        # Ensure that every URL redirects to the active daemon.
        r = requests.get("{}/runtime.js".format(original_uri.strip('/')),
                         allow_redirects=False,
                         verify=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers['Location'], failed_over_uri)

    def test_standby_disable_redirect(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("config", "set", "mgr",
                                                     "mgr/dashboard/standby_behaviour",
                                                     "error")

        original_active_id = self.mgr_cluster.get_active_id()
        original_uri = self._get_uri("dashboard")
        log.info("Originally running manager '{}' at {}".format(
            original_active_id, original_uri))

        # Force a failover and wait until the previously active manager
        # is listed as standby.
        self.mgr_cluster.mgr_fail(original_active_id)
        self.wait_until_true(
            lambda: original_active_id in self.mgr_cluster.get_standby_ids(),
            timeout=30)

        failed_active_id = self.mgr_cluster.get_active_id()
        failed_over_uri = self._get_uri("dashboard")
        log.info("After failover running manager '{}' at {}".format(
            failed_active_id, failed_over_uri))

        self.assertNotEqual(original_uri, failed_over_uri)

        # Wait until web server of the standby node is settled.
        self.wait_until_webserver_available(original_uri)

        # Redirection should be disabled now, instead a 500 must be returned.
        r = requests.get(original_uri, allow_redirects=False, verify=False)
        self.assertEqual(r.status_code, 500)

        self.mgr_cluster.mon_manager.raw_cluster_cmd("config", "set", "mgr",
                                                     "mgr/dashboard/standby_error_status_code",
                                                     "503")

        # The customized HTTP status code (503) must be returned.
        r = requests.get(original_uri, allow_redirects=False, verify=False)
        self.assertEqual(r.status_code, 503)

    def test_urls(self):
        base_uri = self._get_uri("dashboard")

        # This is a very simple smoke test to check that the dashboard can
        # give us a 200 response to requests.  We're not testing that
        # the content is correct or even renders!

        urls = [
            "/",
        ]

        failures = []

        for url in urls:
            r = requests.get(base_uri + url, allow_redirects=False,
                             verify=False)
            if r.status_code >= 300 and r.status_code < 400:
                log.error("Unexpected redirect to: {0} (from {1})".format(
                    r.headers['Location'], base_uri))
            if r.status_code != 200:
                failures.append(url)

            log.info("{0}: {1} ({2} bytes)".format(
                url, r.status_code, len(r.content)
            ))

        self.assertListEqual(failures, [])

    def test_tls(self):
        class CustomHTTPAdapter(HTTPAdapter):
            def __init__(self, ssl_version):
                self.ssl_version = ssl_version
                super().__init__()

            def init_poolmanager(self, *args, **kwargs):
                kwargs['ssl_version'] = self.ssl_version
                return super().init_poolmanager(*args, **kwargs)

        uri = self._get_uri("dashboard")

        # TLSv1
        with self.assertRaises(requests.exceptions.SSLError):
            session = requests.Session()
            session.mount(uri, CustomHTTPAdapter(ssl.PROTOCOL_TLSv1))
            session.get(uri, allow_redirects=False, verify=False)

        # TLSv1.1
        with self.assertRaises(requests.exceptions.SSLError):
            session = requests.Session()
            session.mount(uri, CustomHTTPAdapter(ssl.PROTOCOL_TLSv1_1))
            session.get(uri, allow_redirects=False, verify=False)

        session = requests.Session()
        session.mount(uri, CustomHTTPAdapter(ssl.PROTOCOL_TLS))
        r = session.get(uri, allow_redirects=False, verify=False)
        self.assertEqual(r.status_code, 200)
