

from mgr_test_case import MgrTestCase

import logging
import requests


log = logging.getLogger(__name__)


class TestDashboard(MgrTestCase):
    MGRS_REQUIRED = 3

    def setUp(self):
        super(TestDashboard, self).setUp()

        self._assign_ports("dashboard", "ssl_server_port")
        self._load_module("dashboard")
        self.mgr_cluster.mon_manager.raw_cluster_cmd("dashboard",
                                                     "create-self-signed-cert")

    def test_standby(self):
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

        # The original active daemon should have come back up as a standby
        # and be doing redirects to the new active daemon
        r = requests.get(original_uri, allow_redirects=False, verify=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers['Location'], failed_over_uri)

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
