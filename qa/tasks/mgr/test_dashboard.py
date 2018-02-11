

from mgr_test_case import MgrTestCase

import logging
import requests


log = logging.getLogger(__name__)


class TestDashboard(MgrTestCase):
    MGRS_REQUIRED = 3

    def test_standby(self):
        self._assign_ports("dashboard", "server_port")
        self._load_module("dashboard")

        original_active = self.mgr_cluster.get_active_id()

        original_uri = self._get_uri("dashboard")
        log.info("Originally running at {0}".format(original_uri))

        self.mgr_cluster.mgr_fail(original_active)

        failed_over_uri = self._get_uri("dashboard")
        log.info("After failover running at {0}".format(failed_over_uri))

        self.assertNotEqual(original_uri, failed_over_uri)

        # The original active daemon should have come back up as a standby
        # and be doing redirects to the new active daemon
        r = requests.get(original_uri, allow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers['Location'], failed_over_uri)

    def test_urls(self):
        self._assign_ports("dashboard", "server_port")
        self._load_module("dashboard")

        base_uri = self._get_uri("dashboard")

        # This is a very simple smoke test to check that the dashboard can
        # give us a 200 response to requests.  We're not testing that
        # the content is correct or even renders!

        urls = [
            "/health",
            "/servers",
            "/osd/",
            "/osd/perf/0",
            "/rbd_mirroring",
            "/rbd_iscsi"
        ]

        failures = []

        for url in urls:
            r = requests.get(base_uri + url, allow_redirects=False)
            if r.status_code >= 300 and r.status_code < 400:
                log.error("Unexpected redirect to: {0} (from {1})".format(
                    r.headers['Location'], base_uri))
            if r.status_code != 200:
                failures.append(url)

            log.info("{0}: {1} ({2} bytes)".format(
                url, r.status_code, len(r.content)
            ))

        self.assertListEqual(failures, [])
