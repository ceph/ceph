

from mgr_test_case import MgrTestCase

import logging
import requests


log = logging.getLogger(__name__)


class TestPrometheus(MgrTestCase):
    MGRS_REQUIRED = 3

    def setUp(self):
        self.setup_mgrs()

    def test_standby(self):
        self._assign_ports("prometheus", "server_port")
        self._load_module("prometheus")

        original_active = self.mgr_cluster.get_active_id()

        original_uri = self._get_uri("prometheus")
        log.info("Originally running at {0}".format(original_uri))

        self.mgr_cluster.mgr_fail(original_active)

        failed_over_uri = self._get_uri("prometheus")
        log.info("After failover running at {0}".format(failed_over_uri))

        self.assertNotEqual(original_uri, failed_over_uri)

        # The original active daemon should have come back up as a standby
        # and serve some html under "/" and an empty answer under /metrics
        r = requests.get(original_uri, allow_redirects=False)
        self.assertEqual(r.status_code, 200)
        r = requests.get(original_uri + "metrics", allow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers["content-type"], "text/plain;charset=utf-8")

    def test_urls(self):
        self._assign_ports("prometheus", "server_port")
        self._load_module("prometheus")

        base_uri = self._get_uri("prometheus")

        # This is a very simple smoke test to check that the module can
        # give us a 200 response to requests.  We're not testing that
        # the content is correct or even renders!

        urls = [
            "/",
            "/metrics"
        ]

        failures = []

        for url in urls:
            r = requests.get(base_uri + url, allow_redirects=False)
            if r.status_code != 200:
                failures.append(url)

            log.info("{0}: {1} ({2} bytes)".format(
                url, r.status_code, len(r.content)
            ))

        self.assertListEqual(failures, [])
