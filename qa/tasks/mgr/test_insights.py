import logging
import json
import datetime
import time

from .mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)
UUID = 'd5775432-0742-44a3-a435-45095e32e6b2'
DATEFMT = '%Y-%m-%d %H:%M:%S.%f'

class TestInsights(MgrTestCase):
    def setUp(self):
        super(TestInsights, self).setUp()
        self.setup_mgrs()
        self._load_module("insights")
        self._load_module("selftest")
        self.crash_ids = []

    def tearDown(self):
        self._clear_crashes()

    def _insights(self):
        retstr = self.mgr_cluster.mon_manager.raw_cluster_cmd("insights")
        return json.loads(retstr)

    def _add_crash(self, hours, make_invalid = False):
        now = datetime.datetime.utcnow()
        timestamp = now - datetime.timedelta(hours = hours)
        timestamp = timestamp.strftime(DATEFMT) + 'Z'
        crash_id = '_'.join((timestamp, UUID)).replace(' ', '_')
        crash = {
            'crash_id': crash_id,
            'timestamp': timestamp,
        }
        if make_invalid:
            crash["timestamp"] = "not a timestamp"

        ret = self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
            'crash', 'post', '-i', '-',
            stdin=json.dumps(crash)
        )
        self.crash_ids.append(crash_id)
        self.assertEqual(0, ret)

    def _clear_crashes(self):
        for crash_id in self.crash_ids:
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                'crash', 'rm', crash_id
            )

    def _wait_for_health_history_checks(self, *args):
        """Wait for a set of health checks to appear in the health history"""
        timeout = datetime.datetime.utcnow() + \
            datetime.timedelta(seconds = 15)
        while True:
            report = self._insights()
            missing = False
            for check in args:
                if check not in report["health"]["history"]["checks"]:
                    missing = True
                    break
            if not missing:
                return
            self.assertGreater(timeout,
                    datetime.datetime.utcnow())
            time.sleep(0.25)

    def _wait_for_curr_health_cleared(self, check):
        timeout = datetime.datetime.utcnow() + \
            datetime.timedelta(seconds = 15)
        while True:
            report = self._insights()
            if check not in report["health"]["current"]["checks"]:
                return
            self.assertGreater(timeout,
                    datetime.datetime.utcnow())
            time.sleep(0.25)

    def test_health_history(self):
        # use empty health history as starting point
        self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
            "insights", "prune-health", "0")
        report = self._insights()
        self.assertFalse(report["health"]["history"]["checks"])

        # generate health check history entries. we want to avoid the edge case
        # of running these tests at _exactly_ the top of the hour so we can
        # explicitly control when hourly work occurs. for this we use the
        # current time offset to a half hour.
        now = datetime.datetime.utcnow()
        now = datetime.datetime(
            year = now.year,
            month = now.month,
            day = now.day,
            hour = now.hour,
            minute = 30)

        check_names = set()
        for hours in [-18, -11, -5, -1, 0]:
            # change the insight module's perception of "now" ...
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                "mgr", "self-test", "insights_set_now_offset", str(hours))

            # ... to simulate health check arrivals in the past
            unique_check_name = "insights_health_check_{}".format(hours)
            health_check = {
                unique_check_name: {
                    "severity": "warning",
                    "summary": "summary",
                    "detail": ["detail"]
                }
            }
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                "mgr", "self-test", "health", "set",
                json.dumps(health_check))

            check_names.add(unique_check_name)

            # and also set the same health check to test deduplication
            dupe_check_name = "insights_health_check".format(hours)
            health_check = {
                dupe_check_name: {
                    "severity": "warning",
                    "summary": "summary",
                    "detail": ["detail"]
                }
            }
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                "mgr", "self-test", "health", "set",
                json.dumps(health_check))

            check_names.add(dupe_check_name)

            # wait for the health check to show up in the history report
            self._wait_for_health_history_checks(unique_check_name, dupe_check_name)

            # clear out the current health checks before moving on
            self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
                "mgr", "self-test", "health", "clear")
            self._wait_for_curr_health_cleared(unique_check_name)

        report = self._insights()
        for check in check_names:
            self.assertIn(check, report["health"]["history"]["checks"])

        # restart the manager
        active_id = self.mgr_cluster.get_active_id()
        self.mgr_cluster.mgr_restart(active_id)

        # pruning really removes history
        self.mgr_cluster.mon_manager.raw_cluster_cmd_result(
            "insights", "prune-health", "0")
        report = self._insights()
        self.assertFalse(report["health"]["history"]["checks"])

    def test_schema(self):
        """TODO: assert conformance to a full schema specification?"""
        report = self._insights()
        for key in ["osd_metadata",
                    "pg_summary",
                    "mon_status",
                    "manager_map",
                    "service_map",
                    "mon_map",
                    "crush_map",
                    "fs_map",
                    "osd_tree",
                    "df",
                    "osd_dump",
                    "config",
                    "health",
                    "crashes",
                    "version",
                    "errors"]:
            self.assertIn(key, report)

    def test_crash_history(self):
        self._clear_crashes()
        report = self._insights()
        self.assertFalse(report["crashes"]["summary"])
        self.assertFalse(report["errors"])

        # crashes show up in the report
        self._add_crash(1)
        report = self._insights()
        self.assertTrue(report["crashes"]["summary"])
        self.assertFalse(report["errors"])
        log.warning("{}".format(json.dumps(report["crashes"], indent=2)))

        self._clear_crashes()
