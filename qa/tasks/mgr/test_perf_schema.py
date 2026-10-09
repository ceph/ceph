import json

from .mgr_test_case import MgrTestCase


class TestPerfSchema(MgrTestCase):
    MGRS_REQUIRED = 1

    def setUp(self):
        super().setUp()
        self.setup_mgrs()
        self._load_module("selftest")

    def _get_schema_data(self):
        """Call the selftest command and return the parsed JSON payload."""
        raw = self.mgr_cluster.mon_manager.raw_cluster_cmd(
            "mgr", "self-test", "perf-schema-test")
        return json.loads(raw)

    def test_unlabeled_schema_structure(self):
        """Test that get_unlabeled_perf_schema('osd', '0') returns a non-empty dict keyed by 'osd.0'."""
        unlabeled = self._get_schema_data()["unlabeled"]

        self.assertIsInstance(unlabeled, dict)
        self.assertIn("osd.0", unlabeled)
        svc_schema = unlabeled["osd.0"]
        self.assertIsInstance(svc_schema, dict)
        self.assertGreater(len(svc_schema), 0)

        for counter_name, counter_info in svc_schema.items():
            self.assertIsInstance(counter_info, dict,
                                  "counter info for '{}' must be a dict".format(counter_name))

    def test_labeled_schema_structure(self):
        """Test that get_perf_schema('mon', '<id>') returns lists of {labels, counters} entries for every live mon."""
        labeled = self._get_schema_data()["labeled"]

        self.assertIsInstance(labeled, dict)
        self.assertGreater(len(labeled), 0)

        for full_name, daemon_schema in labeled.items():
            self.assertTrue(full_name.startswith("mon."),
                            "labeled schema key '{}' must start with 'mon.'".format(full_name))
            self.assertIsInstance(daemon_schema, dict)

            for group_name, entries in daemon_schema.items():
                self.assertIsInstance(entries, list,
                                      "group '{}' in '{}' must be a list".format(group_name, full_name))
                self.assertGreater(len(entries), 0)

                for entry in entries:
                    self.assertIsInstance(entry, dict)
                    self.assertIn("labels", entry)
                    self.assertIn("counters", entry)
                    self.assertIsInstance(entry["labels"], dict)
                    self.assertIsInstance(entry["counters"], dict)

                    for cname, cinfo in entry["counters"].items():
                        self.assertIsInstance(cinfo, dict)

    def test_unlabeled_wildcard(self):
        """Test that get_unlabeled_perf_schema('', '') returns schema for every registered daemon including osd.0."""
        wildcard = self._get_schema_data()["unlabeled_wildcard"]

        self.assertIsInstance(wildcard, dict)
        self.assertGreater(len(wildcard), 0)
        self.assertIn("osd.0", wildcard)
        for svc_key, svc_schema in wildcard.items():
            self.assertIsInstance(svc_schema, dict,
                                  "schema for '{}' must be a dict".format(svc_key))

    def test_unlabeled_by_service(self):
        """Test that get_unlabeled_perf_schema('osd', '') returns schema for all OSDs and nothing else."""
        by_svc = self._get_schema_data()["unlabeled_by_service"]

        self.assertIsInstance(by_svc, dict)
        self.assertGreater(len(by_svc), 0)
        for svc_key, svc_schema in by_svc.items():
            self.assertTrue(svc_key.startswith("osd."),
                            "by-service key '{}' must start with 'osd.'".format(svc_key))
            self.assertIsInstance(svc_schema, dict)

    def test_labeled_wildcard(self):
        """Test that get_perf_schema('', '') returns a non-empty dict covering all daemons."""
        wildcard = self._get_schema_data()["labeled_wildcard"]

        self.assertIsInstance(wildcard, dict)
        self.assertGreater(len(wildcard), 0)

    def test_labeled_by_service(self):
        """Test that get_perf_schema('mon', '') returns schema only for mon daemons."""
        by_svc = self._get_schema_data()["labeled_by_service"]

        self.assertIsInstance(by_svc, dict)
        self.assertGreater(len(by_svc), 0)
        for svc_key in by_svc:
            self.assertTrue(svc_key.startswith("mon."),
                            "by-service labeled key '{}' must start with 'mon.'".format(svc_key))

    def test_empty_daemon_state(self):
        """Test that both schema functions return an empty dict for a non-existent service type."""
        empty_unlabeled = json.loads(
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "dump-unlabeled-perf-schema",
                "__noexist__", ""))
        self.assertIsInstance(empty_unlabeled, dict)
        self.assertEqual(len(empty_unlabeled), 0,
                         "get_unlabeled_perf_schema for unknown type must return empty dict")

        empty_labeled = json.loads(
            self.mgr_cluster.mon_manager.raw_cluster_cmd(
                "mgr", "self-test", "dump-perf-schema",
                "__noexist__", ""))
        self.assertIsInstance(empty_labeled, dict)
        self.assertEqual(len(empty_labeled), 0,
                         "get_perf_schema for unknown type must return empty dict")
