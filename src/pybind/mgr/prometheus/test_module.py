from typing import Dict
from unittest import TestCase
from unittest.mock import Mock, patch

from prometheus.module import Metric, LabelValues, Number, Module


class MetricGroupTest(TestCase):
    def setUp(self):
        self.DISK_OCCUPATION = (
            "ceph_daemon",
            "device",
            "db_device",
            "wal_device",
            "instance",
        )
        self.metric_value: Dict[LabelValues, Number] = {
            ("osd.0", "/dev/dm-0", "", "", "node1"): 1,
            ("osd.1", "/dev/dm-0", "", "", "node3"): 1,
            ("osd.2", "/dev/dm-0", "", "", "node2"): 1,
            ("osd.3", "/dev/dm-1", "", "", "node1"): 1,
            ("osd.4", "/dev/dm-1", "", "", "node3"): 1,
            ("osd.5", "/dev/dm-1", "", "", "node2"): 1,
            ("osd.6", "/dev/dm-1", "", "", "node2"): 1,
        }

    def test_metric_group_by(self):
        m = Metric("untyped", "disk_occupation", "", self.DISK_OCCUPATION)
        m.value = self.metric_value
        grouped_metric = m.group_by(
            ["device", "instance"],
            {"ceph_daemon": lambda xs: "+".join(xs)},
            name="disk_occupation_display",
        )
        self.assertEqual(
            grouped_metric.value,
            {
                ("osd.0", "/dev/dm-0", "node1"): 1,
                ("osd.1", "/dev/dm-0", "node3"): 1,
                ("osd.2", "/dev/dm-0", "node2"): 1,
                ("osd.3", "/dev/dm-1", "node1"): 1,
                ("osd.4", "/dev/dm-1", "node3"): 1,
                ("osd.5+osd.6", "/dev/dm-1", "node2"): 1,
            },
        )
        self.maxDiff = None
        self.assertEqual(
            grouped_metric.str_expfmt(),
            """
# HELP ceph_disk_occupation_display
# TYPE ceph_disk_occupation_display untyped
ceph_disk_occupation_display{ceph_daemon="osd.0",device="/dev/dm-0",instance="node1"} 1.0
ceph_disk_occupation_display{ceph_daemon="osd.1",device="/dev/dm-0",instance="node3"} 1.0
ceph_disk_occupation_display{ceph_daemon="osd.2",device="/dev/dm-0",instance="node2"} 1.0
ceph_disk_occupation_display{ceph_daemon="osd.3",device="/dev/dm-1",instance="node1"} 1.0
ceph_disk_occupation_display{ceph_daemon="osd.4",device="/dev/dm-1",instance="node3"} 1.0
ceph_disk_occupation_display{ceph_daemon="osd.5+osd.6",device="/dev/dm-1",instance="node2"} 1.0""",  # noqa: W291
        )
        self.assertEqual(
            grouped_metric.labelnames, ("ceph_daemon", "device", "instance")
        )

    def test_metric_group_by__no_value(self):
        m = Metric("metric_type", "name", "desc", labels=('foo', 'bar'))
        grouped = m.group_by(['foo'], {'bar': lambda bars: ', '.join(bars)})
        self.assertEqual(grouped.value, {})
        self.assertEqual(grouped.str_expfmt(),
                         '\n# HELP ceph_name desc\n# TYPE ceph_name metric_type')

    def test_metric_group_by__no_labels(self):
        m = Metric("metric_type", "name", "desc", labels=None)
        with self.assertRaises(AssertionError) as cm:
            m.group_by([], {})
        self.assertEqual(str(cm.exception), "cannot match keys without label names")

    def test_metric_group_by__key_not_in_labels(self):
        m = Metric("metric_type", "name", "desc", labels=("foo", "bar"))
        m.value = self.metric_value
        with self.assertRaises(AssertionError) as cm:
            m.group_by(["baz"], {})
        self.assertEqual(str(cm.exception), "unknown key: baz")

    def test_metric_group_by__empty_joins(self):
        m = Metric("", "", "", ("foo", "bar"))
        with self.assertRaises(AssertionError) as cm:
            m.group_by(["foo"], joins={})
        self.assertEqual(str(cm.exception), "joins must not be empty")

    def test_metric_group_by__joins_not_callable(self):
        m = Metric("", "", "", ("foo", "bar"))
        m.value = self.metric_value
        with self.assertRaises(AssertionError) as cm:
            m.group_by(["foo"], {"bar": "not callable str"})
        self.assertEqual(str(cm.exception), "joins must be callable")


class ConfigureTest(TestCase):
    @patch('prometheus.module.json.loads')
    def test_configure_with_empty_security_config_no_json_parse(self, mock_json_loads):
        """Test that json.loads is NOT called when output is empty string"""
        module = Mock(spec=Module)
        module.log = Mock()

        # Mock mon_command to return empty string (the problematic case)
        module.mon_command = Mock(return_value=(0, "", ""))

        # Mock setup_default_config
        module.setup_default_config = Mock()

        # Call the actual configure method with our mocks
        Module.configure(module, "127.0.0.1", 9283)

        # Verify that json.loads was NOT called (this is the fix!)
        mock_json_loads.assert_not_called()

        # Verify that setup_default_config was called (fallback behavior)
        module.setup_default_config.assert_called_once_with("127.0.0.1", 9283)

        # Verify no exception was raised and log was not called with exception
        module.log.exception.assert_not_called()

    def test_configure_with_none_security_config(self):
        """Test that configure handles None from orch get-security-config"""
        module = Mock(spec=Module)
        module.log = Mock()

        # Mock mon_command to return None
        module.mon_command = Mock(return_value=(0, None, ""))

        # Mock setup_default_config
        module.setup_default_config = Mock()

        # Call the actual configure method with our mocks
        Module.configure(module, "127.0.0.1", 9283)

        # Verify that setup_default_config was called (fallback behavior)
        module.setup_default_config.assert_called_once_with("127.0.0.1", 9283)

        # Verify no exception was raised
        module.log.exception.assert_not_called()

    def test_configure_with_valid_security_config_disabled(self):
        """Test that configure handles valid JSON with security disabled"""
        module = Mock(spec=Module)
        module.log = Mock()

        # Mock mon_command to return valid JSON with security_enabled: false
        module.mon_command = Mock(return_value=(0, '{"security_enabled": false}', ""))

        # Mock setup_default_config
        module.setup_default_config = Mock()

        # Call the actual configure method with our mocks
        Module.configure(module, "127.0.0.1", 9283)

        # Verify that setup_default_config was called (security disabled)
        module.setup_default_config.assert_called_once_with("127.0.0.1", 9283)

        # Verify no exception was raised
        module.log.exception.assert_not_called()
