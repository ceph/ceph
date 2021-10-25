from typing import Dict
from unittest import TestCase

from prometheus.module import Metric, LabelValues, Number


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
