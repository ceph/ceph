from typing import Dict
from unittest import TestCase, mock

from prometheus.module import Metric, LabelValues, Number, HealthHistory, ThreadSafeLRUCacheDict
import threading


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


class HealthHistoryTest(TestCase):
    def setUp(self):
        self.mgr = mock.MagicMock()
        self.mgr.get_localized_module_option.return_value = 1000
        self.mgr.get_store.return_value = "{}"
        self.health_history = HealthHistory(self.mgr)

    def test_check_save_no_deadlock(self):
        """Verifies that check() can call save() without deadlocking."""
        info = {'severity': 1}
        health_data = {
            'checks': {
                'OSD_DOWN': info
            }
        }

        def call_check():
            self.health_history.check(health_data)

        t = threading.Thread(target=call_check)
        t.start()
        t.join(timeout=2)

        self.assertFalse(t.is_alive(), "Deadlock detected: Thread is still hung!")
        self.mgr.set_store.assert_called()


class ThreadSafeLRUCacheDictTest(TestCase):
    def test_items_returns_snapshot(self):
        """items() should return a list snapshot, not a live view."""
        d = ThreadSafeLRUCacheDict(maxsize=10)
        d['a'] = 1
        d['b'] = 2
        items = d.items()
        self.assertIsInstance(items, list)
        self.assertEqual(items, [('a', 1), ('b', 2)])

    def test_keys_returns_snapshot(self):
        d = ThreadSafeLRUCacheDict(maxsize=10)
        d['a'] = 1
        keys = d.keys()
        self.assertIsInstance(keys, list)
        self.assertEqual(keys, ['a'])

    def test_values_returns_snapshot(self):
        d = ThreadSafeLRUCacheDict(maxsize=10)
        d['a'] = 1
        values = d.values()
        self.assertIsInstance(values, list)
        self.assertEqual(values, [1])

    def test_lru_eviction(self):
        d = ThreadSafeLRUCacheDict(maxsize=2)
        d['a'] = 1
        d['b'] = 2
        d['c'] = 3  # This should evict 'a'
        self.assertNotIn('a', d)
        self.assertIn('b', d)
        self.assertIn('c', d)

    def test_reentrancy(self):
        cache = ThreadSafeLRUCacheDict(maxsize=10)

        with cache._lock:
            cache['key1'] = 'value1'
            self.assertEqual(cache['key1'], 'value1')
            self.assertEqual(len(cache), 1)

    def test_concurrent_writes(self):
        cache = ThreadSafeLRUCacheDict(maxsize=100)

        def writer(start, end):
            for i in range(start, end):
                cache[f'key{i}'] = f'value{i}'

        threads = []
        for i in range(5):
            t = threading.Thread(target=writer, args=(i * 20, (i + 1) * 20))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(len(cache), 100)
        for i in range(100):
            self.assertEqual(cache[f'key{i}'], f'value{i}')


MOCK_HW_FULLREPORT = {
    'at3n2.tuc.stglabs.ibm.com': {
        'host': 'at3n2.tuc.stglabs.ibm.com',
        'sn': '11S03NK990YM30ZP58E02X',
        'status': {
            'storage': {'Self': {
                'nvme_device0_nsid1': {
                    'capacity_bytes': 512110190592,
                    'model': 'Micron_2550_MTFDKBK512TGE',
                    'protocol': 'NVMe',
                    'serial_number': '24424BAA3C40',
                    'firmware_version': 'V6MA001',
                    'slot': '0',
                    'status': {'health': 'OK', 'state': 'Enabled'},
                },
            }},
            'processors': {'Self': {
                'devtype1_cpu0': {
                    'total_cores': 48,
                    'total_threads': 96,
                    'model': 'unknown',
                    'manufacturer': 'Advanced Micro Devices, Inc.',
                    'status': {'health': 'OK', 'state': 'Enabled'},
                },
            }},
            'memory': {'Self': {
                'devtype2_dimm0': {
                    'memory_device_type': 'DDR5',
                    'capacity_mi_b': 131072,
                    'status': {'health': 'OK', 'state': 'Enabled'},
                },
            }},
            'power': {'Self': {
                '38': {
                    'name': 'PSU1_PIN',
                    'status': {'health': 'OK', 'state': 'Enabled'},
                },
                '39': {
                    'name': 'PSU1_POUT',
                    'status': {'health': 'OK', 'state': 'Enabled'},
                },
            }},
            'network': {'Self': {
                'networkinterfaces_devtype7_nic0': {
                    'name': 'NetworkAdapter_0',
                    'status': {'health': 'OK', 'state': 'Enabled'},
                },
            }},
            'fans': {'Self': {
                '0': {
                    'name': 'FAN1_TACH_IN',
                    'reading': 23940,
                    'reading_units': 'RPM',
                    'status': {'health': 'OK', 'state': 'Enabled'},
                },
            }},
            'temperatures': {'Self': {
                '0': {
                    'name': 'C1_DCSCM_TEMP',
                    'reading': 32,
                    'reading_units': 'Cel',
                    'status': {'health': 'OK', 'state': 'Enabled'},
                },
                '6': {
                    'name': 'C1_CPU_TEMP',
                    'reading': 47,
                    'reading_units': 'Cel',
                    'status': {'health': 'OK', 'state': 'Enabled'},
                },
            }},
        },
        'firmware': {
            'bios': {
                'name': 'BIOS',
                'version': 'unknown',
            },
            'bmc': {
                'name': 'BMC',
                'version': '0.50.d57cfd',
            },
            'cpld': {
                'name': 'CPLD',
                'version': 'unknown',
            },
        },
    }
}


class HardwareMetricsTest(TestCase):
    """Tests for hardware metrics processing from node-proxy fullreport data."""

    def setUp(self):
        from prometheus.module import (
            Module, HW_STORAGE_LABELS, HW_CPU_LABELS, HW_MEMORY_LABELS,
            HW_HEALTH_LABELS, HW_TEMP_LABELS, HW_FAN_LABELS, HW_FIRMWARE_LABELS,
        )
        self.module = mock.MagicMock(spec=Module)
        self.module.log = mock.MagicMock()
        self.module.metrics = {
            'hardware_storage_capacity_bytes': Metric(
                'gauge', 'hardware_storage_capacity_bytes', '', HW_STORAGE_LABELS),
            'hardware_cpu_cores': Metric(
                'gauge', 'hardware_cpu_cores', '', HW_CPU_LABELS),
            'hardware_memory_capacity_bytes': Metric(
                'gauge', 'hardware_memory_capacity_bytes', '', HW_MEMORY_LABELS),
            'hardware_health': Metric(
                'gauge', 'hardware_health', '', HW_HEALTH_LABELS),
            'hardware_temperature_celsius': Metric(
                'gauge', 'hardware_temperature_celsius', '', HW_TEMP_LABELS),
            'hardware_fan_rpm': Metric(
                'gauge', 'hardware_fan_rpm', '', HW_FAN_LABELS),
            'hardware_firmware_info': Metric(
                'gauge', 'hardware_firmware_info', '', HW_FIRMWARE_LABELS),
        }
        self.hostname = 'at3n2.tuc.stglabs.ibm.com'
        self.data = MOCK_HW_FULLREPORT[self.hostname]
        self.status = self.data['status']
        self.module._hw_get_health_value = Module._hw_get_health_value.__get__(self.module)
        self.module._hw_set_health_metric = Module._hw_set_health_metric.__get__(self.module)
        self.module._hw_iter_components = Module._hw_iter_components.__get__(self.module)
        self.module._hw_set_sensor_metric = Module._hw_set_sensor_metric.__get__(self.module)
        self.module._process_storage = Module._process_storage.__get__(self.module)
        self.module._process_processors = Module._process_processors.__get__(self.module)
        self.module._process_memory = Module._process_memory.__get__(self.module)
        self.module._process_power_network = Module._process_power_network.__get__(self.module)
        self.module._process_sensors = Module._process_sensors.__get__(self.module)
        self.module._process_firmware = Module._process_firmware.__get__(self.module)

    # --- _hw_get_health_value ---

    def test_health_value_ok(self):
        val = self.module._hw_get_health_value({'health': 'OK'})
        self.assertEqual(val, 0)

    def test_health_value_warning(self):
        val = self.module._hw_get_health_value({'health': 'Warning'})
        self.assertEqual(val, 1)

    def test_health_value_critical(self):
        val = self.module._hw_get_health_value({'health': 'Critical'})
        self.assertEqual(val, 2)

    def test_health_value_string_input(self):
        val = self.module._hw_get_health_value('OK')
        self.assertEqual(val, 0)

    def test_health_value_unknown_logs_warning(self):
        val = self.module._hw_get_health_value(
            {'health': 'Exploded'}, 'host1', 'comp1', 'storage')
        self.assertIsNone(val)
        self.module.log.warning.assert_called_once()

    def test_health_value_unknown_skips_metric(self):
        self.module._hw_set_health_metric(
            {'health': 'Exploded'}, 'host1', 'comp1', 'storage')
        self.assertEqual(self.module.metrics['hardware_health'].value, {})

    # --- _process_storage ---

    def test_storage_capacity_and_labels(self):
        self.module._process_storage(self.status, self.hostname)
        expected_labels = (
            self.hostname, 'nvme_device0_nsid1',
            'Micron_2550_MTFDKBK512TGE', 'NVMe', 'V6MA001', '0', '24424BAA3C40',
        )
        self.assertEqual(
            self.module.metrics['hardware_storage_capacity_bytes'].value[expected_labels],
            512110190592,
        )

    def test_storage_sets_health(self):
        self.module._process_storage(self.status, self.hostname)
        health_labels = (self.hostname, 'nvme_device0_nsid1', 'storage')
        self.assertEqual(
            self.module.metrics['hardware_health'].value[health_labels], 0)

    # --- _process_processors ---

    def test_cpu_cores_and_threads_label(self):
        self.module._process_processors(self.status, self.hostname)
        expected_labels = (
            self.hostname, 'devtype1_cpu0',
            'Advanced Micro Devices, Inc.', 'unknown', 96,
        )
        self.assertEqual(
            self.module.metrics['hardware_cpu_cores'].value[expected_labels], 48)

    # --- _process_memory ---

    def test_memory_mib_to_bytes_conversion(self):
        self.module._process_memory(self.status, self.hostname)
        expected_labels = (self.hostname, 'devtype2_dimm0', 'DDR5')
        self.assertEqual(
            self.module.metrics['hardware_memory_capacity_bytes'].value[expected_labels],
            131072 * 1048576,
        )

    # --- _process_power_network ---

    def test_power_uses_name_not_numeric_id(self):
        """Power components use human-readable name (PSU1_PIN) not dict key (38)."""
        self.module._process_power_network(self.status, self.hostname)
        health = self.module.metrics['hardware_health'].value
        self.assertIn((self.hostname, 'PSU1_PIN', 'power'), health)
        self.assertIn((self.hostname, 'PSU1_POUT', 'power'), health)
        self.assertNotIn((self.hostname, '38', 'power'), health)
        self.assertNotIn((self.hostname, '39', 'power'), health)

    def test_network_uses_comp_id_not_generic_name(self):
        """Network uses descriptive comp_id, not generic 'NetworkAdapter_0'."""
        self.module._process_power_network(self.status, self.hostname)
        health = self.module.metrics['hardware_health'].value
        self.assertIn(
            (self.hostname, 'networkinterfaces_devtype7_nic0', 'network'), health)
        self.assertNotIn(
            (self.hostname, 'NetworkAdapter_0', 'network'), health)

    # --- _process_sensors ---

    def test_temperature_uses_sensor_name(self):
        """Temperature reading uses 'C1_DCSCM_TEMP' not numeric key '0'."""
        self.module._process_sensors(self.status, self.hostname)
        temp = self.module.metrics['hardware_temperature_celsius'].value
        self.assertIn((self.hostname, 'C1_DCSCM_TEMP'), temp)
        self.assertEqual(temp[(self.hostname, 'C1_DCSCM_TEMP')], 32.0)
        self.assertNotIn((self.hostname, '0'), temp)

    def test_temperature_health_uses_sensor_name(self):
        """Health metric for temperatures also uses sensor name, not numeric key."""
        self.module._process_sensors(self.status, self.hostname)
        health = self.module.metrics['hardware_health'].value
        self.assertIn((self.hostname, 'C1_DCSCM_TEMP', 'temperatures'), health)
        self.assertIn((self.hostname, 'C1_CPU_TEMP', 'temperatures'), health)
        self.assertNotIn((self.hostname, '0', 'temperatures'), health)
        self.assertNotIn((self.hostname, '6', 'temperatures'), health)

    def test_fan_uses_fan_name(self):
        """Fan reading uses 'FAN1_TACH_IN' not numeric key '0'."""
        self.module._process_sensors(self.status, self.hostname)
        fan = self.module.metrics['hardware_fan_rpm'].value
        self.assertIn((self.hostname, 'FAN1_TACH_IN'), fan)
        self.assertEqual(fan[(self.hostname, 'FAN1_TACH_IN')], 23940.0)

    def test_sensor_unknown_reading_skipped(self):
        """Sensors with 'unknown' reading should not set reading metric but still set health."""
        status = {'temperatures': {'Self': {
            '99': {
                'name': 'GHOST_TEMP',
                'reading': 'unknown',
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
        }}}
        self.module._process_sensors(status, self.hostname)
        self.assertNotIn(
            (self.hostname, 'GHOST_TEMP'),
            self.module.metrics['hardware_temperature_celsius'].value)
        self.assertIn(
            (self.hostname, 'GHOST_TEMP', 'temperatures'),
            self.module.metrics['hardware_health'].value)

    # --- _process_firmware ---

    def test_firmware_known_version_exported(self):
        self.module._process_firmware(self.hostname, self.data)
        fw = self.module.metrics['hardware_firmware_info'].value
        self.assertIn((self.hostname, 'BMC', '0.50.d57cfd'), fw)
        self.assertEqual(fw[(self.hostname, 'BMC', '0.50.d57cfd')], 1)

    def test_firmware_unknown_version_skipped(self):
        """BIOS and CPLD with version='unknown' should not be exported."""
        self.module._process_firmware(self.hostname, self.data)
        fw = self.module.metrics['hardware_firmware_info'].value
        for labels in fw:
            self.assertNotEqual(labels[1], 'BIOS')
            self.assertNotEqual(labels[1], 'CPLD')

    # --- label count consistency ---

    def test_storage_label_count_matches(self):
        self.module._process_storage(self.status, self.hostname)
        for labels in self.module.metrics['hardware_storage_capacity_bytes'].value:
            self.assertEqual(len(labels), 7)

    def test_cpu_label_count_matches(self):
        self.module._process_processors(self.status, self.hostname)
        for labels in self.module.metrics['hardware_cpu_cores'].value:
            self.assertEqual(len(labels), 5)
