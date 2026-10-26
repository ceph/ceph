import unittest
from dataclasses import fields
from unittest import mock
from urllib.parse import urlencode

from orchestrator import HardwareNoReport, OrchestratorError

from ..controllers.hardware import HardwareHosts
from ..exceptions import DashboardException
from ..model.hardware import Category, Component, HardwareHostResponse, \
    HardwareStatus, Health, MemoryComponent, NetworkComponent, PowerComponent, \
    ProcessorComponent, RawHardwareStatus, RawMemory, RawNetwork, RawPower, \
    RawProcessor, RawSensor, RawStorage, SensorComponent, StorageComponent
from ..services.hardware import HardwareService
from ..services.orchestrator import HardwareManager
from ..tests import ControllerTestCase

# mirrors NodeProxyCache.data[host] shape.
MOCK_HOST_BLOB = {
    'host': 'host1',
    'sn': 'ABC123',
    'status': {
        'storage': {
            '1': {
                'disk-0': {'status': {'health': 'OK'}},
                'disk-1': {'status': {'health': 'Critical'}},
            }
        },
        'fans': {
            '1': {
                'fan-0': {'status': {'health': 'OK'}},
                'fan-1': {'status': {'health': 'Warning'}},
            }
        },
        'processors': {'1': {'cpu-0': {'status': {'health': 'OK'}}}},
        'memory': {'1': {'dimm-0': {'status': {'health': 'OK'}}}},
        'network': {'1': {'nic-0': {'status': {'health': 'OK'}}}},
        'power': {'1': {'psu-0': {'status': {'health': 'OK'}}}},
        'temperatures': {'1': {'temp-0': {'status': {'health': 'OK'}}}},
    },
    'firmware': {
        'id-bmc': {'name': 'BMC', 'version': '2.14'},
        'id-bios': {'name': 'BIOS', 'version': '1.8.3'},
        'id-lc': {'name': 'Lifecycle Controller', 'version': 'unknown'},
    }
}

MOCK_HARDWARE_DATA = {
    'memory': {
        'host1': {
            'SystemBoard': {
                'DIMM.Socket.A1': {
                    'description': 'DIMM DDR5',
                    'status': {'health': 'OK'}
                },
                'DIMM.Socket.A2': {
                    'description': 'DIMM DDR5',
                    'status': {'health': 'OK'}
                }
            }
        },
        'host2': {
            'SystemBoard': {
                'DIMM.Socket.A1': {
                    'description': 'DIMM DDR5',
                    'status': {'health': 'OK'}
                }
            }
        }
    },
    'storage': {
        'host1': {
            'RAID.Integrated.1': {
                'Disk.Bay.0': {
                    'description': 'SSD 960GB',
                    'status': {'health': 'OK'}
                },
                'Disk.Bay.1': {
                    'description': 'SSD 960GB',
                    'status': {'health': 'Critical'}
                }
            }
        }
    },
    'processors': {
        'host1': {
            'SystemBoard': {
                'CPU.Socket.1': {
                    'description': 'Intel Xeon',
                    'status': {'health': 'OK'}
                }
            }
        }
    },
    'network': {
        'host1': {
            'SystemBoard': {
                'NIC.Slot.1': {
                    'description': 'Ethernet 25G',
                    'status': {'health': 'OK'}
                }
            }
        }
    },
    'power': {
        'host1': {
            'SystemBoard': {
                'PSU.Slot.1': {
                    'description': 'PSU 750W',
                    'status': {'health': 'OK'}
                }
            }
        }
    },
    'fans': {
        'host1': {
            'SystemBoard': {
                'Fan.Embedded.1': {
                    'description': 'System Fan',
                    'status': {'health': 'OK'}
                }
            }
        }
    }
}

MOCK_STRING_STATUS_DATA = {
    'host1': {
        'SystemBoard': {
            'DIMM.Socket.A1': {
                'description': 'DIMM DDR5',
                'status': 'OK'
            },
            'DIMM.Socket.A2': {
                'description': 'DIMM DDR5',
                'status': 'Critical'
            }
        }
    }
}

MOCK_MISSING_STATUS_DATA = {
    'host1': {
        'SystemBoard': {
            'DIMM.Socket.A1': {
                'description': 'DIMM DDR5'
            }
        }
    }
}

MOCK_NON_DICT_COMPONENT = {
    'host1': {
        'SystemBoard': {
            'DIMM.Socket.A1': 'not-a-dict'
        }
    }
}

MOCK_WARNING_STATUS_DATA = {
    'host1': {
        'SystemBoard': {
            'DIMM.Socket.A1': {
                'description': 'DIMM DDR5',
                'status': {'health': 'OK'}
            },
            'DIMM.Socket.A2': {
                'description': 'DIMM DDR5',
                'status': {'health': 'Warning'}
            },
            'DIMM.Socket.A3': {
                'description': 'DIMM DDR5',
                'status': {'health': 'Critical'}
            }
        }
    }
}


class HealthTest(unittest.TestCase):
    def test_values(self):
        self.assertEqual([h.value for h in Health], ['OK', 'Warning', 'Critical', 'Unknown'])

    def test_case_insensitive(self):
        self.assertIs(Health('critical'), Health.CRITICAL)

    def test_parse_unknown(self):
        self.assertIs(Health.parse('something-else'), Health.UNKNOWN)
        self.assertIs(Health.parse(None), Health.UNKNOWN)


class ModelConsistencyTest(unittest.TestCase):
    """The response types must match the raw node-proxy types they are built from."""

    COMPONENTS = [
        (StorageComponent, RawStorage),
        (ProcessorComponent, RawProcessor),
        (MemoryComponent, RawMemory),
        (NetworkComponent, RawNetwork),
        (PowerComponent, RawPower),
        (SensorComponent, RawSensor),
    ]

    def test_categories_match_raw_status(self):
        self.assertEqual({f.name for f in fields(HardwareStatus)},
                         set(RawHardwareStatus.__annotations__) - {'fcm'})

    def test_component_fields_exist_in_raw_types(self):
        base = {f.name for f in fields(Component)}
        for component, raw in self.COMPONENTS:
            for name in {f.name for f in fields(component)} - base:
                with self.subTest(component=component.__name__, field=name):
                    self.assertIn(name, raw.__annotations__)

    def test_worst_health(self):
        self.assertIs(Health.worst([Health.OK, Health.CRITICAL, Health.WARNING]),
                      Health.CRITICAL)
        self.assertIs(Health.worst([]), Health.UNKNOWN)


class ComponentModelTest(unittest.TestCase):
    def _component(self, raw):
        return SensorComponent.from_raw('1', 'c-0', raw)

    def test_health_from_status_dict(self):
        self.assertIs(self._component({'status': {'health': 'Warning'}}).health, Health.WARNING)

    def test_health_from_status_string(self):
        self.assertIs(self._component({'status': 'OK'}).health, Health.OK)

    def test_health_missing(self):
        self.assertIs(self._component({}).health, Health.UNKNOWN)
        self.assertIs(self._component({'status': {}}).health, Health.UNKNOWN)
        self.assertIs(self._component({'status': None}).health, Health.UNKNOWN)
        self.assertIs(self._component({'status': 'unknown'}).health, Health.UNKNOWN)

    def test_state(self):
        self.assertEqual(self._component({'status': {'state': 'Enabled'}}).state, 'Enabled')
        self.assertEqual(self._component({'status': 'OK'}).state, '')

    def test_copies_only_own_fields(self):
        fan = self._component({'id': 'Fan1A', 'name': 'Fan1A', 'reading': 4800,
                               'entity': 'x', 'slot': '3'})
        self.assertEqual((fan.id, fan.name, fan.reading, fan.reading_units),
                         ('c-0', 'Fan1A', 4800, None))
        self.assertFalse(hasattr(fan, 'entity'))
        self.assertFalse(hasattr(fan, 'slot'))

    def test_slot_prefers_top_level(self):
        self.assertEqual(StorageComponent.from_raw('1', 'd-0', {'slot': '8'}).slot, '8')

    def test_slot_falls_back_to_physical_location(self):
        raw = {'physical_location': {'partlocation': {'locationordinalvalue': 3}}}
        self.assertEqual(StorageComponent.from_raw('1', 'd-0', raw).slot, 3)
        self.assertIsNone(StorageComponent.from_raw('1', 'd-0', {}).slot)
        self.assertIsNone(
            StorageComponent.from_raw('1', 'd-0', {'physical_location': 'unknown'}).slot)


class CategoryModelTest(unittest.TestCase):
    def test_counts_and_worst_health(self):
        category = Category.from_raw(StorageComponent, MOCK_HOST_BLOB['status']['storage'])
        self.assertEqual((category.total, category.ok, category.warning, category.critical),
                         (2, 1, 0, 1))
        self.assertIs(category.health, Health.CRITICAL)

    def test_empty_is_unknown(self):
        self.assertIs(Category.from_raw(SensorComponent, {}).health, Health.UNKNOWN)

    def test_all_unknown_is_unknown(self):
        category = Category.from_raw(SensorComponent, {'1': {'f': {'status': {}}}})
        self.assertIs(category.health, Health.UNKNOWN)

    def test_non_dict_component_skipped(self):
        category = Category.from_raw(SensorComponent, {'1': {'f': 'garbage'}})
        self.assertEqual(category.total, 0)

    def test_unknown_outranks_ok(self):
        category = Category.from_raw(StorageComponent, {'1': {
            'd-0': {'status': {'health': 'OK'}},
            'd-1': {'status': {'health': 'unknown'}},
        }})
        self.assertEqual((category.total, category.ok, category.unknown), (2, 1, 1))
        self.assertIs(category.health, Health.UNKNOWN)


class HardwareHostResponseTest(unittest.TestCase):
    def test_from_raw(self):
        host = HardwareHostResponse.from_raw(MOCK_HOST_BLOB)
        self.assertEqual((host.hostname, host.sn), ('host1', 'ABC123'))
        self.assertIs(host.health, Health.CRITICAL)
        self.assertEqual(host.firmware, {'BMC': '2.14', 'BIOS': '1.8.3'})

    def test_previous_firmware_skipped(self):
        raw = {**MOCK_HOST_BLOB, 'firmware': {
            'installed-1__idrac': {'name': 'iDRAC', 'version': '7.0'},
            'previous-1__idrac': {'name': 'iDRAC', 'version': '6.1'},
        }}
        self.assertEqual(HardwareHostResponse.from_raw(raw).firmware, {'iDRAC': '7.0'})

    def test_hostname_matches_cephadm_cache_key(self):
        # cephadm stores reports under the lowercased hostname.
        raw = {**MOCK_HOST_BLOB, 'host': 'Host01'}
        self.assertEqual(HardwareHostResponse.from_raw(raw).hostname, 'host01')

    def test_legacy_firmwares_key(self):
        raw = {k: v for k, v in MOCK_HOST_BLOB.items() if k != 'firmware'}
        raw['firmwares'] = MOCK_HOST_BLOB['firmware']
        self.assertEqual(HardwareHostResponse.from_raw(raw).firmware,
                         {'BMC': '2.14', 'BIOS': '1.8.3'})

    def test_missing_required_key_raises(self):
        with self.assertRaises(KeyError):
            HardwareHostResponse.from_raw({'host': 'host1', 'status': {}})

    def test_empty_category_does_not_affect_host_health(self):
        raw = {'host': 'host1', 'sn': 'ABC123', 'status': {
            'fans': {'1': {'0': {'status': {'health': 'OK'}}}}}}
        self.assertIs(HardwareHostResponse.from_raw(raw).health, Health.OK)

    def test_unknown_component_makes_host_unknown(self):
        raw = {'host': 'host1', 'sn': 'ABC123', 'status': {
            'fans': {'1': {'0': {'status': {'health': 'OK'}},
                           '1': {'status': {'health': 'unknown'}}}}}}
        self.assertIs(HardwareHostResponse.from_raw(raw).health, Health.UNKNOWN)

    def test_to_dict_keeps_only_reported_fields(self):
        raw = {'host': 'host1', 'sn': 'ABC123', 'status': {
            'fans': {'1': {'0': {'name': 'Fan1A', 'reading': 4800, 'entity': 'x',
                                 'status': {'health': 'OK', 'state': 'Enabled'}}}},
            'storage': {'1': {'d-0': {'model': 'SSD', 'status': {'health': 'OK'}}}},
        }}
        status = HardwareHostResponse.from_raw(raw).to_dict()['status']
        self.assertEqual(status['fans']['components'], [{
            'id': '0', 'chassis_id': '1', 'health': Health.OK, 'state': 'Enabled',
            'name': 'Fan1A', 'reading': 4800}])
        self.assertEqual(status['storage']['components'], [{
            'id': 'd-0', 'chassis_id': '1', 'health': Health.OK, 'state': '',
            'model': 'SSD', 'slot': None}])


class HardwareValidateCategoriesTest(unittest.TestCase):
    def test_none_returns_all(self):
        result = HardwareService.validate_categories(None)
        self.assertEqual(
            result,
            ['memory', 'storage', 'processors', 'network',
             'power', 'fans', 'temperatures']
        )

    def test_single_string_wrapped_in_list(self):
        result = HardwareService.validate_categories('memory')
        self.assertEqual(result, ['memory'])

    def test_valid_list_passes(self):
        result = HardwareService.validate_categories(['memory', 'fans'])
        self.assertEqual(result, ['memory', 'fans'])

    def test_invalid_category_raises(self):
        with self.assertRaises(DashboardException):
            HardwareService.validate_categories(['nonexistent'])

    def test_non_list_type_raises(self):
        with self.assertRaises(DashboardException):
            HardwareService.validate_categories(123)


class HardwareGetSummaryTest(unittest.TestCase):
    def _mock_common(self, data_map):
        def side_effect(category, _hostname=None):
            return data_map.get(category, {})
        return side_effect

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_dict_status_health_ok(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(MOCK_HARDWARE_DATA)
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        cat = result['total']['category']['memory']
        self.assertEqual(cat['total'], 3)
        self.assertEqual(cat['ok'], 3)
        self.assertEqual(cat['warn'], 0)
        self.assertEqual(cat['critical'], 0)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_dict_status_health_mixed(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(MOCK_HARDWARE_DATA)
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['storage'])
        cat = result['total']['category']['storage']
        self.assertEqual(cat['total'], 2)
        self.assertEqual(cat['ok'], 1)
        self.assertEqual(cat['warn'], 0)
        self.assertEqual(cat['critical'], 1)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_string_status_format(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(
            {'memory': MOCK_STRING_STATUS_DATA}
        )
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        cat = result['total']['category']['memory']
        self.assertEqual(cat['ok'], 1)
        self.assertEqual(cat['warn'], 0)
        self.assertEqual(cat['critical'], 1)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_missing_status_not_counted_as_critical(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(
            {'memory': MOCK_MISSING_STATUS_DATA}
        )
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        cat = result['total']['category']['memory']
        self.assertEqual(cat['ok'], 0)
        self.assertEqual(cat['warn'], 0)
        self.assertEqual(cat['critical'], 0)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_non_dict_component_not_counted_as_critical(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(
            {'memory': MOCK_NON_DICT_COMPONENT}
        )
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        cat = result['total']['category']['memory']
        self.assertEqual(cat['ok'], 0)
        self.assertEqual(cat['warn'], 0)
        self.assertEqual(cat['critical'], 0)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_flawed_host_detected(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(MOCK_HARDWARE_DATA)
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['storage'])
        self.assertEqual(result['host']['flawed'], 1)
        self.assertTrue(result['host']['host1']['flawed'])

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_healthy_host_not_flawed(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(MOCK_HARDWARE_DATA)
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        self.assertEqual(result['host']['flawed'], 0)
        self.assertFalse(result['host']['host1']['flawed'])
        self.assertFalse(result['host']['host2']['flawed'])

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_all_categories_totals(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(MOCK_HARDWARE_DATA)
        mock_instance.return_value = fake_client

        cats = ['memory', 'storage', 'processors', 'network', 'power', 'fans']
        result = HardwareService.get_summary(categories=cats)

        totals = result['total']['total']
        self.assertEqual(totals['total'], 9)
        self.assertEqual(totals['ok'], 8)
        self.assertEqual(totals['warn'], 0)
        self.assertEqual(totals['critical'], 1)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_empty_data_returns_zeros(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common({'memory': {}})
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        cat = result['total']['category']['memory']
        self.assertEqual(cat['total'], 0)
        self.assertEqual(cat['ok'], 0)
        self.assertEqual(cat['warn'], 0)
        self.assertEqual(cat['critical'], 0)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_warning_status_counted_separately(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(
            {'memory': MOCK_WARNING_STATUS_DATA}
        )
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        cat = result['total']['category']['memory']
        self.assertEqual(cat['total'], 3)
        self.assertEqual(cat['ok'], 1)
        self.assertEqual(cat['warn'], 1)
        self.assertEqual(cat['critical'], 1)


class HardwareHostsControllerTest(ControllerTestCase):
    URL = '/api/hardware/hosts'

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([HardwareHosts])

    def _list(self, **params):
        self._get(f'{self.URL}?{urlencode(params)}')
        self.assertStatus(200)
        return self.json_body()

    def _make_mock_client(self, hostnames, blobs):
        """
        Build a mock OrchClient where:
          hardware.list_hosts() returns hostnames
          hardware.fullreport(h) returns blobs[h], or raises HardwareNoReport
        """
        def fullreport(hostname):
            try:
                return blobs[hostname]
            except KeyError:
                raise HardwareNoReport(f"Host '{hostname}' has no node-proxy data")

        fake_client = mock.Mock()
        fake_client.hardware.list_hosts.return_value = hostnames
        fake_client.hardware.fullreport.side_effect = fullreport
        return fake_client

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_response_shape(self, mock_instance):
        mock_instance.return_value = self._make_mock_client(
            ['host1'], {'host1': MOCK_HOST_BLOB}
        )
        result = self._list(offset=0, limit=10)
        self.assertIsInstance(result, list)
        self.assertHeader('X-Total-Count', '1')

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_host_summary_fields(self, mock_instance):
        mock_instance.return_value = self._make_mock_client(
            ['host1'], {'host1': MOCK_HOST_BLOB}
        )
        result = self._list(offset=0, limit=10)
        host = result[0]
        self.assertEqual(host['hostname'], 'host1')
        self.assertEqual(host['sn'], 'ABC123')
        self.assertEqual(set(host), {'hostname', 'sn', 'health', 'status', 'firmware'})
        self.assertEqual(set(host['status']), {'storage', 'processors', 'memory', 'network',
                                               'power', 'fans', 'temperatures'})

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_storage_counts(self, mock_instance):
        # storage has 1 OK + 1 Critical -> storage.critical=1, health=Critical
        mock_instance.return_value = self._make_mock_client(
            ['host1'], {'host1': MOCK_HOST_BLOB}
        )
        result = self._list(offset=0, limit=10)
        storage = result[0]['status']['storage']
        self.assertEqual(storage['total'], 2)
        self.assertEqual(storage['ok'], 1)
        self.assertEqual(storage['critical'], 1)
        self.assertEqual(storage['health'], Health.CRITICAL)

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_fans_counts(self, mock_instance):
        # fans has 1 OK + 1 Warning -> fans.health=Warning
        mock_instance.return_value = self._make_mock_client(
            ['host1'], {'host1': MOCK_HOST_BLOB}
        )
        result = self._list(offset=0, limit=10)
        fans = result[0]['status']['fans']
        self.assertEqual(fans['total'], 2)
        self.assertEqual(fans['ok'], 1)
        self.assertEqual(fans['warning'], 1)
        self.assertEqual(fans['health'], Health.WARNING)

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_firmware_skips_unknown_version(self, mock_instance):
        # 'Lifecycle Controller' has version='unknown' -> should be excluded
        mock_instance.return_value = self._make_mock_client(
            ['host1'], {'host1': MOCK_HOST_BLOB}
        )
        result = self._list(offset=0, limit=10)
        fw = result[0]['firmware']
        self.assertIn('BMC', fw)
        self.assertIn('BIOS', fw)
        self.assertNotIn('Lifecycle Controller', fw)
        self.assertEqual(fw['BMC'], '2.14')

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_overall_health_worst_wins(self, mock_instance):
        # storage has Critical -> overall host health must be Critical
        mock_instance.return_value = self._make_mock_client(
            ['host1'], {'host1': MOCK_HOST_BLOB}
        )
        result = self._list(offset=0, limit=10)
        self.assertEqual(result[0]['health'], Health.CRITICAL)

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_pagination_total_in_header(self, mock_instance):
        # 3 hosts, limit=2 -> X-Total-Count=3, 2 hosts returned
        blobs = {f'host{i}': {**MOCK_HOST_BLOB, 'host': f'host{i}'} for i in range(3)}
        mock_instance.return_value = self._make_mock_client(
            list(blobs.keys()), blobs
        )
        result = self._list(offset=0, limit=2)
        self.assertHeader('X-Total-Count', '3')
        self.assertEqual(len(result), 2)

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_pagination_second_page(self, mock_instance):
        # 3 hosts, offset=2 limit=2 -> 1 host on second page
        blobs = {f'host{i}': {**MOCK_HOST_BLOB, 'host': f'host{i}'} for i in range(3)}
        mock_instance.return_value = self._make_mock_client(
            list(blobs.keys()), blobs
        )
        result = self._list(offset=2, limit=2)
        self.assertEqual(len(result), 1)

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_empty_cluster(self, mock_instance):
        mock_instance.return_value = self._make_mock_client([], {})
        result = self._list(offset=0, limit=10)
        self.assertHeader('X-Total-Count', '0')
        self.assertEqual(result, [])

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_sort_by_hostname(self, mock_instance):
        # Hosts returned in sorted order regardless of list_hosts() order
        blobs = {f'host{i}': {**MOCK_HOST_BLOB, 'host': f'host{i}'} for i in range(3)}
        mock_instance.return_value = self._make_mock_client(
            ['host2', 'host0', 'host1'], blobs
        )
        result = self._list(offset=0, limit=10, sort='+hostname')
        hostnames = [h['hostname'] for h in result]
        self.assertEqual(hostnames, sorted(hostnames))

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_search_by_hostname(self, mock_instance):
        # Search filters hosts
        blobs = {'web-node-01': {**MOCK_HOST_BLOB, 'host': 'web-node-01'},
                 'osd-node-01': {**MOCK_HOST_BLOB, 'host': 'osd-node-01'}}
        mock_instance.return_value = self._make_mock_client(
            list(blobs.keys()), blobs
        )
        result = self._list(offset=0, limit=10, search='osd')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['hostname'], 'osd-node-01')

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_host_removed_after_listing_is_skipped(self, mock_instance):
        # host1 vanishes between list_hosts() and fullreport()
        blobs = {f'host{i}': {**MOCK_HOST_BLOB, 'host': f'host{i}'} for i in range(3)}
        fake_client = self._make_mock_client(list(blobs.keys()), blobs)

        del blobs['host1']
        mock_instance.return_value = fake_client
        result = self._list(offset=0, limit=10)
        self.assertEqual([h['hostname'] for h in result], ['host0', 'host2'])

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_other_orchestrator_error_is_not_skipped(self, mock_instance):
        # Only a missing report is skipped; other orchestrator failures surface.
        fake_client = self._make_mock_client(['host0'], {})
        fake_client.hardware.fullreport.side_effect = OrchestratorError('orchestrator down')
        mock_instance.return_value = fake_client
        self._get(f'{self.URL}?offset=0&limit=10')
        self.assertStatus(400)
        self.assertIn('orchestrator down', self.json_body()['detail'])

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_no_orchestrator_is_503(self, mock_instance):
        mock_instance.return_value.available.return_value = False
        self._get(f'{self.URL}?offset=0&limit=10')
        self.assertStatus(503)


class HardwareHostDetailControllerTest(ControllerTestCase):
    URL = '/api/hardware/hosts'

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([HardwareHosts])

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_detail_shape(self, mock_instance):
        mock_instance.return_value.hardware.fullreport.return_value = MOCK_HOST_BLOB
        self._get(f'{self.URL}/host1')
        self.assertStatus(200)
        detail = self.json_body()
        self.assertEqual(detail['hostname'], 'host1')
        self.assertEqual(detail['sn'], 'ABC123')
        self.assertEqual(detail['health'], Health.CRITICAL)
        self.assertEqual(set(detail), {'hostname', 'sn', 'health', 'status', 'firmware'})
        self.assertEqual(detail['firmware'], {'BMC': '2.14', 'BIOS': '1.8.3'})

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_detail_matches_list_item(self, mock_instance):
        # One representation for both endpoints.
        mock_instance.return_value.hardware.list_hosts.return_value = ['host1']
        mock_instance.return_value.hardware.fullreport.return_value = MOCK_HOST_BLOB
        self._get(f'{self.URL}/host1')
        detail = self.json_body()
        self._get(self.URL)
        self.assertEqual(self.json_body(), [detail])

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_detail_components_flattened(self, mock_instance):
        mock_instance.return_value.hardware.fullreport.return_value = MOCK_HOST_BLOB
        self._get(f'{self.URL}/host1')
        storage = self.json_body()['status']['storage']['components']
        self.assertEqual(len(storage), 2)
        self.assertEqual({d['health'] for d in storage}, {Health.OK, Health.CRITICAL})
        self.assertTrue(all('chassis_id' in d and 'id' in d for d in storage))

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_detail_missing_category_is_empty(self, mock_instance):
        blob = {**MOCK_HOST_BLOB, 'status': {'fans': MOCK_HOST_BLOB['status']['fans']}}
        mock_instance.return_value.hardware.fullreport.return_value = blob
        self._get(f'{self.URL}/host1')
        self.assertStatus(200)
        self.assertEqual(self.json_body()['status']['memory'], {
            'health': Health.UNKNOWN, 'total': 0, 'ok': 0, 'warning': 0, 'critical': 0,
            'unknown': 0, 'components': []})

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_detail_no_report_is_404(self, mock_instance):
        mock_instance.return_value.hardware.fullreport.side_effect = HardwareNoReport(
            "Host 'nope' has no node-proxy data")
        self._get(f'{self.URL}/nope')
        self.assertStatus(404)

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_detail_other_orchestrator_error_is_not_404(self, mock_instance):
        mock_instance.return_value.hardware.fullreport.side_effect = OrchestratorError(
            'orchestrator down')
        self._get(f'{self.URL}/host1')
        self.assertStatus(400)
        self.assertIn('orchestrator down', self.json_body()['detail'])

    @mock.patch('dashboard.controllers.hardware.OrchClient.instance')
    def test_detail_no_orchestrator_is_503(self, mock_instance):
        mock_instance.return_value.available.return_value = False
        self._get(f'{self.URL}/host1')
        self.assertStatus(503)


class HardwareManagerFullreportTest(unittest.TestCase):
    def test_unwraps_single_host(self):
        manager = HardwareManager(mock.Mock())
        with mock.patch.object(HardwareManager, '_fullreport',
                               return_value={'host1': MOCK_HOST_BLOB}):
            self.assertIs(manager.fullreport('host1'), MOCK_HOST_BLOB)

    def test_empty_report_raises(self):
        manager = HardwareManager(mock.Mock())
        with mock.patch.object(HardwareManager, '_fullreport', return_value={'host1': {}}):
            with self.assertRaises(HardwareNoReport):
                manager.fullreport('host1')

    def test_incomplete_report_raises(self):
        manager = HardwareManager(mock.Mock())
        for key in ('host', 'sn', 'status'):
            report = {k: v for k, v in MOCK_HOST_BLOB.items() if k != key}
            with self.subTest(missing=key), \
                    mock.patch.object(HardwareManager, '_fullreport',
                                      return_value={'host1': report}):
                with self.assertRaises(HardwareNoReport):
                    manager.fullreport('host1')


class HardwareCompressionTest(unittest.TestCase):
    GIB = 1024 ** 3

    def _fcm(self, mock_instance, phy_util, log_util, phy_size):
        drive = {'phy_util_bytes': phy_util, 'log_util_bytes': log_util,
                 'phy_size_bytes': phy_size}
        mock_instance.return_value.hardware.common.return_value = {
            'host1': {'1': {'nvme0': drive}}}

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_no_fcm_drives(self, mock_instance):
        mock_instance.return_value.hardware.common.return_value = {}
        stats = HardwareService.get_compression()
        self.assertEqual(stats['fcm_drive_count'], 0)
        self.assertIsNone(stats['compression_ratio'])

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_ratio_and_savings(self, mock_instance):
        self._fcm(mock_instance, 200 * self.GIB, 400 * self.GIB, 1000 * self.GIB)
        stats = HardwareService.get_compression()
        mock_instance.return_value.hardware.common.assert_called_once_with('fcm')
        self.assertEqual(stats, {
            'fcm_drive_count': 1, 'phy_util_percent': 20.0, 'log_util_percent': 40.0,
            'savings_bytes': 200 * self.GIB, 'compression_ratio': 2.0,
            'efficiency_percent': 50.0})

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_small_usage_hides_ratio(self, mock_instance):
        # Below 100 GiB physically used, ratio/savings are not meaningful yet.
        self._fcm(mock_instance, 10 * self.GIB, 20 * self.GIB, 1000 * self.GIB)
        stats = HardwareService.get_compression()
        self.assertEqual(stats['phy_util_percent'], 1.0)
        self.assertIsNone(stats['compression_ratio'])

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_orchestrator_error(self, mock_instance):
        mock_instance.return_value.hardware.common.side_effect = OrchestratorError('down')
        self.assertEqual(HardwareService.get_compression()['fcm_drive_count'], 0)
