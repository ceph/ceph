import unittest
from unittest import mock

from ..exceptions import DashboardException
from ..services.hardware import HardwareService, STATUS_OK, STATUS_UNKNOWN


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


class HardwareConstantsTest(unittest.TestCase):
    def test_status_ok_value(self):
        self.assertEqual(STATUS_OK, 'OK')

    def test_status_unknown_value(self):
        self.assertEqual(STATUS_UNKNOWN, 'Unknown')


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
        def side_effect(category, hostname=None):
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
        self.assertEqual(cat['error'], 0)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_dict_status_health_mixed(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(MOCK_HARDWARE_DATA)
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['storage'])
        cat = result['total']['category']['storage']
        self.assertEqual(cat['total'], 2)
        self.assertEqual(cat['ok'], 1)
        self.assertEqual(cat['error'], 1)

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
        self.assertEqual(cat['error'], 1)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_missing_status_treated_as_unknown(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(
            {'memory': MOCK_MISSING_STATUS_DATA}
        )
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        cat = result['total']['category']['memory']
        self.assertEqual(cat['ok'], 0)
        self.assertEqual(cat['error'], 1)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_non_dict_component_treated_as_unknown(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common(
            {'memory': MOCK_NON_DICT_COMPONENT}
        )
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        cat = result['total']['category']['memory']
        self.assertEqual(cat['ok'], 0)
        self.assertEqual(cat['error'], 1)

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
        self.assertEqual(totals['error'], 1)

    @mock.patch('dashboard.services.hardware.OrchClient.instance')
    def test_empty_data_returns_zeros(self, mock_instance):
        fake_client = mock.Mock()
        fake_client.hardware.common = self._mock_common({'memory': {}})
        mock_instance.return_value = fake_client

        result = HardwareService.get_summary(categories=['memory'])
        cat = result['total']['category']['memory']
        self.assertEqual(cat['total'], 0)
        self.assertEqual(cat['ok'], 0)
        self.assertEqual(cat['error'], 0)
