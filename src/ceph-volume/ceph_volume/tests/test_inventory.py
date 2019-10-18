# -*- coding: utf-8 -*-

import pytest
from ceph_volume.util.device import Devices


@pytest.fixture
def device_report_keys(device_info):
    device_info(devices={
        # example output of disk.get_devices()
        '/dev/sdb': {'human_readable_size': '1.82 TB',
                     'locked': 0,
                     'model': 'PERC H700',
                     'nr_requests': '128',
                     'partitions': {},
                     'path': '/dev/sdb',
                     'removable': '0',
                     'rev': '2.10',
                     'ro': '0',
                     'rotational': '1',
                     'sas_address': '',
                     'sas_device_handle': '',
                     'scheduler_mode': 'cfq',
                     'sectors': 0,
                     'sectorsize': '512',
                     'size': 1999844147200.0,
                     'support_discard': '',
                     'vendor': 'DELL',
                     'device_id': 'Vendor-Model-Serial'}
    }
 )
    report = Devices().json_report()[0]
    return list(report.keys())

@pytest.fixture
def device_sys_api_keys(device_info):
    device_info(devices={
        # example output of disk.get_devices()
        '/dev/sdb': {'human_readable_size': '1.82 TB',
                     'locked': 0,
                     'model': 'PERC H700',
                     'nr_requests': '128',
                     'partitions': {},
                     'path': '/dev/sdb',
                     'removable': '0',
                     'rev': '2.10',
                     'ro': '0',
                     'rotational': '1',
                     'sas_address': '',
                     'sas_device_handle': '',
                     'scheduler_mode': 'cfq',
                     'sectors': 0,
                     'sectorsize': '512',
                     'size': 1999844147200.0,
                     'support_discard': '',
                     'vendor': 'DELL'}
    }
 )
    report = Devices().json_report()[0]
    return list(report['sys_api'].keys())


class TestInventory(object):

    expected_keys = [
        'path',
        'rejected_reasons',
        'sys_api',
        'available',
        'lvs',
        'device_id',
    ]

    expected_sys_api_keys = [
        'human_readable_size',
        'locked',
        'model',
        'nr_requests',
        'partitions',
        'path',
        'removable',
        'rev',
        'ro',
        'rotational',
        'sas_address',
        'sas_device_handle',
        'scheduler_mode',
        'sectors',
        'sectorsize',
        'size',
        'support_discard',
        'vendor',
    ]

    def test_json_inventory_keys_unexpected(self, device_report_keys):
        for k in device_report_keys:
            assert k in self.expected_keys, "unexpected key {} in report".format(k)

    def test_json_inventory_keys_missing(self, device_report_keys):
        for k in self.expected_keys:
            assert k in device_report_keys, "expected key {} in report".format(k)

    def test_sys_api_keys_unexpected(self, device_sys_api_keys):
        for k in device_sys_api_keys:
            assert k in self.expected_sys_api_keys, "unexpected key {} in sys_api field".format(k)

    def test_sys_api_keys_missing(self, device_sys_api_keys):
        for k in self.expected_sys_api_keys:
            assert k in device_sys_api_keys, "expected key {} in sys_api field".format(k)

