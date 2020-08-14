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

@pytest.fixture
def device_data(device_info):
    device_info(
        devices={
            # example output of disk.get_devices()
            '/dev/sdb': {
                'human_readable_size': '1.82 TB',
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
            }
        }
    )

    dev = Devices().devices[0]
    dev.lsm_data = {
        "serialNum": 'S2X9NX0H935283',
        "transport": 'SAS',
        "mediaType": 'HDD',
        "rpm": 10000,
        "linkSpeed": 6000,
        "health": 'Good',
        "ledSupport": {
            "IDENTsupport": 'Supported',
            "IDENTstatus": 'Off',
            "FAILsupport": 'Supported',
            "FAILstatus": 'Off',
        },
        "errors": [],
    }
    return dev.json_report()


class TestInventory(object):

    expected_keys = [
        'path',
        'rejected_reasons',
        'sys_api',
        'available',
        'lvs',
        'device_id',
        'lsm_data',
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

    expected_lsm_keys = [
        'serialNum',
        'transport',
        'mediaType',
        'rpm',
        'linkSpeed',
        'health',
        'ledSupport',
        'errors',
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

    def test_lsm_data_type_unexpected(self, device_data):
        assert isinstance(device_data['lsm_data'], dict), "lsm_data field must be of type dict"

    def test_lsm_data_keys_unexpected(self, device_data):
        for k in device_data['lsm_data'].keys():
            assert k in self.expected_lsm_keys, "unexpected key {} in lsm_data field".format(k)

    def test_lsm_data_keys_missing(self, device_data):
        lsm_keys = device_data['lsm_data'].keys()
        assert lsm_keys
        for k in self.expected_lsm_keys:
            assert k in lsm_keys, "expected key {} in lsm_data field".format(k)

