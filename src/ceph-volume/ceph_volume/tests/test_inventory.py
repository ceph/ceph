# -*- coding: utf-8 -*-

import pytest
from ceph_volume.util.device import Devices
from ceph_volume.util.lsmdisk import LSMDisk
from mock.mock import patch
import ceph_volume.util.lsmdisk as lsmdisk


@pytest.fixture
@patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
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
                     'device_id': 'Vendor-Model-Serial',
                     'device_nodes': 'sdb'}
    }
 )
    report = Devices().json_report()[0]
    return list(report.keys())

@pytest.fixture
@patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
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
                     'vendor': 'DELL',
                     'device_nodes': 'sdb'}
    }
 )
    report = Devices().json_report()[0]
    return list(report['sys_api'].keys())

@pytest.fixture
@patch("ceph_volume.util.disk.has_bluestore_label", lambda x: False)
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
                'device_nodes': 'sdb'
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
        'ceph_device_lvm',
        'path',
        'rejected_reasons',
        'sys_api',
        'available',
        'lvs',
        'device_id',
        'lsm_data',
        'being_replaced'
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
        'device_nodes'
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

    def test_json_inventory_keys_unexpected(self, fake_call, device_report_keys):
        for k in device_report_keys:
            assert k in self.expected_keys, "unexpected key {} in report".format(k)

    def test_json_inventory_keys_missing(self, fake_call, device_report_keys):
        for k in self.expected_keys:
            assert k in device_report_keys, "expected key {} in report".format(k)

    def test_sys_api_keys_unexpected(self, fake_call, device_sys_api_keys):
        for k in device_sys_api_keys:
            assert k in self.expected_sys_api_keys, "unexpected key {} in sys_api field".format(k)

    def test_sys_api_keys_missing(self, fake_call, device_sys_api_keys):
        for k in self.expected_sys_api_keys:
            assert k in device_sys_api_keys, "expected key {} in sys_api field".format(k)

    def test_lsm_data_type_unexpected(self, fake_call, device_data):
        assert isinstance(device_data['lsm_data'], dict), "lsm_data field must be of type dict"

    def test_lsm_data_keys_unexpected(self, fake_call, device_data):
        for k in device_data['lsm_data'].keys():
            assert k in self.expected_lsm_keys, "unexpected key {} in lsm_data field".format(k)

    def test_lsm_data_keys_missing(self, fake_call, device_data):
        lsm_keys = device_data['lsm_data'].keys()
        assert lsm_keys
        for k in self.expected_lsm_keys:
            assert k in lsm_keys, "expected key {} in lsm_data field".format(k)


@pytest.fixture
def lsm_info(monkeypatch):
    def mock_query_lsm(_, func, path):
        query_map = {
            'serial_num_get': "S2X9NX0H935283",
            'link_type_get': 6,
            'rpm_get': 0,
            'link_speed_get': 6000,
            'health_status_get': 2,
            'led_status_get': 36,
        }
        return query_map.get(func, 'Unknown')

    # mocked states and settings taken from the libstoragemgmt code base
    # c_binding/include/libstoragemgmt/libstoragemgmt_types.h at
    # https://github.com/libstorage/libstoragemgmt/
    mock_health_map = {
            -1: "Unknown",
            0: "Fail",
            1: "Warn",
            2: "Good",
    }
    mock_transport_map = {
            -1: "Unavailable",
            0: "Fibre Channel",
            2: "IBM SSA",
            3: "Serial Bus",
            4: "SCSI RDMA",
            5: "iSCSI",
            6: "SAS",
            7: "ADT (Tape)",
            8: "ATA/SATA",
            9: "USB",
            10: "SCSI over PCI-E",
            11: "PCI-E",
    }
    class MockLEDStates():
        LED_STATUS_UNKNOWN = 1
        LED_STATUS_IDENT_ON = 2
        LED_STATUS_IDENT_OFF = 4
        LED_STATUS_IDENT_UNKNOWN = 8
        LED_STATUS_FAULT_ON = 16
        LED_STATUS_FAULT_OFF = 32
        LED_STATUS_FAULT_UNKNOWN = 64

    monkeypatch.setattr(LSMDisk, '_query_lsm', mock_query_lsm)
    monkeypatch.setattr(lsmdisk, 'health_map', mock_health_map)
    monkeypatch.setattr(lsmdisk, 'transport_map', mock_transport_map)
    monkeypatch.setattr(lsmdisk, 'lsm_Disk', MockLEDStates)

    return LSMDisk('/dev/sda')


class TestLSM(object):
    def test_lsmdisk_health(self, lsm_info):
        assert lsm_info.health == "Good"
    def test_lsmdisk_transport(self, lsm_info):
        assert lsm_info.transport == 'SAS'
    def test_lsmdisk_mediatype(self, lsm_info):
        assert lsm_info.media_type == 'Flash'
    def test_lsmdisk_led_ident_support(self, lsm_info):
        assert lsm_info.led_ident_support == 'Supported'
    def test_lsmdisk_led_ident(self, lsm_info):
        assert lsm_info.led_ident_state == 'Off'
    def test_lsmdisk_led_fault_support(self, lsm_info):
        assert lsm_info.led_fault_support == 'Supported'
    def test_lsmdisk_led_fault(self, lsm_info):
        assert lsm_info.led_fault_state == 'Off'
    def test_lsmdisk_report(self, lsm_info):
        assert isinstance(lsm_info.json_report(), dict)
