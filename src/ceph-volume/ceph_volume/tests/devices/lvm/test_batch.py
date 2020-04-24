import pytest
import json
from ceph_volume.devices.lvm import batch


class TestBatch(object):

    def test_batch_instance(self, is_root):
        b = batch.Batch([])
        b.main()

    def test_get_devices(self, monkeypatch):
        return_value = {
            '/dev/vdd': {
                'removable': '0',
                'vendor': '0x1af4',
                'model': '',
                'sas_address': '',
                'sas_device_handle': '',
                'sectors': 0,
                'size': 21474836480.0,
                'support_discard': '',
                'partitions': {
                    'vdd1': {
                        'start': '2048',
                        'sectors': '41940959',
                        'sectorsize': 512,
                        'size': '20.00 GB'
                    }
                },
                'rotational': '1',
                'scheduler_mode': 'mq-deadline',
                'sectorsize': '512',
                'human_readable_size': '20.00 GB',
                'path': '/dev/vdd'
            },
            '/dev/vdf': {
                'removable': '0',
                'vendor': '0x1af4',
                'model': '',
                'sas_address': '',
                'sas_device_handle': '',
                'sectors': 0,
                'size': 21474836480.0,
                'support_discard': '',
                'partitions': {},
                'rotational': '1',
                'scheduler_mode': 'mq-deadline',
                'sectorsize': '512',
                'human_readable_size': '20.00 GB',
                'path': '/dev/vdf'
            }
        }
        monkeypatch.setattr('ceph_volume.devices.lvm.batch.disk.get_devices',
                            lambda: return_value)
        b = batch.Batch([])
        result = b.get_devices().strip()
        assert result == '* /dev/vdf                  20.00 GB   rotational'

    def test_disjoint_device_lists(self, factory):
        device1 = factory(used_by_ceph=False, available=True, abspath="/dev/sda")
        device2 = factory(used_by_ceph=False, available=True, abspath="/dev/sdb")
        b = batch.Batch([])
        b.args.devices = [device1, device2]
        b.args.db_devices = [device2]
        b._filter_devices()
        with pytest.raises(Exception) as disjoint_ex:
            b._ensure_disjoint_device_lists()
        assert 'Device lists are not disjoint' in str(disjoint_ex.value)


class TestFilterDevices(object):

    def test_filter_used_device(self, factory):
        device1 = factory(used_by_ceph=True, abspath="/dev/sda")
        args = factory(devices=[device1], filtered_devices={})
        result, filtered_devices = batch.filter_devices(args)
        assert not result
        assert device1.abspath in filtered_devices

    def test_has_unused_devices(self, factory):
        device1 = factory(
            used_by_ceph=False,
            abspath="/dev/sda",
            rotational=False,
            is_lvm_member=False
        )
        args = factory(devices=[device1], filtered_devices={})
        result, filtered_devices = batch.filter_devices(args)
        assert device1 in result
        assert not filtered_devices

    def test_filter_device_used_as_a_journal(self, factory):
        hdd1 = factory(
            used_by_ceph=True,
            abspath="/dev/sda",
            rotational=True,
            is_lvm_member=True,
        )
        lv = factory(tags={"ceph.type": "journal"})
        ssd1 = factory(
            used_by_ceph=False,
            abspath="/dev/nvme0n1",
            rotational=False,
            is_lvm_member=True,
            lvs=[lv],
        )
        args = factory(devices=[hdd1, ssd1], filtered_devices={})
        result, filtered_devices = batch.filter_devices(args)
        assert not result
        assert ssd1.abspath in filtered_devices

    def test_last_device_is_not_filtered(self, factory):
        hdd1 = factory(
            used_by_ceph=True,
            abspath="/dev/sda",
            rotational=True,
            is_lvm_member=True,
        )
        ssd1 = factory(
            used_by_ceph=False,
            abspath="/dev/nvme0n1",
            rotational=False,
            is_lvm_member=False,
        )
        args = factory(devices=[hdd1, ssd1], filtered_devices={})
        result, filtered_devices = batch.filter_devices(args)
        assert result
        assert len(filtered_devices) == 1

    def test_no_auto_fails_on_unavailable_device(self, factory):
        hdd1 = factory(
            used_by_ceph=False,
            abspath="/dev/sda",
            rotational=True,
            is_lvm_member=False,
            available=True,
        )
        ssd1 = factory(
            used_by_ceph=True,
            abspath="/dev/nvme0n1",
            rotational=False,
            is_lvm_member=True,
            available=False
        )
        args = factory(devices=[hdd1], db_devices=[ssd1], filtered_devices={},
                      yes=True, format="", report=False)
        b = batch.Batch([])
        b.args = args
        with pytest.raises(RuntimeError) as ex:
            b._filter_devices()
            assert '1 devices were filtered in non-interactive mode, bailing out' in str(ex.value)

    def test_no_auto_prints_json_on_unavailable_device_and_report(self, factory, capsys):
        hdd1 = factory(
            used_by_ceph=False,
            abspath="/dev/sda",
            rotational=True,
            is_lvm_member=False,
            available=True,
        )
        ssd1 = factory(
            used_by_ceph=True,
            abspath="/dev/nvme0n1",
            rotational=False,
            is_lvm_member=True,
            available=False
        )
        captured = capsys.readouterr()
        args = factory(devices=[hdd1], db_devices=[ssd1], filtered_devices={},
                      yes=True, format="json", report=True)
        b = batch.Batch([])
        b.args = args
        with pytest.raises(SystemExit):
            b._filter_devices()
            result = json.loads(captured.out)
            assert not result["changed"]
