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
