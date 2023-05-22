import pytest
import json
import random

from argparse import ArgumentError
from mock import MagicMock, patch

from ceph_volume.api import lvm
from ceph_volume.devices.lvm import batch
from ceph_volume.util import arg_validators


class TestBatch(object):

    def test_batch_instance(self, is_root):
        b = batch.Batch([])
        b.main()

    def test_invalid_osd_ids_passed(self):
        with pytest.raises(SystemExit):
            batch.Batch(argv=['--osd-ids', '1', 'foo']).main()

    def test_disjoint_device_lists(self, factory):
        device1 = factory(used_by_ceph=False, available=True, abspath="/dev/sda")
        device2 = factory(used_by_ceph=False, available=True, abspath="/dev/sdb")
        devices = [device1, device2]
        db_devices = [device2]
        with pytest.raises(Exception) as disjoint_ex:
            batch.ensure_disjoint_device_lists(devices, db_devices)
        assert 'Device lists are not disjoint' in str(disjoint_ex.value)

    @patch('ceph_volume.util.arg_validators.Device')
    def test_reject_partition(self, mocked_device):
        mocked_device.return_value = MagicMock(
            is_partition=True,
            has_fs=False,
            is_lvm_member=False,
            has_gpt_headers=False,
            has_partitions=False,
        )
        with pytest.raises(ArgumentError):
            arg_validators.ValidBatchDevice()('foo')

    @pytest.mark.parametrize('format_', ['pretty', 'json', 'json-pretty'])
    def test_report(self, format_, factory, conf_ceph_stub, mock_device_generator):
        # just ensure reporting works
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        devs = [mock_device_generator() for _ in range(5)]
        args = factory(data_slots=1,
                       osds_per_device=1,
                       osd_ids=[],
                       report=True,
                       format=format_,
                       devices=devs,
                       db_devices=[],
                       wal_devices=[],
                       objectstore='bluestore',
                       block_db_size="1G",
                       dmcrypt=True,
                       data_allocate_fraction=1.0,
                      )
        b = batch.Batch([])
        b.args = args
        plan = b.get_deployment_layout()
        b.report(plan)

    @pytest.mark.parametrize('format_', ['json', 'json-pretty'])
    def test_json_report_valid_empty(self, format_, factory, conf_ceph_stub, mock_device_generator):
        # ensure json reports are valid when empty
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        devs = []
        args = factory(data_slots=1,
                       osds_per_device=1,
                       osd_ids=[],
                       report=True,
                       format=format_,
                       devices=devs,
                       db_devices=[],
                       wal_devices=[],
                       bluestore=True,
                       block_db_size="1G",
                       dmcrypt=True,
                       data_allocate_fraction=1.0,
                      )
        b = batch.Batch([])
        plan = b.get_plan(args)
        b.args = args
        report = b._create_report(plan)
        json.loads(report)

    @pytest.mark.parametrize('format_', ['json', 'json-pretty'])
    def test_json_report_valid_empty_unavailable_fast(self, format_, factory, conf_ceph_stub, mock_device_generator):
        # ensure json reports are valid when empty
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        devs = [mock_device_generator() for _ in range(5)]
        fast_devs = [mock_device_generator()]
        fast_devs[0].available_lvm = False
        args = factory(data_slots=1,
                       osds_per_device=1,
                       osd_ids=[],
                       report=True,
                       format=format_,
                       devices=devs,
                       db_devices=fast_devs,
                       wal_devices=[],
                       bluestore=True,
                       block_db_size="1G",
                       dmcrypt=True,
                       data_allocate_fraction=1.0,
                      )
        b = batch.Batch([])
        plan = b.get_plan(args)
        b.args = args
        report = b._create_report(plan)
        json.loads(report)


    @pytest.mark.parametrize('format_', ['json', 'json-pretty'])
    def test_json_report_valid_empty_unavailable_very_fast(self, format_, factory, conf_ceph_stub, mock_device_generator):
        # ensure json reports are valid when empty
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        devs = [mock_device_generator() for _ in range(5)]
        fast_devs = [mock_device_generator()]
        very_fast_devs = [mock_device_generator()]
        very_fast_devs[0].available_lvm = False
        args = factory(data_slots=1,
                       osds_per_device=1,
                       osd_ids=[],
                       report=True,
                       format=format_,
                       devices=devs,
                       db_devices=fast_devs,
                       wal_devices=very_fast_devs,
                       bluestore=True,
                       block_db_size="1G",
                       dmcrypt=True,
                       data_allocate_fraction=1.0,
                      )
        b = batch.Batch([])
        plan = b.get_plan(args)
        b.args = args
        report = b._create_report(plan)
        json.loads(report)

    @pytest.mark.parametrize('rota', [0, 1])
    def test_batch_sort_full(self, factory, rota):
        device1 = factory(used_by_ceph=False, available=True, rotational=rota, abspath="/dev/sda")
        device2 = factory(used_by_ceph=False, available=True, rotational=rota, abspath="/dev/sdb")
        device3 = factory(used_by_ceph=False, available=True, rotational=rota, abspath="/dev/sdc")
        devices = [device1, device2, device3]
        args = factory(report=True,
                       devices=devices,
                      )
        b = batch.Batch([])
        b.args = args
        b._sort_rotational_disks()
        assert len(b.args.devices) == 3

    @pytest.mark.parametrize('objectstore', ['bluestore'])
    def test_batch_sort_mixed(self, factory, objectstore):
        device1 = factory(used_by_ceph=False, available=True, rotational=1, abspath="/dev/sda")
        device2 = factory(used_by_ceph=False, available=True, rotational=1, abspath="/dev/sdb")
        device3 = factory(used_by_ceph=False, available=True, rotational=0, abspath="/dev/sdc")
        devices = [device1, device2, device3]
        args = factory(report=True,
                       devices=devices,
                      )
        b = batch.Batch([])
        b.args = args
        b._sort_rotational_disks()
        assert len(b.args.devices) == 2
        assert len(b.args.db_devices) == 1

    def test_get_physical_osds_return_len(self, factory,
                                          mock_devices_available,
                                          conf_ceph_stub,
                                          osds_per_device):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(data_slots=1, osds_per_device=osds_per_device,
                       osd_ids=[], dmcrypt=False,
                       data_allocate_fraction=1.0)
        osds = batch.get_physical_osds(mock_devices_available, args)
        assert len(osds) == len(mock_devices_available) * osds_per_device

    def test_get_physical_osds_rel_size(self, factory,
                                          mock_devices_available,
                                          conf_ceph_stub,
                                          osds_per_device,
                                          data_allocate_fraction):
        args = factory(data_slots=1, osds_per_device=osds_per_device,
                       osd_ids=[], dmcrypt=False,
                       data_allocate_fraction=data_allocate_fraction)
        osds = batch.get_physical_osds(mock_devices_available, args)
        for osd in osds:
            assert osd.data[1] == data_allocate_fraction / osds_per_device

    def test_get_physical_osds_abs_size(self, factory,
                                          mock_devices_available,
                                          conf_ceph_stub,
                                          osds_per_device,
                                          data_allocate_fraction):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(data_slots=1, osds_per_device=osds_per_device,
                       osd_ids=[], dmcrypt=False,
                       data_allocate_fraction=data_allocate_fraction)
        osds = batch.get_physical_osds(mock_devices_available, args)
        for osd, dev in zip(osds, mock_devices_available):
            assert osd.data[2] == int(dev.vg_size[0] * (data_allocate_fraction / osds_per_device))

    def test_get_physical_osds_osd_ids(self, factory,
                                          mock_devices_available,
                                          osds_per_device):
        pass

    def test_get_physical_fast_allocs_length(self, factory,
                                             conf_ceph_stub,
                                             mock_devices_available):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(block_db_slots=None, get_block_db_size=None)
        fast = batch.get_physical_fast_allocs(mock_devices_available,
                                              'block_db', 2, 2, args)
        assert len(fast) == 2

    def test_get_physical_fast_allocs_abs_size(self, factory,
                                               conf_ceph_stub,
                                               mock_devices_available):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(block_db_slots=None, get_block_db_size=None)
        fasts = batch.get_physical_fast_allocs(mock_devices_available,
                                              'block_db', 2, 2, args)
        for fast, dev in zip(fasts, mock_devices_available):
            assert fast[2] == int(dev.vg_size[0] / 2)

    def test_get_physical_fast_allocs_abs_size_unused_devs(self, factory,
                                               conf_ceph_stub,
                                               mock_devices_available):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(block_db_slots=None, get_block_db_size=None)
        dev_size = 21474836480
        vg_size = dev_size
        for dev in mock_devices_available:
            dev.vg_name = None
            dev.vg_size = [vg_size]
            dev.vg_free = dev.vg_size
            dev.vgs = []
        slots_per_device = 2
        fasts = batch.get_physical_fast_allocs(mock_devices_available,
                                              'block_db', slots_per_device, 2, args)
        expected_slot_size = int(dev_size / slots_per_device)
        for (_, _, slot_size, _) in fasts:
            assert slot_size == expected_slot_size

    def test_get_physical_fast_allocs_abs_size_multi_pvs_per_vg(self, factory,
                                               conf_ceph_stub,
                                               mock_devices_available):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(block_db_slots=None, get_block_db_size=None)
        dev_size = 21474836480
        num_devices = len(mock_devices_available)
        vg_size = dev_size * num_devices
        vg_name = 'vg_foo'
        for dev in mock_devices_available:
            dev.vg_name = vg_name
            dev.vg_size = [vg_size]
            dev.vg_free = dev.vg_size
            dev.vgs = [lvm.VolumeGroup(vg_name=dev.vg_name, lv_name=dev.lv_name)]
        slots_per_device = 2
        slots_per_vg = slots_per_device * num_devices
        fasts = batch.get_physical_fast_allocs(mock_devices_available,
                                              'block_db', slots_per_device, 2, args)
        expected_slot_size = int(vg_size / slots_per_vg)
        for (_, _, slot_size, _) in fasts:
            assert slot_size == expected_slot_size

    def test_batch_fast_allocations_one_block_db_length(self, factory, conf_ceph_stub,
                                                  mock_lv_device_generator):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')

        b = batch.Batch([])
        db_lv_devices = [mock_lv_device_generator()]
        fast = b.fast_allocations(db_lv_devices, 1, 0, 'block_db')
        assert len(fast) == 1

    @pytest.mark.parametrize('occupied_prior', range(7))
    @pytest.mark.parametrize('slots,num_devs',
                             [l for sub in [list(zip([x]*x, range(1, x + 1))) for x in range(1,7)] for l in sub])
    def test_get_physical_fast_allocs_length_existing(self,
                                                      num_devs,
                                                      slots,
                                                      occupied_prior,
                                                      factory,
                                                      conf_ceph_stub,
                                                      mock_device_generator):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        occupied_prior = min(occupied_prior, slots)
        devs = [mock_device_generator() for _ in range(num_devs)]
        already_assigned = 0
        while already_assigned < occupied_prior:
            dev_i = random.randint(0, num_devs - 1)
            dev = devs[dev_i]
            if len(dev.lvs) < occupied_prior:
                dev.lvs.append('foo')
                dev.path = '/dev/bar'
                already_assigned = sum([len(d.lvs) for d in devs])
        args = factory(block_db_slots=None, get_block_db_size=None)
        expected_num_osds = max(len(devs) * slots - occupied_prior, 0)
        fast = batch.get_physical_fast_allocs(devs,
                                              'block_db', slots,
                                              expected_num_osds, args)
        assert len(fast) == expected_num_osds
        expected_assignment_on_used_devices = sum([slots - len(d.lvs) for d in devs if len(d.lvs) > 0])
        assert len([f for f in fast if f[0] == '/dev/bar']) == expected_assignment_on_used_devices
        assert len([f for f in fast if f[0] != '/dev/bar']) == expected_num_osds - expected_assignment_on_used_devices

    def test_get_lvm_osds_return_len(self, factory,
                                     mock_lv_device_generator,
                                     conf_ceph_stub,
                                     osds_per_device):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        args = factory(data_slots=1, osds_per_device=osds_per_device,
                       osd_ids=[], dmcrypt=False)
        mock_lvs = [mock_lv_device_generator()]
        osds = batch.get_lvm_osds(mock_lvs, args)
        assert len(osds) == 1


class TestBatchOsd(object):

    def test_osd_class_ctor(self):
        osd = batch.Batch.OSD('/dev/data', 1, '5G', 1, 1, None)
        assert osd.data == batch.Batch.OSD.VolSpec('/dev/data',
                                                   1,
                                                   '5G',
                                                   1,
                                                   'data')
    def test_add_fast(self):
        osd = batch.Batch.OSD('/dev/data', 1, '5G', 1, 1, None)
        osd.add_fast_device('/dev/db', 1, '5G', 1, 'block_db')
        assert osd.fast == batch.Batch.OSD.VolSpec('/dev/db',
                                                   1,
                                                   '5G',
                                                   1,
                                                   'block_db')

    def test_add_very_fast(self):
        osd = batch.Batch.OSD('/dev/data', 1, '5G', 1, 1, None)
        osd.add_very_fast_device('/dev/wal', 1, '5G', 1)
        assert osd.very_fast == batch.Batch.OSD.VolSpec('/dev/wal',
                                                        1,
                                                        '5G',
                                                        1,
                                                        'block_wal')
