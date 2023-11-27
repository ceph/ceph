# flake8: noqa
import pytest

from ceph.deployment.drive_selection.matchers import _MatchInvalid
from ceph.deployment.inventory import Devices, Device

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection, \
        DriveGroupValidationError

from ceph.deployment import drive_selection
from ceph.deployment.service_spec import PlacementSpec
from ceph.tests.factories import InventoryFactory
from ceph.tests.utils import _mk_inventory, _mk_device


class TestMatcher(object):
    """ Test Matcher base class
    """

    def test_get_disk_key_3(self):
        """
        virtual is False
        key is found
        retrun value of key is expected
        """
        disk_map = Device(path='/dev/vdb', sys_api={'foo': 'bar'})
        ret = drive_selection.Matcher('foo', 'bar')._get_disk_key(disk_map)
        assert ret is disk_map.sys_api.get('foo')

    def test_get_disk_key_4(self):
        """
        virtual is False
        key is not found
        expect raise Exception
        """
        disk_map = Device(path='/dev/vdb')
        with pytest.raises(Exception):
            drive_selection.Matcher('bar', 'foo')._get_disk_key(disk_map)
            pytest.fail("No disk_key found for foo or None")


class TestSubstringMatcher(object):
    def test_compare(self):
        disk_dict = Device(path='/dev/vdb', sys_api=dict(model='samsung'))
        matcher = drive_selection.SubstringMatcher('model', 'samsung')
        ret = matcher.compare(disk_dict)
        assert ret is True

    def test_compare_false(self):
        disk_dict = Device(path='/dev/vdb', sys_api=dict(model='nothing_matching'))
        matcher = drive_selection.SubstringMatcher('model', 'samsung')
        ret = matcher.compare(disk_dict)
        assert ret is False


class TestEqualityMatcher(object):
    def test_compare(self):
        disk_dict = Device(path='/dev/vdb', sys_api=dict(rotates='1'))
        matcher = drive_selection.EqualityMatcher('rotates', '1')
        ret = matcher.compare(disk_dict)
        assert ret is True

    def test_compare_false(self):
        disk_dict = Device(path='/dev/vdb', sys_api=dict(rotates='1'))
        matcher = drive_selection.EqualityMatcher('rotates', '0')
        ret = matcher.compare(disk_dict)
        assert ret is False


class TestAllMatcher(object):
    def test_compare(self):
        disk_dict = Device(path='/dev/vdb')
        matcher = drive_selection.AllMatcher('all', 'True')
        ret = matcher.compare(disk_dict)
        assert ret is True

    def test_compare_value_not_true(self):
        disk_dict = Device(path='/dev/vdb')
        matcher = drive_selection.AllMatcher('all', 'False')
        ret = matcher.compare(disk_dict)
        assert ret is True


class TestSizeMatcher(object):
    def test_parse_filter_exact(self):
        """ Testing exact notation with 20G """
        matcher = drive_selection.SizeMatcher('size', '20G')
        assert isinstance(matcher.exact, tuple)
        assert matcher.exact[0] == '20'
        assert matcher.exact[1] == 'GB'

    def test_parse_filter_exact_GB_G(self):
        """ Testing exact notation with 20G """
        matcher = drive_selection.SizeMatcher('size', '20GB')
        assert isinstance(matcher.exact, tuple)
        assert matcher.exact[0] == '20'
        assert matcher.exact[1] == 'GB'

    def test_parse_filter_high_low(self):
        """ Testing high-low notation with 20G:50G """

        matcher = drive_selection.SizeMatcher('size', '20G:50G')
        assert isinstance(matcher.exact, tuple)
        assert matcher.low[0] == '20'
        assert matcher.high[0] == '50'
        assert matcher.low[1] == 'GB'
        assert matcher.high[1] == 'GB'

    def test_parse_filter_max_high(self):
        """ Testing high notation with :50G """

        matcher = drive_selection.SizeMatcher('size', ':50G')
        assert isinstance(matcher.exact, tuple)
        assert matcher.high[0] == '50'
        assert matcher.high[1] == 'GB'

    def test_parse_filter_min_low(self):
        """ Testing low notation with 20G: """

        matcher = drive_selection.SizeMatcher('size', '50G:')
        assert isinstance(matcher.exact, tuple)
        assert matcher.low[0] == '50'
        assert matcher.low[1] == 'GB'

    def test_to_byte_KB(self):
        """ I doubt anyone ever thought we'd need to understand KB """

        ret = drive_selection.SizeMatcher('size', '4K').to_byte(('4', 'KB'))
        assert ret == 4 * 1e+3

    def test_to_byte_GB(self):
        """ Pretty nonesense test.."""

        ret = drive_selection.SizeMatcher('size', '10G').to_byte(('10', 'GB'))
        assert ret == 10 * 1e+9

    def test_to_byte_MB(self):
        """ Pretty nonesense test.."""

        ret = drive_selection.SizeMatcher('size', '10M').to_byte(('10', 'MB'))
        assert ret == 10 * 1e+6

    def test_to_byte_TB(self):
        """ Pretty nonesense test.."""

        ret = drive_selection.SizeMatcher('size', '10T').to_byte(('10', 'TB'))
        assert ret == 10 * 1e+12

    def test_to_byte_PB(self):
        """ Expect to raise """

        with pytest.raises(_MatchInvalid):
            drive_selection.SizeMatcher('size', '10P').to_byte(('10', 'PB'))
        assert 'Unit \'P\' is not supported'

    def test_compare_exact(self):

        matcher = drive_selection.SizeMatcher('size', '20GB')
        disk_dict = Device(path='/dev/vdb', sys_api=dict(size='20.00 GB'))
        ret = matcher.compare(disk_dict)
        assert ret is True

    def test_compare_exact_decimal(self):

        matcher = drive_selection.SizeMatcher('size', '20.12GB')
        disk_dict = Device(path='/dev/vdb', sys_api=dict(size='20.12 GB'))
        ret = matcher.compare(disk_dict)
        assert ret is True

    @pytest.mark.parametrize("test_input,expected", [
        ("1.00 GB", False),
        ("20.00 GB", True),
        ("50.00 GB", True),
        ("100.00 GB", True),
        ("101.00 GB", False),
        ("1101.00 GB", False),
    ])
    def test_compare_high_low(self, test_input, expected):

        matcher = drive_selection.SizeMatcher('size', '20GB:100GB')
        disk_dict = Device(path='/dev/vdb', sys_api=dict(size=test_input))
        ret = matcher.compare(disk_dict)
        assert ret is expected

    @pytest.mark.parametrize("test_input,expected", [
        ("1.00 GB", True),
        ("20.00 GB", True),
        ("50.00 GB", True),
        ("100.00 GB", False),
        ("101.00 GB", False),
        ("1101.00 GB", False),
    ])
    def test_compare_high(self, test_input, expected):

        matcher = drive_selection.SizeMatcher('size', ':50GB')
        disk_dict = Device(path='/dev/vdb', sys_api=dict(size=test_input))
        ret = matcher.compare(disk_dict)
        assert ret is expected

    @pytest.mark.parametrize("test_input,expected", [
        ("1.00 GB", False),
        ("20.00 GB", False),
        ("50.00 GB", True),
        ("100.00 GB", True),
        ("101.00 GB", True),
        ("1101.00 GB", True),
    ])
    def test_compare_low(self, test_input, expected):

        matcher = drive_selection.SizeMatcher('size', '50GB:')
        disk_dict = Device(path='/dev/vdb', sys_api=dict(size=test_input))
        ret = matcher.compare(disk_dict)
        assert ret is expected

    @pytest.mark.parametrize("test_input,expected", [
        ("1.00 GB", False),
        ("20.00 GB", False),
        ("50.00 GB", False),
        ("100.00 GB", False),
        ("101.00 GB", False),
        ("1101.00 GB", True),
        ("9.10 TB", True),
    ])
    def test_compare_at_least_1TB(self, test_input, expected):

        matcher = drive_selection.SizeMatcher('size', '1TB:')
        disk_dict = Device(path='/dev/sdz', sys_api=dict(size=test_input))
        ret = matcher.compare(disk_dict)
        assert ret is expected

    def test_compare_raise(self):

        matcher = drive_selection.SizeMatcher('size', 'None')
        disk_dict = Device(path='/dev/vdb', sys_api=dict(size='20.00 GB'))
        with pytest.raises(Exception):
            matcher.compare(disk_dict)
            pytest.fail("Couldn't parse size")

    @pytest.mark.parametrize("test_input,expected", [
        ("10G", ('10', 'GB')),
        ("20GB", ('20', 'GB')),
        ("10g", ('10', 'GB')),
        ("20gb", ('20', 'GB')),
    ])
    def test_get_k_v(self, test_input, expected):
        assert drive_selection.SizeMatcher('size', '10G')._get_k_v(test_input) == expected

    @pytest.mark.parametrize("test_input,expected", [
        ("10G", ('GB')),
        ("10g", ('GB')),
        ("20GB", ('GB')),
        ("20gb", ('GB')),
        ("20TB", ('TB')),
        ("20tb", ('TB')),
        ("20T", ('TB')),
        ("20t", ('TB')),
        ("20MB", ('MB')),
        ("20mb", ('MB')),
        ("20M", ('MB')),
        ("20m", ('MB')),
    ])
    def test_parse_suffix(self, test_input, expected):
        assert drive_selection.SizeMatcher('size', '10G')._parse_suffix(test_input) == expected

    @pytest.mark.parametrize("test_input,expected", [
        ("G", 'GB'),
        ("GB", 'GB'),
        ("TB", 'TB'),
        ("T", 'TB'),
        ("MB", 'MB'),
        ("M", 'MB'),
    ])
    def test_normalize_suffix(self, test_input, expected):

        assert drive_selection.SizeMatcher('10G', 'size')._normalize_suffix(test_input) == expected

    def test_normalize_suffix_raises(self):

        with pytest.raises(_MatchInvalid):
            drive_selection.SizeMatcher('10P', 'size')._normalize_suffix("P")
            pytest.fail("Unit 'P' not supported")


class TestDriveGroup(object):
    @pytest.fixture(scope='class')
    def test_fix(self, empty=None):
        def make_sample_data(empty=empty,
                             data_limit=0,
                             wal_limit=0,
                             db_limit=0,
                             osds_per_device='',
                             disk_format='bluestore'):
            raw_sample_bluestore = {
                'service_type': 'osd',
                'service_id': 'foo',
                'placement': {'host_pattern': 'data*'},
                'data_devices': {
                    'size': '30G:50G',
                    'model': '42-RGB',
                    'vendor': 'samsung',
                    'limit': data_limit
                },
                'wal_devices': {
                    'model': 'fast',
                    'limit': wal_limit
                },
                'db_devices': {
                    'size': ':20G',
                    'limit': db_limit
                },
                'db_slots': 5,
                'wal_slots': 5,
                'block_wal_size': '5G',
                'block_db_size': '10G',
                'objectstore': disk_format,
                'osds_per_device': osds_per_device,
                'encrypted': True,
            }
            raw_sample_filestore = {
                'service_type': 'osd',
                'service_id': 'foo',
                'placement': {'host_pattern': 'data*'},
                'objectstore': 'filestore',
                'data_devices': {
                    'size': '30G:50G',
                    'model': 'foo',
                    'vendor': '1x',
                    'limit': data_limit
                },
                'journal_devices': {
                    'size': ':20G'
                },
                'journal_size': '5G',
                'osds_per_device': osds_per_device,
                'encrypted': True,
            }
            if disk_format == 'filestore':
                raw_sample = raw_sample_filestore
            else:
                raw_sample = raw_sample_bluestore

            if empty:
                raw_sample = {
                    'service_type': 'osd',
                    'service_id': 'foo',
                    'placement': {'host_pattern': 'data*'},
                    'data_devices': {
                        'all': True
                    },
                }

            dgo = DriveGroupSpec.from_json(raw_sample)
            return dgo

        return make_sample_data

    def test_encryption_prop(self, test_fix):
        test_fix = test_fix()
        assert test_fix.encrypted is True

    def test_encryption_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.encrypted is False

    def test_db_slots_prop(self, test_fix):
        test_fix = test_fix()
        assert test_fix.db_slots == 5

    def test_db_slots_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.db_slots is None

    def test_wal_slots_prop(self, test_fix):
        test_fix = test_fix()
        assert test_fix.wal_slots == 5

    def test_wal_slots_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.wal_slots is None

    def test_block_wal_size_prop(self, test_fix):
        test_fix = test_fix()
        assert test_fix.block_wal_size == '5G'

    def test_block_wal_size_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.block_wal_size is None

    def test_block_db_size_prop(self, test_fix):
        test_fix = test_fix()
        assert test_fix.block_db_size == '10G'

    def test_block_db_size_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.block_db_size is None

    def test_data_devices_prop(self, test_fix):
        test_fix = test_fix()
        assert test_fix.data_devices == DeviceSelection(
            model='42-RGB',
            size='30G:50G',
            vendor='samsung',
            limit=0,
        )

    def test_data_devices_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.db_devices is None

    def test_db_devices_prop(self, test_fix):
        test_fix = test_fix()
        assert test_fix.db_devices == DeviceSelection(
            size=':20G',
            limit=0,
        )

    def test_db_devices_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.db_devices is None

    def test_wal_device_prop(self, test_fix):
        test_fix = test_fix()
        assert test_fix.wal_devices == DeviceSelection(
            model='fast',
            limit=0,
        )

    def test_wal_device_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.wal_devices is None

    def test_bluestore_format_prop(self, test_fix):
        test_fix = test_fix(disk_format='bluestore')
        assert test_fix.objectstore == 'bluestore'

    def test_default_format_prop(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.objectstore == 'bluestore'

    def test_osds_per_device(self, test_fix):
        test_fix = test_fix(osds_per_device='3')
        assert test_fix.osds_per_device == '3'

    def test_osds_per_device_default(self, test_fix):
        test_fix = test_fix()
        assert test_fix.osds_per_device == ''

    def test_journal_size_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.journal_size is None

    @pytest.fixture
    def inventory(self, available=True):
        def make_sample_data(available=available,
                             data_devices=10,
                             wal_devices=0,
                             db_devices=2,
                             human_readable_size_data='50.00 GB',
                             human_readable_size_wal='20.00 GB',
                             size=5368709121,
                             human_readable_size_db='20.00 GB'):
            factory = InventoryFactory()
            inventory_sample = []
            data_disks = factory.produce(
                pieces=data_devices,
                available=available,
                size=size,
                human_readable_size=human_readable_size_data)
            wal_disks = factory.produce(
                pieces=wal_devices,
                human_readable_size=human_readable_size_wal,
                rotational='0',
                model='ssd_type_model',
                size=size,
                available=available)
            db_disks = factory.produce(
                pieces=db_devices,
                human_readable_size=human_readable_size_db,
                rotational='0',
                size=size,
                model='ssd_type_model',
                available=available)
            inventory_sample.extend(data_disks)
            inventory_sample.extend(wal_disks)
            inventory_sample.extend(db_disks)

            return Devices(devices=inventory_sample)

        return make_sample_data


class TestDriveSelection(object):

    testdata = [
        (
            DriveGroupSpec(
                placement=PlacementSpec(host_pattern='*'),
                service_id='foobar',
                data_devices=DeviceSelection(all=True)),
            _mk_inventory(_mk_device() * 5),
            ['/dev/sda', '/dev/sdb', '/dev/sdc', '/dev/sdd', '/dev/sde'], []
        ),
        (
            DriveGroupSpec(
                placement=PlacementSpec(host_pattern='*'),
                service_id='foobar',
                data_devices=DeviceSelection(all=True, limit=3),
                db_devices=DeviceSelection(all=True)
            ),
            _mk_inventory(_mk_device() * 5),
            ['/dev/sda', '/dev/sdb', '/dev/sdc'], ['/dev/sdd', '/dev/sde']
        ),
        (
            DriveGroupSpec(
                placement=PlacementSpec(host_pattern='*'),
                service_id='foobar',
                data_devices=DeviceSelection(rotational=True),
                db_devices=DeviceSelection(rotational=False)
            ),
            _mk_inventory(_mk_device(rotational=False) + _mk_device(rotational=True)),
            ['/dev/sdb'], ['/dev/sda']
        ),
        (
            DriveGroupSpec(
                placement=PlacementSpec(host_pattern='*'),
                service_id='foobar',
                data_devices=DeviceSelection(rotational=True),
                db_devices=DeviceSelection(rotational=False)
            ),
            _mk_inventory(_mk_device(rotational=True)*2 + _mk_device(rotational=False)),
            ['/dev/sda', '/dev/sdb'], ['/dev/sdc']
        ),
        (
            DriveGroupSpec(
                placement=PlacementSpec(host_pattern='*'),
                service_id='foobar',
                data_devices=DeviceSelection(rotational=True),
                db_devices=DeviceSelection(rotational=False)
            ),
            _mk_inventory(_mk_device(rotational=True)*2),
            ['/dev/sda', '/dev/sdb'], []
        ),
    ]

    @pytest.mark.parametrize("spec,inventory,expected_data,expected_db", testdata)
    def test_disk_selection(self, spec, inventory, expected_data, expected_db):
        sel = drive_selection.DriveSelection(spec, inventory)
        assert [d.path for d in sel.data_devices()] == expected_data
        assert [d.path for d in sel.db_devices()] == expected_db

    def test_disk_selection_raise(self):
        spec = DriveGroupSpec(
                placement=PlacementSpec(host_pattern='*'),
                service_id='foobar',
                data_devices=DeviceSelection(size='wrong'),
            )
        inventory = _mk_inventory(_mk_device(rotational=True)*2)
        m = 'Failed to validate OSD spec "foobar.data_devices": No filters applied'
        with pytest.raises(DriveGroupValidationError, match=m):
            drive_selection.DriveSelection(spec, inventory)


class TestDeviceSelectionLimit:

    def test_limit_existing_devices(self):
        # Initial setup for this test is meant to be that /dev/sda
        # is already being used for an OSD, hence it being marked
        # as a ceph_device. /dev/sdb and /dev/sdc are not being used
        # for OSDs yet. The limit will be set to 2 and the DriveSelection
        # is set to have 1 pre-existing device (corresponding to /dev/sda)
        dev_a = Device('/dev/sda', ceph_device=True, available=False)
        dev_b = Device('/dev/sdb', ceph_device=False, available=True)
        dev_c = Device('/dev/sdc', ceph_device=False, available=True)
        all_devices: List[Device] = [dev_a, dev_b, dev_c]
        processed_devices: List[Device] = []
        filter = DeviceSelection(all=True, limit=2)
        dgs = DriveGroupSpec(data_devices=filter)
        ds = drive_selection.DriveSelection(dgs, all_devices, existing_daemons=1)

        # Check /dev/sda. It's already being used for an OSD and will
        # be counted in existing_daemons. This check should return False
        # as we are not over the limit.
        assert not ds._limit_reached(filter, processed_devices, '/dev/sda')
        processed_devices.append(dev_a)

        # We should still not be over the limit here with /dev/sdb since
        # we will have only one pre-existing daemon /dev/sdb itself. This
        # case previously failed as devices that contributed to existing_daemons
        # would be double counted both as devices and daemons.
        assert not ds._limit_reached(filter, processed_devices, '/dev/sdb')
        processed_devices.append(dev_b)

        # Now, with /dev/sdb and the pre-existing daemon taking up the 2
        # slots, /dev/sdc should be rejected for us being over the limit.
        assert ds._limit_reached(filter, processed_devices, '/dev/sdc')

        # DriveSelection does device assignment on initialization. Let's check
        # it picked up the expected devices
        assert ds._data == [dev_a, dev_b]
