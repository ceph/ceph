import pytest

from ceph.deployment.inventory import Devices, Device

from ceph.deployment.drive_group import DriveGroupSpec, DriveGroupValidationError, DeviceSelection

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch  # type: ignore

from ceph.deployment import drive_selection
from ceph.tests.factories import InventoryFactory


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

        with pytest.raises(ValueError):
            drive_selection.SizeMatcher('size', '10P').to_byte(('10', 'PB'))
        assert 'Unit \'P\' is not supported'

    def test_compare_exact(self):

        matcher = drive_selection.SizeMatcher('size', '20GB')
        disk_dict = Device(path='/dev/vdb', sys_api=dict(size='20.00 GB'))
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

        with pytest.raises(ValueError):
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
                'host_pattern': 'data*',
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
                'host_pattern': 'data*',
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
                raw_sample = {'host_pattern': 'data*'}

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
        assert test_fix.block_wal_size == 5000000000

    def test_block_wal_size_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.block_wal_size is None

    def test_block_db_size_prop(self, test_fix):
        test_fix = test_fix()
        assert test_fix.block_db_size == 10000000000

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
        assert test_fix.data_devices is None

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

    def test_journal_device_prop(self, test_fix):
        test_fix = test_fix(disk_format='filestore')
        assert test_fix.journal_devices == DeviceSelection(
            size=':20G'
        )

    def test_wal_device_prop_empty(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.wal_devices is None

    def test_filestore_format_prop(self, test_fix):
        test_fix = test_fix(disk_format='filestore')
        assert test_fix.objectstore == 'filestore'

    def test_bluestore_format_prop(self, test_fix):
        test_fix = test_fix(disk_format='bluestore')
        assert test_fix.objectstore == 'bluestore'

    def test_default_format_prop(self, test_fix):
        test_fix = test_fix(empty=True)
        assert test_fix.objectstore == 'bluestore'

    def test_journal_size(self, test_fix):
        test_fix = test_fix(disk_format='filestore')
        assert test_fix.journal_size == 5000000000

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

    if False:
        def test_filter_devices_10_size_min_max(self, test_fix, inventory):
            """ Test_fix's data_device_attrs is configured to take any disk from
            30G - 50G or with vendor samsung or with model 42-RGB
            The default inventory setup is configured to have 10 data devices(50G)
            and 2 wal devices(20G).
            The expected match is 12
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(test_fix.data_device_attrs)
            assert len(ret) == 12

        def test_filter_devices_size_exact(self, test_fix, inventory):
            """
            Configure to only take disks with 20G (exact)
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(size='20G'))
            assert len(ret) == 2

        def test_filter_devices_2_max(self, test_fix, inventory):
            """
            Configure to only take disks with a max of 30G
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(size=':30G'))
            assert len(ret) == 2

        def test_filter_devices_0_max(self, test_fix, inventory):
            """
            Configure to only take disks with a max of 10G
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(size=':10G'))
            assert len(ret) == 0

        def test_filter_devices_12_min(self, test_fix, inventory):
            """
            Configure to only take disks with a min of 10G
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(size='10G:'))
            assert len(ret) == 12

        def test_filter_devices_12_min_20G(self, test_fix, inventory):
            """
            Configure to only take disks with a min of 20G
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(size='20G:'))
            assert len(ret) == 12

        def test_filter_devices_0_model(self, test_fix, inventory):
            """
            Configure to only take disks with a model of modelA
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(model='unknown'))
            assert len(ret) == 0

        def test_filter_devices_2_model(self, test_fix, inventory):
            """
            Configure to only take disks with a model of model*(wildcard)
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(model='ssd_type_model'))
            assert len(ret) == 2

        def test_filter_devices_12_vendor(self, test_fix, inventory):
            """
            Configure to only take disks with a vendor of samsung
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(vendor='samsung'))
            assert len(ret) == 12

        def test_filter_devices_2_rotational(self, test_fix, inventory):
            """
            Configure to only take disks with a rotational flag of 0
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(rotational='0'))
            assert len(ret) == 2

        def test_filter_devices_10_rotational(self, test_fix, inventory):
            """
            Configure to only take disks with a rotational flag of 1
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(rotational='1'))
            assert len(ret) == 10

        def test_filter_devices_limit(self, test_fix, inventory):
            """
            Configure to only take disks with a rotational flag of 1
            This should take two disks, but limit=1 is in place
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(rotational='1', limit=1))
            assert len(ret) == 1

        def test_filter_devices_all_limit_2(self, test_fix, inventory):
            """
            Configure to take all disks
            limiting to two
            """
            inventory()
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(all=True, limit=2))
            assert len(ret) == 2

        def test_filter_devices_empty_list_eq_matcher(self, test_fix, inventory):
            """
            Configure to only take disks with a rotational flag of 1
            This should take 10 disks, but limit=1 is in place
            Available is set to False. No disks are assigned
            """
            inventory(available=False)
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(rotational='1', limit=1))
            assert len(ret) == 0

        def test_filter_devices_empty_string_matcher(self, test_fix, inventory):
            """
            Configure to only take disks with a rotational flag of 1
            This should take two disks, but limit=1 is in place
            Available is set to False. No disks are assigned
            """
            inventory(available=False)
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(vendor='samsung', limit=1))
            assert len(ret) == 0

        def test_filter_devices_empty_size_matcher(self, test_fix, inventory):
            """
            Configure to only take disks with a rotational flag of 1
            This should take two disks, but limit=1 is in place
            Available is set to False. No disks are assigned
            """
            inventory(available=False)
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(size='10G:100G', limit=1))
            assert len(ret) == 0

        def test_filter_devices_empty_all_matcher(self, test_fix, inventory):
            """
            Configure to only take disks with a rotational flag of 1
            This should take two disks, but limit=1 is in place
            Available is set to False. No disks are assigned
            """
            inventory(available=False)
            test_fix = test_fix()
            ret = test_fix._filter_devices(dict(all=True, limit=1))
            assert len(ret) == 0

        @patch('ceph.deployment.drive_selection.DriveGroup._check_filter')
        def test_check_filter_support(self, check_filter_mock, test_fix):
            test_fix = test_fix()
            test_fix._check_filter_support()
            check_filter_mock.assert_called

        def test_check_filter(self, test_fix):
            test_fix = test_fix()
            ret = test_fix._check_filter(dict(model='foo'))
            assert ret is None

        def test_check_filter_raise(self, test_fix):
            test_fix = test_fix()
            with pytest.raises(DriveGroupValidationError):
                test_fix._check_filter(dict(unknown='foo'))
                pytest.fail("Filter unknown is not supported")

        def test_list_devices(self):
            pass


class TestFilter(object):
    def test_is_matchable(self):
        ret = drive_selection.Filter(name='name', matcher=None)
        assert ret.is_matchable is False


def _mk_device(rotational=True, locked=False):
    return [Device(
        path='??',
        sys_api={
            "rotational": '1' if rotational else '0',
            "vendor": "Vendor",
            "human_readable_size": "394.27 GB",
            "partitions": {},
            "locked": int(locked),
            "sectorsize": "512",
            "removable": "0",
            "path": "??",
            "support_discard": "",
            "model": "Model",
            "ro": "0",
            "nr_requests": "128",
            "size": 423347879936
        },
        available=not locked,
        rejected_reasons=['locked'] if locked else [],
        lvs=[],
        device_id="Model-Vendor-foobar"
    )]


def _mk_inventory(devices):
    devs = []
    for dev_, name in zip(devices, map(chr, range(ord('a'), ord('z')))):
        dev = Device.from_json(dev_.to_json())
        dev.path = '/dev/sd' + name
        dev.sys_api = dict(dev_.sys_api, path='/dev/sd' + name)
        devs.append(dev)
    return Devices(devices=devs)


class TestDriveSelection(object):

    testdata = [
        (
            DriveGroupSpec(host_pattern='*', data_devices=DeviceSelection(all=True)),
            _mk_inventory(_mk_device() * 5),
            ['/dev/sda', '/dev/sdb', '/dev/sdc', '/dev/sdd', '/dev/sde'], []
        ),
        (
            DriveGroupSpec(
                host_pattern='*',
                data_devices=DeviceSelection(all=True, limit=3),
                db_devices=DeviceSelection(all=True)
            ),
            _mk_inventory(_mk_device() * 5),
            ['/dev/sda', '/dev/sdb', '/dev/sdc'], ['/dev/sdd', '/dev/sde']
        ),
        (
            DriveGroupSpec(
                host_pattern='*',
                data_devices=DeviceSelection(rotational=True),
                db_devices=DeviceSelection(rotational=False)
            ),
            _mk_inventory(_mk_device(rotational=False) + _mk_device(rotational=True)),
            ['/dev/sdb'], ['/dev/sda']
        ),
        (
            DriveGroupSpec(
                host_pattern='*',
                data_devices=DeviceSelection(rotational=True),
                db_devices=DeviceSelection(rotational=False)
            ),
            _mk_inventory(_mk_device(rotational=True)*2 + _mk_device(rotational=False)),
            ['/dev/sda', '/dev/sdb'], ['/dev/sdc']
        ),
        (
            DriveGroupSpec(
                host_pattern='*',
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
