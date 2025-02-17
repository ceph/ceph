import pytest
import stat
from ceph_volume.util import disk
from unittest.mock import patch, Mock, MagicMock, mock_open
from pyfakefs.fake_filesystem_unittest import TestCase


class TestFunctions:
    @patch('ceph_volume.util.disk.os.path.exists', MagicMock(return_value=False))
    def test_is_device_path_does_not_exist(self):
        assert not disk.is_device('/dev/foo')

    @patch('ceph_volume.util.disk.os.path.exists', MagicMock(return_value=True))
    def test_is_device_dev_doesnt_startswith_dev(self):
        assert not disk.is_device('/foo')

    @patch('ceph_volume.util.disk.allow_loop_devices', MagicMock(return_value=False))
    @patch('ceph_volume.util.disk.os.path.exists', MagicMock(return_value=True))
    def test_is_device_loop_not_allowed(self):
        assert not disk.is_device('/dev/loop123')

    @patch('ceph_volume.util.disk.lsblk', MagicMock(return_value={'NAME': 'foo', 'TYPE': 'disk'}))
    @patch('ceph_volume.util.disk.os.path.exists', MagicMock(return_value=True))
    def test_is_device_type_disk(self):
        assert disk.is_device('/dev/foo')

    @patch('ceph_volume.util.disk.lsblk', MagicMock(return_value={'NAME': 'foo', 'TYPE': 'mpath'}))
    @patch('ceph_volume.util.disk.os.path.exists', MagicMock(return_value=True))
    def test_is_device_type_mpath(self):
        assert disk.is_device('/dev/foo')

    @patch('ceph_volume.util.disk.lsblk', MagicMock(return_value={'NAME': 'foo1', 'TYPE': 'part'}))
    @patch('ceph_volume.util.disk.os.path.exists', MagicMock(return_value=True))
    def test_is_device_type_part(self):
        assert not disk.is_device('/dev/foo1')

    @patch('ceph_volume.util.disk.os.path.exists', MagicMock(return_value=True))
    @patch('ceph_volume.util.disk.get_partitions', MagicMock(return_value={"sda1": "sda"}))
    def test_is_partition(self):
        assert disk.is_partition('sda1')


    @patch('os.path.exists', Mock(return_value=True))
    def test_get_lvm_mapper_path_from_dm(self):
        with patch('builtins.open', mock_open(read_data='test--foo--vg-test--foo--lv')):
            assert disk.get_lvm_mapper_path_from_dm('/dev/dm-123') == '/dev/mapper/test--foo--vg-test--foo--lv'

    @patch('ceph_volume.util.disk.get_block_device_holders', MagicMock(return_value={'/dev/dmcrypt-mapper-123': '/dev/sda'}))
    @patch('os.path.realpath', MagicMock(return_value='/dev/sda'))
    def test_has_holders_true(self):
        assert disk.has_holders('/dev/sda')

    @patch('ceph_volume.util.disk.get_block_device_holders', MagicMock(return_value={'/dev/dmcrypt-mapper-123': '/dev/sda'}))
    @patch('os.path.realpath', MagicMock(return_value='/dev/sdb'))
    def test_has_holders_false(self):
        assert not disk.has_holders('/dev/sda')

    @patch('ceph_volume.util.disk.get_block_device_holders', MagicMock(return_value={'/dev/dmcrypt-mapper-123': '/dev/sda'}))
    @patch('os.path.realpath', MagicMock(return_value='/dev/foobar'))
    def test_has_holders_device_does_not_exist(self):
        assert not disk.has_holders('/dev/foobar')

class TestLsblkParser(object):

    def test_parses_whitespace_values(self):
        output = 'NAME="sdaa5" PARTLABEL="ceph data" RM="0" SIZE="10M" RO="0" TYPE="part"'
        result = disk._lsblk_parser(output)
        assert result['PARTLABEL'] == 'ceph data'

    def test_ignores_bogus_pairs(self):
        output = 'NAME="sdaa5" PARTLABEL RM="0" SIZE="10M" RO="0" TYPE="part" MOUNTPOINT=""'
        result = disk._lsblk_parser(output)
        assert result['SIZE'] == '10M'


class TestBlkidParser(object):

    def test_parses_whitespace_values(self):
        output = '''/dev/sdb1: UUID="62416664-cbaf-40bd-9689-10bd337379c3" TYPE="xfs" PART_ENTRY_SCHEME="gpt" PART_ENTRY_NAME="ceph data" PART_ENTRY_UUID="b89c03bc-bf58-4338-a8f8-a2f484852b4f"'''  # noqa
        result = disk._blkid_parser(output)
        assert result['PARTLABEL'] == 'ceph data'

    def test_ignores_unmapped(self):
        output = '''/dev/sdb1: UUID="62416664-cbaf-40bd-9689-10bd337379c3" TYPE="xfs" PART_ENTRY_SCHEME="gpt" PART_ENTRY_NAME="ceph data" PART_ENTRY_UUID="b89c03bc-bf58-4338-a8f8-a2f484852b4f"'''  # noqa
        result = disk._blkid_parser(output)
        assert len(result.keys()) == 4

    def test_translates_to_partuuid(self):
        output = '''/dev/sdb1: UUID="62416664-cbaf-40bd-9689-10bd337379c3" TYPE="xfs" PART_ENTRY_SCHEME="gpt" PART_ENTRY_NAME="ceph data" PART_ENTRY_UUID="b89c03bc-bf58-4338-a8f8-a2f484852b4f"'''  # noqa
        result = disk._blkid_parser(output)
        assert result['PARTUUID'] == 'b89c03bc-bf58-4338-a8f8-a2f484852b4f'


class TestBlkid(object):

    def test_parses_translated(self, stub_call):
        output = '''/dev/sdb1: UUID="62416664-cbaf-40bd-9689-10bd337379c3" TYPE="xfs" PART_ENTRY_SCHEME="gpt" PART_ENTRY_NAME="ceph data" PART_ENTRY_UUID="b89c03bc-bf58-4338-a8f8-a2f484852b4f"'''  # noqa
        stub_call((output.split(), [], 0))
        result = disk.blkid('/dev/sdb1')
        assert result['PARTUUID'] == 'b89c03bc-bf58-4338-a8f8-a2f484852b4f'
        assert result['PARTLABEL'] == 'ceph data'
        assert result['UUID'] == '62416664-cbaf-40bd-9689-10bd337379c3'
        assert result['TYPE'] == 'xfs'

class TestUdevadmProperty(object):

    def test_good_output(self, stub_call):
        output = """ID_MODEL=SK_hynix_SC311_SATA_512GB
ID_PART_TABLE_TYPE=gpt
ID_SERIAL_SHORT=MS83N71801150416A""".split()
        stub_call((output, [], 0))
        result = disk.udevadm_property('dev/sda')
        assert result['ID_MODEL'] == 'SK_hynix_SC311_SATA_512GB'
        assert result['ID_PART_TABLE_TYPE'] == 'gpt'
        assert result['ID_SERIAL_SHORT'] == 'MS83N71801150416A'

    def test_property_filter(self, stub_call):
        output = """ID_MODEL=SK_hynix_SC311_SATA_512GB
ID_PART_TABLE_TYPE=gpt
ID_SERIAL_SHORT=MS83N71801150416A""".split()
        stub_call((output, [], 0))
        result = disk.udevadm_property('dev/sda', ['ID_MODEL',
                                                   'ID_SERIAL_SHORT'])
        assert result['ID_MODEL'] == 'SK_hynix_SC311_SATA_512GB'
        assert 'ID_PART_TABLE_TYPE' not in result

    def test_fail_on_broken_output(self, stub_call):
        output = ["ID_MODEL:SK_hynix_SC311_SATA_512GB"]
        stub_call((output, [], 0))
        with pytest.raises(ValueError):
            disk.udevadm_property('dev/sda')


class TestDeviceFamily(object):

    def test_groups_multiple_devices(self, stub_call):
        out = [
            'NAME="sdaa5" PARLABEL="ceph lockbox"',
            'NAME="sdaa" RO="0"',
            'NAME="sdaa1" PARLABEL="ceph data"',
            'NAME="sdaa2" PARLABEL="ceph journal"',
        ]
        stub_call((out, '', 0))
        result = disk.device_family('sdaa5')
        assert len(result) == 4

    def test_parses_output_correctly(self, stub_call):
        names = ['sdaa', 'sdaa5', 'sdaa1', 'sdaa2']
        out = [
            'NAME="sdaa5" PARLABEL="ceph lockbox"',
            'NAME="sdaa" RO="0"',
            'NAME="sdaa1" PARLABEL="ceph data"',
            'NAME="sdaa2" PARLABEL="ceph journal"',
        ]
        stub_call((out, '', 0))
        result = disk.device_family('sdaa5')
        for parsed in result:
            assert parsed['NAME'] in names


class TestHumanReadableSize(object):

    def test_bytes(self):
        result = disk.human_readable_size(800)
        assert result == '800.00 B'

    def test_kilobytes(self):
        result = disk.human_readable_size(800*1024)
        assert result == '800.00 KB'

    def test_megabytes(self):
        result = disk.human_readable_size(800*1024*1024)
        assert result == '800.00 MB'

    def test_gigabytes(self):
        result = disk.human_readable_size(8.19*1024*1024*1024)
        assert result == '8.19 GB'

    def test_terabytes(self):
        result = disk.human_readable_size(81.2*1024*1024*1024*1024)
        assert result == '81.20 TB'

    def test_petabytes(self):
        result = disk.human_readable_size(9.23*1024*1024*1024*1024*1024)
        assert result == '9.23 PB'

class TestSizeFromHumanReadable(object):

    def test_bytes(self):
        result = disk.size_from_human_readable('2')
        assert result == disk.Size(b=2)

    def test_kilobytes(self):
        result = disk.size_from_human_readable('2 K')
        assert result == disk.Size(kb=2)

    def test_megabytes(self):
        result = disk.size_from_human_readable('2 M')
        assert result == disk.Size(mb=2)

    def test_gigabytes(self):
        result = disk.size_from_human_readable('2 G')
        assert result == disk.Size(gb=2)

    def test_terabytes(self):
        result = disk.size_from_human_readable('2 T')
        assert result == disk.Size(tb=2)

    def test_petabytes(self):
        result = disk.size_from_human_readable('2 P')
        assert result == disk.Size(pb=2)

    def test_case(self):
        result = disk.size_from_human_readable('2 t')
        assert result == disk.Size(tb=2)

    def test_space(self):
        result = disk.size_from_human_readable('2T')
        assert result == disk.Size(tb=2)

    def test_float(self):
        result = disk.size_from_human_readable('2.0')
        assert result == disk.Size(b=2)
        result = disk.size_from_human_readable('2.0T')
        assert result == disk.Size(tb=2)
        result = disk.size_from_human_readable('1.8T')
        assert result == disk.Size(tb=1.8)


class TestSizeParse(object):

    def test_bytes(self):
        result = disk.Size.parse('2')
        assert result == disk.Size(b=2)

    def test_kilobytes(self):
        result = disk.Size.parse('2K')
        assert result == disk.Size(kb=2)

    def test_megabytes(self):
        result = disk.Size.parse('2M')
        assert result == disk.Size(mb=2)

    def test_gigabytes(self):
        result = disk.Size.parse('2G')
        assert result == disk.Size(gb=2)

    def test_terabytes(self):
        result = disk.Size.parse('2T')
        assert result == disk.Size(tb=2)

    def test_petabytes(self):
        result = disk.Size.parse('2P')
        assert result == disk.Size(pb=2)

    def test_tb(self):
        result = disk.Size.parse('2Tb')
        assert result == disk.Size(tb=2)

    def test_case(self):
        result = disk.Size.parse('2t')
        assert result == disk.Size(tb=2)

    def test_space(self):
        result = disk.Size.parse('2T')
        assert result == disk.Size(tb=2)

    def test_float(self):
        result = disk.Size.parse('2.0')
        assert result == disk.Size(b=2)
        result = disk.Size.parse('2.0T')
        assert result == disk.Size(tb=2)
        result = disk.Size.parse('1.8T')
        assert result == disk.Size(tb=1.8)


class TestGetDevices(object):

    def test_no_devices_are_found(self, tmpdir, patched_get_block_devs_sysfs):
        patched_get_block_devs_sysfs.return_value = []
        result = disk.get_devices(_sys_block_path=str(tmpdir))
        assert result == {}

    @patch('ceph_volume.util.disk.udevadm_property')
    def test_sda_block_is_found(self, m_udev_adm_property, patched_get_block_devs_sysfs, fake_filesystem):
        sda_path = '/dev/sda'
        patched_get_block_devs_sysfs.return_value = [[sda_path, sda_path, 'disk', sda_path]]
        result = disk.get_devices()
        assert len(result.keys()) == 1
        assert result[sda_path]['human_readable_size'] == '0.00 B'
        assert result[sda_path]['model'] == ''
        assert result[sda_path]['partitions'] == {}

    @patch('ceph_volume.util.disk.udevadm_property')
    def test_sda_size(self, m_udev_adm_property, patched_get_block_devs_sysfs, fake_filesystem):
        sda_path = '/dev/sda'
        patched_get_block_devs_sysfs.return_value = [[sda_path, sda_path, 'disk', sda_path]]
        fake_filesystem.create_file('/sys/block/sda/size', contents = '1024')
        result = disk.get_devices()
        assert list(result.keys()) == [sda_path]
        assert result[sda_path]['human_readable_size'] == '512.00 KB'

    @patch('ceph_volume.util.disk.udevadm_property')
    def test_sda_sectorsize_fallsback(self, m_udev_adm_property, patched_get_block_devs_sysfs, fake_filesystem):
        # if no sectorsize, it will use queue/hw_sector_size
        sda_path = '/dev/sda'
        patched_get_block_devs_sysfs.return_value = [[sda_path, sda_path, 'disk', sda_path]]
        fake_filesystem.create_file('/sys/block/sda/queue/hw_sector_size', contents = '1024')
        result = disk.get_devices()
        assert list(result.keys()) == [sda_path]
        assert result[sda_path]['sectorsize'] == '1024'

    @patch('ceph_volume.util.disk.udevadm_property')
    def test_sda_sectorsize_from_logical_block(self, m_udev_adm_property, patched_get_block_devs_sysfs, fake_filesystem):
        sda_path = '/dev/sda'
        patched_get_block_devs_sysfs.return_value = [[sda_path, sda_path, 'disk', sda_path]]
        fake_filesystem.create_file('/sys/block/sda/queue/logical_block_size', contents = '99')
        result = disk.get_devices()
        assert result[sda_path]['sectorsize'] == '99'

    @patch('ceph_volume.util.disk.udevadm_property')
    def test_sda_sectorsize_does_not_fallback(self, m_udev_adm_property, patched_get_block_devs_sysfs, fake_filesystem):
        sda_path = '/dev/sda'
        patched_get_block_devs_sysfs.return_value = [[sda_path, sda_path, 'disk', sda_path]]
        fake_filesystem.create_file('/sys/block/sda/queue/logical_block_size', contents = '99')
        fake_filesystem.create_file('/sys/block/sda/queue/hw_sector_size', contents = '1024')
        result = disk.get_devices()
        assert result[sda_path]['sectorsize'] == '99'

    @patch('ceph_volume.util.disk.udevadm_property')
    def test_is_rotational(self, m_udev_adm_property, patched_get_block_devs_sysfs, fake_filesystem):
        sda_path = '/dev/sda'
        patched_get_block_devs_sysfs.return_value = [[sda_path, sda_path, 'disk', sda_path]]
        fake_filesystem.create_file('/sys/block/sda/queue/rotational', contents = '1')
        result = disk.get_devices()
        assert result[sda_path]['rotational'] == '1'

    @patch('ceph_volume.util.disk.udevadm_property')
    def test_is_ceph_rbd(self, m_udev_adm_property, patched_get_block_devs_sysfs, fake_filesystem):
        rbd_path = '/dev/rbd0'
        patched_get_block_devs_sysfs.return_value = [[rbd_path, rbd_path, 'disk', rbd_path]]
        result = disk.get_devices()
        assert rbd_path not in result

    @patch('ceph_volume.util.disk.udevadm_property')
    def test_actuator_device(self, m_udev_adm_property, patched_get_block_devs_sysfs, fake_filesystem):
        sda_path = '/dev/sda'
        fake_actuator_nb = 2
        patched_get_block_devs_sysfs.return_value = [[sda_path, sda_path, 'disk', sda_path]]
        for actuator in range(0, fake_actuator_nb):
            fake_filesystem.create_dir(f'/sys/block/sda/queue/independent_access_ranges/{actuator}')
        result = disk.get_devices()
        assert result[sda_path]['actuators'] == fake_actuator_nb


class TestSizeCalculations(object):

    @pytest.mark.parametrize('aliases', [
        ('b', 'bytes'),
        ('kb', 'kilobytes'),
        ('mb', 'megabytes'),
        ('gb', 'gigabytes'),
        ('tb', 'terabytes'),
    ])
    def test_aliases(self, aliases):
        short_alias, long_alias = aliases
        s = disk.Size(b=1)
        short_alias = getattr(s, short_alias)
        long_alias = getattr(s, long_alias)
        assert short_alias == long_alias

    @pytest.mark.parametrize('values', [
        ('b', 857619069665.28),
        ('kb', 837518622.72),
        ('mb', 817889.28),
        ('gb', 798.72),
        ('tb', 0.78),
    ])
    def test_terabytes(self, values):
        # regardless of the input value, all the other values correlate to each
        # other the same, every time
        unit, value = values
        s = disk.Size(**{unit: value})
        assert s.b == 857619069665.28
        assert s.kb == 837518622.72
        assert s.mb == 817889.28
        assert s.gb == 798.72
        assert s.tb == 0.78


class TestSizeOperators(object):

    @pytest.mark.parametrize('larger', [1025, 1024.1, 1024.001])
    def test_gigabytes_is_smaller(self, larger):
        assert disk.Size(gb=1) < disk.Size(mb=larger)

    @pytest.mark.parametrize('smaller', [1023, 1023.9, 1023.001])
    def test_gigabytes_is_larger(self, smaller):
        assert disk.Size(gb=1) > disk.Size(mb=smaller)

    @pytest.mark.parametrize('larger', [1025, 1024.1, 1024.001, 1024])
    def test_gigabytes_is_smaller_or_equal(self, larger):
        assert disk.Size(gb=1) <= disk.Size(mb=larger)

    @pytest.mark.parametrize('smaller', [1023, 1023.9, 1023.001, 1024])
    def test_gigabytes_is_larger_or_equal(self, smaller):
        assert disk.Size(gb=1) >= disk.Size(mb=smaller)

    @pytest.mark.parametrize('values', [
        ('b', 857619069665.28),
        ('kb', 837518622.72),
        ('mb', 817889.28),
        ('gb', 798.72),
        ('tb', 0.78),
    ])
    def test_equality(self, values):
        unit, value = values
        s = disk.Size(**{unit: value})
        # both tb and b, since b is always calculated regardless, and is useful
        # when testing tb
        assert disk.Size(tb=0.78) == s
        assert disk.Size(b=857619069665.28) == s

    @pytest.mark.parametrize('values', [
        ('b', 857619069665.28),
        ('kb', 837518622.72),
        ('mb', 817889.28),
        ('gb', 798.72),
        ('tb', 0.78),
    ])
    def test_inequality(self, values):
        unit, value = values
        s = disk.Size(**{unit: value})
        # both tb and b, since b is always calculated regardless, and is useful
        # when testing tb
        assert disk.Size(tb=1) != s
        assert disk.Size(b=100) != s


class TestSizeOperations(object):

    def test_assignment_addition_with_size_objects(self):
        result = disk.Size(mb=256) + disk.Size(gb=1)
        assert result.gb == 1.25
        assert result.gb.as_int() == 1
        assert result.gb.as_float() == 1.25

    def test_self_addition_with_size_objects(self):
        base = disk.Size(mb=256)
        base += disk.Size(gb=1)
        assert base.gb == 1.25

    def test_self_addition_does_not_alter_state(self):
        base = disk.Size(mb=256)
        base + disk.Size(gb=1)
        assert base.mb == 256

    def test_addition_with_non_size_objects(self):
        with pytest.raises(TypeError):
            disk.Size(mb=100) + 4

    def test_assignment_subtraction_with_size_objects(self):
        base = disk.Size(gb=1)
        base -= disk.Size(mb=256)
        assert base.mb == 768

    def test_self_subtraction_does_not_alter_state(self):
        base = disk.Size(gb=1)
        base - disk.Size(mb=256)
        assert base.gb == 1

    def test_subtraction_with_size_objects(self):
        result = disk.Size(gb=1) - disk.Size(mb=256)
        assert result.mb == 768

    def test_subtraction_with_non_size_objects(self):
        with pytest.raises(TypeError):
            disk.Size(mb=100) - 4

    def test_multiplication_with_size_objects(self):
        with pytest.raises(TypeError):
            disk.Size(mb=100) * disk.Size(mb=1)

    def test_multiplication_with_non_size_objects(self):
        base = disk.Size(gb=1)
        result = base * 2
        assert result.gb == 2
        assert result.gb.as_int() == 2

    def test_division_with_size_objects(self):
        result = disk.Size(gb=1) / disk.Size(mb=1)
        assert int(result) == 1024

    def test_division_with_non_size_objects(self):
        base = disk.Size(gb=1)
        result = base / 2
        assert result.mb == 512
        assert result.mb.as_int() == 512

    def test_division_with_non_size_objects_without_state(self):
        base = disk.Size(gb=1)
        base / 2
        assert base.gb == 1
        assert base.gb.as_int() == 1


class TestSizeAttributes(object):

    def test_attribute_does_not_exist(self):
        with pytest.raises(AttributeError):
            disk.Size(mb=1).exabytes


class TestSizeFormatting(object):

    def test_default_formatting_tb_to_b(self):
        size = disk.Size(tb=0.0000000001)
        result = "%s" % size
        assert result == "109.95 B"

    def test_default_formatting_tb_to_kb(self):
        size = disk.Size(tb=0.00000001)
        result = "%s" % size
        assert result == "10.74 KB"

    def test_default_formatting_tb_to_mb(self):
        size = disk.Size(tb=0.000001)
        result = "%s" % size
        assert result == "1.05 MB"

    def test_default_formatting_tb_to_gb(self):
        size = disk.Size(tb=0.001)
        result = "%s" % size
        assert result == "1.02 GB"

    def test_default_formatting_tb_to_tb(self):
        size = disk.Size(tb=10)
        result = "%s" % size
        assert result == "10.00 TB"


class TestSizeSpecificFormatting(object):

    def test_formatting_b(self):
        size = disk.Size(b=2048)
        result = "%s" % size.b
        assert "%s" % size.b == "%s" % size.bytes
        assert result == "2048.00 B"

    def test_formatting_kb(self):
        size = disk.Size(kb=5700)
        result = "%s" % size.kb
        assert "%s" % size.kb == "%s" % size.kilobytes
        assert result == "5700.00 KB"

    def test_formatting_mb(self):
        size = disk.Size(mb=4000)
        result = "%s" % size.mb
        assert "%s" % size.mb == "%s" % size.megabytes
        assert result == "4000.00 MB"

    def test_formatting_gb(self):
        size = disk.Size(gb=77777)
        result = "%s" % size.gb
        assert "%s" % size.gb == "%s" % size.gigabytes
        assert result == "77777.00 GB"

    def test_formatting_tb(self):
        size = disk.Size(tb=1027)
        result = "%s" % size.tb
        assert "%s" % size.tb == "%s" % size.terabytes
        assert result == "1027.00 TB"


class TestHasBlueStoreLabel(object):
    def test_device_path_is_a_path(self, fake_filesystem):
        device_path = '/var/lib/ceph/osd/ceph-0'
        fake_filesystem.create_dir(device_path)
        assert not disk.has_bluestore_label(device_path)


class TestBlockSysFs(TestCase):
    def setUp(self) -> None:
        self.setUpPyfakefs()
        self.fs.create_dir('/fake-area/foo/holders')
        self.fs.create_dir('/fake-area/bar2/holders')
        self.fs.create_file('/fake-area/bar2/holders/dm-0')
        self.fs.create_file('/fake-area/foo/holders/dm-1')
        self.fs.create_file('/fake-area/bar2/partition', contents='2')
        self.fs.create_file('/fake-area/foo/size', contents='1024')
        self.fs.create_file('/fake-area/foo/queue/logical_block_size', contents='512')
        self.fs.create_file('/fake-area/foo/random-data', contents='some-random data\n')
        self.fs.create_dir('/sys/dev/block')
        self.fs.create_dir('/sys/block/foo')
        self.fs.create_symlink('/sys/dev/block/8:0', '/fake-area/foo')
        self.fs.create_symlink('/sys/dev/block/252:2', '/fake-area/bar2')
        self.fs.create_file('/sys/block/dm-0/dm/uuid', contents='CRYPT-LUKS2-1234-abcdef')
        self.fs.create_file('/sys/block/dm-1/dm/uuid', contents='LVM-abcdef')

    def test_init(self) -> None:
        b = disk.BlockSysFs('/dev/foo')
        assert b.path == '/dev/foo'
        assert b.sys_dev_block_dir == '/sys/dev/block'
        assert b.sys_block == '/sys/block'

    def test_get_sysfs_file_content(self) -> None:
        b = disk.BlockSysFs('/dev/foo')
        assert b._get_sysfs_file_content('random-data') == 'some-random data'

    def test_blocks(self) -> None:
        b = disk.BlockSysFs('/dev/foo')
        assert b.blocks == 1024

    def test_logical_block_size(self) -> None:
        b = disk.BlockSysFs('/dev/foo')
        assert b.logical_block_size == 512

    def test_size(self) -> None:
        b = disk.BlockSysFs('/dev/foo')
        assert b.size == 524288

    def test_get_sys_dev_block_path(self) -> None:
        b = disk.BlockSysFs('/dev/foo')
        assert b.sys_dev_block_path == '/sys/dev/block/8:0'

    def test_is_partition_true(self) -> None:
        b = disk.BlockSysFs('/dev/bar2')
        assert b.is_partition

    def test_is_partition_false(self) -> None:
        b = disk.BlockSysFs('/dev/foo')
        assert not b.is_partition

    def test_holders(self) -> None:
        b1 = disk.BlockSysFs('/dev/bar2')
        b2 = disk.BlockSysFs('/dev/foo')
        assert b1.holders == ['dm-0']
        assert b2.holders == ['dm-1']

    def test_has_active_dmcrypt_mapper(self) -> None:
        b = disk.BlockSysFs('/dev/bar2')
        assert b.has_active_dmcrypt_mapper

    def test_has_active_mappers(self) -> None:
        b = disk.BlockSysFs('/dev/foo')
        assert b.has_active_mappers

    def test_active_mappers_dmcrypt(self) -> None:
        b = disk.BlockSysFs('/dev/bar2')
        assert b.active_mappers()
        assert b.active_mappers()['dm-0']
        assert b.active_mappers()['dm-0']['type'] == 'CRYPT'
        assert b.active_mappers()['dm-0']['dmcrypt_mapping'] == 'abcdef'
        assert b.active_mappers()['dm-0']['dmcrypt_type'] == 'LUKS2'
        assert b.active_mappers()['dm-0']['dmcrypt_uuid'] == '1234'

    def test_active_mappers_lvm(self) -> None:
        b = disk.BlockSysFs('/dev/foo')
        assert b.active_mappers()
        assert b.active_mappers()['dm-1']
        assert b.active_mappers()['dm-1']['type'] == 'LVM'
        assert b.active_mappers()['dm-1']['uuid'] == 'abcdef'


class TestUdevData(TestCase):
    def setUp(self) -> None:
        udev_data_lv_device: str = """
S:disk/by-id/dm-uuid-LVM-1f1RaxWlzQ61Sbc7oCIHRMdh0M8zRTSnU03ekuStqWuiA6eEDmwoGg3cWfFtE2li
S:mapper/vg1-lv1
S:disk/by-id/dm-name-vg1-lv1
S:vg1/lv1
I:837060642207
E:DM_UDEV_DISABLE_OTHER_RULES_FLAG=
E:DM_UDEV_DISABLE_LIBRARY_FALLBACK_FLAG=1
E:DM_UDEV_PRIMARY_SOURCE_FLAG=1
E:DM_UDEV_RULES_VSN=2
E:DM_NAME=fake_vg1-fake-lv1
E:DM_UUID=LVM-1f1RaxWlzQ61Sbc7oCIHRMdh0M8zRTSnU03ekuStqWuiA6eEDmwoGg3cWfFtE2li
E:DM_SUSPENDED=0
E:DM_VG_NAME=fake_vg1
E:DM_LV_NAME=fake-lv1
E:DM_LV_LAYER=
E:NVME_HOST_IFACE=none
E:SYSTEMD_READY=1
G:systemd
Q:systemd
V:1"""
        udev_data_bare_device: str = """
S:disk/by-path/pci-0000:00:02.0
S:disk/by-path/virtio-pci-0000:00:02.0
S:disk/by-diskseq/1
I:3037919
E:ID_PATH=pci-0000:00:02.0
E:ID_PATH_TAG=pci-0000_00_02_0
E:ID_PART_TABLE_UUID=baefa409
E:ID_PART_TABLE_TYPE=dos
E:NVME_HOST_IFACE=none
G:systemd
Q:systemd
V:1"""
        self.fake_device: str = '/dev/cephtest'
        self.setUpPyfakefs()
        self.fs.create_file(self.fake_device, st_mode=(stat.S_IFBLK | 0o600))
        self.fs.create_file('/run/udev/data/b999:0', create_missing_dirs=True, contents=udev_data_bare_device)
        self.fs.create_file('/run/udev/data/b998:1', create_missing_dirs=True, contents=udev_data_lv_device)

    def test_device_not_found(self) -> None:
        self.fs.remove(self.fake_device)
        with pytest.raises(RuntimeError):
            disk.UdevData(self.fake_device)

    @patch('ceph_volume.util.disk.os.stat', MagicMock())
    @patch('ceph_volume.util.disk.os.minor', Mock(return_value=0))
    @patch('ceph_volume.util.disk.os.major', Mock(return_value=999))
    def test_no_data(self) -> None:
        self.fs.remove('/run/udev/data/b999:0')
        with pytest.raises(RuntimeError):
            disk.UdevData(self.fake_device)

    @patch('ceph_volume.util.disk.os.stat', MagicMock())
    @patch('ceph_volume.util.disk.os.minor', Mock(return_value=0))
    @patch('ceph_volume.util.disk.os.major', Mock(return_value=999))
    def test_is_dm_false(self) -> None:
        assert not disk.UdevData(self.fake_device).is_dm

    @patch('ceph_volume.util.disk.os.stat', MagicMock())
    @patch('ceph_volume.util.disk.os.minor', Mock(return_value=1))
    @patch('ceph_volume.util.disk.os.major', Mock(return_value=998))
    def test_is_dm_true(self) -> None:
        assert disk.UdevData(self.fake_device).is_dm

    @patch('ceph_volume.util.disk.os.stat', MagicMock())
    @patch('ceph_volume.util.disk.os.minor', Mock(return_value=1))
    @patch('ceph_volume.util.disk.os.major', Mock(return_value=998))
    def test_is_lvm_true(self) -> None:
        assert disk.UdevData(self.fake_device).is_dm

    @patch('ceph_volume.util.disk.os.stat', MagicMock())
    @patch('ceph_volume.util.disk.os.minor', Mock(return_value=0))
    @patch('ceph_volume.util.disk.os.major', Mock(return_value=999))
    def test_is_lvm_false(self) -> None:
        assert not disk.UdevData(self.fake_device).is_dm

    @patch('ceph_volume.util.disk.os.stat', MagicMock())
    @patch('ceph_volume.util.disk.os.minor', Mock(return_value=1))
    @patch('ceph_volume.util.disk.os.major', Mock(return_value=998))
    def test_slashed_path_with_lvm(self) -> None:
        assert disk.UdevData(self.fake_device).slashed_path == '/dev/fake_vg1/fake-lv1'

    @patch('ceph_volume.util.disk.os.stat', MagicMock())
    @patch('ceph_volume.util.disk.os.minor', Mock(return_value=1))
    @patch('ceph_volume.util.disk.os.major', Mock(return_value=998))
    def test_dashed_path_with_lvm(self) -> None:
        assert disk.UdevData(self.fake_device).dashed_path == '/dev/mapper/fake_vg1-fake-lv1'

    @patch('ceph_volume.util.disk.os.stat', MagicMock())
    @patch('ceph_volume.util.disk.os.minor', Mock(return_value=0))
    @patch('ceph_volume.util.disk.os.major', Mock(return_value=999))
    def test_slashed_path_with_bare_device(self) -> None:
        assert disk.UdevData(self.fake_device).slashed_path == '/dev/cephtest'

    @patch('ceph_volume.util.disk.os.stat', MagicMock())
    @patch('ceph_volume.util.disk.os.minor', Mock(return_value=0))
    @patch('ceph_volume.util.disk.os.major', Mock(return_value=999))
    def test_dashed_path_with_bare_device(self) -> None:
        assert disk.UdevData(self.fake_device).dashed_path == '/dev/cephtest'