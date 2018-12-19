import os
import pytest
from ceph_volume.util import disk


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


class TestMapDevPaths(object):

    def test_errors_return_empty_mapping(self, tmpdir):
        bad_dir = os.path.join(str(tmpdir), 'nonexisting')
        assert disk._map_dev_paths(bad_dir) == {}

    def test_base_name_and_abspath(self, tmpfile):
        sda_path = tmpfile(name='sda', contents='')
        directory = os.path.dirname(sda_path)
        result = disk._map_dev_paths(directory)
        assert len(result.keys()) == 1
        assert result['sda'] == sda_path

    def test_abspath_included(self, tmpfile):
        sda_path = tmpfile(name='sda', contents='')
        directory = os.path.dirname(sda_path)
        result = disk._map_dev_paths(directory, include_abspath=True)
        assert sorted(result.keys()) == sorted(['sda', sda_path])
        assert result['sda'] == sda_path
        assert result[sda_path] == 'sda'

    def test_realpath_included(self, tmpfile):
        sda_path = tmpfile(name='sda', contents='')
        directory = os.path.dirname(sda_path)
        dm_path = os.path.join(directory, 'dm-0')
        os.symlink(sda_path, os.path.join(directory, 'dm-0'))
        result = disk._map_dev_paths(directory, include_realpath=True)
        assert sorted(result.keys()) == sorted(['sda', 'dm-0'])
        assert result['sda'] == dm_path
        assert result['dm-0'] == dm_path

    def test_absolute_and_realpath_included(self, tmpfile):
        dm_path = tmpfile(name='dm-0', contents='')
        directory = os.path.dirname(dm_path)
        sda_path = os.path.join(directory, 'sda')
        os.symlink(sda_path, os.path.join(directory, 'sda'))
        result = disk._map_dev_paths(directory, include_realpath=True, include_abspath=True)
        assert sorted(result.keys()) == sorted([dm_path, sda_path, 'sda', 'dm-0'])
        assert result['sda'] == sda_path
        assert result['dm-0'] == dm_path
        assert result[sda_path] == sda_path
        assert result[dm_path] == 'dm-0'


class TestGetBlockDevs(object):

    def test_loop_devices_are_missing(self, tmpfile):
        path = os.path.dirname(tmpfile(name='loop0', contents=''))
        result = disk.get_block_devs(sys_block_path=path)
        assert result == []

    def test_loop_devices_are_included(self, tmpfile):
        path = os.path.dirname(tmpfile(name='loop0', contents=''))
        result = disk.get_block_devs(sys_block_path=path, skip_loop=False)
        assert len(result) == 1
        assert result == ['loop0']


class TestGetDevDevs(object):

    def test_abspaths_are_included(self, tmpfile):
        sda_path = tmpfile(name='sda', contents='')
        directory = os.path.dirname(sda_path)
        result = disk.get_dev_devs(directory)
        assert sorted(result.keys()) == sorted(['sda', sda_path])
        assert result['sda'] == sda_path
        assert result[sda_path] == 'sda'


class TestGetMapperDevs(object):

    def test_abspaths_and_realpaths_are_included(self, tmpfile):
        dm_path = tmpfile(name='dm-0', contents='')
        directory = os.path.dirname(dm_path)
        sda_path = os.path.join(directory, 'sda')
        os.symlink(sda_path, os.path.join(directory, 'sda'))
        result = disk.get_mapper_devs(directory)
        assert sorted(result.keys()) == sorted([dm_path, sda_path, 'sda', 'dm-0'])
        assert result['sda'] == sda_path
        assert result['dm-0'] == dm_path
        assert result[sda_path] == sda_path
        assert result[dm_path] == 'dm-0'


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


class TestGetDevices(object):

    def setup_paths(self, tmpdir):
        paths = []
        for directory in ['block', 'dev', 'mapper']:
            path = os.path.join(str(tmpdir), directory)
            paths.append(path)
            os.makedirs(path)
        return paths

    def test_no_devices_are_found(self, tmpdir):
        result = disk.get_devices(
            _sys_block_path=str(tmpdir),
            _dev_path=str(tmpdir),
            _mapper_path=str(tmpdir))
        assert result == {}

    def test_no_devices_are_found_errors(self, tmpdir):
        block_path, dev_path, mapper_path = self.setup_paths(tmpdir)
        os.makedirs(os.path.join(block_path, 'sda'))
        result = disk.get_devices(
            _sys_block_path=block_path, # has 1 device
            _dev_path=str(tmpdir), # exists but no devices
            _mapper_path='/does/not/exist/path') # does not exist
        assert result == {}

    def test_sda_block_is_found(self, tmpfile, tmpdir):
        block_path, dev_path, mapper_path = self.setup_paths(tmpdir)
        dev_sda_path = os.path.join(dev_path, 'sda')
        os.makedirs(os.path.join(block_path, 'sda'))
        os.makedirs(dev_sda_path)
        result = disk.get_devices(
            _sys_block_path=block_path,
            _dev_path=dev_path,
            _mapper_path=mapper_path)
        assert len(result.keys()) == 1
        assert result[dev_sda_path]['human_readable_size'] == '0.00 B'
        assert result[dev_sda_path]['model'] == ''
        assert result[dev_sda_path]['partitions'] == {}


    def test_dm_device_is_not_used(self, monkeypatch, tmpdir):
        # the link to the mapper is used instead
        monkeypatch.setattr(disk.lvm, 'is_lv', lambda: True)
        block_path, dev_path, mapper_path = self.setup_paths(tmpdir)
        dev_dm_path = os.path.join(dev_path, 'dm-0')
        ceph_data_path = os.path.join(mapper_path, 'ceph-data')
        os.symlink(dev_dm_path, ceph_data_path)
        block_dm_path = os.path.join(block_path, 'dm-0')
        os.makedirs(block_dm_path)

        result = disk.get_devices(
            _sys_block_path=block_path,
            _dev_path=dev_path,
            _mapper_path=mapper_path)
        result = list(result.keys())
        assert len(result) == 1
        assert result == [ceph_data_path]

    def test_sda_size(self, tmpfile, tmpdir):
        block_path, dev_path, mapper_path = self.setup_paths(tmpdir)
        block_sda_path = os.path.join(block_path, 'sda')
        dev_sda_path = os.path.join(dev_path, 'sda')
        os.makedirs(block_sda_path)
        os.makedirs(dev_sda_path)
        tmpfile('size', '1024', directory=block_sda_path)
        result = disk.get_devices(
            _sys_block_path=block_path,
            _dev_path=dev_path,
            _mapper_path=mapper_path)
        assert list(result.keys()) == [dev_sda_path]
        assert result[dev_sda_path]['human_readable_size'] == '512.00 KB'

    def test_sda_sectorsize_fallsback(self, tmpfile, tmpdir):
        # if no sectorsize, it will use queue/hw_sector_size
        block_path, dev_path, mapper_path = self.setup_paths(tmpdir)
        block_sda_path = os.path.join(block_path, 'sda')
        sda_queue_path = os.path.join(block_sda_path, 'queue')
        dev_sda_path = os.path.join(dev_path, 'sda')
        os.makedirs(block_sda_path)
        os.makedirs(sda_queue_path)
        os.makedirs(dev_sda_path)
        tmpfile('hw_sector_size', contents='1024', directory=sda_queue_path)
        result = disk.get_devices(
            _sys_block_path=block_path,
            _dev_path=dev_path,
            _mapper_path=mapper_path)
        assert list(result.keys()) == [dev_sda_path]
        assert result[dev_sda_path]['sectorsize'] == '1024'

    def test_sda_sectorsize_from_logical_block(self, tmpfile, tmpdir):
        block_path, dev_path, mapper_path = self.setup_paths(tmpdir)
        block_sda_path = os.path.join(block_path, 'sda')
        sda_queue_path = os.path.join(block_sda_path, 'queue')
        dev_sda_path = os.path.join(dev_path, 'sda')
        os.makedirs(block_sda_path)
        os.makedirs(sda_queue_path)
        os.makedirs(dev_sda_path)
        tmpfile('logical_block_size', contents='99', directory=sda_queue_path)
        result = disk.get_devices(
            _sys_block_path=block_path,
            _dev_path=dev_path,
            _mapper_path=mapper_path)
        assert result[dev_sda_path]['sectorsize'] == '99'

    def test_sda_sectorsize_does_not_fallback(self, tmpfile, tmpdir):
        block_path, dev_path, mapper_path = self.setup_paths(tmpdir)
        block_sda_path = os.path.join(block_path, 'sda')
        sda_queue_path = os.path.join(block_sda_path, 'queue')
        dev_sda_path = os.path.join(dev_path, 'sda')
        os.makedirs(block_sda_path)
        os.makedirs(sda_queue_path)
        os.makedirs(dev_sda_path)
        tmpfile('logical_block_size', contents='99', directory=sda_queue_path)
        tmpfile('hw_sector_size', contents='1024', directory=sda_queue_path)
        result = disk.get_devices(
            _sys_block_path=block_path,
            _dev_path=dev_path,
            _mapper_path=mapper_path)
        assert result[dev_sda_path]['sectorsize'] == '99'

    def test_is_rotational(self, tmpfile, tmpdir):
        block_path, dev_path, mapper_path = self.setup_paths(tmpdir)
        block_sda_path = os.path.join(block_path, 'sda')
        sda_queue_path = os.path.join(block_sda_path, 'queue')
        dev_sda_path = os.path.join(dev_path, 'sda')
        os.makedirs(block_sda_path)
        os.makedirs(sda_queue_path)
        os.makedirs(dev_sda_path)
        tmpfile('rotational', contents='1', directory=sda_queue_path)
        result = disk.get_devices(
            _sys_block_path=block_path,
            _dev_path=dev_path,
            _mapper_path=mapper_path)
        assert result[dev_sda_path]['rotational'] == '1'


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
