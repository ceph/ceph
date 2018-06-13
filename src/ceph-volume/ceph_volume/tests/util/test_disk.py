import os
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

    def test_sda_is_removable_gets_skipped(self, tmpfile, tmpdir):
        block_path, dev_path, mapper_path = self.setup_paths(tmpdir)
        dev_sda_path = os.path.join(dev_path, 'sda')
        block_sda_path = os.path.join(block_path, 'sda')
        os.makedirs(block_sda_path)
        os.makedirs(dev_sda_path)

        tmpfile('removable', contents='1', directory=block_sda_path)
        result = disk.get_devices(
            _sys_block_path=block_path,
            _dev_path=dev_path,
            _mapper_path=mapper_path)
        assert result == {}

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
