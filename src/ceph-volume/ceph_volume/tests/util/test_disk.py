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
        assert result.keys() == ['sda']
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
