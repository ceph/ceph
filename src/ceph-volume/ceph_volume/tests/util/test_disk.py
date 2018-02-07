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
