import pytest
from ceph_volume.devices import lvm


class TestCreate(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        lvm.create.Create([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Create an OSD by assigning an ID and FSID' in stdout

    def test_main_shows_full_help(self, capsys):
        with pytest.raises(SystemExit):
            lvm.create.Create(argv=['--help']).main()
        stdout, stderr = capsys.readouterr()
        assert 'Use the bluestore objectstore' in stdout
        assert 'A physical device or logical' in stdout

