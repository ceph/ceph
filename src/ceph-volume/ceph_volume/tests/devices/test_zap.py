import pytest
from ceph_volume.devices import lvm


class TestZap(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        lvm.zap.Zap([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Zaps the given logical volume or partition' in stdout

    def test_main_shows_full_help(self, capsys):
        with pytest.raises(SystemExit):
            lvm.zap.Zap(argv=['--help']).main()
        stdout, stderr = capsys.readouterr()
        assert 'optional arguments' in stdout
        assert 'positional arguments' in stdout
