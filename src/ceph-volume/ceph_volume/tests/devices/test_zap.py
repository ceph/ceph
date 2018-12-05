import pytest
from ceph_volume.devices import lvm


class TestZap(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        lvm.zap.Zap([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Zaps the given logical volume(s), raw device(s) or partition(s)' in stdout

    def test_main_shows_full_help(self, capsys):
        with pytest.raises(SystemExit):
            lvm.zap.Zap(argv=['--help']).main()
        stdout, stderr = capsys.readouterr()
        assert 'optional arguments' in stdout

    @pytest.mark.parametrize('device_name', [
        '/dev/mapper/foo',
        '/dev/dm-0',
    ])
    def test_can_not_zap_mapper_device(self, monkeypatch, device_info, capsys, is_root, device_name):
        monkeypatch.setattr('os.path.exists', lambda x: True)
        device_info()
        with pytest.raises(SystemExit):
            lvm.zap.Zap(argv=[device_name]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Refusing to zap' in stdout
