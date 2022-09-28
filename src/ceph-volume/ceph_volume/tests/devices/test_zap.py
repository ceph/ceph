import pytest
from ceph_volume.devices import lvm
from mock.mock import patch, MagicMock


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
    @patch('ceph_volume.util.arg_validators.Device')
    def test_can_not_zap_mapper_device(self, mocked_device, monkeypatch, device_info, capsys, is_root, device_name, fake_filesystem):
        monkeypatch.setattr('os.path.exists', lambda x: True)
        monkeypatch.setattr('ceph_volume.util.disk.is_locked_raw_device', lambda dev_path: False)
        monkeypatch.setattr('ceph_volume.util.arg_validators.get_osd_ids_up', lambda: [123])
        mocked_device.return_value = MagicMock(
            has_bluestore_label=False,
            is_mapper=True,
            is_mpath=False,
            used_by_ceph=True,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False
        )
        with pytest.raises(SystemExit):
            lvm.zap.Zap(argv=[device_name]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Refusing to zap' in stderr
