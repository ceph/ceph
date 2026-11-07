import pytest
from unittest.mock import patch
from ceph_volume.devices.raw import main, rotate_dmcrypt


class TestRotateDmcryptKey(object):
    def test_registered_in_mapper(self):
        assert main.Raw([]).mapper['rotate-dmcrypt-key'] is rotate_dmcrypt.RotateDmcryptKey

    def test_main_spits_help_with_no_arguments(self, capsys):
        rotate_dmcrypt.RotateDmcryptKey([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Rotate the dmcrypt (LUKS) passphrase' in stdout

    def test_osd_fsid_is_required(self, is_root, capsys):
        with pytest.raises(SystemExit):
            rotate_dmcrypt.RotateDmcryptKey(['--force']).main()

    @patch('ceph_volume.util.dmcrypt_rotation.rotate_from_args')
    def test_main_parses_args(self, m_rotate, is_root):
        rotate_dmcrypt.RotateDmcryptKey(['--osd-fsid', 'aaaa']).main()
        args, mode = m_rotate.call_args[0]
        assert mode == 'raw'
        assert args.osd_fsid == 'aaaa'
        assert args.force is False

    def test_config_key_credentials_are_rejected(self, is_root):
        with pytest.raises(SystemExit):
            rotate_dmcrypt.RotateDmcryptKey(
                ['--osd-fsid', 'aaaa', '--name', 'client.admin']).main()
