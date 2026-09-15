import pytest
from unittest.mock import patch
from ceph_volume.devices.lvm import main, rotate_dmcrypt


class TestRotateDmcryptKey(object):
    def test_registered_in_mapper(self):
        assert main.LVM([]).mapper['rotate-dmcrypt-key'] is rotate_dmcrypt.RotateDmcryptKey

    def test_main_spits_help_with_no_arguments(self, capsys):
        rotate_dmcrypt.RotateDmcryptKey([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Rotate the dmcrypt (LUKS) passphrase' in stdout

    def test_main_requires_root(self):
        with pytest.raises(Exception):
            rotate_dmcrypt.RotateDmcryptKey(['--osd-id', '0']).main()

    @patch('ceph_volume.util.dmcrypt_rotation.rotate_from_args')
    def test_main_parses_args(self, m_rotate, is_root):
        rotate_dmcrypt.RotateDmcryptKey(
            ['--osd-id', '0', '--key-store', 'external', '--phase', 'stage',
             '--force']).main()
        args, mode = m_rotate.call_args[0]
        assert mode == 'lvm'
        assert args.osd_id == '0'
        assert args.key_store == 'external'
        assert args.phase == 'stage'
        assert args.force is True

    def test_unknown_argument_exits(self, capsys):
        with pytest.raises(SystemExit):
            rotate_dmcrypt.RotateDmcryptKey(['--bogus']).main()
