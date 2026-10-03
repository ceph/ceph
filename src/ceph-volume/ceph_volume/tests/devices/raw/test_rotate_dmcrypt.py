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
            rotate_dmcrypt.RotateDmcryptKey(['--phase', 'stage']).main()

    @patch('ceph_volume.util.dmcrypt_rotation.rotate_from_args')
    def test_main_parses_args(self, m_rotate, is_root):
        rotate_dmcrypt.RotateDmcryptKey(
            ['--osd-fsid', 'aaaa', '--key-store', 'external',
             '--phase', 'finish']).main()
        args, mode = m_rotate.call_args[0]
        assert mode == 'raw'
        assert args.osd_fsid == 'aaaa'
        assert args.key_store == 'external'
        assert args.phase == 'finish'

    @patch('ceph_volume.util.dmcrypt_rotation.rotate_from_args')
    def test_defaults_to_the_mon_key_store(self, m_rotate, is_root):
        rotate_dmcrypt.RotateDmcryptKey(['--osd-fsid', 'aaaa']).main()
        args, _ = m_rotate.call_args[0]
        assert args.key_store == 'mon'
        assert args.phase is None
