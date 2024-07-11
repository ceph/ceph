import pytest
from ceph_volume.devices import raw
from mock.mock import patch, MagicMock
from ceph_volume import objectstore

class TestRaw(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        raw.main.Raw([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Manage a single-device OSD on a raw block device.' in stdout

    def test_main_shows_activate_subcommands(self, capsys):
        raw.main.Raw([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'activate ' in stdout
        assert 'Discover and prepare' in stdout

    def test_main_shows_prepare_subcommands(self, capsys):
        raw.main.Raw([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'prepare ' in stdout
        assert 'Format a raw device' in stdout

@patch('ceph_volume.util.prepare.create_key', return_value='fake-secret')
class TestPrepare(object):
    def _setup(self, **kw):
        args = kw.get('args', [])
        self.p = raw.prepare.Prepare([])
        self.p.objectstore = objectstore.rawbluestore.RawBlueStore(args=args)
        for k, v in kw.items():
            setattr(self.p.objectstore, k, v)

    def test_main_spits_help_with_no_arguments(self, m_create_key, capsys):
        raw.prepare.Prepare([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Prepare an OSD by assigning an ID and FSID' in stdout

    def test_main_shows_full_help(self, m_create_key, capsys):
        with pytest.raises(SystemExit):
            raw.prepare.Prepare(argv=['--help']).main()
        stdout, stderr = capsys.readouterr()
        assert 'a raw device to use for the OSD' in stdout
        assert 'Crush device class to assign this OSD to' in stdout
        assert 'Use BlueStore backend' in stdout
        assert 'Path to bluestore block.db block device' in stdout
        assert 'Path to bluestore block.wal block device' in stdout
        assert 'Enable device encryption via dm-crypt' in stdout

    @patch('ceph_volume.util.arg_validators.set_dmcrypt_no_workqueue', return_value=MagicMock())
    @patch('ceph_volume.util.arg_validators.ValidRawDevice.__call__')
    def test_prepare_dmcrypt_no_secret_passed(self,
                                              m_valid_device,
                                              m_set_dmcrypt_no_workqueue,
                                              m_create_key,
                                              capsys):
        m_valid_device.return_value = '/dev/foo'
        with pytest.raises(SystemExit):
            raw.prepare.Prepare(argv=['--bluestore', '--data', '/dev/foo', '--dmcrypt']).main()
        stdout, stderr = capsys.readouterr()
        assert 'CEPH_VOLUME_DMCRYPT_SECRET is not set, you must set' in stderr

    @patch('ceph_volume.util.encryption.luks_open')
    @patch('ceph_volume.util.encryption.luks_format')
    @patch('ceph_volume.util.disk.lsblk')
    def test_prepare_dmcrypt_block(self, m_lsblk, m_luks_format, m_luks_open, m_create_key, factory):
        m_lsblk.return_value = {'KNAME': 'foo'}
        m_luks_format.return_value = True
        m_luks_open.return_value = True
        self._setup(block_device_path='/dev/foo',
                    osd_fsid='123',
                    secrets=dict(dmcrypt_key='foo'))
        self.p.objectstore.prepare_dmcrypt()
        m_luks_open.assert_called_with('foo', '/dev/foo', 'ceph-123-foo-block-dmcrypt')
        m_luks_format.assert_called_with('foo', '/dev/foo')
        assert self.p.objectstore.__dict__['block_device_path'] == '/dev/mapper/ceph-123-foo-block-dmcrypt'

    @patch('ceph_volume.util.encryption.luks_open')
    @patch('ceph_volume.util.encryption.luks_format')
    @patch('ceph_volume.util.disk.lsblk')
    def test_prepare_dmcrypt_db(self, m_lsblk, m_luks_format, m_luks_open, m_create_key):
        m_lsblk.return_value = {'KNAME': 'foo'}
        m_luks_format.return_value = True
        m_luks_open.return_value = True
        self._setup(db_device_path='/dev/db-foo',
                    osd_fsid='456',
                    secrets=dict(dmcrypt_key='foo'))
        self.p.objectstore.prepare_dmcrypt()
        m_luks_open.assert_called_with('foo', '/dev/db-foo', 'ceph-456-foo-db-dmcrypt')
        m_luks_format.assert_called_with('foo', '/dev/db-foo')
        assert self.p.objectstore.__dict__['db_device_path'] == '/dev/mapper/ceph-456-foo-db-dmcrypt'

    @patch('ceph_volume.util.encryption.luks_open')
    @patch('ceph_volume.util.encryption.luks_format')
    @patch('ceph_volume.util.disk.lsblk')
    def test_prepare_dmcrypt_wal(self, m_lsblk, m_luks_format, m_luks_open, m_create_key):
        m_lsblk.return_value = {'KNAME': 'foo'}
        m_luks_format.return_value = True
        m_luks_open.return_value = True
        self._setup(wal_device_path='/dev/wal-foo',
                    osd_fsid='789',
                    secrets=dict(dmcrypt_key='foo'))
        self.p.objectstore.prepare_dmcrypt()
        m_luks_open.assert_called_with('foo', '/dev/wal-foo', 'ceph-789-foo-wal-dmcrypt')
        m_luks_format.assert_called_with('foo', '/dev/wal-foo')
        assert self.p.objectstore.__dict__['wal_device_path'] == '/dev/mapper/ceph-789-foo-wal-dmcrypt'

    @patch('ceph_volume.objectstore.rawbluestore.rollback_osd')
    @patch('ceph_volume.objectstore.rawbluestore.RawBlueStore.prepare')
    @patch('ceph_volume.util.arg_validators.ValidRawDevice.__call__')
    def test_safe_prepare_exception_raised(self, m_valid_device, m_prepare, m_rollback_osd, m_create_key):
        m_valid_device.return_value = '/dev/foo'
        m_prepare.side_effect=Exception('foo')
        m_rollback_osd.return_value = 'foobar'
        with pytest.raises(Exception):
            raw.prepare.Prepare(argv=['--bluestore', '--data', '/dev/foo']).main()
        m_rollback_osd.assert_called()
