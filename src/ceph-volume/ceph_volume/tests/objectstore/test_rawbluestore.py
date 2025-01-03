import pytest
from mock import patch, Mock, MagicMock, call
from ceph_volume.objectstore.rawbluestore import RawBlueStore
from ceph_volume.util import system


class TestRawBlueStore:
    @patch('ceph_volume.objectstore.rawbluestore.prepare_utils.create_key', Mock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    def setup_method(self, m_create_key):
        self.raw_bs = RawBlueStore([])

    def test_prepare_dmcrypt(self,
                             device_info,
                             fake_call,
                             key_size):
        self.raw_bs.secrets = {'dmcrypt_key': 'foo'}
        self.raw_bs.block_device_path = '/dev/foo0'
        self.raw_bs.db_device_path = '/dev/foo1'
        self.raw_bs.wal_device_path = '/dev/foo2'
        lsblk = {"TYPE": "disk",
                 "NAME": "foo0",
                 'KNAME': 'foo0'}
        device_info(lsblk=lsblk)
        self.raw_bs.prepare_dmcrypt()
        assert self.raw_bs.block_device_path == "/dev/mapper/ceph--foo0-block-dmcrypt"
        assert self.raw_bs.db_device_path == "/dev/mapper/ceph--foo0-db-dmcrypt"
        assert self.raw_bs.wal_device_path == "/dev/mapper/ceph--foo0-wal-dmcrypt"

    @patch('ceph_volume.objectstore.rawbluestore.RawBlueStore.enroll_tpm2', Mock(return_value=MagicMock()))
    def test_prepare_dmcrypt_with_tpm(self,
                                      device_info,
                                      fake_call,
                                      key_size):
        self.raw_bs.block_device_path = '/dev/foo0'
        self.raw_bs.db_device_path = '/dev/foo1'
        self.raw_bs.wal_device_path = '/dev/foo2'
        self.raw_bs.with_tpm = 1
        lsblk = {"TYPE": "disk",
                 "NAME": "foo0",
                 'KNAME': 'foo0'}
        device_info(lsblk=lsblk)
        self.raw_bs.prepare_dmcrypt()
        assert 'dmcrypt_key' not in self.raw_bs.secrets.keys()
        assert self.raw_bs.block_device_path == "/dev/mapper/ceph--foo0-block-dmcrypt"
        assert self.raw_bs.db_device_path == "/dev/mapper/ceph--foo0-db-dmcrypt"
        assert self.raw_bs.wal_device_path == "/dev/mapper/ceph--foo0-wal-dmcrypt"
        assert self.raw_bs.enroll_tpm2.mock_calls == [call('/dev/foo0'), call('/dev/foo1'), call('/dev/foo2')]

    @patch('ceph_volume.objectstore.rawbluestore.rollback_osd')
    @patch('ceph_volume.objectstore.rawbluestore.RawBlueStore.prepare')
    def test_safe_prepare_raises_exception(self,
                                           m_prepare,
                                           m_rollback_osd,
                                           factory,
                                           capsys):
        m_prepare.side_effect = Exception
        m_rollback_osd.return_value = MagicMock()
        args = factory(osd_id='1')
        self.raw_bs.args = args
        self.raw_bs.osd_id = self.raw_bs.args.osd_id
        with pytest.raises(Exception):
            self.raw_bs.safe_prepare()
        assert m_rollback_osd.mock_calls == [call(self.raw_bs.args, '1')]

    @patch('ceph_volume.objectstore.rawbluestore.RawBlueStore.prepare', MagicMock())
    def test_safe_prepare(self,
                          factory,
                          capsys):
        args = factory(dmcrypt=True,
                       data='/dev/foo')
        self.raw_bs.safe_prepare(args)
        _, stderr = capsys.readouterr()
        assert "prepare successful for: /dev/foo" in stderr

    @patch.dict('os.environ', {'CEPH_VOLUME_DMCRYPT_SECRET': 'dmcrypt-key'})
    @patch('ceph_volume.objectstore.rawbluestore.prepare_utils.create_id')
    @patch('ceph_volume.objectstore.rawbluestore.system.generate_uuid')
    def test_prepare(self, m_generate_uuid, m_create_id, is_root, factory):
        m_generate_uuid.return_value = 'fake-uuid'
        m_create_id.return_value = MagicMock()
        self.raw_bs.prepare_dmcrypt = MagicMock()
        self.raw_bs.prepare_osd_req = MagicMock()
        self.raw_bs.osd_mkfs = MagicMock()
        args = factory(crush_device_class='foo',
                       no_tmpfs=False,
                       block_wal='/dev/foo1',
                       block_db='/dev/foo2',)
        self.raw_bs.args = args
        self.raw_bs.secrets = dict()
        self.raw_bs.encrypted = True
        self.raw_bs.prepare()
        assert self.raw_bs.prepare_osd_req.mock_calls == [call(tmpfs=True)]
        assert self.raw_bs.osd_mkfs.called
        assert self.raw_bs.prepare_dmcrypt.called

    @patch('ceph_volume.conf.cluster', 'ceph')
    @patch('ceph_volume.objectstore.rawbluestore.prepare_utils.link_wal')
    @patch('ceph_volume.objectstore.rawbluestore.prepare_utils.link_db')
    @patch('ceph_volume.objectstore.rawbluestore.prepare_utils.link_block')
    @patch('os.path.exists')
    @patch('os.unlink')
    @patch('ceph_volume.objectstore.rawbluestore.prepare_utils.create_osd_path')
    @patch('ceph_volume.objectstore.rawbluestore.process.run')
    def test__activate(self,
                       m_run,
                       m_create_osd_path,
                       m_unlink,
                       m_exists,
                       m_link_block,
                       m_link_db,
                       m_link_wal,
                       monkeypatch,
                       factory):
        args = factory(no_tmpfs=False)
        self.raw_bs.args = args
        self.raw_bs.block_device_path = '/dev/sda'
        self.raw_bs.db_device_path = '/dev/sdb'
        self.raw_bs.wal_device_path = '/dev/sdc'
        m_run.return_value = MagicMock()
        m_exists.side_effect = lambda path: True
        m_create_osd_path.return_value = MagicMock()
        m_unlink.return_value = MagicMock()
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr(system, 'path_is_mounted', lambda path: 0)
        self.raw_bs._activate('1', True)
        calls = [call('/var/lib/ceph/osd/ceph-1/block'),
                 call('/var/lib/ceph/osd/ceph-1/block.db'),
                 call('/var/lib/ceph/osd/ceph-1/block.wal')]
        assert m_run.mock_calls == [call(['ceph-bluestore-tool',
                                         'prime-osd-dir',
                                         '--path', '/var/lib/ceph/osd/ceph-1',
                                         '--no-mon-config', '--dev', '/dev/sda'])]
        assert m_unlink.mock_calls == calls
        assert m_exists.mock_calls == calls
        assert m_create_osd_path.mock_calls == [call('1', tmpfs=True)]

    def test_activate_raises_exception(self,
                                       is_root,
                                       mock_raw_direct_report):
        with pytest.raises(RuntimeError) as error:
            self.raw_bs.osd_id = '1'
            self.raw_bs.activate()
        assert str(error.value) == 'did not find any matching OSD to activate'

    def test_activate_osd_id_and_fsid(self,
                                      is_root,
                                      mock_raw_direct_report):
        self.raw_bs._activate = MagicMock()
        self.raw_bs.osd_id = '8'
        self.raw_bs.osd_fsid = '824f7edf-371f-4b75-9231-4ab62a32d5c0'
        self.raw_bs.activate()
        self.raw_bs._activate.mock_calls == [call({'ceph_fsid': '7dccab18-14cf-11ee-837b-5254008f8ca5',
                                                   'device': '/dev/mapper/ceph--40bc7bd7--4aee--483e--ba95--89a64bc8a4fd-osd--block--824f7edf--371f--4b75--9231--4ab62a32d5c0',
                                                   'device_db': '/dev/mapper/ceph--73d6d4db--6528--48f2--a4e2--1c82bc87a9ac-osd--db--b82d920d--be3c--4e4d--ba64--18f7e8445892',
                                                   'osd_id': 8,
                                                   'osd_uuid': '824f7edf-371f-4b75-9231-4ab62a32d5c0',
                                                   'type': 'bluestore'},
                                                   tmpfs=True)]

    @patch('ceph_volume.objectstore.rawbluestore.encryption_utils.rename_mapper', Mock(return_value=MagicMock()))
    @patch('ceph_volume.util.disk.get_bluestore_header')
    @patch('ceph_volume.objectstore.rawbluestore.encryption_utils.luks_close', Mock(return_value=MagicMock()))
    @patch('ceph_volume.objectstore.rawbluestore.encryption_utils.luks_open', Mock(return_value=MagicMock()))
    def test_activate_dmcrypt_tpm(self, m_bs_header, rawbluestore, fake_lsblk_all, mock_raw_direct_report, is_root) -> None:
        m_bs_header.return_value = {
            "/dev/mapper/activating-sdb": {
            "osd_uuid": "db32a338-b640-4cbc-af17-f63808b1c36e",
            "size": 20000572178432,
            "btime": "2024-06-13T12:16:57.607442+0000",
            "description": "main",
            "bfm_blocks": "4882952192",
            "bfm_blocks_per_key": "128",
            "bfm_bytes_per_block": "4096",
            "bfm_size": "20000572178432",
            "bluefs": "1",
            "ceph_fsid": "c301d0aa-288d-11ef-b535-c84bd6975560",
            "ceph_version_when_created": "ceph version 19.0.0-4242-gf2f7cc60 (f2f7cc609cdbae767486cf2fe6872a4789adffb2) squid (dev)",
            "created_at": "2024-06-13T12:17:20.122565Z",
            "elastic_shared_blobs": "1",
            "kv_backend": "rocksdb",
            "magic": "ceph osd volume v026",
            "mkfs_done": "yes",
            "osd_key": "AQAk42pmt7tqFxAAHlaETFm33yFtEuoQAh/cpQ==",
            "ready": "ready",
            "whoami": "0"}
        }
        mock_luks2_1 = Mock()
        mock_luks2_1.is_ceph_encrypted = True
        mock_luks2_1.is_tpm2_enrolled = True
        mock_luks2_1.osd_fsid = 'db32a338-b640-4cbc-af17-f63808b1c36e'

        mock_luks2_2 = Mock()
        mock_luks2_2.is_ceph_encrypted = True
        mock_luks2_2.is_tpm2_enrolled = False
        mock_luks2_2.osd_fsid = 'db32a338-b640-4cbc-af17-f63808b1c36e'

        mock_luks2_3 = Mock()
        mock_luks2_3.is_ceph_encrypted = False
        mock_luks2_3.is_tpm2_enrolled = False
        mock_luks2_3.osd_fsid = ''

        mock_luks2_4 = Mock()
        mock_luks2_4.is_ceph_encrypted = True
        mock_luks2_4.is_tpm2_enrolled = True
        mock_luks2_4.osd_fsid = 'abcd'
        with patch('ceph_volume.objectstore.rawbluestore.encryption_utils.CephLuks2', side_effect=[mock_luks2_1,
                                                                                                   mock_luks2_2,
                                                                                                   mock_luks2_3,
                                                                                                   mock_luks2_4]):
            fake_lsblk_all([{'NAME': '/dev/sdb', 'FSTYPE': 'crypto_LUKS'},
                            {'NAME': '/dev/sdc', 'FSTYPE': 'crypto_LUKS'},
                            {'NAME': '/dev/sdd', 'FSTYPE': ''}])
            rawbluestore.osd_fsid = 'db32a338-b640-4cbc-af17-f63808b1c36e'
            rawbluestore.osd_id = '0'
            rawbluestore._activate = MagicMock()
            rawbluestore.activate()
            assert rawbluestore._activate.mock_calls == [call(0, 'db32a338-b640-4cbc-af17-f63808b1c36e')]
            assert rawbluestore.block_device_path == '/dev/mapper/ceph-db32a338-b640-4cbc-af17-f63808b1c36e-sdb-block-dmcrypt'
            assert rawbluestore.db_device_path == '/dev/mapper/ceph-db32a338-b640-4cbc-af17-f63808b1c36e-sdc-db-dmcrypt'
