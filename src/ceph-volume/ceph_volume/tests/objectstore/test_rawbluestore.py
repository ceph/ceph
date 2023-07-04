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
        # self.raw_bs.args = args
        self.raw_bs.safe_prepare(args)
        stdout, stderr = capsys.readouterr()
        assert "prepare successful for: /dev/foo" in stderr

    # @patch('ceph_volume.objectstore.rawbluestore.prepare_utils.create_id')
    # @patch('ceph_volume.objectstore.rawbluestore.system.generate_uuid', return_value='fake-uuid')
    @patch.dict('os.environ', {'CEPH_VOLUME_DMCRYPT_SECRET': 'dmcrypt-key'})
    @patch('ceph_volume.objectstore.rawbluestore.prepare_utils.create_id')
    @patch('ceph_volume.objectstore.rawbluestore.system.generate_uuid')
    def test_prepare(self, m_generate_uuid, m_create_id, factory):
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
                       monkeypatch):
        meta = dict(osd_id='1',
                    osd_uuid='fake-uuid',
                    device='/dev/foo',
                    device_db='/dev/foo1',
                    device_wal='/dev/foo2')
        m_run.return_value = MagicMock()
        m_exists.side_effect = lambda path: True
        m_create_osd_path.return_value = MagicMock()
        m_unlink.return_value = MagicMock()
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr(system, 'path_is_mounted', lambda path: 0)
        self.raw_bs._activate(meta, True)
        calls = [call('/var/lib/ceph/osd/ceph-1/block'),
                 call('/var/lib/ceph/osd/ceph-1/block.db'),
                 call('/var/lib/ceph/osd/ceph-1/block.wal')]
        assert m_run.mock_calls == [call(['ceph-bluestore-tool',
                                         'prime-osd-dir',
                                         '--path', '/var/lib/ceph/osd/ceph-1',
                                         '--no-mon-config', '--dev', '/dev/foo'])]
        assert m_unlink.mock_calls == calls
        assert m_exists.mock_calls == calls
        assert m_create_osd_path.mock_calls == [call('1', tmpfs=True)]

    def test_activate_raises_exception(self,
                                       mock_raw_direct_report):
        with pytest.raises(RuntimeError) as error:
            self.raw_bs.activate([],
                                '123',
                                'fake-uuid',
                                True)
        assert str(error.value) == 'did not find any matching OSD to activate'

    def test_activate_osd_id(self,
                             mock_raw_direct_report):
        self.raw_bs._activate = MagicMock()
        self.raw_bs.activate([],
                            '8',
                            '824f7edf-371f-4b75-9231-4ab62a32d5c0',
                            True)
        self.raw_bs._activate.mock_calls == [call({'ceph_fsid': '7dccab18-14cf-11ee-837b-5254008f8ca5',
                                                   'device': '/dev/mapper/ceph--40bc7bd7--4aee--483e--ba95--89a64bc8a4fd-osd--block--824f7edf--371f--4b75--9231--4ab62a32d5c0',
                                                   'device_db': '/dev/mapper/ceph--73d6d4db--6528--48f2--a4e2--1c82bc87a9ac-osd--db--b82d920d--be3c--4e4d--ba64--18f7e8445892',
                                                   'osd_id': 8,
                                                   'osd_uuid': '824f7edf-371f-4b75-9231-4ab62a32d5c0',
                                                   'type': 'bluestore'},
                                                  tmpfs=True)]

    def test_activate_osd_fsid(self,
                               mock_raw_direct_report):
        self.raw_bs._activate = MagicMock()
        with pytest.raises(RuntimeError):
            self.raw_bs.activate([],
                                '8',
                                'a0e07c5b-bee1-4ea2-ae07-cb89deda9b27',
                                True)
        self.raw_bs._activate.mock_calls == [call({'ceph_fsid': '7dccab18-14cf-11ee-837b-5254008f8ca5',
                                                   'device': '/dev/mapper/ceph--e34cc3f5--a70d--49df--82b3--46bcbd63d4b0-osd--block--a0e07c5b--bee1--4ea2--ae07--cb89deda9b27',
                                                   'osd_id': 9,
                                                   'osd_uuid': 'a0e07c5b-bee1-4ea2-ae07-cb89deda9b27',
                                                   'type': 'bluestore'},
                                                  tmpfs=True)]