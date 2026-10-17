import pytest
from unittest.mock import patch, MagicMock

from ceph_volume.objectstore import mapping
from ceph_volume.objectstore.raw import RawSeastore


class TestRawSeastoreMapping:
    def test_raw_mapping_includes_seastore(self):
        assert mapping['RAW']['seastore'] is RawSeastore


class TestRawSeastorePrepare:
    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key',
           MagicMock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    def test_prepare_rejects_block_db(self, factory, is_root):
        args = factory(
            objectstore='seastore',
            data='/dev/foo',
            block_db='/dev/db',
            block_wal='',
            no_tmpfs=False,
            dmcrypt=False,
            with_tpm=False,
            osd_id=None,
            osd_type='crimson',
        )
        rs = RawSeastore(args=args)
        with pytest.raises(RuntimeError, match='SeaStore raw OSDs do not support'):
            rs.prepare()

    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key',
           MagicMock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    @patch('ceph_volume.objectstore.raw.Raw.prepare')
    def test_prepare_passes_without_db_wal(self, m_raw_prepare, factory, is_root):
        args = factory(
            objectstore='seastore',
            data='/dev/foo',
            block_db='',
            block_wal='',
            no_tmpfs=False,
            dmcrypt=False,
            with_tpm=False,
            osd_id=None,
            osd_type='crimson',
        )
        rs = RawSeastore(args=args)
        rs.prepare()
        m_raw_prepare.assert_called_once()


class TestRawSeastoreMkfs:
    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key',
           MagicMock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    def test_mkfs_entrypoint_is_crimson(self, factory):
        args = factory(
            objectstore='seastore',
            data='/dev/foo',
            block_db='',
            block_wal='',
            no_tmpfs=False,
            dmcrypt=False,
            with_tpm=False,
            osd_id=None,
            osd_type='crimson',
        )
        rs = RawSeastore(args=args)
        rs.objectstore = 'seastore'
        assert rs.get_default_entrypoint_cmd() == 'ceph-osd-crimson'


class TestRawSeastorePreActivateTpm2:
    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key',
           MagicMock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    @patch('ceph_volume.objectstore.raw.encryption_utils.luks_open',
           MagicMock())
    @patch('ceph_volume.objectstore.raw.encryption_utils.luks_close',
           MagicMock())
    @patch('ceph_volume.objectstore.raw.disk.get_parent_device_from_mapper',
           return_value='sda')
    @patch('ceph_volume.objectstore.raw.disk.BlockSysFs')
    def test_pre_activate_tpm2_builds_block_mapper(self,
                                                    m_blocksysfs,
                                                    m_get_parent,
                                                    factory):
        m_blocksysfs.return_value.has_active_dmcrypt_mapper = False
        args = factory(objectstore='seastore', no_tmpfs=False)
        rs = RawSeastore(args=args)
        rs.osd_fsid = 'aaaa-bbbb'
        rs.devices = []
        rs.pre_activate_tpm2('/dev/sda')
        assert rs.block_device_path == '/dev/mapper/ceph-aaaa-bbbb-sda-block-dmcrypt'
        assert '/dev/mapper/ceph-aaaa-bbbb-sda-block-dmcrypt' in rs.devices

    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key',
           MagicMock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    @patch('ceph_volume.objectstore.raw.disk.BlockSysFs')
    def test_pre_activate_tpm2_skips_when_mapper_already_active(self,
                                                                  m_blocksysfs,
                                                                  factory):
        m_blocksysfs.return_value.has_active_dmcrypt_mapper = True
        args = factory(objectstore='seastore', no_tmpfs=False)
        rs = RawSeastore(args=args)
        rs.osd_fsid = 'aaaa-bbbb'
        rs.devices = []
        rs.pre_activate_tpm2('/dev/sda')
        # no mapper was opened, block_device_path untouched
        assert rs.block_device_path == ''
        assert rs.devices == []


class TestRawSeastoreActivateDmcrypt:
    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key',
           MagicMock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    @patch('ceph_volume.objectstore.raw.RawOsdCryptMappers.backing_device_path',
           return_value='/dev/sda')
    @patch('ceph_volume.objectstore.raw.RawOsdCryptMappers')
    @patch('ceph_volume.objectstore.raw.prepare_utils.link_block', MagicMock())
    @patch('ceph_volume.objectstore.raw.prepare_utils.create_osd_path', MagicMock())
    @patch('ceph_volume.objectstore.raw.system.chown', MagicMock())
    @patch('ceph_volume.objectstore.raw.system.path_is_mounted', return_value=False)
    @patch('ceph_volume.conf.cluster', 'ceph')
    def test_activate_with_dmcrypt_opens_mapper(self,
                                                 m_mounted,
                                                 m_mappers_cls,
                                                 m_backing,
                                                 factory):
        mapper_instance = MagicMock()
        mapper_instance.applies.return_value = True
        mapper_instance.mapper_paths.return_value = (
            '/dev/mapper/ceph-fsid-sda-block-dmcrypt', '', '',
        )
        m_mappers_cls.return_value = mapper_instance

        args = factory(objectstore='seastore', no_tmpfs=False)
        rs = RawSeastore(args=args)
        rs.block_device_path = '/dev/mapper/ceph-fsid-sda-block-dmcrypt'
        rs.osd_id = '1'
        rs.osd_fsid = 'test-fsid'
        rs._activate()

        m_mappers_cls.assert_called_once_with(
            '1', 'test-fsid',
            '/dev/mapper/ceph-fsid-sda-block-dmcrypt',
            cluster_name='ceph',
            dmcrypt_secret=None,
            with_tpm=False,
        )
        mapper_instance.refresh.assert_called_once()
        assert rs.block_device_path == '/dev/mapper/ceph-fsid-sda-block-dmcrypt'

    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key',
           MagicMock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    @patch('ceph_volume.objectstore.raw.RawOsdCryptMappers')
    @patch('ceph_volume.objectstore.raw.prepare_utils.link_block', MagicMock())
    @patch('ceph_volume.objectstore.raw.prepare_utils.create_osd_path', MagicMock())
    @patch('ceph_volume.objectstore.raw.system.chown', MagicMock())
    @patch('ceph_volume.objectstore.raw.system.path_is_mounted', return_value=False)
    @patch('ceph_volume.conf.cluster', 'ceph')
    def test_activate_clear_skips_mappers(self,
                                           m_mounted,
                                           m_mappers_cls,
                                           factory):
        m_mappers_cls.backing_device_path.return_value = ''
        # backing_device_path returns '' → no RawOsdCryptMappers instantiated
        args = factory(objectstore='seastore', no_tmpfs=False)
        rs = RawSeastore(args=args)
        rs.block_device_path = '/dev/sda'
        rs.osd_id = '1'
        rs.osd_fsid = 'test-fsid'
        rs._activate()
        m_mappers_cls.assert_not_called()


class TestRawSeastoreActivate:
    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key')
    @patch('ceph_volume.objectstore.raw.RawSeastore._activate')
    @patch('ceph_volume.objectstore.raw.disk.has_seastore_label', return_value=True)
    @patch('ceph_volume.objectstore.raw.disk.lsblk_all', return_value=[])
    @patch('ceph_volume.objectstore.raw.lvm_api.is_ceph_volume_lvm_prepared',
           return_value=False)
    @patch('ceph_volume.objectstore.raw.lvm_api.ceph_volume_lvm_prepare_lv_paths',
           return_value=[])
    @patch('ceph_volume.objectstore.raw.encryption_utils.CephLuks2',
           return_value=MagicMock(is_ceph_encrypted=False))
    def test_explicit_activate(self, m_luks2, m_lvm_paths, m_is_lvm,
                               m_lsblk, m_label, m_activate,
                               m_create_key, factory, is_root):
        m_create_key.return_value = ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']
        args = factory(
            objectstore='seastore',
            osd_id='7',
            osd_fsid='aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
            devices=['/dev/sdz'],
            no_tmpfs=False,
        )
        rs = RawSeastore(args=args)
        rs.devices = list(args.devices)
        rs.osd_id = args.osd_id
        rs.osd_fsid = args.osd_fsid
        rs.activate()
        assert rs.block_device_path == '/dev/sdz'
        m_label.assert_called()
        m_activate.assert_called_once_with()

    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key')
    def test_activate_requires_both_osd_id_and_fsid(self, m_create_key,
                                                     factory, is_root):
        m_create_key.return_value = ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']
        # osd_id provided but no osd_fsid
        args = factory(objectstore='seastore', osd_id='1', osd_fsid=None,
                       devices=[], no_tmpfs=False)
        rs = RawSeastore(args=args)
        rs.devices = []
        rs.osd_id = '1'
        rs.osd_fsid = ''
        with pytest.raises(RuntimeError, match='requires both --osd-id and --osd-uuid'):
            rs.activate()

    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key')
    def test_activate_requires_at_least_one_arg(self, m_create_key,
                                                factory, is_root):
        m_create_key.return_value = ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']
        args = factory(
            objectstore='seastore',
            osd_id=None,
            osd_fsid=None,
            devices=[],
            no_tmpfs=False,
        )
        rs = RawSeastore(args=args)
        rs.devices = []
        rs.osd_id = ''
        rs.osd_fsid = ''
        with pytest.raises(RuntimeError, match='requires at least'):
            rs.activate()

    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key')
    @patch('ceph_volume.objectstore.raw.RawSeastore._activate')
    @patch('ceph_volume.objectstore.raw.disk.lsblk_all',
           return_value=[{'NAME': '/dev/sda'}])
    @patch('ceph_volume.objectstore.raw.lvm_api.is_ceph_volume_lvm_prepared',
           return_value=False)
    @patch('ceph_volume.objectstore.raw.lvm_api.ceph_volume_lvm_prepare_lv_paths',
           return_value=[])
    @patch('ceph_volume.objectstore.raw.encryption_utils.CephLuks2',
           return_value=MagicMock(is_ceph_encrypted=False))
    def test_activate_dmcrypt_key_based_opens_mapper(self, m_luks2, m_lvm_paths,
                                                     m_is_lvm, m_lsblk,
                                                     m_activate, m_create_key,
                                                     factory, is_root,
                                                     monkeypatch):
        """Key-based dmcrypt: LUKS device matching osd_fsid is opened before
        the seastore signature scan so the magic is readable."""
        m_create_key.return_value = ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']
        luks2_match = MagicMock()
        luks2_match.is_ceph_encrypted = True
        luks2_match.is_tpm2_enrolled = False
        luks2_match.osd_fsid = 'aaaa-bbbb'
        m_luks2.return_value = luks2_match
        monkeypatch.setattr(
            'ceph_volume.objectstore.raw.disk.BlockSysFs',
            lambda dev: MagicMock(has_active_dmcrypt_mapper=False),
        )
        m_luks_open = MagicMock()
        monkeypatch.setattr('ceph_volume.objectstore.raw.encryption_utils.luks_open',
                            m_luks_open)
        monkeypatch.setattr('ceph_volume.objectstore.raw.disk.has_seastore_label',
                            lambda dev: dev.startswith('/dev/mapper/'))
        monkeypatch.setattr('os.path.basename', lambda p: 'sda')
        monkeypatch.setattr('os.path.realpath', lambda p: p)

        args = factory(objectstore='seastore', no_tmpfs=False)
        rs = RawSeastore(args=args)
        rs.devices = []
        rs.osd_id = '1'
        rs.osd_fsid = 'aaaa-bbbb'
        rs.activate()

        m_luks_open.assert_called_once()
        assert rs.block_device_path == '/dev/mapper/ceph-aaaa-bbbb-sda-block-dmcrypt'
        m_activate.assert_called_once_with()

    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key')
    @patch('ceph_volume.objectstore.raw.disk.has_seastore_label', return_value=False)
    @patch('ceph_volume.objectstore.raw.disk.lsblk_all', return_value=[])
    @patch('ceph_volume.objectstore.raw.lvm_api.is_ceph_volume_lvm_prepared',
           return_value=False)
    @patch('ceph_volume.objectstore.raw.lvm_api.ceph_volume_lvm_prepare_lv_paths',
           return_value=[])
    @patch('ceph_volume.objectstore.raw.encryption_utils.CephLuks2',
           return_value=MagicMock(is_ceph_encrypted=False))
    def test_activate_errors_without_signature(self, m_luks2, m_lvm_paths,
                                               m_is_lvm, m_lsblk, m_label,
                                               m_create_key, factory, is_root):
        m_create_key.return_value = ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']
        args = factory(
            objectstore='seastore',
            osd_id='1',
            osd_fsid='aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
            devices=['/dev/sdz'],
            no_tmpfs=False,
        )
        rs = RawSeastore(args=args)
        rs.devices = list(args.devices)
        rs.osd_id = args.osd_id
        rs.osd_fsid = args.osd_fsid
        with pytest.raises(RuntimeError, match='did not find any SeaStore device'):
            rs.activate()

    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key')
    @patch('ceph_volume.objectstore.raw.RawSeastore._activate')
    @patch('ceph_volume.objectstore.raw.disk.lsblk_all',
           return_value=[{'NAME': '/dev/sda'}, {'NAME': '/dev/sdb'}])
    @patch('ceph_volume.objectstore.raw.lvm_api.is_ceph_volume_lvm_prepared',
           return_value=False)
    @patch('ceph_volume.objectstore.raw.lvm_api.ceph_volume_lvm_prepare_lv_paths',
           return_value=[])
    @patch('ceph_volume.objectstore.raw.encryption_utils.CephLuks2',
           return_value=MagicMock(is_ceph_encrypted=False))
    def test_activate_without_devices_scans_all(self, m_luks2, m_lvm_paths,
                                                m_is_lvm, m_lsblk_all,
                                                m_activate,
                                                m_create_key, factory, is_root,
                                                monkeypatch):
        """cephadm path: --osd-id + --osd-uuid without explicit devices.
        Only /dev/sda has a seastore signature, /dev/sdb does not."""
        m_create_key.return_value = ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']
        monkeypatch.setattr(
            'ceph_volume.objectstore.raw.disk.has_seastore_label',
            lambda dev: dev == '/dev/sda',
        )
        args = factory(
            objectstore='seastore',
            osd_id='3',
            osd_fsid='aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
            no_tmpfs=False,
        )
        rs = RawSeastore(args=args)
        rs.devices = []
        rs.osd_id = '3'
        rs.osd_fsid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
        rs.activate()
        assert rs.block_device_path == '/dev/sda'
        m_activate.assert_called_once_with()

    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key')
    @patch('ceph_volume.objectstore.raw.RawSeastore._activate')
    @patch('ceph_volume.objectstore.raw.disk.lsblk_all',
           return_value=[{'NAME': '/dev/sda'}, {'NAME': '/dev/dm-0'}])
    @patch('ceph_volume.objectstore.raw.lvm_api.ceph_volume_lvm_prepare_lv_paths',
           return_value=['/dev/dm-0'])
    @patch('ceph_volume.objectstore.raw.encryption_utils.CephLuks2',
           return_value=MagicMock(is_ceph_encrypted=False))
    def test_activate_skips_lvm_prepared_devices(self, m_luks2, m_lvm_paths,
                                                 m_lsblk_all, m_activate,
                                                 m_create_key, factory, is_root,
                                                 monkeypatch):
        """LVM-prepared devices must be excluded from both the LUKS pre-scan and
        the candidate scan so they fall through to LVMActivate instead of being
        claimed by the raw SeaStore path."""
        m_create_key.return_value = ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']
        # /dev/dm-0 is LVM-prepared; /dev/sda is a raw SeaStore device.
        # is_ceph_volume_lvm_prepared returns True only for the LVM path.
        monkeypatch.setattr(
            'ceph_volume.objectstore.raw.lvm_api.is_ceph_volume_lvm_prepared',
            lambda dev, paths: dev == '/dev/dm-0',
        )
        monkeypatch.setattr(
            'ceph_volume.objectstore.raw.disk.has_seastore_label',
            lambda dev: dev == '/dev/sda',
        )
        args = factory(
            objectstore='seastore',
            osd_id='5',
            osd_fsid='bbbbbbbb-cccc-dddd-eeee-ffffffffffff',
            no_tmpfs=False,
        )
        rs = RawSeastore(args=args)
        rs.devices = []
        rs.osd_id = '5'
        rs.osd_fsid = 'bbbbbbbb-cccc-dddd-eeee-ffffffffffff'
        rs.activate()
        # Only /dev/sda (the raw device) should be picked up.
        assert rs.block_device_path == '/dev/sda'
        m_activate.assert_called_once_with()
