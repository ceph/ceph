import argparse
import os

import pytest

from ceph_volume.api import lvm as api
from ceph_volume.devices.lvm import migrate_block_devices
from ceph_volume.devices.lvm.migrate_block_devices import (
    MigrateBlockDevices,
    is_lvm_prefix_layout,
    parse_what,
    restore_size,
)


def make_volume(**kw):
    defaults = {
        'lv_name': 'osd-block-1',
        'lv_uuid': 'lv-uuid',
        'vg_name': 'ceph-1',
        'lv_path': '/dev/ceph-1/osd-block-1',
        'lv_size': '107374182400',
        'lv_tags': '',
    }
    defaults.update(kw)
    return api.Volume(**defaults)


class TestIsLvmPrefixLayout(object):

    def test_all_raw(self):
        assert is_lvm_prefix_layout(['raw', 'raw', 'raw', 'raw'])

    def test_all_lvm(self):
        assert is_lvm_prefix_layout(['lvm', 'lvm', 'lvm', 'lvm'])

    def test_lvm_prefix(self):
        assert is_lvm_prefix_layout(['lvm', 'lvm', 'raw', 'raw'])

    def test_single_lvm(self):
        assert is_lvm_prefix_layout(['lvm'])

    def test_single_raw(self):
        assert is_lvm_prefix_layout(['raw'])

    def test_raw_below_lvm(self):
        assert not is_lvm_prefix_layout(['raw', 'raw', 'lvm', 'lvm'])

    def test_mixed_raw_below_lvm(self):
        assert not is_lvm_prefix_layout(['lvm', 'raw', 'lvm'])

    def test_none_entries_are_ignored(self):
        assert is_lvm_prefix_layout(['lvm', None, 'raw', None])

    def test_none_entries_are_ignored_reversed(self):
        assert not is_lvm_prefix_layout(['lvm', None, 'raw', None, 'lvm'])

    def test_empty(self):
        assert is_lvm_prefix_layout([])


class TestParseWhat(object):

    def test_default_both(self):
        assert parse_what('db,wal') == ['db', 'wal']

    def test_single_role(self):
        assert parse_what('db') == ['db']
        assert parse_what('wal') == ['wal']

    def test_order_is_preserved(self):
        assert parse_what('wal,db') == ['wal', 'db']

    def test_spaces_are_stripped(self):
        assert parse_what(' db , wal ') == ['db', 'wal']

    def test_duplicates_are_removed(self):
        assert parse_what('db,db,wal') == ['db', 'wal']

    def test_invalid_role(self):
        with pytest.raises(RuntimeError) as error:
            parse_what('journal')
        assert 'invalid value for --what' in str(error.value)

    def test_empty_value(self):
        with pytest.raises(RuntimeError) as error:
            parse_what('')
        assert '--what cannot be empty' in str(error.value)

    def test_commas_only(self):
        with pytest.raises(RuntimeError) as error:
            parse_what(',,')
        assert '--what cannot be empty' in str(error.value)


class TestRestoreSize(object):

    def test_image_fits(self):
        assert restore_size(100, 200) == (100, False)

    def test_image_fits_exactly(self):
        assert restore_size(200, 200) == (200, False)

    def test_image_is_trimmed(self):
        assert restore_size(300, 200) == (200, True)

    def test_zero_target(self):
        assert restore_size(100, 0) == (0, True)


class TestParser(object):

    def make_args(self, argv):
        m = MigrateBlockDevices([])
        parser = m.make_parser('ceph-volume lvm migrate-block-devices', '')
        return parser.parse_args(argv)

    def test_defaults(self):
        args = self.make_args(['--osd-id', '1'])
        assert args.what == 'db,wal'
        assert args.strategy == 'in-place'
        assert args.fsck == 'shallow'
        assert args.maintenance_flags is True
        assert args.dry_run is False
        assert args.force_unhealthy is False

    def test_no_maintenance_flags(self):
        args = self.make_args(['--osd-id', '1', '--no-maintenance-flags'])
        assert args.maintenance_flags is False

    def test_dry_run(self):
        args = self.make_args(['--osd-id', '1', '--dry-run'])
        assert args.dry_run is True

    def test_osd_id_is_normalized(self):
        args = self.make_args(['--osd-id', '012'])
        assert args.osd_id == '12'

    def test_invalid_strategy(self):
        with pytest.raises(SystemExit):
            self.make_args(['--osd-id', '1', '--strategy', 'bogus'])

    def test_invalid_fsck(self):
        with pytest.raises(SystemExit):
            self.make_args(['--osd-id', '1', '--fsck', 'bogus'])

    def test_invalid_what_is_caught_at_runtime(self):
        # --what is a plain string at the argparse level, invalid values
        # are rejected by parse_what()
        args = self.make_args(['--osd-id', '1', '--what', 'journal'])
        with pytest.raises(RuntimeError):
            parse_what(args.what)


class TestMain(object):

    def test_no_args_prints_help(self, capsys):
        MigrateBlockDevices([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'migrate-block-devices' in stdout

    def test_recreate_strategy_is_not_implemented(self):
        m = MigrateBlockDevices(['--osd-id', '1',
                                 '--strategy', 'recreate-osd'])
        with pytest.raises(SystemExit) as error:
            m.main()
        assert 'not implemented' in str(error.value)

    def test_missing_backup_path(self, monkeypatch, is_root):
        data_lv = make_volume(
            lv_tags='ceph.osd_id=1,ceph.osd_fsid=osd-fsid,'
                    'ceph.cluster_name=ceph,ceph.cluster_fsid=cluster-fsid,'
                    'ceph.type=block,ceph.encrypted=0,'
                    'ceph.db_uuid=db-partuuid,ceph.wal_uuid=wal-partuuid')

        def mock_get_lvs(tags=None, **kwargs):
            if tags and tags.get('ceph.type') in ('db', 'wal'):
                return []
            return [data_lv]

        monkeypatch.setattr(migrate_block_devices.api, 'get_lvs',
                            mock_get_lvs)
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_device_from_partuuid',
            lambda partuuid: '/dev/nvme0n1p%d' % (1 if 'db' in partuuid else 2))
        monkeypatch.setattr(migrate_block_devices.disk, 'is_partition',
                            lambda dev: True)
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_partitions',
            lambda: {'nvme0n1p1': 'nvme0n1', 'nvme0n1p2': 'nvme0n1'})
        monkeypatch.setattr(migrate_block_devices.api, 'get_device_lvs',
                            lambda device: [])
        monkeypatch.setattr(migrate_block_devices.disk,
                            'get_bluestore_header', lambda device: {})
        monkeypatch.setattr(
            migrate_block_devices.MigrateBlockDevices, 'partition_size',
            staticmethod(lambda partition: 10 * 1024 * 1024 * 1024))

        m = MigrateBlockDevices(['--osd-id', '1'])
        with pytest.raises(SystemExit) as error:
            m.main()
        assert '--backup-path is required' in str(error.value)

    def test_backup_path_required_for_in_place(self, monkeypatch, is_root):
        # an OSD with partition-backed db/wal must not proceed without a
        # backup path
        data_lv = make_volume(
            lv_tags='ceph.osd_id=0,ceph.osd_fsid=osd-fsid,'
                    'ceph.cluster_name=ceph,ceph.cluster_fsid=cluster-fsid,'
                    'ceph.type=block,ceph.encrypted=0,'
                    'ceph.db_uuid=db-partuuid,ceph.wal_uuid=wal-partuuid')

        def mock_get_lvs(tags=None, **kwargs):
            if tags and tags.get('ceph.type') in ('db', 'wal'):
                return []
            return [data_lv]

        monkeypatch.setattr(migrate_block_devices.api, 'get_lvs',
                            mock_get_lvs)
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_device_from_partuuid',
            lambda partuuid: '/dev/nvme0n1p%d' % (1 if 'db' in partuuid else 2))

        m = MigrateBlockDevices(['--osd-id', '0'])
        m.args = m.make_parser('ceph-volume lvm migrate-block-devices',
                               '').parse_args(['--osd-id', '0'])
        m.roles = ['db', 'wal']
        m.find_osd_devices()
        assert m.partitions == {'db': '/dev/nvme0n1p1',
                                'wal': '/dev/nvme0n1p2'}
        with pytest.raises(SystemExit) as error:
            m.check_backup_path()
        assert '--backup-path is required' in str(error.value)


class TestFindOsdDevices(object):

    def setup_method(self):
        self.m = MigrateBlockDevices([])
        self.m.args = argparse.Namespace(osd_id='0')
        self.m.roles = ['db', 'wal']

    def test_partition_backed_roles(self, monkeypatch):
        data_lv = make_volume(
            lv_tags='ceph.osd_id=0,ceph.osd_fsid=osd-fsid,'
                    'ceph.cluster_name=ceph,ceph.cluster_fsid=cluster-fsid,'
                    'ceph.type=block,ceph.encrypted=0,'
                    'ceph.db_uuid=db-partuuid,ceph.wal_uuid=wal-partuuid')

        def mock_get_lvs(tags=None, **kwargs):
            if tags and tags.get('ceph.type') in ('db', 'wal'):
                return []
            return [data_lv]

        monkeypatch.setattr(migrate_block_devices.api, 'get_lvs',
                            mock_get_lvs)
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_device_from_partuuid',
            lambda partuuid: '/dev/nvme0n1p%d' % (1 if 'db' in partuuid else 2))

        self.m.find_osd_devices()
        assert self.m.data_lv == data_lv
        assert self.m.osd_fsid == 'osd-fsid'
        assert self.m.cluster_name == 'ceph'
        assert self.m.partitions == {'db': '/dev/nvme0n1p1',
                                     'wal': '/dev/nvme0n1p2'}
        assert self.m.existing_lvs == {}

    def test_already_migrated_is_a_noop(self, monkeypatch):
        data_lv = make_volume(
            lv_tags='ceph.osd_id=0,ceph.osd_fsid=osd-fsid,'
                    'ceph.cluster_name=ceph,ceph.cluster_fsid=cluster-fsid,'
                    'ceph.type=block,ceph.encrypted=0,'
                    'ceph.db_uuid=db-lv-uuid,ceph.wal_uuid=wal-lv-uuid')
        db_lv = make_volume(
            lv_name='osd-db-1', lv_path='/dev/ceph-db/osd-db-1',
            lv_uuid='db-lv-uuid',
            lv_tags='ceph.osd_id=0,ceph.osd_fsid=osd-fsid,ceph.type=db')
        wal_lv = make_volume(
            lv_name='osd-wal-1', lv_path='/dev/ceph-wal/osd-wal-1',
            lv_uuid='wal-lv-uuid',
            lv_tags='ceph.osd_id=0,ceph.osd_fsid=osd-fsid,ceph.type=wal')

        def mock_get_lvs(tags=None, **kwargs):
            if tags and tags.get('ceph.type') == 'db':
                return [db_lv]
            if tags and tags.get('ceph.type') == 'wal':
                return [wal_lv]
            return [data_lv]

        monkeypatch.setattr(migrate_block_devices.api, 'get_lvs',
                            mock_get_lvs)

        self.m.find_osd_devices()
        assert self.m.partitions == {}
        assert self.m.existing_lvs == {'db': db_lv, 'wal': wal_lv}

    def test_no_lvs_at_all(self, monkeypatch):
        monkeypatch.setattr(migrate_block_devices.api, 'get_lvs',
                            lambda tags=None, **kwargs: [])
        with pytest.raises(SystemExit) as error:
            self.m.find_osd_devices()
        assert 'Unable to find any LV' in str(error.value)

    def test_encrypted_osd_is_rejected(self, monkeypatch):
        data_lv = make_volume(
            lv_tags='ceph.osd_id=0,ceph.osd_fsid=osd-fsid,'
                    'ceph.cluster_name=ceph,ceph.cluster_fsid=cluster-fsid,'
                    'ceph.type=block,ceph.encrypted=1,'
                    'ceph.db_uuid=db-partuuid,ceph.wal_uuid=wal-partuuid')

        def mock_get_lvs(tags=None, **kwargs):
            if tags and tags.get('ceph.type') in ('db', 'wal'):
                return []
            return [data_lv]

        monkeypatch.setattr(migrate_block_devices.api, 'get_lvs',
                            mock_get_lvs)
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_device_from_partuuid',
            lambda partuuid: '/dev/nvme0n1p1')

        self.m.find_osd_devices()
        with pytest.raises(SystemExit) as error:
            self.m.preflight()
        assert 'encrypted' in str(error.value)

    def test_missing_role_is_rejected(self, monkeypatch):
        data_lv = make_volume(
            lv_tags='ceph.osd_id=0,ceph.osd_fsid=osd-fsid,'
                    'ceph.cluster_name=ceph,ceph.cluster_fsid=cluster-fsid,'
                    'ceph.type=block,ceph.encrypted=0,'
                    'ceph.db_uuid=db-partuuid')

        def mock_get_lvs(tags=None, **kwargs):
            if tags and tags.get('ceph.type') in ('db', 'wal'):
                return []
            return [data_lv]

        monkeypatch.setattr(migrate_block_devices.api, 'get_lvs',
                            mock_get_lvs)
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_device_from_partuuid',
            lambda partuuid: '/dev/nvme0n1p1')

        self.m.find_osd_devices()
        with pytest.raises(SystemExit) as error:
            self.m.preflight()
        assert 'does not have a block.wal device' in str(error.value)


class TestCheckLayoutPrefix(object):

    def setup_method(self):
        self.m = MigrateBlockDevices([])
        self.m.args = argparse.Namespace(osd_id='0')
        self.m.roles = ['db', 'wal']

    def test_greenfield_layout_is_accepted(self, monkeypatch):
        self.m.partitions = {'db': '/dev/nvme0n1p1',
                             'wal': '/dev/nvme0n1p2'}
        monkeypatch.setattr(
            self.m, 'disk_partitions',
            lambda parent: ['/dev/nvme0n1p%d' % n for n in range(1, 9)])
        monkeypatch.setattr(
            migrate_block_devices.api, 'get_device_lvs', lambda device: [])
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_bluestore_header',
            lambda device: {device: {'description': 'bluefs db'}}
            if device not in ('/dev/nvme0n1p1', '/dev/nvme0n1p2') else {})
        monkeypatch.setattr(
            self.m, 'parent_device',
            lambda partition: '/dev/nvme0n1')
        # must not raise
        self.m.check_layout_prefix()

    def test_raw_pair_below_lvm_pair_is_rejected(self, monkeypatch):
        # osd at p5/p6 was recreated on LVM, p1-p4 are still raw
        self.m.partitions = {'db': '/dev/nvme0n1p1',
                             'wal': '/dev/nvme0n1p2'}
        monkeypatch.setattr(
            self.m, 'disk_partitions',
            lambda parent: ['/dev/nvme0n1p%d' % n for n in range(1, 7)])

        lvm_parts = ('/dev/nvme0n1p5', '/dev/nvme0n1p6')

        def mock_get_device_lvs(device):
            if device in lvm_parts:
                return [make_volume(lv_tags='ceph.type=db')]
            return []

        def mock_get_bluestore_header(device):
            if device in lvm_parts:
                return {}
            return {device: {'description': 'bluefs db'}}

        monkeypatch.setattr(migrate_block_devices.api, 'get_device_lvs',
                            mock_get_device_lvs)
        monkeypatch.setattr(migrate_block_devices.disk,
                            'get_bluestore_header',
                            mock_get_bluestore_header)
        monkeypatch.setattr(
            self.m, 'parent_device',
            lambda partition: '/dev/nvme0n1')
        with pytest.raises(SystemExit) as error:
            self.m.check_layout_prefix()
        assert 'must form a prefix' in str(error.value)

    def test_migrating_the_straggler_fixes_layout(self, monkeypatch):
        # p1/p2 raw (migrating), p3/p4 lvm: after migration everything
        # below the first lvm pair is lvm, this is a valid prefix
        self.m.partitions = {'db': '/dev/nvme0n1p1',
                             'wal': '/dev/nvme0n1p2'}
        monkeypatch.setattr(
            self.m, 'disk_partitions',
            lambda parent: ['/dev/nvme0n1p%d' % n for n in range(1, 5)])

        lvm_parts = ('/dev/nvme0n1p3', '/dev/nvme0n1p4')

        def mock_get_device_lvs(device):
            if device in lvm_parts:
                return [make_volume(lv_tags='ceph.type=wal')]
            return []

        monkeypatch.setattr(migrate_block_devices.api, 'get_device_lvs',
                            mock_get_device_lvs)
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_bluestore_header',
            lambda device: {})
        monkeypatch.setattr(
            self.m, 'parent_device',
            lambda partition: '/dev/nvme0n1')
        # must not raise
        self.m.check_layout_prefix()

    def test_classify_partition(self, monkeypatch):
        db_lv = make_volume(lv_tags='ceph.type=db')
        monkeypatch.setattr(migrate_block_devices.api, 'get_device_lvs',
                            lambda device: [db_lv])
        assert self.m.classify_partition('/dev/nvme0n1p1', False) == 'lvm'

        monkeypatch.setattr(migrate_block_devices.api, 'get_device_lvs',
                            lambda device: [])
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_bluestore_header',
            lambda device: {device: {'description': 'bluefs wal'}})
        assert self.m.classify_partition('/dev/nvme0n1p2', False) == 'raw'

        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_bluestore_header',
            lambda device: {device: {'description': 'main'}})
        assert self.m.classify_partition('/dev/nvme0n1p3', False) is None

        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_bluestore_header',
            lambda device: {})
        assert self.m.classify_partition('/dev/nvme0n1p4', False) is None

        assert self.m.classify_partition('/dev/nvme0n1p5', True) == 'lvm'


class TestBackupSpace(object):

    def test_partition_size(self, monkeypatch):
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_file_contents',
            lambda path, default='': '2048')
        assert MigrateBlockDevices.partition_size('/dev/nvme0n1p1') == \
            2048 * 512

    def test_partition_size_failure(self, monkeypatch):
        monkeypatch.setattr(
            migrate_block_devices.disk, 'get_file_contents',
            lambda path, default='': '0')
        assert MigrateBlockDevices.partition_size('/dev/nvme0n1p1') == 0

    def test_backup_space_required_includes_margin(self, monkeypatch):
        m = MigrateBlockDevices([])
        monkeypatch.setattr(
            migrate_block_devices.MigrateBlockDevices, 'partition_size',
            staticmethod(lambda partition: 100 * 1024 * 1024))
        m.partitions = {'db': '/dev/nvme0n1p1', 'wal': '/dev/nvme0n1p2'}
        assert m.backup_space_required() == int(
            200 * 1024 * 1024 * (1 + migrate_block_devices.BACKUP_SPACE_MARGIN))

    def test_check_backup_path_requires_space(self, monkeypatch, tmpdir):
        m = MigrateBlockDevices([])
        m.args = argparse.Namespace(backup_path=str(tmpdir))
        monkeypatch.setattr(
            migrate_block_devices.MigrateBlockDevices, 'partition_size',
            staticmethod(lambda partition: 100 * 1024 * 1024))
        m.partitions = {'db': '/dev/nvme0n1p1', 'wal': '/dev/nvme0n1p2'}

        class FakeStatVfs(object):
            f_bavail = 10
            f_frsize = 4096

        monkeypatch.setattr(os, 'statvfs', lambda path: FakeStatVfs())
        with pytest.raises(SystemExit) as error:
            m.check_backup_path()
        assert 'not enough free space' in str(error.value)

    def test_check_backup_path_missing_dir(self):
        m = MigrateBlockDevices([])
        m.args = argparse.Namespace(backup_path='/does/not/exist')
        with pytest.raises(SystemExit) as error:
            m.check_backup_path()
        assert 'not an existing directory' in str(error.value)


class TestBackupDevices(object):

    def test_backup_and_metadata(self, monkeypatch, tmpdir):
        m = MigrateBlockDevices([])
        m.args = argparse.Namespace(osd_id='0', backup_path=str(tmpdir))
        m.roles_pending = ['db', 'wal']
        m.partitions = {'db': '/dev/nvme0n1p1', 'wal': '/dev/nvme0n1p2'}
        m.osd_fsid = 'osd-fsid'
        m.cluster_name = 'ceph'

        monkeypatch.setattr(
            migrate_block_devices.MigrateBlockDevices, 'partition_size',
            staticmethod(lambda partition: 4096))
        monkeypatch.setattr(migrate_block_devices.disk, 'get_partuuid',
                            lambda device: '%s-partuuid' % device)

        calls = []

        def fake_run(command, **kw):
            calls.append(command)
            if command[0] == 'dd':
                # simulate dd creating the backup image
                image = command[2].split('=', 1)[1]
                with open(image, 'wb') as f:
                    f.write(b'\0' * 4096)

        monkeypatch.setattr(migrate_block_devices.process, 'run', fake_run)
        m.backup_devices()

        assert len(calls) == 2
        assert calls[0] == ['dd', 'if=/dev/nvme0n1p1',
                            'of={}'.format(
                                os.path.join(str(tmpdir), 'osd-0-db.img')),
                            'bs=4M', 'conv=fsync']
        assert m.backup_images == {
            'db': os.path.join(str(tmpdir), 'osd-0-db.img'),
            'wal': os.path.join(str(tmpdir), 'osd-0-wal.img'),
        }

        with open(os.path.join(str(tmpdir),
                               'ceph-volume-migrate-0.json')) as f:
            import json
            metadata = json.load(f)
        assert metadata['osd_id'] == '0'
        assert metadata['osd_fsid'] == 'osd-fsid'
        assert metadata['cluster_name'] == 'ceph'
        assert metadata['devices'] == [
            {'role': 'db', 'device': '/dev/nvme0n1p1',
             'partuuid': '/dev/nvme0n1p1-partuuid', 'size': 4096,
             'image': os.path.join(str(tmpdir), 'osd-0-db.img')},
            {'role': 'wal', 'device': '/dev/nvme0n1p2',
             'partuuid': '/dev/nvme0n1p2-partuuid', 'size': 4096,
             'image': os.path.join(str(tmpdir), 'osd-0-wal.img')},
        ]

    def test_incomplete_backup_is_an_error(self, monkeypatch, tmpdir):
        m = MigrateBlockDevices([])
        m.args = argparse.Namespace(osd_id='0', backup_path=str(tmpdir))
        m.roles_pending = ['db']
        m.partitions = {'db': '/dev/nvme0n1p1'}

        monkeypatch.setattr(
            migrate_block_devices.MigrateBlockDevices, 'partition_size',
            staticmethod(lambda partition: 4096))

        def fake_run(command, **kw):
            # dd "succeeds" but produces a short image
            image = command[2].split('=', 1)[1]
            with open(image, 'wb') as f:
                f.write(b'\0' * 512)

        monkeypatch.setattr(migrate_block_devices.process, 'run', fake_run)
        with pytest.raises(RuntimeError) as error:
            m.backup_devices()
        assert 'incomplete' in str(error.value)


class TestConvertPartition(object):

    def test_wipe_create_and_tag(self, monkeypatch):
        data_lv = make_volume(
            lv_tags='ceph.osd_id=0,ceph.osd_fsid=osd-fsid,'
                    'ceph.cluster_name=ceph,ceph.cluster_fsid=cluster-fsid,'
                    'ceph.crush_device_class=hdd,'
                    'ceph.osdspec_affinity=,ceph.objectstore=bluestore,'
                    'ceph.type=block,ceph.encrypted=0,ceph.with_tpm=0')
        m = MigrateBlockDevices([])
        m.data_lv = data_lv
        m.partitions = {'db': '/dev/nvme0n1p1'}

        new_lv = make_volume(
            lv_name='osd-db-1', lv_path='/dev/ceph-db/osd-db-1',
            lv_uuid='new-db-uuid', lv_tags='ceph.osd_id=0')

        calls = []

        def fake_run(command, **kw):
            calls.append(command)

        monkeypatch.setattr(migrate_block_devices.process, 'run', fake_run)
        monkeypatch.setattr(migrate_block_devices.zap, 'zap_device',
                            lambda path: calls.append(('zap', path)))
        monkeypatch.setattr(migrate_block_devices.api, 'create_pv',
                            lambda device: calls.append(
                                ('pvcreate', device)))
        monkeypatch.setattr(
            migrate_block_devices.api, 'create_vg',
            lambda devices, name_prefix='': api.VolumeGroup(
                vg_name='ceph-db-vg', pv_name='/dev/nvme0n1p1'))
        create_lv_args = {}

        def fake_create_lv(**kw):
            create_lv_args.update(kw)
            return new_lv

        monkeypatch.setattr(migrate_block_devices.api, 'create_lv',
                            fake_create_lv)
        monkeypatch.setattr(migrate_block_devices.api, 'is_vdo',
                            lambda path: '0')

        set_tags_calls = []
        monkeypatch.setattr(
            api.Volume, 'set_tags',
            lambda self, tags: set_tags_calls.append((self, tags)))

        result = m.convert_partition('db')
        assert result == new_lv
        assert ('zap', '/dev/nvme0n1p1') in calls
        assert ('pvcreate', '/dev/nvme0n1p1') in calls
        # tag parity with what prepare sets on db volumes
        assert create_lv_args['name_prefix'] == 'osd-db'
        assert create_lv_args['tags'] == {
            'ceph.osd_fsid': 'osd-fsid',
            'ceph.osd_id': '0',
            'ceph.cluster_fsid': 'cluster-fsid',
            'ceph.cluster_name': 'ceph',
            'ceph.crush_device_class': 'hdd',
            'ceph.osdspec_affinity': '',
            'ceph.objectstore': 'bluestore',
            'ceph.encrypted': '0',
            'ceph.with_tpm': '0',
            'ceph.type': 'db',
        }
        # the uuid tag is applied once the LV exists
        assert set_tags_calls == [(new_lv, {'ceph.db_uuid': 'new-db-uuid',
                                            'ceph.vdo': '0'})]


class TestUpdateOsdMetadata(object):

    def test_tags_and_symlink_are_updated(self, monkeypatch, tmpdir):
        data_lv = make_volume(
            lv_tags='ceph.osd_id=0,ceph.osd_fsid=osd-fsid,'
                    'ceph.cluster_name=ceph,ceph.type=block,'
                    'ceph.db_uuid=old-partuuid')
        new_lv = make_volume(
            lv_name='osd-db-1', lv_path='/dev/ceph-db/osd-db-1',
            lv_uuid='new-db-uuid', lv_tags='ceph.osd_id=0')

        m = MigrateBlockDevices([])
        m.data_lv = data_lv
        m.new_lvs = {'db': new_lv}
        m.partitions = {'db': '/dev/nvme0n1p1'}
        m.args = argparse.Namespace(osd_id='0')
        m.cluster_name = 'ceph'

        osd_path = os.path.join(str(tmpdir), 'ceph-0')
        os.makedirs(osd_path)
        # existing symlink pointing at the partition
        link = os.path.join(osd_path, 'block.db')
        os.symlink('/dev/nvme0n1p1', link)

        # point the osd path at the temporary directory
        monkeypatch.setattr(MigrateBlockDevices, 'osd_path',
                            property(lambda self: osd_path))

        set_tags_calls = []
        monkeypatch.setattr(
            api.Volume, 'set_tags',
            lambda self, tags: set_tags_calls.append((self, tags)))
        monkeypatch.setattr(migrate_block_devices.system, 'chown',
                            lambda path, recursive=True: None)

        def fake_run(command, **kw):
            assert command[0] == 'ln'
            # emulate ln -s
            os.symlink(command[2], command[3])

        monkeypatch.setattr(migrate_block_devices.process, 'run', fake_run)

        m.update_osd_metadata('db')

        assert set_tags_calls == [(data_lv,
                                   {'ceph.db_uuid': 'new-db-uuid',
                                    'ceph.db_device':
                                        '/dev/ceph-db/osd-db-1'})]
        assert os.readlink(link) == '/dev/ceph-db/osd-db-1'


class TestRestoreDevice(object):

    def test_restore_uses_size_guard(self, monkeypatch, tmpdir):
        new_lv = make_volume(
            lv_name='osd-db-1', lv_path='/dev/ceph-db/osd-db-1',
            lv_size=str(100 * 1024 * 1024))
        image = os.path.join(str(tmpdir), 'osd-0-db.img')
        with open(image, 'wb') as f:
            f.write(b'\0' * (110 * 1024 * 1024))

        m = MigrateBlockDevices([])
        m.new_lvs = {'db': new_lv}
        m.backup_images = {'db': image}

        calls = []
        monkeypatch.setattr(migrate_block_devices.process, 'run',
                            lambda command, **kw: calls.append(command))

        m.restore_device('db')
        assert calls == [['dd',
                          'if={}'.format(image),
                          'of=/dev/ceph-db/osd-db-1',
                          'bs=4M',
                          'iflag=count_bytes',
                          'count={}'.format(100 * 1024 * 1024),
                          'conv=fsync']]


class TestFailureReporting(object):

    def test_flags_and_backups_are_reported(self, capsys):
        m = MigrateBlockDevices([])
        m.args = argparse.Namespace(osd_id='0', backup_path='/backup')
        m.cluster_name = 'ceph'
        m.flags_set = list(migrate_block_devices.MAINTENANCE_FLAGS)
        m.backup_images = {'db': '/backup/osd-0-db.img'}
        m.report_failure()
        stdout, stderr = capsys.readouterr()
        assert 'ceph --cluster ceph osd unset noout' in stderr
        assert 'ceph --cluster ceph osd unset norebalance' in stderr
        assert 'ceph --cluster ceph osd unset nobackfill' in stderr
        assert '/backup' in stderr
