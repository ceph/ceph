from mock import patch, DEFAULT, Mock
import argparse
import pytest
import ceph_disk

def fail_to_mount(dev, fstype, options):
    raise ceph_disk.MountError(dev + " mount fail")

class TestCephDisk(object):

    def setup_class(self):
        ceph_disk.setup_logging(verbose=True, log_stdout=False)

    def test_main_list_json(self, capsys):
        args = ceph_disk.parse_args(['list', '--format', 'json'])
        with patch.multiple(
                ceph_disk,
                list_devices=lambda args: {}):
            ceph_disk.main_list(args)
            out, err = capsys.readouterr()
            assert '{}\n' == out

    def test_main_list_plain(self, capsys):
        args = ceph_disk.parse_args(['list'])
        with patch.multiple(
                ceph_disk,
                list_devices=lambda args: {}):
            ceph_disk.main_list(args)
            out, err = capsys.readouterr()
            assert '' == out

    def test_list_format_more_osd_info_plain(self):
        dev = {
            'ceph_fsid': 'UUID',
            'cluster': 'ceph',
            'whoami': '1234',
            'journal_dev': '/dev/Xda2',
        }
        out = ceph_disk.list_format_more_osd_info_plain(dev)
        assert dev['cluster'] in " ".join(out)
        assert dev['journal_dev'] in " ".join(out)
        assert dev['whoami'] in " ".join(out)

        dev = {
            'ceph_fsid': 'UUID',
            'whoami': '1234',
            'journal_dev': '/dev/Xda2',
        }
        out = ceph_disk.list_format_more_osd_info_plain(dev)
        assert 'unknown cluster' in " ".join(out)

    def test_list_format_plain(self):
        payload = [{
            'path': '/dev/Xda',
            'ptype': 'unknown',
            'type': 'other',
            'mount': '/somewhere',
        }]
        out = ceph_disk.list_format_plain(payload)
        assert payload[0]['path'] in out
        assert payload[0]['type'] in out
        assert payload[0]['mount'] in out

        payload = [{
            'path': '/dev/Xda1',
            'ptype': 'unknown',
            'type': 'swap',
        }]
        out = ceph_disk.list_format_plain(payload)
        assert payload[0]['path'] in out
        assert payload[0]['type'] in out

        payload = [{
            'path': '/dev/Xda',
            'partitions': [
                {
                    'dmcrypt': {},
                    'ptype': 'whatever',
                    'is_partition': True,
                    'fs_type': 'ext4',
                    'path': '/dev/Xda1',
                    'mounted': '/somewhere',
                    'type': 'other',
                }
            ],
        }]
        out = ceph_disk.list_format_plain(payload)
        assert payload[0]['path'] in out
        assert payload[0]['partitions'][0]['path'] in out

    def test_list_format_dev_plain(dev):
        #
        # data
        #
        dev = {
            'path': '/dev/Xda1',
            'ptype': ceph_disk.OSD_UUID,
            'state': 'prepared',
            'whoami': '1234',
        }
        out = ceph_disk.list_format_dev_plain(dev)
        assert 'data' in out
        assert dev['whoami'] in out
        assert dev['state'] in out
        #
        # journal
        #
        dev = {
            'path': '/dev/Xda2',
            'ptype': ceph_disk.JOURNAL_UUID,
            'journal_for': '/dev/Xda1',
        }
        out = ceph_disk.list_format_dev_plain(dev)
        assert 'journal' in out
        assert dev['journal_for'] in out

        #
        # dmcrypt data
        #
        ptype2type = {
            ceph_disk.DMCRYPT_OSD_UUID: 'plain',
            ceph_disk.DMCRYPT_LUKS_OSD_UUID: 'LUKS',
        }
        for (ptype, type) in ptype2type.iteritems():
            for holders in ((), ("dm_0",), ("dm_0", "dm_1")):
                devices = [{
                    'path': '/dev/dm_0',
                    'whoami': '1234',
                }]
                dev = {
                    'dmcrypt': {
                        'holders': holders,
                        'type': type,
                    },
                    'path': '/dev/Xda1',
                    'ptype': ptype,
                    'state': 'prepared',
                }
                out = ceph_disk.list_format_dev_plain(dev, devices)
                assert 'data' in out
                assert 'dmcrypt' in out
                assert type in out
                if len(holders) == 1:
                    assert devices[0]['whoami'] in out
                for holder in holders:
                    assert holder in out

        #
        # dmcrypt journal
        #
        ptype2type = {
            ceph_disk.DMCRYPT_JOURNAL_UUID: 'plain',
            ceph_disk.DMCRYPT_LUKS_JOURNAL_UUID: 'LUKS',
        }
        for (ptype, type) in ptype2type.iteritems():
            for holders in ((), ("dm_0",)):
                dev = {
                    'path': '/dev/Xda2',
                    'ptype': ptype,
                    'journal_for': '/dev/Xda1',
                    'dmcrypt': {
                        'holders': holders,
                        'type': type,
                    },
                }
                out = ceph_disk.list_format_dev_plain(dev, devices)
                assert 'journal' in out
                assert 'dmcrypt' in out
                assert type in out
                assert dev['journal_for'] in out
                if len(holders) == 1:
                    assert holders[0] in out

    def test_list_dev_osd(self):
        dev = "Xda"
        mount_path = '/mount/path'
        fs_type = 'ext4'
        cluster = 'ceph'
        uuid_map = {}
        def more_osd_info(path, uuid_map, desc):
            desc['cluster'] = cluster
        #
        # mounted therefore active
        #
        with patch.multiple(
                ceph_disk,
                is_mounted=lambda dev: mount_path,
                get_dev_fs=lambda dev: fs_type,
                more_osd_info=more_osd_info
        ):
            desc = {}
            ceph_disk.list_dev_osd(dev, uuid_map, desc)
            assert {'cluster': 'ceph',
                    'fs_type': 'ext4',
                    'mount': '/mount/path',
                    'state': 'active'} == desc
        #
        # not mounted and cannot mount: unprepared
        #
        mount_path = None
        with patch.multiple(
                ceph_disk,
                is_mounted=lambda dev: mount_path,
                get_dev_fs=lambda dev: fs_type,
                mount=fail_to_mount,
                more_osd_info=more_osd_info
        ):
            desc = {}
            ceph_disk.list_dev_osd(dev, uuid_map, desc)
            assert {'fs_type': 'ext4',
                    'mount': mount_path,
                    'state': 'unprepared'} == desc
        #
        # not mounted and magic found: prepared
        #
        def get_oneliner(path, what):
            if what == 'magic':
                return ceph_disk.CEPH_OSD_ONDISK_MAGIC
            else:
                raise Exception('unknown ' + what)
        with patch.multiple(
                ceph_disk,
                is_mounted=lambda dev: mount_path,
                get_dev_fs=lambda dev: fs_type,
                mount=DEFAULT,
                unmount=DEFAULT,
                get_oneliner=get_oneliner,
                more_osd_info=more_osd_info
        ):
            desc = {}
            ceph_disk.list_dev_osd(dev, uuid_map, desc)
            assert {'cluster': 'ceph',
                    'fs_type': 'ext4',
                    'mount': mount_path,
                    'magic': ceph_disk.CEPH_OSD_ONDISK_MAGIC,
                    'state': 'prepared'} == desc

    @patch('os.path.exists')
    def test_list_paths_to_names(self, m_exists):

        def exists(path):
            return path in (
                '/sys/block/sda',
                '/sys/block/sdb',
                '/sys/block/cciss!c0d0',
                '/sys/block/cciss!c0d1',
                '/sys/block/cciss!c0d2',
            )

        m_exists.side_effect = exists
        paths = [
            '/dev/sda',
            '/dev/cciss/c0d0',
            'cciss!c0d1',
            'cciss/c0d2',
            'sdb',
        ]
        expected = [
            'sda',
            'cciss!c0d0',
            'cciss!c0d1',
            'cciss!c0d2',
            'sdb',
        ]
        assert expected == ceph_disk.list_paths_to_names(paths)
        with pytest.raises(ceph_disk.Error) as excinfo:
            ceph_disk.list_paths_to_names(['unknown'])
        assert 'unknown' in excinfo.value.message

    def test_list_all_partitions(self):
        partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
        disk = "Xda"
        partition = "Xda1"

        with patch(
                'ceph_disk.os',
                listdir=lambda path: [disk],
        ), patch.multiple(
            ceph_disk,
            list_partitions=lambda dev: [partition],
        ):
                assert {disk: [partition]} == ceph_disk.list_all_partitions([])

        with patch.multiple(
                ceph_disk,
                list_partitions=lambda dev: [partition],
        ):
                assert {disk: [partition]} == ceph_disk.list_all_partitions([disk])

    def test_list_data(self):
        args = ceph_disk.parse_args(['list'])
        #
        # a data partition that fails to mount is silently
        # ignored
        #
        partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
        disk = "Xda"
        partition = "Xda1"
        fs_type = "ext4"

        with patch.multiple(
                ceph_disk,
                list_all_partitions=lambda names: { disk: [partition] },
                get_partition_uuid=lambda dev: partition_uuid,
                get_partition_type=lambda dev: ceph_disk.OSD_UUID,
                get_dev_fs=lambda dev: fs_type,
                mount=fail_to_mount,
                unmount=DEFAULT,
                is_partition=lambda dev: True,
                ):
            expect = [{'path': '/dev/' + disk,
                       'partitions': [{
                           'dmcrypt': {},
                           'fs_type': fs_type,
                           'is_partition': True,
                           'mount': None,
                           'path': '/dev/' + partition,
                           'ptype': ceph_disk.OSD_UUID,
                           'state': 'unprepared',
                           'type': 'data',
                           'uuid': partition_uuid,
                       }]}]
            assert expect == ceph_disk.list_devices(args)

    def test_list_dmcrypt_data(self):
        args = ceph_disk.parse_args(['list'])
        partition_type2type = {
            ceph_disk.DMCRYPT_OSD_UUID: 'plain',
            ceph_disk.DMCRYPT_LUKS_OSD_UUID: 'LUKS',
        }
        for (partition_type, type) in partition_type2type.iteritems():
            #
            # dmcrypt data partition with one holder
            #
            partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
            disk = "Xda"
            partition = "Xda1"
            holders = ["dm-0"]
            with patch.multiple(
                    ceph_disk,
                    is_held=lambda dev: holders,
                    list_all_partitions=lambda names: { disk: [partition] },
                    get_partition_uuid=lambda dev: partition_uuid,
                    get_partition_type=lambda dev: partition_type,
                    is_partition=lambda dev: True,
                    ):
                expect = [{'path': '/dev/' + disk,
                           'partitions': [{
                               'dmcrypt': {
                                   'holders': holders,
                                   'type': type,
                               },
                               'fs_type': None,
                               'is_partition': True,
                               'mount': None,
                               'path': '/dev/' + partition,
                               'ptype': partition_type,
                               'state': 'unprepared',
                               'type': 'data',
                               'uuid': partition_uuid,
                           }]}]
                assert expect == ceph_disk.list_devices(args)
            #
            # dmcrypt data partition with two holders
            #
            partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
            disk = "Xda"
            partition = "Xda1"
            holders = ["dm-0","dm-1"]
            with patch.multiple(
                    ceph_disk,
                    is_held=lambda dev: holders,
                    list_all_partitions=lambda names: { disk: [partition] },
                    get_partition_uuid=lambda dev: partition_uuid,
                    get_partition_type=lambda dev: partition_type,
                    is_partition=lambda dev: True,
                    ):
                expect = [{'path': '/dev/' + disk,
                           'partitions': [{
                               'dmcrypt': {
                                   'holders': holders,
                                   'type': type,
                               },
                               'is_partition': True,
                               'path': '/dev/' + partition,
                               'ptype': partition_type,
                               'type': 'data',
                               'uuid': partition_uuid,
                           }]}]
                assert expect == ceph_disk.list_devices(args)

    def test_list_multipath(self):
        args = ceph_disk.parse_args(['list'])
        #
        # multipath data partition
        #
        partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
        disk = "Xda"
        partition = "Xda1"
        with patch.multiple(
                ceph_disk,
                list_all_partitions=lambda names: { disk: [partition] },
                get_partition_uuid=lambda dev: partition_uuid,
                get_partition_type=lambda dev: ceph_disk.MPATH_OSD_UUID,
                is_partition=lambda dev: True,
                ):
            expect = [{'path': '/dev/' + disk,
                       'partitions': [{
                           'dmcrypt': {},
                           'fs_type': None,
                           'is_partition': True,
                           'mount': None,
                           'multipath': True,
                           'path': '/dev/' + partition,
                           'ptype': ceph_disk.MPATH_OSD_UUID,
                           'state': 'unprepared',
                           'type': 'data',
                           'uuid': partition_uuid,
                       }]}]
            assert expect == ceph_disk.list_devices(args)
        #
        # multipath journal partition
        #
        journal_partition_uuid = "2cc40457-259e-4542-b029-785c7cc37871"
        with patch.multiple(
                ceph_disk,
                list_all_partitions=lambda names: { disk: [partition] },
                get_partition_uuid=lambda dev: journal_partition_uuid,
                get_partition_type=lambda dev: ceph_disk.MPATH_JOURNAL_UUID,
                is_partition=lambda dev: True,
                ):
            expect = [{'path': '/dev/' + disk,
                       'partitions': [{
                           'dmcrypt': {},
                           'is_partition': True,
                           'multipath': True,
                           'path': '/dev/' + partition,
                           'ptype': ceph_disk.MPATH_JOURNAL_UUID,
                           'type': 'journal',
                           'uuid': journal_partition_uuid,
                       }]}]
            assert expect == ceph_disk.list_devices(args)

    def test_list_dmcrypt(self):
        self.list(ceph_disk.DMCRYPT_OSD_UUID, ceph_disk.DMCRYPT_JOURNAL_UUID)
        self.list(ceph_disk.DMCRYPT_LUKS_OSD_UUID, ceph_disk.DMCRYPT_LUKS_JOURNAL_UUID)

    def test_list_normal(self):
        self.list(ceph_disk.OSD_UUID, ceph_disk.JOURNAL_UUID)

    def list(self, data_ptype, journal_ptype):
        args = ceph_disk.parse_args(['--verbose', 'list'])
        #
        # a single disk has a data partition and a journal
        # partition and the osd is active
        #
        data_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
        disk = "Xda"
        data = "Xda1"
        data_holder = "dm-0"
        journal = "Xda2"
        journal_holder = "dm-0"
        mount_path = '/mount/path'
        fs_type = 'ext4'
        journal_uuid = "7ad5e65a-0ca5-40e4-a896-62a74ca61c55"
        ceph_fsid = "60a2ef70-d99b-4b9b-a83c-8a86e5e60091"
        osd_id = '1234'
        def get_oneliner(path, what):
            if what == 'journal_uuid':
                return journal_uuid
            elif what == 'ceph_fsid':
                return ceph_fsid
            elif what == 'whoami':
                return osd_id
            else:
                raise Exception('unknown ' + what)
        def get_partition_uuid(dev):
            if dev == '/dev/' + data:
                return data_uuid
            elif dev == '/dev/' + journal:
                return journal_uuid
            else:
                raise Exception('unknown ' + dev)
        def get_partition_type(dev):
            if (dev == '/dev/' + data or
                dev == '/dev/' + data_holder):
                return data_ptype
            elif (dev == '/dev/' + journal or
                  dev == '/dev/' + journal_holder):
                return journal_ptype
            else:
                raise Exception('unknown ' + dev)
        cluster = 'ceph'
        if data_ptype == ceph_disk.OSD_UUID:
            data_dmcrypt = {}
        elif data_ptype == ceph_disk.DMCRYPT_OSD_UUID:
            data_dmcrypt = {
                'type': 'plain',
                'holders': [data_holder],
            }
        elif data_ptype == ceph_disk.DMCRYPT_LUKS_OSD_UUID:
            data_dmcrypt = {
                'type': 'LUKS',
                'holders': [data_holder],
            }
        else:
            raise Exception('unknown ' + data_ptype)

        if journal_ptype == ceph_disk.JOURNAL_UUID:
            journal_dmcrypt = {}
        elif journal_ptype == ceph_disk.DMCRYPT_JOURNAL_UUID:
            journal_dmcrypt = {
                'type': 'plain',
                'holders': [journal_holder],
            }
        elif journal_ptype == ceph_disk.DMCRYPT_LUKS_JOURNAL_UUID:
            journal_dmcrypt = {
                'type': 'LUKS',
                'holders': [journal_holder],
            }
        else:
            raise Exception('unknown ' + journal_ptype)

        if data_dmcrypt:
            def is_held(dev):
                if dev == '/dev/' + data:
                    return [data_holder]
                elif dev == '/dev/' + journal:
                    return [journal_holder]
                else:
                    raise Exception('unknown ' + dev)
        else:
            def is_held(dev):
                return []

        with patch.multiple(
                ceph_disk,
                list_all_partitions=lambda names: { disk: [data, journal] },
                get_dev_fs=lambda dev: fs_type,
                is_mounted=lambda dev: mount_path,
                get_partition_uuid=get_partition_uuid,
                get_partition_type=get_partition_type,
                find_cluster_by_uuid=lambda ceph_fsid: cluster,
                is_partition=lambda dev: True,
                mount=DEFAULT,
                unmount=DEFAULT,
                get_oneliner=get_oneliner,
                is_held=is_held,
                ):
            expect = [{'path': '/dev/' + disk,
                       'partitions': [{
                           'ceph_fsid': ceph_fsid,
                           'cluster': cluster,
                           'dmcrypt': data_dmcrypt,
                           'fs_type': fs_type,
                           'is_partition': True,
                           'journal_dev': '/dev/' + journal,
                           'journal_uuid': journal_uuid,
                           'mount': mount_path,
                           'path': '/dev/' + data,
                           'ptype': data_ptype,
                           'state': 'active',
                           'type': 'data',
                           'whoami': osd_id,
                           'uuid': data_uuid,
                       }, {
                           'dmcrypt': journal_dmcrypt,
                           'is_partition': True,
                           'journal_for': '/dev/' + data,
                           'path': '/dev/' + journal,
                           'ptype': journal_ptype,
                           'type': 'journal',
                           'uuid': journal_uuid,
                       },
                                  ]}]
            assert expect == ceph_disk.list_devices(args)

    def test_list_other(self):
        args = ceph_disk.parse_args(['list'])
        #
        # not swap, unknown fs type, not mounted, with uuid
        #
        partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
        partition_type = "e51adfb9-e9fd-4718-9fc1-7a0cb03ea3f4"
        disk = "Xda"
        partition = "Xda1"
        with patch.multiple(
                ceph_disk,
                list_all_partitions=lambda names: { disk: [partition] },
                get_partition_uuid=lambda dev: partition_uuid,
                get_partition_type=lambda dev: partition_type,
                is_partition=lambda dev: True,
                ):
            expect = [{'path': '/dev/' + disk,
                       'partitions': [{'dmcrypt': {},
                                       'is_partition': True,
                                       'path': '/dev/' + partition,
                                       'ptype': partition_type,
                                       'type': 'other',
                                       'uuid': partition_uuid}]}]
            assert expect == ceph_disk.list_devices(args)
        #
        # not swap, mounted, ext4 fs type, with uuid
        #
        partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
        partition_type = "e51adfb9-e9fd-4718-9fc1-7a0cb03ea3f4"
        disk = "Xda"
        partition = "Xda1"
        mount_path = '/mount/path'
        fs_type = 'ext4'
        with patch.multiple(
                ceph_disk,
                list_all_partitions=lambda names: { disk: [partition] },
                get_dev_fs=lambda dev: fs_type,
                is_mounted=lambda dev: mount_path,
                get_partition_uuid=lambda dev: partition_uuid,
                get_partition_type=lambda dev: partition_type,
                is_partition=lambda dev: True,
                ):
            expect = [{'path': '/dev/' + disk,
                       'partitions': [{'dmcrypt': {},
                                       'is_partition': True,
                                       'mount': mount_path,
                                       'fs_type': fs_type,
                                       'path': '/dev/' + partition,
                                       'ptype': partition_type,
                                       'type': 'other',
                                       'uuid': partition_uuid,
                                   }]}]
            assert expect == ceph_disk.list_devices(args)

        #
        # swap, with uuid
        #
        partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
        partition_type = "e51adfb9-e9fd-4718-9fc1-7a0cb03ea3f4"
        disk = "Xda"
        partition = "Xda1"
        with patch.multiple(
                ceph_disk,
                list_all_partitions=lambda names: { disk: [partition] },
                is_swap=lambda dev: True,
                get_partition_uuid=lambda dev: partition_uuid,
                get_partition_type=lambda dev: partition_type,
                is_partition=lambda dev: True,
                ):
            expect = [{'path': '/dev/' + disk,
                       'partitions': [{'dmcrypt': {},
                                       'is_partition': True,
                                       'path': '/dev/' + partition,
                                       'ptype': partition_type,
                                       'type': 'swap',
                                       'uuid': partition_uuid}]}]
            assert expect == ceph_disk.list_devices(args)

        #
        # whole disk
        #
        partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
        disk = "Xda"
        partition = "Xda1"
        with patch.multiple(
                ceph_disk,
                list_all_partitions=lambda names: { disk: [] },
                is_partition=lambda dev: False,
                ):
            expect = [{'path': '/dev/' + disk,
                       'dmcrypt': {},
                       'is_partition': False,
                       'ptype': 'unknown',
                       'type': 'other'}]
            assert expect == ceph_disk.list_devices(args)
