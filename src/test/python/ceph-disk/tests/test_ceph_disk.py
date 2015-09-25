from mock import patch, DEFAULT, Mock
import os
import subprocess
import unittest
import argparse
import pytest
import ceph_disk
import StringIO

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
                with patch.multiple(
                        ceph_disk,
                        list_devices=lambda path: devices,
                        ):
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

class TestCephDiskDeactivateAndDestroy(unittest.TestCase):

    def setup_class(self):
        ceph_disk.setup_logging(verbose=True, log_stdout=False)

    def test_main_deactivate_by_id(self):
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '--deactivate-by-id', '5566'])
        mount_path = '/tmp'
        with patch.multiple(
                ceph_disk,
                _check_osd_status=lambda ceph, osd_id: 3,
                stop_daemon=lambda ceph, osd_id: True,
                convert_osd_id=lambda ceph, osd_id: ['dev', mount_path],
                _remove_osd_directory_files=lambda mount_path, ceph: True,
                unmount=lambda path: True,
                ):
            ceph_disk.main_deactivate(args)
            # clear the created file by unit test
            os.remove(os.path.join(mount_path, 'deactive'))

    def test_main_deactivate_osd_directory_non_exists_by_id(self):
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '--deactivate-by-id', '5566'])
        mount_path = '/tmp'
        with patch.multiple(
                ceph_disk,
                _check_osd_status=lambda ceph, osd_id: 3,
                stop_daemon=lambda ceph, osd_id: True,
                convert_osd_id=lambda ceph, osd_id: ['dev', '/somewhere'],
                _remove_osd_directory_files=lambda mount_path, ceph: True,
                unmount=lambda path: True,
                ):
            self.assertRaises(IOError, ceph_disk.main_deactivate, args)
            # clear the created file by unit test

    def test_main_deactivate_non_exists_non_cluster_by_dev(self):
        args = ceph_disk.parse_args(['deactivate', \
                                     '/dev/Xda1'])
        self.assertRaises(Exception, ceph_disk.main_deactivate, args)

    def test_main_deactivate_non_mounted_by_dev(self):
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '/dev/Xda1'])
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        with patch.multiple(
                ceph_disk,
                patch_path,
                is_mounted=lambda dev:None,
                ):
            self.assertRaises(Exception, ceph_disk.main_deactivate, args)

    def test_main_deactivate_osd_in_and_down_by_dev(self):
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '--mark-out', \
                                     '/dev/Xda1'])
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        mount_path = '/mnt'
        with patch.multiple(
                ceph_disk,
                patch_path,
                is_mounted=lambda dev: mount_path,
                get_oneliner=lambda mount_path, filen: 5566,
                _check_osd_status=lambda ceph, status: 2,
                _mark_osd_out=lambda ceph, osd_id: True
                ):
            ceph_disk.main_deactivate(args)

    def test_main_deactivate_osd_out_and_down_by_dev(self):
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '/dev/Xda1'])
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        mount_path = '/mnt'
        with patch.multiple(
                ceph_disk,
                patch_path,
                is_mounted=lambda dev: mount_path,
                get_oneliner=lambda mount_path, filen: 5566,
                _check_osd_status=lambda ceph, osd_id: 0,
                ):
            ceph_disk.main_deactivate(args)

    def test_main_deactivate_osd_out_and_up_by_dev(self):
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '/dev/Xda1'])
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        ceph = 'ceph'
        mount_path = '/tmp'
        with patch.multiple(
                ceph_disk,
                patch_path,
                is_mounted=lambda dev: mount_path,
                get_oneliner=lambda mount_path, filen: 5566,
                _check_osd_status=lambda ceph, osd_id: 1,
                stop_daemon=lambda ceph, osd_id: True,
                _remove_osd_directory_files=lambda mount_path, ceph: True,
                unmount=lambda path: True,
                ):
            ceph_disk.main_deactivate(args)
            # clear the created file by unit test
            os.remove(os.path.join(mount_path, 'deactive'))

    def test_main_deactivate_osd_in_and_up_by_dev(self):
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '--mark-out', \
                                     '/dev/Xda1'])
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        ceph = 'ceph'
        mount_path = '/tmp'
        with patch.multiple(
                ceph_disk,
                patch_path,
                is_mounted=lambda dev: mount_path,
                get_oneliner=lambda mount_path, filen: 5566,
                _check_osd_status=lambda ceph, osd_id: 3,
                _mark_osd_out=lambda ceph, osd_id: True,
                stop_daemon=lambda ceph, osd_id: True,
                _remove_osd_directory_files=lambda mount_path, ceph: True,
                unmount=lambda path: True,
                ):
            ceph_disk.main_deactivate(args)
            # clear the created file by unit test
            os.remove(os.path.join(mount_path, 'deactive'))

    def test_mark_out_out(self):
        dev = {
            'cluster': 'ceph',
            'osd_id': '5566',
        }

        def mark_osd_out_fail(osd_id):
            raise ceph_disk.Error('Could not find osd.%s, is a vaild/exist osd id?' % osd_id)

        with patch.multiple(
                ceph_disk,
                command=mark_osd_out_fail,
                ):
            self.assertRaises(Exception, ceph_disk._mark_osd_out, 'ceph', '5566')

    def test_check_osd_status_fail(self):
        with patch.multiple(
                ceph_disk,
                command=raise_command_error,
                ):
            self.assertRaises(Exception, ceph_disk._check_osd_status, 'ceph', '5566')

    def test_check_osd_status_osd_not_found(self):

        fake_value = '{"osds":[{"osd":0,"up":1,"in":1},{"osd":1,"up":1,"in":1}]}'

        def return_fake_value(cmd):
            return fake_value, 0

        with patch.multiple(
                ceph_disk,
                command=return_fake_value,
                ):
            #ceph_disk._check_osd_status('ceph', '5566')
            self.assertRaises(Exception, ceph_disk._check_osd_status, 'ceph', '5566')

    def test_check_osd_status_success(self):

        fake_value = '{"osds":[{"osd":0,"up":1,"in":1},{"osd":5566,"up":1,"in":1}]}'

        def return_fake_value(cmd):
            return fake_value, 0

        with patch.multiple(
                ceph_disk,
                command=return_fake_value,
                ):
            ceph_disk._check_osd_status('ceph', '5566')

    @patch('os.path.exists', return_value=False)
    def test_stop_daemon_fail_all_init_type(self, mock_path_exists):
        self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

    @patch('os.path.exists', return_value=Exception)
    def test_stop_daemon_fail_on_os_path_check(self, mock_path_exists):
        self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

    def test_stop_daemon_fail_upstart(self):
        STATEDIR = '/var/lib/ceph'
        cluster = 'ceph'
        osd_id = '5566'

        path = (STATEDIR + '/osd/{cluster}-{osd_id}/upstart').format(
               cluster=cluster, osd_id=osd_id)

        def path_file_test(check_path):
            if check_path == path:
                return True
            else:
                False

        def stop_daemon_fail(cmd):
            raise Exception('ceph osd stop failed')

        patcher = patch('os.path.exists')
        check_path = patcher.start()
        check_path.side_effect = path_file_test
        with patch.multiple(
                ceph_disk,
                check_path,
                command_check_call=stop_daemon_fail,
                ):
            self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

    def test_stop_daemon_fail_sysvinit_usr_sbin_service(self):
        STATEDIR = '/var/lib/ceph'
        cluster = 'ceph'
        osd_id = '5566'

        path = (STATEDIR + '/osd/{cluster}-{osd_id}/sysvinit').format(
        cluster=cluster, osd_id=osd_id)

        def path_file_test(check_path):
            if check_path == path:
                return True
            elif check_path == '/usr/sbin/service':
                return True
            else:
                False

        def stop_daemon_fail(cmd):
            raise Exception('ceph osd stop failed')

        patcher = patch('os.path.exists')
        check_path = patcher.start()
        check_path.side_effect = path_file_test
        with patch.multiple(
                ceph_disk,
                check_path,
                #join_path,
                command_check_call=stop_daemon_fail,
                ):
            self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

    def test_stop_daemon_fail_sysvinit_sbin_service(self):
        STATEDIR = '/var/lib/ceph'
        cluster = 'ceph'
        osd_id = '5566'

        path = (STATEDIR + '/osd/{cluster}-{osd_id}/sysvinit').format(
        cluster=cluster, osd_id=osd_id)

        def path_file_test(check_path):
            if check_path == path:
                return True
            elif check_path == '/sbin/service':
                return True
            else:
                False

        def stop_daemon_fail(cmd):
            raise Exception('ceph osd stop failed')

        patcher = patch('os.path.exists')
        check_path = patcher.start()
        check_path.side_effect = path_file_test
        with patch.multiple(
                ceph_disk,
                check_path,
                command_check_call=stop_daemon_fail,
                ):
            self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

    def test_stop_daemon_fail_systemd_disable_stop(self):
        STATEDIR = '/var/lib/ceph'
        cluster = 'ceph'
        osd_id = '5566'

        path = (STATEDIR + '/osd/{cluster}-{osd_id}/systemd').format(
        cluster=cluster, osd_id=osd_id)

        def path_file_test(check_path):
            if check_path == path:
                return True
            else:
                False

        def stop_daemon_fail(cmd):
            if 'stop' in cmd:
                raise Exception('ceph osd stop failed')
            else:
                return True

        patcher = patch('os.path.exists')
        check_path = patcher.start()
        check_path.side_effect = path_file_test
        with patch.multiple(
                ceph_disk,
                check_path,
                command_check_call=stop_daemon_fail,
                ):
            self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

    def test_convert_osd_id(self):
        file_output = StringIO.StringIO('/dev/sdX1 /var/lib/ceph/osd/ceph-1234 xfs rw,noatime 0 0\n' \
                                        '/dev/sdX1 /var/lib/ceph/osd/ceph-5566 xfs rw,noatime 0 0\n')
        with patch('__builtin__.open', return_value=file_output):
            ceph_disk.convert_osd_id('ceph', '5566')

    def test_convert_osd_id_not_found(self):
        file_output = StringIO.StringIO('/dev/sdX1 /var/lib/ceph/osd/ceph-1234 xfs rw,noatime 0 0\n' \
                                        '/dev/sdY1 /var/lib/ceph/osd/ceph-5678 xfs rw,noatime 0 0\n')
        with patch('__builtin__.open', return_value=file_output):
            self.assertRaises(Exception, ceph_disk.convert_osd_id, 'ceph', '5566')

    def test_convert_osd_id_get_mounts_fail(self):
        with patch('__builtin__.open', return_value=Exception):
            self.assertRaises(Exception, ceph_disk.convert_osd_id, 'ceph', '5566')

    def test_convert_osd_id_fail(self):
        dev = {
            'cluster': 'ceph',
            'osd_id': '5566',
            'mount_info': '[]',
        }
        self.assertRaises(Exception, ceph_disk.convert_osd_id, dev)

    @patch('os.remove', return_value=True)
    def test_remove_osd_directory_files(self, mock_remove):
        cluster = 'ceph'
        init_file = 'init'
        with patch.multiple(
                ceph_disk,
                get_conf=lambda cluster, **kwargs: init_file,
                ):
            ceph_disk._remove_osd_directory_files('/somewhere', cluster)

    def test_remove_osd_directory_files_remove_OSError(self):
        cluster = 'ceph'
        init_file = 'init'
        with patch.multiple(
                ceph_disk,
                get_conf=lambda cluster, **kwargs: None,
                init_get=lambda : init_file
                ):
            ceph_disk._remove_osd_directory_files('/somewhere', cluster)

    @patch('os.path.exists', return_value=False)
    def test_remove_osd_directory_files_already_remove(self, mock_exists):
        cluster = 'ceph'
        init_file = 'upstart'
        with patch.multiple(
                ceph_disk,
                get_conf=lambda cluster, **kwargs: init_file,
                ):
            ceph_disk._remove_osd_directory_files('/tmp', cluster)

    def test_path_set_context(self):
        path = '/somewhere'
        with patch.multiple(
                ceph_disk,
                get_ceph_user=lambda **kwargs: 'ceph',
                ):
            ceph_disk.path_set_context(path)

    def test_mount_dev_none(self):
        dev = None
        fs_type = 'ext4'
        option = ''
        self.assertRaises(Exception, ceph_disk.mount, dev, fs_type, option)

    def test_mount_fstype_none(self):
        dev = '/dev/Xda1'
        fs_type = None
        option = ''
        self.assertRaises(Exception, ceph_disk.mount, dev, fs_type, option)

    def test_mount_fail(self):
        dev = '/dev/Xda1'
        fstype = 'ext4'
        options = ''
        with patch('tempfile.mkdtemp', return_value='/mnt'):
            self.assertRaises(Exception, ceph_disk.mount, dev, fstype, options)

    def test_mount(self):
        def create_temp_directory(*args, **kwargs):
            return '/mnt'

        dev = '/dev/Xda1'
        fstype = 'ext4'
        options = ''
        patcher = patch('tempfile.mkdtemp')
        create_tmpdir = patcher.start()
        create_tmpdir.side_effect = create_temp_directory
        with patch.multiple(
                ceph_disk,
                create_tmpdir,
                command_check_call=lambda cmd: True,
                ):
            ceph_disk.mount(dev, fstype, options)

    def test_unmount_fail(self):
        path = '/somewhere'
        self.assertRaises(Exception, ceph_disk.unmount, path)

    def test_unmount(self):
        def remove_directory_successfully(path):
            return True

        path = '/somewhere'
        patcher = patch('os.rmdir')
        rm_directory = patcher.start()
        rm_directory.side_effect = remove_directory_successfully
        with patch.multiple(
                ceph_disk,
                rm_directory,
                command_check_call=lambda cmd: True,
                ):
            ceph_disk.unmount(path)

    def test_main_destroy_without_zap_by_id(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--destroy-by-id', '5566'])
        cluster = 'ceph'
        osd_id = '5566'
        with patch.multiple(
                ceph_disk,
                _check_osd_status=lambda cluster, osd_id: 0,
                _remove_from_crush_map=lambda cluster, osd_id: True,
                _delete_osd_auth_key=lambda cluster, osd_id: True,
                _deallocate_osd_id=lambda cluster, osd_id: True,
                ):
            ceph_disk.main_destroy(args)

    def test_main_destroy_with_zap_find_part_fail_by_id(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--zap', \
                                     '--destroy-by-id', '5566'])
        cluster = 'ceph'
        osd_id = '5566'
        fake_part_return = {'Xda': ['Xda1'], 'Xdb': []}
        disk = 'Xda'
        partition = 'Xda1'
        with patch.multiple(
                ceph_disk,
                _check_osd_status=lambda cluster, osd_id: 0,
                _remove_from_crush_map=lambda cluster, osd_id: True,
                _delete_osd_auth_key=lambda cluster, osd_id: True,
                _deallocate_osd_id=lambda cluster, osd_id: True,
                list_all_partitions=lambda names: fake_part_return,
                split_dev_base_partnum=lambda names: (disk, 1)
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

    def test_main_destroy_with_zap_mount_part_fail_by_id(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--zap', \
                                     '--destroy-by-id', '5566'])
        cluster = 'ceph'
        osd_id = '5566'
        fake_part_return = {'Xda': ['Xda1'], 'Xdb': []}
        disk = 'Xda'
        partition = 'Xda1'
        fstype = 'ext4'
        with patch.multiple(
                ceph_disk,
                _check_osd_status=lambda cluster, osd_id: 0,
                _remove_from_crush_map=lambda cluster, osd_id: True,
                _delete_osd_auth_key=lambda cluster, osd_id: True,
                _deallocate_osd_id=lambda cluster, osd_id: True,
                list_all_partitions=lambda names: fake_part_return,
                split_dev_base_partnum=lambda names: (disk, 1),
                get_dev_fs=lambda dev:fstype,
                mount=lambda dev, fstype, options:Exception
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

    def test_main_destroy_with_zap_by_id(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--zap', \
                                     '--destroy-by-id', '5566'])
        cluster = 'ceph'
        osd_id = '5566'
        disk = 'Xda'
        partition = 'Xda1'
        fstype = 'ext4'
        with patch.multiple(
                ceph_disk,
                _check_osd_status=lambda cluster, osd_id: 0,
                _remove_from_crush_map=lambda cluster, osd_id: True,
                _delete_osd_auth_key=lambda cluster, osd_id: True,
                _deallocate_osd_id=lambda cluster, osd_id: True,
                list_all_partitions=lambda names: { disk: [partition] },
                split_dev_base_partnum=lambda names: (disk, 1),
                get_dev_fs=lambda dev:fstype,
                mount=lambda dev, fstype, options: '/somewhere',
                unmount=lambda path: True,
                get_oneliner=lambda tpath, whoami: osd_id,
                zap=lambda dev: True
                ):
            ceph_disk.main_destroy(args)

    def test_main_destroy_fs_type_error_by_dev(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '/dev/Xda1'])
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        with patch.multiple(
                ceph_disk,
                patch_path,
                get_dev_fs=lambda dev:None,
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

    def test_main_destroy_mount_error_by_dev(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '/dev/Xda1'])
        fstype = 'ext4'
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        with patch.multiple(
                ceph_disk,
                patch_path,
                get_dev_fs=lambda dev:fstype,
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

    def test_main_destroy_with_zap_failure_by_dev(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--zap', \
                                     '/dev/Xda1'])
        dev = '/dev/Xda1'
        base_dev = '/dev/Xda'
        cluster = 'ceph'
        fstype = 'ext4'
        tpath = 'mnt'
        osd_id = '5566'
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        with patch.multiple(
                ceph_disk,
                patch_path,
                get_dev_fs=lambda dev:fstype,
                mount=lambda dev, fstype, options:tpath,
                get_oneliner=lambda mount_path, filen: osd_id,
                unmount=lambda path:True,
                _check_osd_status=lambda cluster, osd_id: 0,
                _remove_from_crush_map=lambda cluster, osd_id: True,
                _delete_osd_auth_key=lambda cluster, osd_id: True,
                _deallocate_osd_id=lambda cluster, osd_id: True,
                get_partition_base=lambda dev: base_dev,
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

    def test_main_destroy_with_zap_by_dev(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--zap', \
                                     '/dev/Xda1'])
        dev = '/dev/Xda1'
        base_dev = '/dev/Xda'
        cluster = 'ceph'
        fstype = 'ext4'
        tpath = 'mnt'
        osd_id = '5566'
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        with patch.multiple(
                ceph_disk,
                patch_path,
                get_dev_fs=lambda dev:fstype,
                mount=lambda dev, fstype, options:tpath,
                get_oneliner=lambda mount_path, filen: osd_id,
                unmount=lambda path:True,
                _check_osd_status=lambda cluster, osd_id: 0,
                _remove_from_crush_map=lambda cluster, osd_id: True,
                _delete_osd_auth_key=lambda cluster, osd_id: True,
                _deallocate_osd_id=lambda cluster, osd_id: True,
                get_partition_base=lambda dev: base_dev,
                zap=lambda base_dev: True
                ):
            ceph_disk.main_destroy(args)

    def test_main_destroy_without_zap_by_dev(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '/dev/Xda1'])
        cluster = 'ceph'
        fstype = 'ext4'
        tpath = 'mnt'
        osd_id = '5566'
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        with patch.multiple(
                ceph_disk,
                patch_path,
                get_dev_fs=lambda dev:fstype,
                mount=lambda dev, fstype, options:tpath,
                get_oneliner=lambda mount_path, filen: osd_id,
                unmount=lambda path:True,
                _check_osd_status=lambda cluster, osd_id: 0,
                _remove_from_crush_map=lambda cluster, osd_id: True,
                _delete_osd_auth_key=lambda cluster, osd_id: True,
                _deallocate_osd_id=lambda cluster, osd_id: True,
                ):
            ceph_disk.main_destroy(args)

    def test_main_destroy_status_incorrect_by_dev(self):
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '/dev/Xda1'])
        cluster = 'ceph'
        fstype = 'ext4'
        tpath = 'mnt'
        osd_id = '5566'
        patcher = patch('os.path.exists')
        patch_path = patcher.start()
        patch_path.side_effect = path_exists
        with patch.multiple(
                ceph_disk,
                patch_path,
                get_dev_fs=lambda dev:fstype,
                mount=lambda dev, fstype, options:tpath,
                get_oneliner=lambda mount_path, filen: osd_id,
                unmount=lambda path:True,
                _check_osd_status=lambda cluster, osd_id: 3,
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

    @patch('os.path.exists', return_value=False)
    def test_main_destroy_non_exist_non_cluster_by_dev(self, mock_exists):
        args = ceph_disk.parse_args(['destroy', \
                                     '/dev/Xda1'])
        self.assertRaises(Exception, ceph_disk.main_destroy, args)

    def test_remove_from_crush_map_fail(self):
        cluster = 'ceph'
        osd_id = '5566'
        with patch.multiple(
                ceph_disk,
                command=raise_command_error
                ):
            self.assertRaises(Exception, ceph_disk._remove_from_crush_map, cluster, osd_id)

    def test_delete_osd_auth_key_fail(self):
        cluster = 'ceph'
        osd_id = '5566'
        with patch.multiple(
                ceph_disk,
                command=raise_command_error
                ):
            self.assertRaises(Exception, ceph_disk._delete_osd_auth_key, cluster, osd_id)

    def test_deallocate_osd_id_fail(self):
        cluster = 'ceph'
        osd_id = '5566'
        with patch.multiple(
                ceph_disk,
                command=raise_command_error
                ):
            self.assertRaises(Exception, ceph_disk._deallocate_osd_id, cluster, osd_id)


##### Help function #####

def raise_command_error(*args):
    e = subprocess.CalledProcessError('aaa', 'bbb', 'ccc')
    raise e

def path_exists(target_paths=None):
    """
    A quick helper that enforces a check for the existence of a path. Since we
    are dealing with fakes, we allow to pass in a list of paths that are OK to
    return True, otherwise return False.
    """
    target_paths = target_paths or []

    def exists(path):
        return path in target_paths
    return exists
