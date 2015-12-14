from mock import patch, DEFAULT, Mock
import os
import io
import subprocess
import unittest
import argparse
import pytest
import struct
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

    @patch('__builtin__.open')
    def test_main_deactivate(self, mock_open):
        DMCRYPT_OSD_UUID = '4fbd7e29-9d25-41b8-afd0-5ec00ceff05d'
        DMCRYPT_LUKS_OSD_UUID = '4fbd7e29-9d25-41b8-afd0-35865ceff05d'
        part_uuid = '0ce28a16-6d5d-11e5-aec3-fa163e5c167b'
        disk = 'sdX'
        cluster = 'ceph'
        #
        # Can not find match device by osd-id
        #
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '--deactivate-by-id', '5566'])
        fake_device = [{'path': '/dev/' + disk,
                          'partitions': [{
                                  'path': '/dev/sdX1',
                                  'whoami': '-1',
                                  }]}]
        with patch.multiple(
                ceph_disk,
                list_devices=lambda dev:fake_device,
                ):
            self.assertRaises(Exception, ceph_disk.main_deactivate, args)

        #
        # find match device by osd-id, status: OSD_STATUS_IN_DOWN
        # with --mark-out option
        #
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '--deactivate-by-id', '5566', \
                                     '--mark-out'])
        fake_device = [{'path': '/dev/' + disk,
                          'partitions': [{
                                  'ptype': DMCRYPT_LUKS_OSD_UUID,
                                  'path': '/dev/sdX1',
                                  'whoami': '5566',
                                  'mount': '/var/lib/ceph/osd/ceph-5566/',
                                  'uuid': part_uuid,
                                  }]}]
        with patch.multiple(
                ceph_disk,
                list_devices=lambda dev:fake_device,
                _check_osd_status=lambda cluster, osd_id: 2,
                _mark_osd_out=lambda cluster, osd_id: True
                ):
            ceph_disk.main_deactivate(args)

        #
        # find match device by device partition, status: OSD_STATUS_IN_DOWN
        #
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '/dev/sdX1'])
        fake_device = [{'path': '/dev/' + disk,
                          'partitions': [{
                                  'ptype': DMCRYPT_LUKS_OSD_UUID,
                                  'path': '/dev/sdX1',
                                  'whoami': '5566',
                                  'mount': '/var/lib/ceph/osd/ceph-5566/',
                                  'uuid': part_uuid,
                                  }]}]
        with patch.multiple(
                ceph_disk,
                list_devices=lambda dev:fake_device,
                _check_osd_status=lambda cluster, osd_id: 0,
                ):
            ceph_disk.main_deactivate(args)

        #
        # find match device by device partition, status: OSD_STATUS_IN_UP
        # with --mark-out option
        #
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '/dev/sdX1', \
                                     '--mark-out'])
        fake_device = [{'path': '/dev/' + disk,
                          'partitions': [{
                                  'ptype': DMCRYPT_LUKS_OSD_UUID,
                                  'path': '/dev/sdX1',
                                  'whoami': '5566',
                                  'mount': '/var/lib/ceph/osd/ceph-5566/',
                                  'uuid': part_uuid,
                                  }]}]

        # mock the file open.
        file_opened = io.StringIO()
        file_opened.write(u'deactive')
        mock_open.return_value = file_opened

        with patch.multiple(
                ceph_disk,
                mock_open,
                list_devices=lambda dev:fake_device,
                _check_osd_status=lambda cluster, osd_id: 3,
                _mark_osd_out=lambda cluster, osd_id: True,
                stop_daemon=lambda cluster, osd_id: True,
                _remove_osd_directory_files=lambda path, cluster: True,
                path_set_context=lambda path: True,
                unmount=lambda path: True,
                dmcrypt_unmap=lambda part_uuid: True,
                ):
            ceph_disk.main_deactivate(args)

        #
        # find match device by osd-id, status: OSD_STATUS_OUT_UP
        #
        args = ceph_disk.parse_args(['deactivate', \
                                     '--cluster', 'ceph', \
                                     '--deactivate-by-id', '5566'])
        fake_device = [{'path': '/dev/' + disk,
                          'partitions': [{
                                  'ptype': DMCRYPT_LUKS_OSD_UUID,
                                  'path': '/dev/sdX1',
                                  'whoami': '5566',
                                  'mount': '/var/lib/ceph/osd/ceph-5566/',
                                  'uuid': part_uuid,
                                  }]}]

        # mock the file open.
        file_opened = io.StringIO()
        file_opened.write(u'deactive')
        mock_open.return_value = file_opened

        with patch.multiple(
                ceph_disk,
                mock_open,
                list_devices=lambda dev:fake_device,
                _check_osd_status=lambda cluster, osd_id: 1,
                _mark_osd_out=lambda cluster, osd_id: True,
                stop_daemon=lambda cluster, osd_id: True,
                _remove_osd_directory_files=lambda path, cluster: True,
                path_set_context=lambda path: True,
                unmount=lambda path: True,
                dmcrypt_unmap=lambda part_uuid: True,
                ):
            ceph_disk.main_deactivate(args)

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

    def test_check_osd_status(self):
        #
        # command failure
        #
        with patch.multiple(
                ceph_disk,
                command=raise_command_error,
                ):
            self.assertRaises(Exception, ceph_disk._check_osd_status, 'ceph', '5566')

        #
        # osd not found
        #

        fake_data = '{"osds":[{"osd":0,"up":1,"in":1},{"osd":1,"up":1,"in":1}]}'

        def return_fake_value(cmd):
            return fake_data, 0

        with patch.multiple(
                ceph_disk,
                command=return_fake_value,
                ):
            self.assertRaises(Exception, ceph_disk._check_osd_status, 'ceph', '5566')

        #
        # successfully
        #

        fake_data = '{"osds":[{"osd":0,"up":1,"in":1},{"osd":5566,"up":1,"in":1}]}'

        def return_fake_value(cmd):
            return fake_data, 0

        with patch.multiple(
                ceph_disk,
                command=return_fake_value,
                ):
            ceph_disk._check_osd_status('ceph', '5566')

    def test_stop_daemon(self):
        STATEDIR = '/var/lib/ceph'
        cluster = 'ceph'
        osd_id = '5566'

        def stop_daemon_fail(cmd):
            raise Exception('ceph osd stop failed')

        #
        # fail on init type
        #
        with patch('os.path.exists', return_value=False):
            self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

        #
        # faile on os path
        #
        with patch('os.path.exists', return_value=Exception):
            self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

        #
        # upstart failure
        #
        fake_path = (STATEDIR + '/osd/{cluster}-{osd_id}/upstart').format(
                    cluster=cluster, osd_id=osd_id)

        def path_exist(check_path):
            if check_path == fake_path:
                return True
            else:
                False

        patcher = patch('os.path.exists')
        check_path = patcher.start()
        check_path.side_effect = path_exist
        with patch.multiple(
                ceph_disk,
                check_path,
                command_check_call=stop_daemon_fail,
                ):
            self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

        #
        # sysvinit failure
        #
        fake_path = (STATEDIR + '/osd/{cluster}-{osd_id}/sysvinit').format(
                    cluster=cluster, osd_id=osd_id)

        def path_exist(check_path):
            if check_path == fake_path:
                return True
            else:
                return False

        patcher = patch('os.path.exists')
        check_path = patcher.start()
        check_path.side_effect = path_exist
        with patch.multiple(
                ceph_disk,
                check_path,
                which=lambda name: True,
                command_check_call=stop_daemon_fail,
                ):
            self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

        #
        # systemd failure
        #
        fake_path = (STATEDIR + '/osd/{cluster}-{osd_id}/systemd').format(
                    cluster=cluster, osd_id=osd_id)

        def path_exist(check_path):
            if check_path == fake_path:
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
        check_path.side_effect = path_exist
        with patch.multiple(
                ceph_disk,
                check_path,
                command_check_call=stop_daemon_fail,
                ):
            self.assertRaises(Exception, ceph_disk.stop_daemon, 'ceph', '5566')

    def test_remove_osd_directory_files(self):
        cluster = 'ceph'
        mounted_path = 'somewhere'
        fake_path_2 = None
        fake_path_remove_2 = None
        fake_path_remove_init = None

        def handle_path_exist(check_path):
            if check_path == fake_path:
                return True
            elif fake_path_2 and check_path == fake_path_2:
                return True
            else:
                return False

        def handle_path_remove(remove_path):
            if remove_path == fake_path_remove:
                return True
            elif fake_path_remove_2 and remove_path == fake_path_remove_2:
                return True
            elif fake_path_remove_init and remove_path == fake_path_remove_init:
                return True
            else:
                raise OSError

        #
        # remove ready file failure
        #
        fake_path = os.path.join(mounted_path, 'ready')
        fake_path_remove = os.path.join(mounted_path, 'no_ready')

        patcher_exist = patch('os.path.exists')
        patcher_remove = patch('os.remove')
        path_exist = patcher_exist.start()
        path_remove = patcher_remove.start()
        path_exist.side_effect = handle_path_exist
        path_remove.side_effect = handle_path_remove
        with patch.multiple(
                ceph_disk,
                path_exist,
                path_remove,
                get_conf=lambda cluster, **kwargs: True,
                ):
            self.assertRaises(Exception, ceph_disk._remove_osd_directory_files, 'somewhere', cluster)

        #
        # remove active fil failure
        #
        fake_path = os.path.join(mounted_path, 'ready')
        fake_path_2 = os.path.join(mounted_path, 'active')
        fake_path_remove = os.path.join(mounted_path, 'ready')
        fake_path_remove_2 = os.path.join(mounted_path, 'no_active')

        patcher_exist = patch('os.path.exists')
        patcher_remove = patch('os.remove')
        path_exist = patcher_exist.start()
        path_remove = patcher_remove.start()
        path_exist.side_effect = handle_path_exist
        path_remove.side_effect = handle_path_remove
        with patch.multiple(
                ceph_disk,
                path_exist,
                path_remove,
                get_conf=lambda cluster, **kwargs: True,
                ):
            self.assertRaises(Exception, ceph_disk._remove_osd_directory_files, 'somewhere', cluster)

        #
        # conf_val is None and remove init file failure
        #
        fake_path = os.path.join(mounted_path, 'ready')
        fake_path_2 = os.path.join(mounted_path, 'active')
        fake_path_remove = os.path.join(mounted_path, 'ready')
        fake_path_remove_2 = os.path.join(mounted_path, 'active')
        fake_path_remove_init = os.path.join(mounted_path, 'init_failure')

        patcher_exist = patch('os.path.exists')
        patcher_remove = patch('os.remove')
        path_exist = patcher_exist.start()
        path_remove = patcher_remove.start()
        path_exist.side_effect = handle_path_exist
        path_remove.side_effect = handle_path_remove
        with patch.multiple(
                ceph_disk,
                path_exist,
                path_remove,
                get_conf=lambda cluster, **kwargs: None,
                init_get=lambda: 'upstart',
                ):
            self.assertRaises(Exception, ceph_disk._remove_osd_directory_files, 'somewhere', cluster)

        #
        # already remove `ready`, `active` and remove init file successfully
        #
        fake_path = os.path.join(mounted_path, 'no_ready')
        fake_path_2 = os.path.join(mounted_path, 'no_active')
        fake_path_remove = os.path.join(mounted_path, 'upstart')

        patcher_exist = patch('os.path.exists')
        patcher_remove = patch('os.remove')
        path_exist = patcher_exist.start()
        path_remove = patcher_remove.start()
        path_exist.side_effect = handle_path_exist
        path_remove.side_effect = handle_path_remove
        with patch.multiple(
                ceph_disk,
                path_exist,
                path_remove,
                get_conf=lambda cluster, **kwargs: 'upstart',
                ):
            ceph_disk._remove_osd_directory_files('somewhere', cluster)

    def test_path_set_context(self):
        path = '/somewhere'
        with patch.multiple(
                ceph_disk,
                get_ceph_user=lambda **kwargs: 'ceph',
                ):
            ceph_disk.path_set_context(path)

    def test_mount(self):
        #
        # None to mount
        #
        dev = None
        fs_type = 'ext4'
        option = ''
        self.assertRaises(Exception, ceph_disk.mount, dev, fs_type, option)

        #
        # fstype undefine
        #
        dev = '/dev/Xda1'
        fs_type = None
        option = ''
        self.assertRaises(Exception, ceph_disk.mount, dev, fs_type, option)

        #
        # mount failure
        #
        dev = '/dev/Xda1'
        fstype = 'ext4'
        options = ''
        with patch('tempfile.mkdtemp', return_value='/mnt'):
            self.assertRaises(Exception, ceph_disk.mount, dev, fstype, options)

        #
        # mount successfully
        #
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

    def test_umount(self):
        #
        # umount failure
        #
        path = '/somewhere'
        self.assertRaises(Exception, ceph_disk.unmount, path)

        #
        # umount successfully
        #
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

    def test_main_destroy(self):
        DMCRYPT_OSD_UUID = '4fbd7e29-9d25-41b8-afd0-5ec00ceff05d'
        DMCRYPT_LUKS_OSD_UUID = '4fbd7e29-9d25-41b8-afd0-35865ceff05d'
        OSD_UUID = '4fbd7e29-9d25-41b8-afd0-062c0ceff05d'
        MPATH_OSD_UUID = '4fbd7e29-8ae0-4982-bf9d-5a8d867af560'
        part_uuid = '0ce28a16-6d5d-11e5-aec3-fa163e5c167b'
        journal_uuid = "7ad5e65a-0ca5-40e4-a896-62a74ca61c55"
        cluster = 'ceph'

        fake_devices_normal = [{'path': '/dev/sdY',
                                    'partitions': [{
                                    'dmcrypt': {},
                                    'ptype': OSD_UUID,
                                    'path': '/dev/sdY1',
                                    'whoami': '5566',
                                    'mount': '/var/lib/ceph/osd/ceph-5566/',
                                    'uuid': part_uuid,
                                    'journal_uuid': journal_uuid}]},
                               {'path': '/dev/sdX',
                               'partitions': [{
                                    'dmcrypt': {},
                                    'ptype': MPATH_OSD_UUID,
                                    'path': '/dev/sdX1',
                                    'whoami': '7788',
                                    'mount': '/var/lib/ceph/osd/ceph-7788/',
                                    'uuid': part_uuid,
                                    'journal_uuid': journal_uuid}]}
                              ]
        fake_devices_dmcrypt_unmap = [{'path': '/dev/sdY',
                                        'partitions': [{
                                        'dmcrypt': {
                                            'holders': '',
                                            'type': type,
                                        },
                                        'ptype': DMCRYPT_OSD_UUID,
                                        'path': '/dev/sdX1',
                                        'whoami': '5566',
                                        'mount': '/var/lib/ceph/osd/ceph-5566/',
                                        'uuid': part_uuid,
                                        'journal_uuid': journal_uuid}]}]
        fake_devices_dmcrypt_luk_unmap = [{'path': '/dev/sdY',
                                            'partitions': [{
                                            'dmcrypt': {
                                                'holders': '',
                                                'type': type,
                                            },
                                            'ptype': DMCRYPT_LUKS_OSD_UUID,
                                            'path': '/dev/sdX1',
                                            'whoami': '5566',
                                            'mount': '/var/lib/ceph/osd/ceph-5566/',
                                            'uuid': part_uuid,
                                            'journal_uuid': journal_uuid}]}]
        fake_devices_dmcrypt_unknow = [{'path': '/dev/sdY',
                                            'partitions': [{
                                            'dmcrypt': {
                                                'holders': '',
                                                'type': type,
                                            },
                                            'ptype': '00000000-0000-0000-0000-000000000000',
                                            'path': '/dev/sdX1',
                                            'whoami': '5566',
                                            'mount': '/var/lib/ceph/osd/ceph-5566/',
                                            'uuid': part_uuid,
                                            'journal_uuid': journal_uuid}]}]
        fake_devices_dmcrypt_map = [{'dmcrypt': {
                                        'holders': 'dm_0',
                                        'type': type,
                                        },
                                     'ptype': DMCRYPT_OSD_UUID,
                                     'path': '/dev/sdX1',
                                     'whoami': '5566',
                                     'mount': '/var/lib/ceph/osd/ceph-5566/',
                                     'uuid': part_uuid,
                                     'journal_uuid': journal_uuid}]

        def list_devices_return(dev):
            if dev == []:
                return fake_devices_normal

        #
        # input device is not the device partition
        #
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '/dev/sdX'])
        with patch.multiple(
                ceph_disk,
                is_partition=lambda path: False,
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

        #
        # skip the redundent devices and not found by dev
        #
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '/dev/sdZ1'])
        with patch.multiple(
                ceph_disk,
                is_partition=lambda path: True,
                list_devices=list_devices_return,
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

        #
        # skip the redundent devices and not found by osd-id
        #
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--destroy-by-id', '1234'])
        with patch.multiple(
                ceph_disk,
                is_partition=lambda path: True,
                list_devices=list_devices_return,
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

        #
        # skip the redundent devices and found by dev
        #
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '/dev/sdY1', '--zap'])
        with patch.multiple(
                ceph_disk,
                is_partition=lambda path: True,
                list_devices=list_devices_return,
                get_partition_base=lambda dev_path: '/dev/sdY',
                _check_osd_status=lambda cluster, osd_id: 0,
                _remove_from_crush_map=lambda cluster, osd_id: True,
                _delete_osd_auth_key=lambda cluster, osd_id: True,
                _deallocate_osd_id=lambda cluster, osd_id: True,
                zap=lambda dev: True
                ):
            ceph_disk.main_destroy(args)
            #self.assertRaises(Exception, ceph_disk.main_destroy, args)

        #
        # skip the redundent devices and found by osd-id
        # with active status and MPATH_OSD
        #
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--destroy-by-id', '7788'])
        with patch.multiple(
                ceph_disk,
                is_partition=lambda path: True,
                list_devices=list_devices_return,
                get_partition_base_mpath=lambda dev_path: '/dev/sdX',
                _check_osd_status=lambda cluster, osd_id: 1,
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

        #
        # skip the redundent devices and found by dev
        # with dmcrypt (plain)
        #
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '/dev/sdX1', '--zap'])
        def list_devices_return(dev):
            if dev == []:
                return fake_devices_dmcrypt_unmap
            elif dev == ['/dev/sdX1']:
                return fake_devices_dmcrypt_map

        with patch.multiple(
                ceph_disk,
                is_partition=lambda path: True,
                list_devices=list_devices_return,
                get_dmcrypt_key_path=lambda part_uuid, dmcrypt_key_dir, luks: True,
                dmcrypt_map=lambda rawdev, keypath, _uuid, \
                                   cryptsetup_parameters, luks, format_dev: True,
                dmcrypt_unmap=lambda part_uuid: True,
                get_partition_base=lambda dev_path: '/dev/sdX',
                _check_osd_status=lambda cluster, osd_id: 0,
                _remove_from_crush_map=lambda cluster, osd_id: True,
                _delete_osd_auth_key=lambda cluster, osd_id: True,
                _deallocate_osd_id=lambda cluster, osd_id: True,
                zap=lambda dev: True
                ):
            ceph_disk.main_destroy(args)
            #self.assertRaises(Exception, ceph_disk.main_destroy, args)

        #
        # skip the redundent devices and found by osd-id
        # with dmcrypt (luk) and status: active
        #
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--destroy-by-id', '5566'])
        def list_devices_return(dev):
            if dev == []:
                return fake_devices_dmcrypt_luk_unmap
            elif dev == ['/dev/sdX1']:
                return fake_devices_dmcrypt_map

        with patch.multiple(
                ceph_disk,
                is_partition=lambda path: True,
                list_devices=list_devices_return,
                get_dmcrypt_key_path=lambda part_uuid, dmcrypt_key_dir, luks: True,
                dmcrypt_map=lambda rawdev, keypath, _uuid, \
                                   cryptsetup_parameters, luks, format_dev: True,
                dmcrypt_unmap=lambda part_uuid: True,
                get_partition_base=lambda dev_path: '/dev/sdX',
                _check_osd_status=lambda cluster, osd_id: 1,
                ):
            self.assertRaises(Exception, ceph_disk.main_destroy, args)

        #
        # skip the redundent devices and found by osd-id
        # with unknow dmcrypt type
        #
        args = ceph_disk.parse_args(['destroy', \
                                     '--cluster', 'ceph', \
                                     '--destroy-by-id', '5566'])
        def list_devices_return(dev):
            if dev == []:
                return fake_devices_dmcrypt_unknow

        with patch.multiple(
                ceph_disk,
                is_partition=lambda path: True,
                list_devices=list_devices_return,
                ):
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

    def test_gpt_rawread_partition_guids(self):

        gpt_hdr = struct.pack('<QIIIIQQQQQQQIII',
            # 8 bytes	Signature ("EFI PART")
            0x5452415020494645,
            # 4 bytes	Revision
            0x00010000,
            # 4 bytes Header size in little endian
            0x0000005C,
            # 4 bytes CRC32 of header
            0x4C72F79D,
            # 4 bytes Reserved; must be zero
            0x00000000,
            # 8 bytes Current LBA (location of this header copy)
            0x00000001,
            # 8 bytes Backup LBA (location of the other header copy)
            0x6FC7D255,
            # 8 bytes First usable LBA for partitions
            0x00000006,
            # 8 bytes Last usable LBA
            0x6FC7D250,
            # 16 bytes Disk GUID
            0x40576A0BFA1F857E,
            0x6E1D63E20B56A5A7,
            # 8 bytes Starting LBA of array of partition entries
            0x00000002,
            # 4 bytes Number of partition entries in array
            0x00000080,
            # 4 bytes Size of a single partition entry
            0x00000080,
            # 4 bytes CRC32 of partition array
            0x0793AD0B)

        p1name = 'ceph data'
        p1uuid = 'f4aee2f3-b699-4068-8721-1f92462c95e0'
        p1type = '4fbd7e29-9d25-41b8-afd0-062c0ceff05d'
        gpt_part1 = struct.pack('<QQQQQQQ72s',
            # 16 bytes	Partition type GUID
            0x41B89D254FBD7E29,
            0x5DF0EF0C2C06D0AF,
            # 16 bytes	Unique partition GUID
            0x4068B699F4AEE2F3,
            0xE0952C46921F2187,
            # 8 bytes	First LBA (little endian)
            0x00040100,
            # 8 bytes	Last LBA
            0x6FC7D250,
            # 8 bytes	Attribute flags
            0x00000000,
            # 72 bytes	Partition name (36 UTF-16LE code units)
            p1name.encode('utf-16le'))

        p2name = 'ceph journal'
        p2uuid='e50882fe-b180-4ec5-a3ac-4a3cb0d9cc53'
        p2type='45b0969e-9b03-4f30-b4c6-b4b80ceff106'
        gpt_part2 = struct.pack('<QQQQQQQ72s',
            # 16 bytes	Partition type GUID
            0x4F309B0345B0969E,
            0x6F1EF0CB8B4C6B4,
            # 16 bytes	Unique partition GUID
            0x4EC5B180E50882FE,
            0x53CCD9B03C4AACA3,
            # 8 bytes	First LBA (little endian)
            0x00000100,
            # 8 bytes	Last LBA
            0x00040000,
            # 8 bytes	Attribute flags
            0x00000000,
            # 72 bytes	Partition name (36 UTF-16LE code units)
            p2name.encode('utf-16le'))

        fdesc = {}
        fdesc['offset'] = 0
        sz_sect = 512
        hdr_off = 1
        part_tbl_off = 2

        def gpt_lseek(fd, offset, mode):
            fd['offset'] = offset

        def gpt_read(fd, rlen):
            curr_offset = fd['offset']
            if curr_offset == (hdr_off * sz_sect):
                return gpt_hdr
            elif curr_offset == (part_tbl_off * sz_sect):
                assert(rlen == 16384)
                pad = struct.pack('16128s', '\0')
                return gpt_part1 + gpt_part2 + struct.pack('16128x')
            else:
                raise Exception('unknown offset' + curr_offset)

        with patch.multiple(
                'ceph_disk.os',
                open=lambda dev, m : fdesc,
                close=lambda fd: 0,
                lseek=lambda fd, off, m: gpt_lseek(fd, off, m),
                read=lambda fd, rlen: gpt_read(fd, rlen),
        ):
            ptype, puuid = ceph_disk.rawread_partition_guids('dummy', 1, sz_sect)
            assert(ptype == p1type)
            assert(puuid == p1uuid)
            ptype, puuid = ceph_disk.rawread_partition_guids('dummy', 2, sz_sect)
            assert(ptype == p2type)
            assert(puuid == p2uuid)
            sz_sect = 4096
            ptype, puuid = ceph_disk.rawread_partition_guids('dummy', 1, sz_sect)
            assert(ptype == p1type)
            assert(puuid == p1uuid)
            ptype, puuid = ceph_disk.rawread_partition_guids('dummy', 2, sz_sect)
            assert(ptype == p2type)
            assert(puuid == p2uuid)
            # corrupt part entry
            gpt_part1 = struct.pack('<QQQQQQQ72s',
                # 16 bytes	Partition type GUID
                0x41B89D254FBD7E29,
                0x5DF0EF0C2C06D0AF,
                # 16 bytes	Unique partition GUID
                0x4068B699F4AEE2F3,
                0xE0952C46921F2187,
                # 8 bytes	First LBA (little endian)
                0x00000000, # CORRUPTED HERE
                # 8 bytes	Last LBA
                0x6FC7D250,
                # 8 bytes	Attribute flags
                0x00000000,
                # 72 bytes	Partition name (36 UTF-16LE code units)
                p1name.encode('utf-16le'))
            ptype, puuid = ceph_disk.rawread_partition_guids('dummy', 1, sz_sect)
            assert(ptype == None)
            assert(puuid == None)
            # Bad GPT header
            gpt_hdr = struct.pack('<Q84x',
                # 8 bytes	Signature ("EFI PART")
                0x5452415020494645)
            ptype, puuid = ceph_disk.rawread_partition_guids('dummy', 1, sz_sect)
            assert(ptype == None)
            assert(puuid == None)
            # Bad Signature, Null Header
            gpt_hdr = struct.pack('<Q84x',
                # 8 bytes	Signature ("EFI PART")
                0x0452410000494640)
            ptype, puuid = ceph_disk.rawread_partition_guids('dummy', 1, sz_sect)
            assert(ptype == None)
            assert(puuid == None)



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
