from mock import patch, DEFAULT, Mock
import argparse
import pytest
import ceph_disk

def fail_to_mount(dev, fstype, options):
    raise ceph_disk.MountError(dev + " mount fail")

class utSystemContext:
    def __init__(self):
        self.devices  = []
        self.devtree  = []
        self.sysblock = []
        self.rootfs   = {}

        self.rootfs['/sys/block'] = self.sysblock
        self.rootfs['/dev']       = self.devtree

    def add_rootfs_path(self, path, path_items):
        self.rootfs[path] = path_items

    def add_device(self, device, name, need_sys_block_entry = True):
        self.devices.append(device)
        self.devtree.append(name)
        if need_sys_block_entry:
            self.sysblock.append(name)

    def listdir(self, path):
        if self.rootfs.has_key(path):
            return self.rootfs[path]
        else:
            print 'Path Not Registered? %s' %path

    def device_from_device_file(self, path):
        for device in self.devices:
            if device.device_node == path:
                return device
        print 'DeviceNotFound: %s' %path
        assert False

    def device_from_name(self, name):
        for device in self.devices:
            if device.name == name:
                return device
        print 'DeviceNotFound: %s' %name
        assert False


class utDevice:
    def __init__(self, utSysCtx, name, part_no = 0, parent = None, dm_name = None, dm_uuid = None, need_sys_block_entry = True):
        self.name        = name
        self.device_node = '/dev/' + name
        self.parent      = parent
        self.properties  = {}
        self.holders     = []
        self.slaves      = []
        self.children    = []

        self.set_property('SUBSYSTEM', 'block')
        self.set_property('DEVNAME', self.device_node)
        self.set_property('DEVPATH', '/block/' + name)
        if not dm_name:
            if part_no == 0:
                assert parent == None
                self.set_property('DEVTYPE', 'disk')
                holders_path = '/sys/block/' + name + '/holders'
                slaves_path = '/sys/block/' + name + '/slaves'
            else:
                assert parent != None
                self.set_property('ID_PART_ENTRY_NUMBER', str(part_no))
                self.set_property('DEVTYPE', 'partition')
                holders_path = '/sys/block/' + parent.name + '/' + name + '/holders'
                slaves_path  = '/sys/block/' + parent.name + '/' + name + '/slaves'
                parent.add_child(self)
                need_sys_block_entry = False
        else:
            self.set_property('DEVTYPE', 'disk')
            self.set_property('DM_NAME', dm_name)
            self.set_property('DM_UUID', dm_uuid)
            holders_path = '/sys/block/' + name + '/holders'
            slaves_path = '/sys/block/' + name + '/slaves'
            if part_no != 0:
                self.set_property('DM_PART', part_no)
            if parent:
                parent.add_holder(name)
                self.add_slave(parent.name)

        utSysCtx.add_device(self, name, need_sys_block_entry)
        utSysCtx.add_rootfs_path(holders_path, self.holders)
        utSysCtx.add_rootfs_path(slaves_path, self.slaves)

    def add_child(self, childdev):
        self.children.append(childdev)

    def add_holder(self, holder):
        self.holders.append(holder)

    def add_slave(self, slave):
        self.slaves.append(slave)

    def set_property(self, key, value):
        self.properties[key] = value

    def get(self, key):
        if self.properties.has_key(key):
            return self.properties[key]
        else:
            return None

    def find_parent(self, subsys, dtype = None):
        return self.parent

def ut_sysctx_device_from_device_file(utSysCtx, path):
    return utSysCtx.device_from_device_file(path)

def ut_sysctx_device_from_name(utSysCtx, system, name):
    return utSysCtx.device_from_name(name)

def ut_listdir(utSysCtx, path):
    return utSysCtx.listdir(path)

class TestCephDisk(object):

    def setup_class(self):
        ceph_disk.setup_logging(verbose=True, log_stdout=False)

    def test_generic_block_device(self):
        disk = 'Xda'
        partition = 'Xda1'
        utSysCtx = utSystemContext()
        # Add base Device
        base = utDevice(utSysCtx, disk)
        # Add partition device
        part1 = utDevice(utSysCtx, partition, 1, base)
        hw_sector_size = 512
        hw_sector_size_path = '/sys/block/' + disk + '/queue'

        def read_one_line(path, what):
            if path == hw_sector_size_path and what == 'hw_sector_size':
                return hw_sector_size
            else:
                raise Exception('unknown ' + what)

        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
        ), patch.multiple(
            ceph_disk,
            read_one_line=read_one_line
        ):
            self.ut_device_test(utSysCtx, base, part1, 'generic-block-dev', hw_sector_size)

    def test_dm_block_device(self):
        disk = 'dm-0'
        partition = 'dm-1'
        hw_sector_size_path = '/sys/block/' + disk + '/queue'
        utSysCtx = utSystemContext()
        # Add base Device
        base = utDevice(utSysCtx, 'Xda')
        # Add DM base dev
        dm_base = utDevice(utSysCtx, disk, 0, base, 'dm-device', 'mpath-dm-device')
        # Add DM Partition
        dm_part = utDevice(utSysCtx, partition, 1, dm_base, 'part-dm-basedev', 'mpath-part-dm-device')
        hw_sector_size = 4096
        hw_sector_size_path = '/sys/block/' + disk + '/queue'

        def read_one_line(path, what):
            if path == hw_sector_size_path and what == 'hw_sector_size':
                return hw_sector_size
            else:
                raise Exception('unknown ' + what)

        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
        ), patch.multiple(
            ceph_disk,
            read_one_line=read_one_line
        ):
            self.ut_device_test(utSysCtx, dm_base, dm_part, 'dm-multipath', hw_sector_size)

    def ut_device_test(self, utSysCtx, baseDev, partDev, dev_class, hw_sect_size):

        holderDev = utDevice(utSysCtx, 'dm-x', 0, None, 'dm-holder', 'CRYPT-LUKS-holder')
        '''
        Test Base Device
        '''
        baseDevCtx = ceph_disk.get_block_device_ctx(baseDev.device_node)
        assert baseDevCtx != None
        # Get Device Node
        assert baseDevCtx.get_dev_node() == baseDev.device_node
        # Partition Test: Must Fail for base device
        assert baseDevCtx.is_partition() == False
        # Get partition Number: Must Fail for base device
        assert baseDevCtx.get_partition_num() == None
        # Get Partition Device
        assert baseDevCtx.get_partition_dev(1) == partDev.device_node
        # Get the underlying base device for a partition, Must fail for base device
        assert baseDevCtx.get_partition_base() == None
        # Get list of partitions
        part_list = baseDevCtx.list_partitions()
        assert len(part_list) == 1
        assert part_list[0] == partDev.name
        # Get Partition TYPE and GUID, Must Fail for base
        part_type, part_uuid =  baseDevCtx.get_partition_guids()
        assert part_type == None
        assert part_uuid == None
        # Test is_held without any holders
        holders = baseDevCtx.is_held()
        assert len(holders) == 0
        # Test With holders
        baseDev.add_holder(holderDev.name)
        holders = baseDevCtx.is_held()
        assert len(holders) == 1
        assert holders[0] == 'dm-x'

        assert baseDevCtx.get_device_class() == dev_class

        assert baseDevCtx.get_hw_sector_size() == hw_sect_size

        '''
        Test partition device
        '''
        partDevCtx = ceph_disk.get_block_device_ctx(partDev.device_node)
        assert partDevCtx != None
        # Get Device Node
        assert partDevCtx.get_dev_node() == partDev.device_node
        # Partition Test
        assert partDevCtx.is_partition()
        # Get partition Number
        assert partDevCtx.get_partition_num() == 1
        # Get Partition Device, Must fail
        assert partDevCtx.get_partition_dev(1) == None
        # Get the underlying part device for a partition
        assert partDevCtx.get_partition_base() == baseDev.device_node
        # Get list of partitions
        assert len(partDevCtx.list_partitions()) == 0
        # Get Partition TYPE and GUID, Must Fail for part
        partDev.set_property('ID_PART_ENTRY_TYPE', ceph_disk.OSD_UUID)
        test_uuid = 'e76dca2e-6ad9-4022-a348-f6b2e68fed4a'
        partDev.set_property('ID_PART_ENTRY_UUID', test_uuid)
        part_type, part_uuid =  partDevCtx.get_partition_guids()
        assert part_type == ceph_disk.OSD_UUID
        assert part_uuid == test_uuid
        assert partDevCtx.get_partition_uuid() == part_uuid
        assert partDevCtx.get_partition_type() == part_type
        # Test is_held without any holders
        holders = partDevCtx.is_held()
        assert len(holders) == 0
        # Test With holders
        partDev.add_holder(holderDev.name)
        holders = partDevCtx.is_held()
        assert len(holders) == 1
        assert holders[0] == 'dm-x'

        assert partDevCtx.get_device_class() == dev_class

        assert partDevCtx.get_hw_sector_size() == hw_sect_size

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

        disk = "Xda"
        partition = "Xda1"
        utSysCtx = utSystemContext()
        # Add base Device
        base = utDevice(utSysCtx, disk)
        # Add partition device
        part1 = utDevice(utSysCtx, partition, 1, base)

        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
        ):
            assert {disk: [partition]} == ceph_disk.list_all_partitions([])

        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
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

        utSysCtx = utSystemContext()
        # Add base Device
        base = utDevice(utSysCtx, disk)
        # Add partition device
        part1 = utDevice(utSysCtx, partition, 1, base)
        part1.set_property('ID_PART_ENTRY_TYPE', ceph_disk.OSD_UUID)
        part1.set_property('ID_PART_ENTRY_UUID', partition_uuid)

        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
        ), patch.multiple(
            ceph_disk,
            get_dev_fs=lambda dev: fs_type,
            mount=fail_to_mount,
            unmount=DEFAULT,
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
            utSysCtx = utSystemContext()
            # Add base Device
            base = utDevice(utSysCtx, disk)
            # Add partition device
            part1 = utDevice(utSysCtx, partition, 1, base)
            part1.set_property('ID_PART_ENTRY_TYPE', partition_type)
            part1.set_property('ID_PART_ENTRY_UUID', partition_uuid)
            #Add one holder
            dm_name0 = 'testholder0'
            dm_uuid0 = 'CRYPT-' + type + dm_name0
            utDevice(utSysCtx, holders[0], 0, part1, dm_name0, dm_uuid0, need_sys_block_entry = False)

            with patch(
                'ceph_disk.pyudev',
                Context=lambda : utSysCtx,
            ), patch.multiple(
                'ceph_disk.os.path',
                exists=lambda path : True,
                realpath=lambda path : path,
            ), patch.multiple(
                'ceph_disk.os',
                 listdir= lambda path: ut_listdir(utSysCtx, path),
            ), patch.multiple(
                'ceph_disk.pyudev.Device',
                from_name=ut_sysctx_device_from_name,
                from_device_file=ut_sysctx_device_from_device_file,
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
                holders = ["dm-0","dm-1"]
                dm_name1 = 'testholder1'
                dm_uuid1 = 'CRYPT-' + type + dm_name1
                utDevice(utSysCtx, holders[1], 0, part1, dm_name1, dm_uuid1, need_sys_block_entry = False)
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
        disk = "dm-0"
        partition = "dm-1"
        utSysCtx = utSystemContext()
        dm_name0 = 'dm_device'
        dm_uuid0 = 'mpath-' + dm_name0
        dm_base = utDevice(utSysCtx, disk, 0, None, dm_name0, dm_uuid0)
        dm_part1 = utDevice(utSysCtx, partition, 1, dm_base, 'part1' + dm_name0, 'part1' + dm_uuid0)
        dm_part1.set_property('ID_PART_ENTRY_TYPE', ceph_disk.MPATH_OSD_UUID)
        dm_part1.set_property('ID_PART_ENTRY_UUID', partition_uuid)

        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
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
            dm_part1.set_property('ID_PART_ENTRY_TYPE', ceph_disk.MPATH_JOURNAL_UUID)
            dm_part1.set_property('ID_PART_ENTRY_UUID', journal_partition_uuid)
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
        journal_holder = "dm-1"
        mount_path = '/mount/path'
        fs_type = 'ext4'
        journal_uuid = "7ad5e65a-0ca5-40e4-a896-62a74ca61c55"
        ceph_fsid = "60a2ef70-d99b-4b9b-a83c-8a86e5e60091"
        osd_id = '1234'

        utSysCtx = utSystemContext()
        # Add base Device
        base = utDevice(utSysCtx, disk)
        # Add data partition device
        data_part = utDevice(utSysCtx, data, 1, base)
        data_part.set_property('ID_PART_ENTRY_TYPE', data_ptype)
        data_part.set_property('ID_PART_ENTRY_UUID', data_uuid)
        # Add journal partition device
        journal_part = utDevice(utSysCtx, journal, 2, base)
        journal_part.set_property('ID_PART_ENTRY_TYPE', journal_ptype)
        journal_part.set_property('ID_PART_ENTRY_UUID', journal_uuid)

        #Add one holder

        def get_oneliner(path, what):
            if what == 'journal_uuid':
                return journal_uuid
            elif what == 'ceph_fsid':
                return ceph_fsid
            elif what == 'whoami':
                return osd_id
            else:
                raise Exception('unknown ' + what)

        cluster = 'ceph'
        if data_ptype == ceph_disk.OSD_UUID:
            data_dmcrypt = {}
        elif data_ptype == ceph_disk.DMCRYPT_OSD_UUID:
            data_dmcrypt = {
                'type': 'plain',
                'holders': [data_holder],
            }
            utDevice(utSysCtx, data_holder, 0, data_part,
                'crypt-holder', 'CRYPT-PLAIN-crypt-holder', need_sys_block_entry = False)
        elif data_ptype == ceph_disk.DMCRYPT_LUKS_OSD_UUID:
            data_dmcrypt = {
                'type': 'LUKS',
                'holders': [data_holder],
            }
            utDevice(utSysCtx, data_holder, 0, data_part,
                'crypt-holder', 'CRYPT-LUKS-crypt-holder', need_sys_block_entry = False)
        else:
            raise Exception('unknown ' + data_ptype)

        if journal_ptype == ceph_disk.JOURNAL_UUID:
            journal_dmcrypt = {}
        elif journal_ptype == ceph_disk.DMCRYPT_JOURNAL_UUID:
            journal_dmcrypt = {
                'type': 'plain',
                'holders': [journal_holder],
            }
            utDevice(utSysCtx, journal_holder, 0, journal_part,
                'crypt-holder', 'CRYPT-PLAIN-crypt-holder', need_sys_block_entry = False)
        elif journal_ptype == ceph_disk.DMCRYPT_LUKS_JOURNAL_UUID:
            journal_dmcrypt = {
                'type': 'LUKS',
                'holders': [journal_holder],
            }
            utDevice(utSysCtx, journal_holder, 0, journal_part,
                'crypt-holder', 'CRYPT-LUKS-crypt-holder', need_sys_block_entry = False)
        else:
            raise Exception('unknown ' + journal_ptype)

        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
        ), patch.multiple(
            ceph_disk,
            get_dev_fs=lambda dev: fs_type,
            is_mounted=lambda dev: mount_path,
            find_cluster_by_uuid=lambda ceph_fsid: cluster,
            mount=DEFAULT,
            unmount=DEFAULT,
            get_oneliner=get_oneliner,
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
        utSysCtx = utSystemContext()
        # Add base Device
        base = utDevice(utSysCtx, disk)
        # Add data partition device
        data_part = utDevice(utSysCtx, partition, 1, base)
        data_part.set_property('ID_PART_ENTRY_TYPE', partition_type)
        data_part.set_property('ID_PART_ENTRY_UUID', partition_uuid)
        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
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
        mount_path = '/mount/path'
        fs_type = 'ext4'
        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
        ), patch.multiple(
            ceph_disk,
            get_dev_fs=lambda dev: fs_type,
            is_mounted=lambda dev: mount_path,
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
        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
        ), patch.multiple(
            ceph_disk,
            is_swap=lambda dev: True,
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
        utSysCtx = utSystemContext()
        # Add base Device
        partition_uuid = "56244cf5-83ef-4984-888a-2d8b8e0e04b2"
        disk = "Xda"
        # Add base Device
        base = utDevice(utSysCtx, disk)
        with patch(
            'ceph_disk.pyudev',
            Context=lambda : utSysCtx,
        ), patch.multiple(
            'ceph_disk.os.path',
            exists=lambda path : True,
            realpath=lambda path : path,
        ), patch.multiple(
            'ceph_disk.os',
             listdir= lambda path: ut_listdir(utSysCtx, path),
        ), patch.multiple(
            'ceph_disk.pyudev.Device',
            from_name=ut_sysctx_device_from_name,
            from_device_file=ut_sysctx_device_from_device_file,
        ):
            expect = [{'path': '/dev/' + disk,
                       'dmcrypt': {},
                       'is_partition': False,
                       'ptype': 'unknown',
                       'type': 'other'}]
            assert expect == ceph_disk.list_devices(args)
