from nose.tools import eq_ as eq, assert_raises
import os
import ceph_disk
import subprocess

class TestGet_get_blkid_udev(object):

    def setup(self):
        self.all_devices = ceph_disk.list_all_partitions()

    def test_all_device(self):
        for device in self.all_devices:
            status = ceph_disk.get_blkid_udev("/dev/" + device)
            assert ceph_disk.BLKID_UDEV.issuperset(status.keys())

    def test_all_parition(self):
        for device in self.all_devices:
            for partition in self.all_devices[device]:
                status = ceph_disk.get_blkid_udev("/dev/" + device)
                assert ceph_disk.BLKID_UDEV.issuperset(status.keys())

    def test_all_devices_with_parition(self):
        for device in self.all_devices:
            if len(self.all_devices[device]) > 0:
                status = ceph_disk.get_blkid_udev("/dev/" + device)
                assert ceph_disk.view_blkid_udev_is_partioned(status) == True



class TestGet_get_blkid_udev_with_loop_back(object):
    def setup(self):
        """
         dd if=/dev/zero of=/tmp/dev0-backstore bs=1M count=100

        # create the loopback block device
        # where 7 is the major number of loop device driver, grep loop /proc/devices

        mknod /dev/fake-dev0 b 7 200
        losetup /dev/fake-dev0  /tmp/dev0-backstore
        """
        self.blockfile = "/tmp/dev0-backstore"
        self.device_name = "/dev/fake-dev0"
        try:
            ceph_disk.command_check_call(
                [
                    'sudo',
                    'dd',
                    'if=/dev/zero',
                    'of=%s' % (self.blockfile),
                    'bs=1M',
                    'count=100',
                ],
            )
        except ceph_disk.Error as e:
            raise ceph_disk.Error(e)
        try:
            ceph_disk.command_check_call(
                [
                    'sudo',
                    'mknod',
                    self.device_name,
                    'b',
                    '7',
                    '200'
                ],
            )
        except ceph_disk.Error as e:
            raise ceph_disk.Error(e)
        try:
            ceph_disk.command_check_call(
                [
                    'sudo',
                    'losetup',
                    self.device_name,
                    self.blockfile
                ],
            )
        except ceph_disk.Error as e:
            raise ceph_disk.Error(e)

    def teardown(self):
        try:
            ceph_disk.command_check_call(
                [
                    'sudo',
                    'losetup',
                    '--detach',
                    self.device_name,
                ],
            )
        except ceph_disk.Error as e:
            raise ceph_disk.Error(e)
        os.unlink(self.device_name)
        os.unlink(self.blockfile)

    def test_read_loop(self):
        status = ceph_disk.get_blkid_udev(self.device_name)
        assert status['ID_PART_TABLE_UUID'] == None
        assert status['ID_PART_TABLE_TYPE'] == None
        assert ceph_disk.view_blkid_udev_is_partioned(status) == False

    def test_read_zapped_loop(self):
        ceph_disk.zap(self.device_name)
        status = ceph_disk.get_blkid_udev(self.device_name)
        assert status['ID_PART_TABLE_UUID'] != None
        assert status['ID_PART_TABLE_TYPE'] != None
        assert ceph_disk.view_blkid_udev_is_partioned(status) == True
