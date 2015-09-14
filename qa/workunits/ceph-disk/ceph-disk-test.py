#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
# When debugging these tests, here are a few useful commands:
#
#  export PATH=..:$PATH
#  python ceph-disk-test.py --verbose --destroy-osd 0
#  py.test -s -v -k test_activate_dmcrypt_luks ceph-disk-test.py
#
import argparse
import json
import logging
import os
import pytest
import re
import subprocess
import sys
import tempfile
import uuid

LOG = logging.getLogger('CephDisk')

class CephDisk:

    @staticmethod
    def helper(command):
        command = "ceph-helpers-root.sh " + command
        return CephDisk.sh(command)

    @staticmethod
    def sh(command):
        output = subprocess.check_output(command, shell=True)
        LOG.debug("sh: " + command + ": " + output)
        return output.strip()

    def unused_disks(self, pattern='[vs]d.'):
        names = filter(lambda x: re.match(pattern, x), os.listdir("/sys/block"))
        if not names:
            return []
        disks = json.loads(self.helper("ceph-disk list --format json " + " ".join(names)))
        unused = []
        for disk in disks:
            if 'partitions' not in disk:
                unused.append(disk['path'])
        return unused

    def ensure_sd(self):
        LOG.debug(self.unused_disks('sd.'))
        if self.unused_disks('sd.'):
            return
        modprobe = "modprobe scsi_debug vpd_use_hostno=0 add_host=1 dev_size_mb=200 ; udevadm settle"
        try:
            self.sh(modprobe)
        except:
            self.helper("install linux-image-extra-3.13.0-61-generic")
            self.sh(modprobe)

    def ensure_lv(self):
        '''
        Create the lvm logical volume necessary to test ceph-disk
        '''
        LOG.debug(self.sh("lvdisplay -c"))
        LOG.debug(self.unused_disks())
        try:
            self.sh("lvdisplay -c vg/lv")
            return "/dev/vg/lv"
        except:
            pass
        disk = self.unused_disks()[0]
        self.lvm_disk = disk
        lvm_part_uuid = str(uuid.uuid1())
        lvm_part_uuid_dev = '/dev/disk/by-partuuid/' + lvm_part_uuid
        self.lvm_part_uuid_dev = lvm_part_uuid_dev
        try:
            self.sh("sgdisk -n0:0:0 -t0:8e00 -u0:" + lvm_part_uuid)
            self.sh("pvcreate " + lvm_part_uuid_dev)
            self.sh("vgcreate vg " + lvm_part_uuid_dev)
            self.sh("lvcreate -n lv -l 100%FREE vg " + lvm_part_uuid_dev)
        except:
            raise Exception("Unable to create LV.")
        return "/dev/vg/lv"

    def remove_lv(self):
        '''
        Remove the lvm configuration after testing
        '''
        try:
            self.sh("lvremove -f vg/lv")
            self.sh("vgremove -f vg")
            self.sh("pvremove -f " + self.lvm_part_uuid_dev)
            self.sh("sgdisk -Z " + self.lvm_disk)
        except:
            raise Exception("Unable to remove LVM configuration.")

    def unload_scsi_debug(self):
        self.sh("rmmod scsi_debug || true")

    def get_osd_partition(self, uuid):
        disks = json.loads(self.helper("ceph-disk list --format json"))
        for disk in disks:
            if 'partitions' in disk:
                for partition in disk['partitions']:
                    if partition.get('uuid') == uuid:
                        return partition
        raise Exception("uuid = " + uuid + " not found in " + str(disks))

    def get_journal_partition(self, uuid):
        data_partition = self.get_osd_partition(uuid)
        journal_dev = data_partition['journal_dev']
        disks = json.loads(self.helper("ceph-disk list --format json"))
        for disk in disks:
            if 'partitions' in disk:
                for partition in disk['partitions']:
                    if partition['path'] == journal_dev:
                        if 'journal_for' in partition:
                            assert partition['journal_for'] == data_partition['path']
                        return partition
        raise Exception("journal for uuid = " + uuid + " not found in " + str(disks))

    def destroy_osd(self, uuid):
        id = self.sh("ceph osd create " + uuid)
        self.helper("control_osd stop " + id + " || true")
        try:
            partition = self.get_journal_partition(uuid)
            if partition:
                if partition.get('mount'):
                    self.sh("umount '" + partition['mount'] + "' || true")
                if partition['dmcrypt']:
                    holder = partition['dmcrypt']['holders'][0]
                    self.sh("cryptsetup close $(cat /sys/block/" + holder + "/dm/name) || true")
        except:
            pass
        try:
            partition = self.get_osd_partition(uuid)
            if partition.get('mount'):
                self.sh("umount '" + partition['mount'] + "' || true")
            if partition['dmcrypt']:
                holder = partition['dmcrypt']['holders'][0]
                self.sh("cryptsetup close $(cat /sys/block/" + holder + "/dm/name) || true")
        except:
            pass
        self.sh("""
        ceph osd down {id}
        ceph osd rm {id}
        ceph auth del osd.{id}
        ceph osd crush rm osd.{id}
        """.format(id=id))

    def run_osd(self, uuid, data, journal=None):
        prepare = ("ceph-disk prepare --osd-uuid " + uuid +
                   " " + data)
        if journal:
            prepare += " " + journal
        self.sh(prepare)
        self.sh("ceph osd create " + uuid)
        partition = self.get_osd_partition(uuid)
        assert partition['type'] == 'data'
        assert partition['state'] == 'active'

    @staticmethod
    def augtool(command):
        return CephDisk.sh("""
        augtool <<'EOF'
        set /augeas/load/IniFile/lens Puppet.lns
        set /augeas/load/IniFile/incl "/etc/ceph/ceph.conf"
        load
        {command}
        save
EOF
        """.format(command=command))

class TestCephDisk(object):

    def setup_class(self):
        logging.basicConfig(level=logging.DEBUG)
        c = CephDisk()
        c.helper("install augeas-tools augeas")
        c.helper("install multipath-tools device-mapper-multipath")
        c.augtool("set /files/etc/ceph/ceph.conf/global/osd_journal_size 100")

    def test_destroy_osd(self):
        c = CephDisk()
        if c.sh("lsb_release -si") != 'Ubuntu':
            pytest.skip("see issue http://tracker.ceph.com/issues/12787")
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        c.run_osd(osd_uuid, disk)
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk zap " + disk)

    def test_augtool(self):
        c = CephDisk()
        out = c.augtool("ls /files/etc/ceph/ceph.conf")
        assert 'global' in out

    def test_activate_dmcrypt_plain(self):
        CephDisk.augtool("set /files/etc/ceph/ceph.conf/global/osd_dmcrypt_type plain")
        self.activate_dmcrypt('plain')
        CephDisk.augtool("rm /files/etc/ceph/ceph.conf/global/osd_dmcrypt_type")

    def test_activate_dmcrypt_luks(self):
        CephDisk.augtool("rm /files/etc/ceph/ceph.conf/global/osd_dmcrypt_type")
        self.activate_dmcrypt('luks')

    def activate_dmcrypt(self, type):
        c = CephDisk()
        if c.sh("lsb_release -si") != 'Ubuntu':
            pytest.skip("see issue http://tracker.ceph.com/issues/12787")
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        journal_uuid = str(uuid.uuid1())
        c.sh("ceph-disk zap " + disk)
        c.sh("ceph-disk prepare " +
             " --osd-uuid " + osd_uuid +
             " --journal-uuid " + journal_uuid +
             " --dmcrypt " +
             " " + disk)
        data_partition = c.get_osd_partition(osd_uuid)
        c.sh("ceph-disk activate --dmcrypt " + data_partition['path'])
        data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['type'] == 'data'
        assert data_partition['state'] == 'active'
        journal_partition = c.get_journal_partition(osd_uuid)
        assert journal_partition
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk zap " + disk)

    def test_activate_no_journal(self):
        c = CephDisk()
        if c.sh("lsb_release -si") != 'Ubuntu':
            pytest.skip("see issue http://tracker.ceph.com/issues/12787")
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk zap " + disk)
        c.augtool("set /files/etc/ceph/ceph.conf/global/osd_objectstore memstore")
        c.sh("ceph-disk prepare --osd-uuid " + osd_uuid +
             " " + disk)
        device = json.loads(c.helper("ceph-disk list --format json " + disk))[0]
        assert len(device['partitions']) == 1
        partition = device['partitions'][0]
        assert partition['type'] == 'data'
        assert partition['state'] == 'active'
        assert 'journal_dev' not in partition
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk zap " + disk)
        c.augtool("rm /files/etc/ceph/ceph.conf/global/osd_objectstore")

    def test_activate_with_journal(self):
        c = CephDisk()
        if c.sh("lsb_release -si") != 'Ubuntu':
            pytest.skip("see issue http://tracker.ceph.com/issues/12787")
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk zap " + disk)
        c.sh("ceph-disk prepare --osd-uuid " + osd_uuid +
             " " + disk)
        device = json.loads(c.helper("ceph-disk list --format json " + disk))[0]
        assert len(device['partitions']) == 2
        data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['type'] == 'data'
        assert data_partition['state'] == 'active'
        journal_partition = c.get_journal_partition(osd_uuid)
        assert journal_partition
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk zap " + disk)

    def test_activate_separated_journal(self):
        c = CephDisk()
        if c.sh("lsb_release -si") != 'Ubuntu':
            pytest.skip("see issue http://tracker.ceph.com/issues/12787")
        disks = c.unused_disks()
        data_disk = disks[0]
        journal_disk = disks[1]
        osd_uuid = self.activate_separated_journal(data_disk, journal_disk)
        c.helper("pool_read_write 1") # 1 == pool size
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk zap " + data_disk + " " + journal_disk)

    def activate_separated_journal(self, data_disk, journal_disk):
        c = CephDisk()
        if c.sh("lsb_release -si") != 'Ubuntu':
            pytest.skip("see issue http://tracker.ceph.com/issues/12787")
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk prepare --osd-uuid " + osd_uuid +
             " " + data_disk + " " + journal_disk)
        device = json.loads(c.helper("ceph-disk list --format json " + data_disk))[0]
        assert len(device['partitions']) == 1
        data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['type'] == 'data'
        assert data_partition['state'] == 'active'
        journal_partition = c.get_journal_partition(osd_uuid)
        assert journal_partition
        return osd_uuid

    #
    # Create an OSD and get a journal partition from a disk that
    # already contains a journal partition which is in use. Updates of
    # the kernel partition table may behave differently when a
    # partition is in use. See http://tracker.ceph.com/issues/7334 for
    # more information.
    #
    def test_activate_two_separated_journal(self):
        c = CephDisk()
        if c.sh("lsb_release -si") != 'Ubuntu':
            pytest.skip("see issue http://tracker.ceph.com/issues/12787")
        disks = c.unused_disks()
        data_disk = disks[0]
        other_data_disk = disks[1]
        journal_disk = disks[2]
        osd_uuid = self.activate_separated_journal(data_disk, journal_disk)
        other_osd_uuid = self.activate_separated_journal(other_data_disk, journal_disk)
        #
        # read/write can only succeed if the two osds are up because
        # the pool needs two OSD
        #
        c.helper("pool_read_write 2") # 2 == pool size
        c.destroy_osd(osd_uuid)
        c.destroy_osd(other_osd_uuid)
        c.sh("ceph-disk zap " + data_disk + " " + journal_disk + " " + other_data_disk)

    #
    # Create an OSD and reuse an existing journal partition
    #
    def test_activate_reuse_journal(self):
        c = CephDisk()
        if c.sh("lsb_release -si") != 'Ubuntu':
            pytest.skip("see issue http://tracker.ceph.com/issues/12787")
        disks = c.unused_disks()
        data_disk = disks[0]
        journal_disk = disks[1]
        #
        # Create an OSD with a separated journal and destroy it.
        #
        osd_uuid = self.activate_separated_journal(data_disk, journal_disk)
        journal_partition = c.get_journal_partition(osd_uuid)
        journal_path = journal_partition['path']
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk zap " + data_disk)
        osd_uuid = str(uuid.uuid1())
        #
        # Create another OSD with the journal partition of the previous OSD
        #
        c.sh("ceph-disk prepare --osd-uuid " + osd_uuid +
             " " + data_disk + " " + journal_path)
        c.helper("pool_read_write 1") # 1 == pool size
        device = json.loads(c.helper("ceph-disk list --format json " + data_disk))[0]
        assert len(device['partitions']) == 1
        data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['type'] == 'data'
        assert data_partition['state'] == 'active'
        journal_partition = c.get_journal_partition(osd_uuid)
        #
        # Verify the previous OSD partition has been reused
        #
        assert journal_partition['path'] == journal_path
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk zap " + data_disk + " " + journal_disk)

    def test_activate_multipath(self):
        c = CephDisk()
        if c.sh("lsb_release -si") != 'CentOS':
            pytest.skip("see issue https://bugs.launchpad.net/ubuntu/+source/multipath-tools/+bug/1488688")
        c.ensure_sd()
        #
        # Figure out the name of the multipath device
        #
        disk = c.unused_disks('sd.')[0]
        c.sh("mpathconf --enable || true")
        c.sh("multipath " + disk)
        holders = os.listdir("/sys/block/" + os.path.basename(disk) + "/holders")
        assert 1 == len(holders)
        name = open("/sys/block/" + holders[0] + "/dm/name").read()
        multipath = "/dev/mapper/" + name
        #
        # Prepare the multipath device
        #
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk zap " + multipath)
        c.sh("ceph-disk prepare --osd-uuid " + osd_uuid +
             " " + multipath)
        device = json.loads(c.helper("ceph-disk list --format json " + multipath))[0]
        assert len(device['partitions']) == 2
        data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['type'] == 'data'
        #
        # Activate it although it should auto activate
        #
        if True: # remove this block when http://tracker.ceph.com/issues/12786 is fixed
            c.sh("ceph-disk activate " + data_partition['path'])
            device = json.loads(c.helper("ceph-disk list --format json " + multipath))[0]
            assert len(device['partitions']) == 2
            data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['state'] == 'active'
        journal_partition = c.get_journal_partition(osd_uuid)
        assert journal_partition
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk zap " + multipath)
        c.sh("udevadm settle")
        c.sh("multipath -F")
        c.unload_scsi_debug()

    def test_activate_lvm(self):
        '''
        Test ceph-disk osd creation with lvm logical volumes
        '''
        c = CephDisk()
        lv = c.ensure_lv()
        #
        # Prepare the lvm logical volume
        #
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk zap " + lv)
        c.sh("ceph-disk prepare --osd-uuid " + osd_uuid +
             " " + lv)
        device = json.loads(c.helper("ceph-disk list --format json " + lv))[0]
        assert len(device['partitions']) == 2
        data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['type'] == 'data'
        #
        # Activate it although it should auto activate
        #
#        if True: # remove this block when http://tracker.ceph.com/issues/12786 is fixed
#            c.sh("ceph-disk activate " + data_partition['path'])
#            device = json.loads(c.helper("ceph-disk list --format json " + multipath))[0]
#            assert len(device['partitions']) == 2
#            data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['state'] == 'active'
        journal_partition = c.get_journal_partition(osd_uuid)
        assert journal_partition
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk zap " + lv)
        c.sh("udevadm settle")
        c.remove_pv()

class CephDiskTest(CephDisk):

    def main(self, argv):
        parser = argparse.ArgumentParser(
            'ceph-disk-test',
        )
        parser.add_argument(
            '-v', '--verbose',
            action='store_true', default=None,
            help='be more verbose',
        )
        parser.add_argument(
            '--destroy-osd',
            help='stop, umount and destroy',
        )
        args = parser.parse_args(argv)

        if args.verbose:
            logging.basicConfig(level=logging.DEBUG)

        if args.destroy_osd:
            dump = json.loads(CephDisk.sh("ceph osd dump -f json"))
            osd_uuid = None
            for osd in dump['osds']:
                if str(osd['osd']) == args.destroy_osd:
                    osd_uuid = osd['uuid']
            if osd_uuid:
                self.destroy_osd(osd_uuid)
            else:
                raise Exception("cannot find OSD " + args.destroy_osd +
                                " ceph osd dump -f json")
            return

if __name__ == '__main__':
    sys.exit(CephDiskTest().main(sys.argv[1:]))
