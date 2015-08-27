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
        id = self.sh("ceph osd create " + uuid)
        partition = self.get_osd_partition(id)
        assert partition['type'] == 'data'
        assert partition['state'] == 'active'
        return id

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
        d = tempfile.mkdtemp()
        c.sh("ceph-disk zap " + disk)
        c.sh("ceph-disk prepare " +
             " --dmcrypt-key-dir " + d +
             " --osd-uuid " + osd_uuid +
             " --journal-uuid " + journal_uuid +
             " --dmcrypt " +
             " " + disk)
        if type == 'plain':
            c.sh("cryptsetup --key-file " + d + "/" + osd_uuid +
                 " --key-size 256 create " + osd_uuid +
                 " " + disk + "1")
        else:
            c.sh("cryptsetup --key-file " + d + "/" + osd_uuid + ".luks.key" +
                 " luksOpen " +
                 " " + disk + "1" +
                 " " + osd_uuid)
        if type == 'plain':
            c.sh("cryptsetup --key-file " + d + "/" + journal_uuid +
                 " --key-size 256 create " + journal_uuid +
                 " " + disk + "2")
        else:
            c.sh("cryptsetup --key-file " + d + "/" + journal_uuid + ".luks.key" +
                 " luksOpen " +
                 " " + disk + "2" +
                 " " + journal_uuid)
        c.sh("ceph-disk activate /dev/mapper/" + osd_uuid)
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
            self.destroy_osd(args.destroy_osd)
            return

if __name__ == '__main__':
    sys.exit(CephDiskTest().main(sys.argv[1:]))
