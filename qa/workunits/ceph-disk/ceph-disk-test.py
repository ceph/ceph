#
# Copyright (C) 2015, 2016 Red Hat <contact@redhat.com>
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
# When debugging these tests (must be root), here are a few useful commands:
#
#  export PATH=.:..:$PATH
#  ceph-disk.sh # run once to prepare the environment as it would be by teuthology
#  ln -sf /home/ubuntu/ceph/src/ceph-disk/ceph_disk/main.py $(which ceph-disk)
#  ln -sf /home/ubuntu/ceph/udev/95-ceph-osd.rules /lib/udev/rules.d/95-ceph-osd.rules
#  ln -sf /home/ubuntu/ceph/systemd/ceph-disk@.service /usr/lib/systemd/system/ceph-disk@.service
#  ceph-disk.conf will be silently ignored if it is a symbolic link or a hard link /var/log/upstart for logs
#  cp /home/ubuntu/ceph/src/upstart/ceph-disk.conf /etc/init/ceph-disk.conf
#  id=3 ; ceph-disk deactivate --deactivate-by-id $id ; ceph-disk destroy --zap --destroy-by-id $id
#  py.test -s -v -k test_activate_dmcrypt_luks ceph-disk-test.py
#
#  CentOS 7
#    udevadm monitor --property & tail -f /var/log/messages
#    udev rules messages are logged in /var/log/messages
#    systemctl stop ceph-osd@2
#    systemctl start ceph-osd@2
#
#  udevadm monitor --property & tail -f /var/log/syslog /var/log/upstart/*  # on Ubuntu 14.04
#  udevadm test --action=add /block/vdb/vdb1 # verify the udev rule is run as expected
#  udevadm control --reload # when changing the udev rules
#  sudo /usr/sbin/ceph-disk -v trigger /dev/vdb1 # activates if vdb1 is data
#
#  integration tests coverage
#  pip install coverage
#  perl -pi -e 's|"ceph-disk |"coverage run --source=/usr/sbin/ceph-disk --append /usr/sbin/ceph-disk |' ceph-disk-test.py
#  rm -f .coverage ; py.test -s -v ceph-disk-test.py
#  coverage report --show-missing
#
import argparse
import json
import logging
import configobj
import os
import pytest
import re
import subprocess
import sys
import tempfile
import time
import uuid

LOG = logging.getLogger('CephDisk')


class CephDisk:

    def __init__(self):
        self.conf = configobj.ConfigObj('/etc/ceph/ceph.conf')

    def save_conf(self):
        self.conf.write(open('/etc/ceph/ceph.conf', 'w'))

    @staticmethod
    def helper(command):
        command = "ceph-helpers-root.sh " + command
        return CephDisk.sh(command)

    @staticmethod
    def sh(command):
        LOG.debug(":sh: " + command)
        proc = subprocess.Popen(
            args=command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            bufsize=1)
        lines = []
        with proc.stdout:
            for line in iter(proc.stdout.readline, b''):
                line = line.decode('utf-8')
                if 'dangerous and experimental' in line:
                    LOG.debug('SKIP dangerous and experimental')
                    continue
                lines.append(line)
                LOG.debug(line.strip().encode('ascii', 'ignore'))
        if proc.wait() != 0:
            raise subprocess.CalledProcessError(
                returncode=proc.returncode,
                cmd=command
            )
        return "".join(lines)

    def unused_disks(self, pattern='[vs]d.'):
        names = filter(
            lambda x: re.match(pattern, x), os.listdir("/sys/block"))
        if not names:
            return []
        disks = json.loads(
            self.sh("ceph-disk list --format json " + " ".join(names)))
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

    def get_lockbox(self):
        disks = json.loads(self.sh("ceph-disk list --format json"))
        for disk in disks:
            if 'partitions' in disk:
                for partition in disk['partitions']:
                    if partition.get('type') == 'lockbox':
                        return partition
        raise Exception("no lockbox found " + str(disks))

    def get_osd_partition(self, uuid):
        disks = json.loads(self.sh("ceph-disk list --format json"))
        for disk in disks:
            if 'partitions' in disk:
                for partition in disk['partitions']:
                    if partition.get('uuid') == uuid:
                        return partition
        raise Exception("uuid = " + uuid + " not found in " + str(disks))

    def get_journal_partition(self, uuid):
        return self.get_space_partition('journal', uuid)

    def get_space_partition(self, name, uuid):
        data_partition = self.get_osd_partition(uuid)
        space_dev = data_partition[name + '_dev']
        disks = json.loads(self.sh("ceph-disk list --format json"))
        for disk in disks:
            if 'partitions' in disk:
                for partition in disk['partitions']:
                    if partition['path'] == space_dev:
                        if name + '_for' in partition:
                            assert partition[
                                name + '_for'] == data_partition['path']
                        return partition
        raise Exception(
            name + " for uuid = " + uuid + " not found in " + str(disks))

    def destroy_osd(self, uuid):
        id = self.sh("ceph osd create " + uuid).strip()
        self.sh("""
        set -xe
        ceph-disk --verbose deactivate --deactivate-by-id {id}
        ceph-disk --verbose destroy --destroy-by-id {id} --zap
        """.format(id=id))

    def deactivate_osd(self, uuid):
        id = self.sh("ceph osd create " + uuid).strip()
        self.sh("""
        set -xe
        ceph-disk --verbose deactivate --once --deactivate-by-id {id}
        """.format(id=id))

    @staticmethod
    def osd_up_predicate(osds, uuid):
        for osd in osds:
            if osd['uuid'] == uuid and 'up' in osd['state']:
                return True
        return False

    @staticmethod
    def wait_for_osd_up(uuid):
        CephDisk.wait_for_osd(uuid, CephDisk.osd_up_predicate, 'up')

    @staticmethod
    def osd_down_predicate(osds, uuid):
        found = False
        for osd in osds:
            if osd['uuid'] == uuid:
                found = True
                if 'down' in osd['state'] or ['exists'] == osd['state']:
                    return True
        return not found

    @staticmethod
    def wait_for_osd_down(uuid):
        CephDisk.wait_for_osd(uuid, CephDisk.osd_down_predicate, 'down')

    @staticmethod
    def wait_for_osd(uuid, predicate, info):
        LOG.info("wait_for_osd " + info + " " + uuid)
        for delay in (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024):
            dump = json.loads(CephDisk.sh("ceph osd dump -f json"))
            if predicate(dump['osds'], uuid):
                return True
            time.sleep(delay)
        raise Exception('timeout waiting for osd ' + uuid + ' to be ' + info)

    def check_osd_status(self, uuid, space_name=None):
        data_partition = self.get_osd_partition(uuid)
        assert data_partition['type'] == 'data'
        assert data_partition['state'] == 'active'
        if space_name is not None:
            space_partition = self.get_space_partition(space_name, uuid)
            assert space_partition


class TestCephDisk(object):

    def setup_class(self):
        logging.basicConfig(level=logging.DEBUG)
        c = CephDisk()
        if c.sh("lsb_release -si").strip() == 'CentOS':
            c.helper("install multipath-tools device-mapper-multipath")
        c.conf['global']['pid file'] = '/var/run/ceph/$cluster-$name.pid'
        #
        # objecstore
        #
        c.conf['global']['osd journal size'] = 100
        #
        # bluestore
        #
        c.conf['global']['enable experimental unrecoverable data corrupting features'] = '*'
        c.conf['global']['bluestore fsck on mount'] = 'true'
        c.save_conf()

    def setup(self):
        c = CephDisk()
        for key in ('osd objectstore', 'osd dmcrypt type'):
            if key in c.conf['global']:
                del c.conf['global'][key]
        c.save_conf()

    def test_deactivate_reactivate_osd(self):
        c = CephDisk()
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk --verbose zap " + disk)
        c.sh("ceph-disk --verbose prepare --osd-uuid " + osd_uuid +
             " " + disk)
        c.wait_for_osd_up(osd_uuid)
        device = json.loads(c.sh("ceph-disk list --format json " + disk))[0]
        assert len(device['partitions']) == 2
        c.check_osd_status(osd_uuid, 'journal')
        data_partition = c.get_osd_partition(osd_uuid)
        c.sh("ceph-disk --verbose deactivate " + data_partition['path'])
        c.wait_for_osd_down(osd_uuid)
        c.sh("ceph-disk --verbose activate " + data_partition['path'] + " --reactivate")
        # check again
        c.wait_for_osd_up(osd_uuid)
        device = json.loads(c.sh("ceph-disk list --format json " + disk))[0]
        assert len(device['partitions']) == 2
        c.check_osd_status(osd_uuid, 'journal')
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)

    def test_destroy_osd_by_id(self):
        c = CephDisk()
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk --verbose prepare --osd-uuid " + osd_uuid + " " + disk)
        c.wait_for_osd_up(osd_uuid)
        c.check_osd_status(osd_uuid)
        c.destroy_osd(osd_uuid)

    def test_destroy_osd_by_dev_path(self):
        c = CephDisk()
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk --verbose prepare --osd-uuid " + osd_uuid + " " + disk)
        c.wait_for_osd_up(osd_uuid)
        partition = c.get_osd_partition(osd_uuid)
        assert partition['type'] == 'data'
        assert partition['state'] == 'active'
        c.sh("ceph-disk --verbose deactivate " + partition['path'])
        c.wait_for_osd_down(osd_uuid)
        c.sh("ceph-disk --verbose destroy " + partition['path'] + " --zap")

    def test_deactivate_reactivate_dmcrypt_plain(self):
        c = CephDisk()
        c.conf['global']['osd dmcrypt type'] = 'plain'
        c.save_conf()
        osd_uuid = self.activate_dmcrypt('ceph-disk-no-lockbox')
        data_partition = c.get_osd_partition(osd_uuid)
        c.sh("ceph-disk --verbose deactivate " + data_partition['path'])
        c.wait_for_osd_down(osd_uuid)
        c.sh("ceph-disk --verbose activate-journal " + data_partition['journal_dev'] +
             " --reactivate" + " --dmcrypt")
        c.wait_for_osd_up(osd_uuid)
        c.check_osd_status(osd_uuid, 'journal')
        c.destroy_osd(osd_uuid)
        c.save_conf()

    def test_deactivate_reactivate_dmcrypt_luks(self):
        c = CephDisk()
        osd_uuid = self.activate_dmcrypt('ceph-disk')
        data_partition = c.get_osd_partition(osd_uuid)
        lockbox_partition = c.get_lockbox()
        c.sh("ceph-disk --verbose deactivate " + data_partition['path'])
        c.wait_for_osd_down(osd_uuid)
        c.sh("ceph-disk --verbose trigger --sync " + lockbox_partition['path'])
        c.sh("ceph-disk --verbose activate-journal " + data_partition['journal_dev'] +
             " --reactivate" + " --dmcrypt")
        c.wait_for_osd_up(osd_uuid)
        c.check_osd_status(osd_uuid, 'journal')
        c.destroy_osd(osd_uuid)

    def test_activate_dmcrypt_plain_no_lockbox(self):
        c = CephDisk()
        c.conf['global']['osd dmcrypt type'] = 'plain'
        c.save_conf()
        osd_uuid = self.activate_dmcrypt('ceph-disk-no-lockbox')
        c.destroy_osd(osd_uuid)
        c.save_conf()

    def test_activate_dmcrypt_luks_no_lockbox(self):
        c = CephDisk()
        osd_uuid = self.activate_dmcrypt('ceph-disk-no-lockbox')
        c.destroy_osd(osd_uuid)

    def test_activate_dmcrypt_luks_with_lockbox(self):
        c = CephDisk()
        osd_uuid = self.activate_dmcrypt('ceph-disk')
        c.destroy_osd(osd_uuid)

    def test_activate_lockbox(self):
        c = CephDisk()
        osd_uuid = self.activate_dmcrypt('ceph-disk')
        lockbox = c.get_lockbox()
        assert lockbox['state'] == 'active'
        c.sh("umount " + lockbox['path'])
        lockbox = c.get_lockbox()
        assert lockbox['state'] == 'prepared'
        c.sh("ceph-disk --verbose trigger " + lockbox['path'])
        success = False
        for delay in (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024):
            lockbox = c.get_lockbox()
            if lockbox['state'] == 'active':
                success = True
                break
            time.sleep(delay)
        if not success:
            raise Exception('timeout waiting for lockbox ' + lockbox['path'])
        c.destroy_osd(osd_uuid)

    def activate_dmcrypt(self, ceph_disk):
        c = CephDisk()
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        journal_uuid = str(uuid.uuid1())
        c.sh("ceph-disk --verbose zap " + disk)
        c.sh(ceph_disk + " --verbose prepare " +
             " --osd-uuid " + osd_uuid +
             " --journal-uuid " + journal_uuid +
             " --dmcrypt " +
             " " + disk)
        c.wait_for_osd_up(osd_uuid)
        c.check_osd_status(osd_uuid, 'journal')
        return osd_uuid

    def test_trigger_dmcrypt_journal_lockbox(self):
        c = CephDisk()
        osd_uuid = self.activate_dmcrypt('ceph-disk')
        data_partition = c.get_osd_partition(osd_uuid)
        lockbox_partition = c.get_lockbox()
        c.deactivate_osd(osd_uuid)
        c.wait_for_osd_down(osd_uuid)
        with pytest.raises(subprocess.CalledProcessError):
            # fails because the lockbox is not mounted yet
            c.sh("ceph-disk --verbose trigger --sync " + data_partition['journal_dev'])
        c.sh("ceph-disk --verbose trigger --sync " + lockbox_partition['path'])
        c.wait_for_osd_up(osd_uuid)
        c.destroy_osd(osd_uuid)

    def test_trigger_dmcrypt_data_lockbox(self):
        c = CephDisk()
        osd_uuid = self.activate_dmcrypt('ceph-disk')
        data_partition = c.get_osd_partition(osd_uuid)
        lockbox_partition = c.get_lockbox()
        c.deactivate_osd(osd_uuid)
        c.wait_for_osd_down(osd_uuid)
        with pytest.raises(subprocess.CalledProcessError):
            # fails because the lockbox is not mounted yet
            c.sh("ceph-disk --verbose trigger --sync " + data_partition['path'])
        c.sh("ceph-disk --verbose trigger --sync " + lockbox_partition['path'])
        c.wait_for_osd_up(osd_uuid)
        c.destroy_osd(osd_uuid)

    def test_trigger_dmcrypt_lockbox(self):
        c = CephDisk()
        osd_uuid = self.activate_dmcrypt('ceph-disk')
        data_partition = c.get_osd_partition(osd_uuid)
        lockbox_partition = c.get_lockbox()
        c.deactivate_osd(osd_uuid)
        c.wait_for_osd_down(osd_uuid)
        c.sh("ceph-disk --verbose trigger --sync " + lockbox_partition['path'])
        c.wait_for_osd_up(osd_uuid)
        c.destroy_osd(osd_uuid)

    def test_activate_no_journal(self):
        c = CephDisk()
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk --verbose zap " + disk)
        c.conf['global']['osd objectstore'] = 'memstore'
        c.save_conf()
        c.sh("ceph-disk --verbose prepare --osd-uuid " + osd_uuid +
             " " + disk)
        c.wait_for_osd_up(osd_uuid)
        device = json.loads(c.sh("ceph-disk list --format json " + disk))[0]
        assert len(device['partitions']) == 1
        partition = device['partitions'][0]
        assert partition['type'] == 'data'
        assert partition['state'] == 'active'
        assert 'journal_dev' not in partition
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)
        c.save_conf()

    def test_activate_with_journal_dev_no_symlink(self):
        c = CephDisk()
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk --verbose zap " + disk)
        c.sh("ceph-disk --verbose prepare --osd-uuid " + osd_uuid +
             " " + disk)
        c.wait_for_osd_up(osd_uuid)
        device = json.loads(c.sh("ceph-disk list --format json " + disk))[0]
        assert len(device['partitions']) == 2
        c.check_osd_status(osd_uuid, 'journal')
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)

    def test_activate_bluestore(self):
        c = CephDisk()
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk --verbose zap " + disk)
        c.conf['global']['osd objectstore'] = 'bluestore'
        c.save_conf()
        c.sh("ceph-disk --verbose prepare --bluestore --osd-uuid " + osd_uuid +
             " " + disk)
        c.wait_for_osd_up(osd_uuid)
        device = json.loads(c.sh("ceph-disk list --format json " + disk))[0]
        assert len(device['partitions']) == 2
        c.check_osd_status(osd_uuid, 'block')
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)

    def test_activate_with_journal_dev_is_symlink(self):
        c = CephDisk()
        disk = c.unused_disks()[0]
        osd_uuid = str(uuid.uuid1())
        tempdir = tempfile.mkdtemp()
        symlink = os.path.join(tempdir, 'osd')
        os.symlink(disk, symlink)
        c.sh("ceph-disk --verbose zap " + symlink)
        c.sh("ceph-disk --verbose prepare --osd-uuid " + osd_uuid +
             " " + symlink)
        c.wait_for_osd_up(osd_uuid)
        device = json.loads(c.sh("ceph-disk list --format json " + symlink))[0]
        assert len(device['partitions']) == 2
        data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['type'] == 'data'
        assert data_partition['state'] == 'active'
        journal_partition = c.get_journal_partition(osd_uuid)
        assert journal_partition
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk --verbose zap " + symlink)
        os.unlink(symlink)
        os.rmdir(tempdir)

    def test_activate_separated_journal(self):
        c = CephDisk()
        disks = c.unused_disks()
        data_disk = disks[0]
        journal_disk = disks[1]
        osd_uuid = self.activate_separated_journal(data_disk, journal_disk)
        c.helper("pool_read_write 1")  # 1 == pool size
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk --verbose zap " + data_disk + " " + journal_disk)

    def test_activate_separated_journal_dev_is_symlink(self):
        c = CephDisk()
        disks = c.unused_disks()
        data_disk = disks[0]
        journal_disk = disks[1]
        tempdir = tempfile.mkdtemp()
        data_symlink = os.path.join(tempdir, 'osd')
        os.symlink(data_disk, data_symlink)
        journal_symlink = os.path.join(tempdir, 'journal')
        os.symlink(journal_disk, journal_symlink)
        osd_uuid = self.activate_separated_journal(
            data_symlink, journal_symlink)
        c.helper("pool_read_write 1")  # 1 == pool size
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk --verbose zap " + data_symlink + " " + journal_symlink)
        os.unlink(data_symlink)
        os.unlink(journal_symlink)
        os.rmdir(tempdir)

    def activate_separated_journal(self, data_disk, journal_disk):
        c = CephDisk()
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk --verbose prepare --osd-uuid " + osd_uuid +
             " " + data_disk + " " + journal_disk)
        c.wait_for_osd_up(osd_uuid)
        device = json.loads(
            c.sh("ceph-disk list --format json " + data_disk))[0]
        assert len(device['partitions']) == 1
        c.check_osd_status(osd_uuid, 'journal')
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
        disks = c.unused_disks()
        data_disk = disks[0]
        other_data_disk = disks[1]
        journal_disk = disks[2]
        osd_uuid = self.activate_separated_journal(data_disk, journal_disk)
        other_osd_uuid = self.activate_separated_journal(
            other_data_disk, journal_disk)
        #
        # read/write can only succeed if the two osds are up because
        # the pool needs two OSD
        #
        c.helper("pool_read_write 2")  # 2 == pool size
        c.destroy_osd(osd_uuid)
        c.destroy_osd(other_osd_uuid)
        c.sh("ceph-disk --verbose zap " + data_disk + " " +
             journal_disk + " " + other_data_disk)

    #
    # Create an OSD and reuse an existing journal partition
    #
    def test_activate_reuse_journal(self):
        c = CephDisk()
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
        c.sh("ceph-disk --verbose zap " + data_disk)
        osd_uuid = str(uuid.uuid1())
        #
        # Create another OSD with the journal partition of the previous OSD
        #
        c.sh("ceph-disk --verbose prepare --osd-uuid " + osd_uuid +
             " " + data_disk + " " + journal_path)
        c.helper("pool_read_write 1")  # 1 == pool size
        c.wait_for_osd_up(osd_uuid)
        device = json.loads(
            c.sh("ceph-disk list --format json " + data_disk))[0]
        assert len(device['partitions']) == 1
        c.check_osd_status(osd_uuid)
        journal_partition = c.get_journal_partition(osd_uuid)
        #
        # Verify the previous OSD partition has been reused
        #
        assert journal_partition['path'] == journal_path
        c.destroy_osd(osd_uuid)
        c.sh("ceph-disk --verbose zap " + data_disk + " " + journal_disk)

    def test_activate_multipath(self):
        c = CephDisk()
        if c.sh("lsb_release -si").strip() != 'CentOS':
            pytest.skip(
                "see issue https://bugs.launchpad.net/ubuntu/+source/multipath-tools/+bug/1488688")
        c.ensure_sd()
        #
        # Figure out the name of the multipath device
        #
        disk = c.unused_disks('sd.')[0]
        c.sh("mpathconf --enable || true")
        c.sh("multipath " + disk)
        holders = os.listdir(
            "/sys/block/" + os.path.basename(disk) + "/holders")
        assert 1 == len(holders)
        name = open("/sys/block/" + holders[0] + "/dm/name").read()
        multipath = "/dev/mapper/" + name
        #
        # Prepare the multipath device
        #
        osd_uuid = str(uuid.uuid1())
        c.sh("ceph-disk --verbose zap " + multipath)
        c.sh("ceph-disk --verbose prepare --osd-uuid " + osd_uuid +
             " " + multipath)
        c.wait_for_osd_up(osd_uuid)
        device = json.loads(
            c.sh("ceph-disk list --format json " + multipath))[0]
        assert len(device['partitions']) == 2
        data_partition = c.get_osd_partition(osd_uuid)
        assert data_partition['type'] == 'data'
        assert data_partition['state'] == 'active'
        journal_partition = c.get_journal_partition(osd_uuid)
        assert journal_partition
        c.helper("pool_read_write")
        c.destroy_osd(osd_uuid)
        c.sh("udevadm settle")
        c.sh("multipath -F")
        c.unload_scsi_debug()


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
