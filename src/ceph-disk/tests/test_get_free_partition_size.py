# Copyright (C) 2015, 2016 SUSE LINUX GmbH <osynge@suse.com>
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
import mock
import logging
import pytest

from ceph_disk import main

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger("tester")


class TestGetFreePartitionSize(object):
    @mock.patch('ceph_disk.main._check_output')
    def test_disk_empty(self, m__check_output):
        m__check_output.return_value = """BYT;
/dev/testdisk:20480MiB:virtblk:512:512:gpt:Virtio Block Device:;
1:0.02MiB:20480MiB:20480MiB:free;
"""
        size = main.get_free_partition_size('/dev/testdisk')
        assert size == 20480

    @mock.patch('ceph_disk.main._check_output')
    def test_disk_empty_gib(self, m__check_output):
        m__check_output.return_value = """BYT;
/dev/testdisk:20.0GiB:virtblk:512:512:gpt:Virtio Block Device:;
1:0.00GiB:20.0GiB:20.0GiB:free;
"""
        size = main.get_free_partition_size('/dev/testdisk', 'gibibyte')
        LOG.error(size)
        assert size == 20

    @mock.patch('ceph_disk.main._check_output')
    def test_disk_empty_byte(self, m__check_output):
        m__check_output.return_value = """BYT;
/dev/testdisk:21474836480B:virtblk:512:512:gpt:Virtio Block Device:;
1:17408B:21474819583B:21474802176B:free;
"""
        size = main.get_free_partition_size('/dev/testdisk', 'bytes')
        LOG.error(size)
        assert size == 21474802176

    @mock.patch('ceph_disk.main._check_output')
    def test_disk_empty_mb(self, m__check_output):
        m__check_output.return_value = """BYT;
/dev/testdisk:21475MB:virtblk:512:512:gpt:Virtio Block Device:;
1:0.02MB:21475MB:21475MB:free;
"""
        size = main.get_free_partition_size('/dev/testdisk', 'megabytes')
        LOG.error(size)
        assert size == 21475

    @mock.patch('ceph_disk.main._check_output')
    def test_disk_empty_mib(self, m__check_output):
        m__check_output.return_value = """BYT;
/dev/testdisk:20480MiB:virtblk:512:512:gpt:Virtio Block Device:;
1:0.02MiB:20480MiB:20480MiB:free;
"""
        size = main.get_free_partition_size('/dev/testdisk', 'mebibyte')
        LOG.error(size)
        assert size == 20480

    @mock.patch('ceph_disk.main._check_output')
    def test_disk_empty_gb(self, m__check_output):
        m__check_output.return_value = """BYT;
/dev/testdisk:21.5GB:virtblk:512:512:gpt:Virtio Block Device:;
1:0.00GB:21.5GB:21.5GB:free;
"""
        size = main.get_free_partition_size('/dev/testdisk', 'gigabyte')
        LOG.error(size)
        assert size == 21

    @mock.patch('ceph_disk.main._check_output')
    def test_disk_full(self, m__check_output):
        m__check_output.return_value = """BYT;
/dev/testdisk:20480MiB:virtblk:512:512:gpt:Virtio Block Device:;
1:0.02MiB:1.00MiB:0.98MiB:free;
1:1.00MiB:20480MiB:20479MiB::Linux filesystem:;
"""
        size = main.get_free_partition_size('/dev/testdisk')
        assert size == 0

    @mock.patch('ceph_disk.main._check_output')
    def test_disk_no_partition_table(self, m__check_output):
        m__check_output.return_value = """BYT;
/dev/testdisk:20480MiB:virtblk:512:512:unknown:Virtio Block Device:;
"""
        size = main.get_free_partition_size('/dev/testdisk')
        assert size == 20480

    @mock.patch('ceph_disk.main._check_output')
    def test_disk_parted_no_output(self, m__check_output):
        m__check_output.return_value = ''
        with pytest.raises(main.Error) as err:
            main.get_free_partition_size('/dev/testdisk')
        assert "parted failed to output anything for" in str(err)
        assert "/dev/testdisk" in str(err)

    @mock.patch('ceph_disk.main._check_output')
    def test_disk_parted_output_silly(self, m__check_output):
        m__check_output.return_value = 'silly'
        with pytest.raises(main.Error) as err:
            main.get_free_partition_size('/dev/testdisk')
        assert "Failed to get size free space in" in str(err)
        assert "/dev/testdisk" in str(err)
