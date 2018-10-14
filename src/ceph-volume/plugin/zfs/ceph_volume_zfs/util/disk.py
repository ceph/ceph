# vim: expandtab smarttab shiftwidth=4 softtabstop=4

import logging
import os
import re
import stat
from ceph_volume import process
from ceph_volume_zfs.api import zfs
from ceph_volume_zfs.util.system import get_file_contents

logger = logging.getLogger(__name__)

class Disk(object):
    """
    Represents a disk on the system, with some top-level attributes like
    name and path
    """

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class Disks(list):
    """
    A list of all disks in the system
    """

    def __init__(self):
        self._populate()

    def _populate(self):
        # get all the disks in the current system
        for disk_item in get_api_geom_list():
            self.append(Disk(**disk_item))


def _output_geom_parser(output):
    """
    kern.geom.conftxt contains DISK, PART and LABEL lines
    filter on those properties and only return those
    rank, class, name, size sectorsize, 'hd' heads, 'sc' sectors
    0 DISK vtbd1 268435456000 512 hd 16 sc 63
    0 DISK vtbd0 536870912000 512 hd 16 sc 63
    rank, class, name, size, 'i' index, 'o' offset, 'ty' type,
        'xs' scheme, 'xt' rawtype,
    1 PART vtbd0p2 536870805504 512 i 2 o 86016 ty freebsd-ufs \
        xs GPT xt 516e7cb6-6ecf-11d6-8ff8-00022d09712b
    1 PART vtbd0p1 65536 512 i 1 o 20480 ty freebsd-boot \
        xs GPT xt 83bd6b9d-7f41-11dc-be0b-001560b84f0f
    ank, class, name, size, secorsize, 'i' index, 'o' offset
    2 LABEL gptid/2481ea40-dbb8-11e5-b242-dd112b2c50de 65536 512 i 0 o 0

    :param fields: A string, possibly using ',' to group many items, as it
                   would be used on the CLI
    :param output: The CLI output from the sysctl kern.geom.conftxt call
    """
    report = []
    fields = ['rank', 'class', 'name', 'size', 'rest']
    for line in output:
        # clear the leading/trailing whitespace
        line = line.strip()
        # prevent moving forward with empty contents
        if not line:
            continue

        output_items = [i.strip() for i in line.split(None, 4)]
        # map the output to the fields
        report.append(
            dict(zip(fields, output_items))
        )
    return report


def get_api_geom_list():
    stdout, stderr, returncode = process.call(
        ['sysctl', '-n', 'kern.geom.conftxt']
    )
    return _output_geom_parser(stdout)


def _stat_is_device(stat_obj):
    """
    Helper function that will interpret ``os.stat`` output directly,
    so that other functions can call ``os.stat`` once and interpret
    that result several times
    """
    return stat.S_ISBLK(stat_obj)


def is_device(dev):
    """
    Boolean to determine if a given device is a disk device
    (**not** a partition!)
    param dev: should start with '/dev/'
    return:    True if it is the raw disk device
    """
    if not os.path.exists(dev):
        return False
    dev = dev[5:]
    stdout, stderr, returncode = process.call(
        ['sysctl', '-n', 'kern.disks']
    )
    disks = stdout[0].split()
    if dev in disks:
        return True
    return False

def is_partition(dev):
    """
    Boolean to determine if a given device is a partition, like /dev/sda1
    """
    if not os.path.exists(dev):
        return False

    # fallback to stat
    stat_obj = os.stat(dev)
    if _stat_is_device(stat_obj.st_mode):
        return False

    return True

