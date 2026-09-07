import re

from ceph_volume.util.disk import human_readable_size
from ceph_volume import process
from ceph_volume import sys_info

report_template = """
/dev/{geomname:<16} {mediasize:<16} {rotational!s:<7} {descr}"""
# aligned under the main row: 4-space indent + "/dev/" (5) + name padded to
# 12 == 21 chars, same offset where the main row's mediasize column starts
usage_template = """
    /dev/{name:<12} {mediasize:<16} {mount_path:<24} {reason}"""
# {geomname:<25} {mediasize:<12} {rotational!s:<7} {mode!s:<9} {descr}"""

def geom_disk_parser(block):
    """
    Parses lines in 'geom disk list` output.

    Geom name: ada3
    Providers:
    1. Name: ada3
       Mediasize: 40018599936 (37G)
       Sectorsize: 512
       Stripesize: 4096
       Stripeoffset: 0
       Mode: r2w2e4
       descr: Corsair CSSD-F40GB2
       lunid: 5000000000000236
       ident: 111465010000101800EC
       rotationrate: 0
       fwsectors: 63
       fwheads: 16

    :param line: A string, with the full block for `geom disk list`
    """
    pairs = block.split(';')
    parsed = {}
    for pair in pairs:
        if 'Providers' in pair:
            continue
        try:
            column, value = pair.split(':')
        except ValueError:
            continue
        # fixup
        column = re.sub(r"\s+", "", column)
        column= re.sub(r"^[0-9]+\.", "", column)
        value = value.strip()
        value = re.sub(r'\s*\([0-9A-Za-z.]+\)', '', value).strip()
        column = re.sub(r"^[0-9]+\.", "", column)
        value = value.strip()
        value = re.sub(r"\([0-9A-Z]+\)", '', value)
        parsed[column.lower()] = value
    return parsed

def get_disk(diskname):
    """
    Captures all available info from geom
    along with interesting metadata like sectors, size, vendor,
    solid/rotational, etc...

    Returns a dictionary, with all the geom fields as keys.
    """

    command = ['/sbin/geom', 'disk', 'list', re.sub('/dev/', '', diskname)]
    out, err, rc = process.call(command)
    geom_block = ""
    for line in out:
        line.strip()
        geom_block += ";" + line
    disk = geom_disk_parser(geom_block)
    return disk

def get_disks():
    command = ['/sbin/geom', 'disk', 'status', '-s']
    out, err, rc = process.call(command)
    disks = {}
    for path in out:
        dsk, rest1, rest2 = path.split()
        disk = get_disk(dsk)
        disks['/dev/'+dsk] = disk
    return disks

def get_partitions(diskname):
    """
    Returns a dict of partition name (e.g. 'ada0p2') -> {'type': ...,
    'mediasize': ...} (e.g. type 'freebsd-zfs', 'freebsd-swap') for the
    given disk, via `gpart list`. Empty if the disk has no partition table.
    """
    command = ['/sbin/gpart', 'list', re.sub('/dev/', '', diskname)]
    # a disk with no partition table makes gpart exit non-zero; that's an
    # expected, normal case here, not a failure worth logging
    out, err, rc = process.call(command, verbose_on_failure=False)
    partitions = {}
    name = None
    entry = {}
    for line in out:
        line = line.strip()
        match = re.match(r'^\d+\.\s*Name:\s*(\S+)', line)
        if match:
            if name:
                partitions[name] = entry
            name = match.group(1)
            entry = {}
            continue
        match = re.match(r'^Mediasize:\s*(\d+)', line)
        if match and name:
            entry['mediasize'] = match.group(1)
            continue
        match = re.match(r'^type:\s*(\S+)', line)
        if match and name:
            entry['type'] = match.group(1)
    if name:
        partitions[name] = entry
    return partitions

def get_mounts():
    """
    Returns a dict of GEOM device name (e.g. 'ada0p2') -> (mountpoint, fstype)
    for everything currently mounted, via `mount -p`.
    """
    command = ['/sbin/mount', '-p']
    out, err, rc = process.call(command)
    mounts = {}
    for line in out:
        fields = line.split()
        if len(fields) < 3:
            continue
        special, mountpoint, fstype = fields[0], fields[1], fields[2]
        mounts[re.sub('^/dev/', '', special)] = (mountpoint, fstype)
    return mounts

def get_swap_devices():
    """
    Returns a set of GEOM device names (e.g. 'ada0p3') configured as swap,
    via `swapinfo`.
    """
    command = ['/usr/sbin/swapinfo']
    out, err, rc = process.call(command)
    devices = set()
    for line in out:
        if not line.startswith('/dev/'):
            continue
        devices.add(re.sub('^/dev/', '', line.split()[0]))
    return devices

def get_zfs_mountpoints():
    """
    Returns a dict of ZFS dataset/pool name -> mountpoint, via
    `zfs list -H -o name,mountpoint`. Keyed by full dataset name (a pool's
    root dataset shares the pool's name), so a leaf vdev's pool can be
    looked up directly to get where that pool is mounted.
    """
    command = ['/sbin/zfs', 'list', '-H', '-o', 'name,mountpoint']
    out, err, rc = process.call(command)
    mountpoints = {}
    for line in out:
        fields = line.split('\t')
        if len(fields) < 2:
            continue
        name, mountpoint = fields[0], fields[1]
        mountpoints[name] = mountpoint
    return mountpoints

def get_zpool_devices():
    """
    Returns a dict of GEOM device name (e.g. 'ada0p4') -> zpool name, for
    every leaf vdev in every imported pool, via `zpool status -PL`.
    """
    command = ['/sbin/zpool', 'status', '-PL']
    out, err, rc = process.call(command)
    devices = {}
    pool = None
    for line in out:
        stripped = line.strip()
        if stripped.startswith('pool:'):
            pool = stripped.split(':', 1)[1].strip()
        elif pool and stripped.startswith('/dev/'):
            devices[re.sub('^/dev/', '', stripped.split()[0])] = pool
    return devices

class Disks(object):

    def __init__(self, path=None):
        if not sys_info.devices:
            sys_info.devices = get_disks()
        mounts = get_mounts()
        swap_devices = get_swap_devices()
        zpool_devices = get_zpool_devices()
        zfs_mountpoints = get_zfs_mountpoints()
        self.disks = {}
        for k in sys_info.devices:
            if path != None:
                if path in k:
                    self.disks[k] = Disk(k, mounts, swap_devices, zpool_devices, zfs_mountpoints)
            else:
                self.disks[k] = Disk(k, mounts, swap_devices, zpool_devices, zfs_mountpoints)

    def pretty_report(self, all=True):
        output = [
            report_template.format(
                geomname='Device Path',
                mediasize='Size',
                rotational='rotates',
                descr='Model name',
            )]
        for disk in sorted(self.disks):
            output.append(self.disks[disk].report())
        return ''.join(output)

    def json_report(self):
        output = []
        for disk in sorted(self.disks):
            output.append(self.disks[disk].json_report())
        return output


class Disk(object):

    report_fields = [
        'rejected_reasons',
        'available',
        'path',
        'sys_api',
    ]
    pretty_report_sys_fields = [
        'human_readable_size',
        'model',
        'removable',
        'ro',
        'rotational',
        'sas_address',
        'scheduler_mode',
        'vendor',
    ]

    def __init__(self, path, mounts=None, swap_devices=None, zpool_devices=None, zfs_mountpoints=None):
        self.abspath = path
        self.path = path
        self.sys_api = sys_info.devices.get(path)
        geomname = self.sys_api.get('geomname') if self.sys_api else None
        mounts = mounts or {}
        swap_devices = swap_devices or set()
        zpool_devices = zpool_devices or {}
        zfs_mountpoints = zfs_mountpoints or {}

        self.partitions = get_partitions(geomname) if geomname else {}

        disk_mediasize = self.sys_api.get('mediasize') if self.sys_api else None
        used_by = []
        for name in [geomname] + list(self.partitions):
            if not name:
                continue
            size = disk_mediasize if name == geomname else self.partitions.get(name, {}).get('mediasize')
            if name in zpool_devices:
                pool = zpool_devices[name]
                mount_path = zfs_mountpoints.get(pool, '-')
                used_by.append((name, size, 'zpool %s' % pool, mount_path))
            elif name in mounts:
                mountpoint, fstype = mounts[name]
                used_by.append((name, size, 'mounted on %s (%s)' % (mountpoint, fstype), mountpoint))
            elif name in swap_devices:
                used_by.append((name, size, 'swap', '-'))
            elif name in self.partitions:
                used_by.append((name, size, '%s partition' % self.partitions[name].get('type', 'unknown'), '-'))

        self.used_by = used_by
        self.reject_reasons = used_by
        self.available = not used_by

    def report(self):
        rotationrate = self.sys_api.get('rotationrate')
        rotational = int(rotationrate) != 0 if rotationrate.isdigit() else 'unknown'
        lines = [report_template.format(
            geomname=self.sys_api.get('geomname'),
            mediasize=human_readable_size(int(self.sys_api.get('mediasize'))),
            rotational=rotational,
            descr=self.sys_api.get('descr')
        )]
        for name, size, reason, mount_path in self.used_by:
            size_str = human_readable_size(int(size)) if size and size.isdigit() else 'unknown'
            lines.append(usage_template.format(name=name, mediasize=size_str, mount_path=mount_path, reason=reason))
        return ''.join(lines)

    def json_report(self):
        output = {k.strip('_'): v for k, v in vars(self).items()}
        return output

