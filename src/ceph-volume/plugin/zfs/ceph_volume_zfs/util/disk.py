import re

from typing import Any, Dict, List, Optional

from ceph_volume.util.disk import human_readable_size
from ceph_volume import process
from ceph_volume import sys_info
from ceph_volume.util.zfs.gpart import (
    get_glabel_aliases,
    get_gpart_info,
    get_label_aliases,
    gpart_show_all,
)
from ceph_volume.util.zfs.mount import (
    get_mount_entries,
    get_mount_info,
    get_swap_entries,
    get_swap_info,
)
from ceph_volume.util.zfs.zpool import (
    get_zpool_leaf_vdevs,
    get_zpool_membership,
)

report_template = """
{geomname:<21} {mediasize:<16} {rotational!s:<7} {available:<6} {descr}  {reason}"""


def camcontrol_devlist_parser() -> Dict[str, Dict[str, Any]]:
    """
    Parses `camcontrol devlist` output to enumerate all CAM-visible
    devices, including their pass-through node. This is the primary
    disk enumeration source: it catches devices the kernel sees on the
    CAM bus even if geom hasn't (yet) attached a provider for them.

    Example line:
    <ATA CORSAIR CSSD-F40GB2 M028>    at scbus2 target 0 lun 0 (ada0,pass0)

    Returns a dict keyed by disk device name (e.g. 'ada0'), each value
    a dict with 'descr', 'scbus', 'target', 'lun', 'pass'.
    """
    command = ['/sbin/camcontrol', 'devlist']
    out, err, rc = process.call(command)
    devices = {}
    line_re = re.compile(
        r'^<(?P<descr>.*?)>\s+at\s+scbus(?P<scbus>\d+)\s+'
        r'target\s+(?P<target>\d+)\s+lun\s+(?P<lun>\d+)\s+'
        r'\((?P<nodes>[^)]+)\)'
    )
    for line in out:
        m = line_re.match(line.strip())
        if not m:
            continue
        nodes = [n.strip() for n in m.group('nodes').split(',')]
        disk_node = next((n for n in nodes if not n.startswith('pass')), None)
        pass_node = next((n for n in nodes if n.startswith('pass')), None)
        if disk_node is None:
            continue
        devices[disk_node] = {
            'descr': m.group('descr'),
            'scbus': m.group('scbus'),
            'target': m.group('target'),
            'lun': m.group('lun'),
            'pass': pass_node,
        }
    return devices


def geom_disk_parser(block: str) -> Dict[str, str]:
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
        column = re.sub(r"^[0-9]+\.", "", column)
        value = value.strip()
        value = re.sub(r"\([0-9A-Z.]+\)", '', value).strip()
        # lunname is the SCSI INQUIRY vendor, product and serial fields
        # concatenated, each padded to its fixed width, so it arrives full
        # of runs of spaces. Collapse them here rather than in every
        # consumer.
        value = re.sub(r'\s+', ' ', value)
        parsed[column.lower()] = value
    return parsed


def get_camcontrol_identify(diskname: str) -> Dict[str, str]:
    """
    Runs `camcontrol identify <diskname>` to get disk geometry for
    SAS/SCSI (da*) devices where `geom disk list` can't report the
    mediasize via ATA commands.

    Returns a dict with 'mediasize' (bytes as int) and
    'sectorsize' (logical sector size), or empty dict on failure.
    """
    command = ['/sbin/camcontrol', 'identify', diskname]
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc != 0 or not out:
        return {}
    result = {}
    sector_size = 512  # default
    lba48 = None
    lba = None
    for line in out:
        stripped = line.strip()
        # "sector size    logical 512, physical 4096, offset 0"
        m = re.match(r'sector size\s+logical\s+(\d+)', stripped)
        if m:
            sector_size = int(m.group(1))
        # "LBA48 supported    5860533168 sectors"
        m = re.match(r'LBA48 supported\s+(\d+)\s+sectors', stripped)
        if m:
            lba48 = int(m.group(1))
        # "LBA supported      268435455 sectors"
        m = re.match(r'LBA supported\s+(\d+)\s+sectors', stripped)
        if m:
            lba = int(m.group(1))
    sectors = lba48 or lba
    if sectors:
        result['mediasize'] = str(sectors * sector_size)
        result['sectorsize'] = str(sector_size)
    return result


def get_geom_disk(diskname: str) -> Dict[str, Any]:
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
    return geom_disk_parser(geom_block)


def natural_key(name: str) -> List[Any]:
    """
    Sort key that puts da2 before da10, which a plain string sort does
    not.  Splits a device name into runs of digits and runs of anything
    else, and compares the digit runs as numbers.

    Each part is tagged with 0 for text or 1 for a number so that a name
    without a trailing unit number never ends up compared against one,
    which would raise.
    """
    return [(1, int(part)) if part.isdigit() else (0, part)
            for part in re.split(r'(\d+)', name) if part != '']


def list_kern_disks() -> List[str]:
    """
    Every disk GEOM knows about, from sysctl kern.disks, which is a
    single space separated line: "ada0 da0 vtbd0".

    This is the enumeration source rather than `camcontrol devlist`,
    because camcontrol only knows devices on the CAM bus.  Virtio disks
    in a VM (vtbd), NVMe with hw.nvme.use_nvd=1 (nvd), SD/eMMC (mmcsd),
    memory disks (md) and the RAID drivers that do not use CAM (mfid,
    amrd, aacd, twed, mlxd) never appear there, and a host with only
    those would report no disks at all.  camcontrol devlist still
    supplies scbus/target/lun/pass for the devices that are on the bus.
    """
    command = ['/sbin/sysctl', '-n', 'kern.disks']
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc != 0 or not out:
        return []
    return sorted(out[0].split(), key=natural_key)


def get_disks() -> Dict[str, Dict[str, Any]]:
    cam_devices = camcontrol_devlist_parser()
    # Both of these describe the whole system rather than one disk, and
    # `zpool status` in particular is expensive: read them once here
    # instead of once per disk.
    glabel = get_glabel_aliases()
    leaves = get_zpool_leaf_vdevs()
    tables = gpart_show_all()
    mounts = get_mount_entries()
    swaps = get_swap_entries()
    disks = {}
    for dsk in list_kern_disks():
        # cd = optical, ses = SCSI Enclosure Services, pass = CAM
        # passthrough -- never OSD targets
        if re.match(r'^(cd|ses|pass)\d+$', dsk):
            continue
        disk = get_geom_disk(dsk)
        # For SAS/SCSI (da*) devices geom sometimes can't report mediasize
        # via ATA commands, or reports 0. Augment from camcontrol identify.
        if re.match(r'^da\d+$', dsk):
            raw_size = disk.get('mediasize', '').strip()
            try:
                geom_size = int(raw_size.split()[0]) if raw_size else 0
            except (ValueError, IndexError):
                geom_size = 0
            if geom_size == 0:
                cam_id = get_camcontrol_identify(dsk)
                if cam_id.get('mediasize'):
                    disk.update(cam_id)
        # empty for a disk that is not on the CAM bus, e.g. vtbd0
        disk['cam'] = cam_devices.get(dsk, {})
        disk['gpart'] = get_gpart_info(dsk, tables=tables)
        partitions = disk['gpart'].get('partitions')
        aliases = get_label_aliases(partitions, glabel=glabel)
        disk['zpool'] = get_zpool_membership(dsk, partitions=partitions,
                                             aliases=aliases, leaves=leaves)
        disk['mount'] = get_mount_info(dsk, partitions=partitions,
                                       aliases=aliases, entries=mounts)
        disk['swap'] = get_swap_info(dsk, partitions=partitions,
                                     aliases=aliases, entries=swaps)
        disks['/dev/' + dsk] = disk
    return disks


class Disks(object):

    def __init__(self, path: Optional[str] = None) -> None:
        if not sys_info.devices:
            sys_info.devices = get_disks()  # type: ignore[assignment]
        self.disks = {}
        if path is None:
            for k in sys_info.devices:
                self.disks[k] = Disk(k)
            return
        # Match the device, not every name it is a substring of: 'ada1'
        # must not also select /dev/ada10.
        wanted = '/dev/' + re.sub(r'^/dev/', '', path)
        for k in sys_info.devices:
            if k == wanted:
                self.disks[k] = Disk(k)

    def pretty_report(self, all: bool = True) -> str:
        output = [
            report_template.format(
                geomname='Device Path',
                mediasize='Size',
                rotational='rotates',
                available='avail',
                descr='Model name',
                reason='',
            )]
        for disk in sorted(self.disks, key=natural_key):
            output.append(self.disks[disk].report())
        return ''.join(output)

    def verbose_report(self) -> str:
        """
        Detailed report for -v: one block per disk with its status, the
        reasons it is not available, its partitions and mount state.
        """
        order = sorted(self.disks, key=natural_key)
        return '\n\n'.join(self.disks[disk].describe() for disk in order)

    def json_report(self) -> List[Dict[str, Any]]:
        output: List[Dict[str, Any]] = []
        for disk in sorted(self.disks, key=natural_key):
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

    def __init__(self, path: str) -> None:
        self.abspath = path
        self.path = path
        self.reject_reasons: List[str] = []
        self.available = True
        self.sys_api: Dict[str, Any] = sys_info.devices.get(path, {})  # type: ignore[assignment]
        self._evaluate_availability()

    def _evaluate_availability(self) -> None:
        """
        Populates reject_reasons / available based on the augmented
        info gathered in get_disks(): existing partitions, zpool
        membership, and active mounts. A disk failing any of these
        checks is not safe for prepare/zap to touch without an
        explicit override.

        Order matters: zpool/partition/mount checks run first so that
        the most actionable reason appears first -- especially for
        SAS/SCSI (da*) disks where geom reports no mediasize but the
        real reason is zpool membership.
        """
        zpool = self.sys_api.get('zpool', {})
        if zpool.get('in_pool'):
            vdev_name = zpool.get('vdev_name')
            if vdev_name:
                self.reject_reasons.append(
                    'Member of {} vdev in zpool "{}"'.format(
                        vdev_name, zpool.get('pool_name'))
                )
            else:
                self.reject_reasons.append(
                    'Single-disk (no redundancy) member of zpool "{}"'.format(
                        zpool.get('pool_name'))
                )
            self.available = False

        gpart = self.sys_api.get('gpart', {})
        if gpart.get('has_partitions'):
            self.reject_reasons.append(
                'Has an existing {} partition table'.format(gpart.get('scheme'))
            )
            self.available = False

        mount = self.sys_api.get('mount', {})
        if mount.get('mounted'):
            self.reject_reasons.append(
                'Has mounted filesystem(s) at {}'.format(', '.join(mount.get('mountpoints', [])))
            )
            self.available = False

        swap = self.sys_api.get('swap', {})
        if swap.get('active'):
            self.reject_reasons.append(
                'Has active swap on {}'.format(', '.join(swap.get('devices', [])))
            )
            self.available = False

        # mediasize check last -- for SAS/SCSI (da*) disks via HBA
        # geom often can't read the size via ATA commands, but the
        # real reason they're unavailable is usually zpool membership
        # (checked above). Only flag missing size if nothing else
        # already marked the disk unavailable.
        if not self.reject_reasons:
            mediasize = self.sys_api.get('mediasize')
            try:
                has_usable_size = int(mediasize) > 0  # type: ignore[arg-type]
            except (TypeError, ValueError):
                has_usable_size = False
            if not has_usable_size:
                if re.match(r'^da\d+$', self.sys_api.get('geomname', '')):
                    reason = 'No usable media size from geom (SAS/SCSI device via HBA)'
                else:
                    reason = 'No usable media size reported (e.g. empty optical drive, or device not yet attached)'
                self.reject_reasons.append(reason)
                self.available = False

    @staticmethod
    def _rotational(value: Any) -> Optional[bool]:
        """
        True for a spinning disk, False for flash, None when geom does
        not know.  geom reports "rotationrate: unknown" for a SATA disk
        behind a SAS HBA, and reporting those as non-rotational would
        say they are flash, which is worse than saying nothing.
        """
        try:
            rate = int(value)
        except (TypeError, ValueError):
            return None
        return rate != 0

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        """
        geom fields are sometimes non-numeric strings, e.g.
        'Mediasize: Unknown' on an optical drive with no media
        inserted. Fall back to `default` instead of raising.
        """
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def report(self) -> str:
        if self.available:
            available_str = 'True'
            reason_str = ''
        else:
            available_str = 'False'
            reason_str = '(' + self.reject_reasons[0] + ')' if self.reject_reasons else ''
        rotational = self._rotational(self.sys_api.get('rotationrate'))
        return report_template.format(
            geomname='/dev/' + self.sys_api.get('geomname', self.path).replace('/dev/', ''),
            mediasize=human_readable_size(self._safe_int(self.sys_api.get('mediasize'))),
            rotational=('unknown' if rotational is None else rotational),
            available=available_str,
            descr=self.sys_api.get('descr'),
            reason=reason_str,
        )

    def describe(self) -> str:
        """
        Human-readable, multi-line description of this disk: status,
        size, model, partition layout, and per-partition mount state.
        Used by Disks.verbose_report() for the -v style detail view.
        """
        size = human_readable_size(self._safe_int(self.sys_api.get('mediasize')))
        model = self.sys_api.get('descr', 'unknown model')
        status = 'AVAILABLE' if self.available else 'NOT AVAILABLE'

        lines = ['{path}  ({model}, {size})'.format(
            path=self.path, model=model, size=size)]
        lines.append('  status: {}'.format(status))
        if self.reject_reasons:
            for reason in self.reject_reasons:
                lines.append('    - {}'.format(reason))

        gpart = self.sys_api.get('gpart', {})
        partitions = gpart.get('partitions', [])
        if not partitions:
            zpool = self.sys_api.get('zpool', {})
            if zpool.get('in_pool'):
                vdev_name = zpool.get('vdev_name')
                if vdev_name:
                    lines.append(
                        '  partitions: none (whole-disk member of {} in zpool "{}")'.format(
                            vdev_name, zpool.get('pool_name')))
                else:
                    lines.append(
                        '  partitions: none (single-disk member of zpool "{}")'.format(
                            zpool.get('pool_name')))
            else:
                lines.append('  partitions: none (disk is empty)')
        else:
            lines.append('  partitions:')
            rows = []
            for part in partitions:
                # a disklabel inside a slice is indented under it, and the
                # indent counts towards the name column so the rest lines up
                indent = '  ' if part.get('parent') else ''
                rows.append({
                    'name': indent + part['name'],
                    'type': part['type'],
                    'size': part['size_human'],
                    'label': '[{}]'.format(part['label']) if part.get('label') else '',
                    'state': self._partition_state(part),
                })
            widths = dict((key, max(len(row[key]) for row in rows))
                          for key in ('name', 'type', 'size', 'label'))
            for row in rows:
                fields = [
                    row['name'].ljust(widths['name']),
                    row['type'].ljust(widths['type']),
                    row['size'].rjust(widths['size']),
                ]
                # leave the column out entirely when nothing here is labelled
                if widths['label']:
                    fields.append(row['label'].ljust(widths['label']))
                fields.append('({})'.format(row['state']))
                lines.append('    ' + '  '.join(fields))
        return '\n'.join(lines)

    def _partition_state(self, part: Dict[str, str]) -> str:
        """
        What this partition is being used for: a mount, active swap,
        membership of an imported pool, or - failing those - what its type
        says it is. "not mounted" would be wrong for a ZFS partition, which
        is never in `mount -p`, and for boot code and swap, which are not
        filesystems at all.
        """
        name = part['name']
        mount = self.sys_api.get('mount', {}).get('by_partition', {}).get(name, {})
        swap = self.sys_api.get('swap', {}).get('by_partition', {}).get(name, {})
        member = self.sys_api.get('zpool', {}).get('partitions', {}).get(name)
        if mount.get('mounted'):
            return 'mounted at {}'.format(mount.get('mountpoint'))
        if swap.get('active'):
            return 'active swap'
        if member:
            where = 'zpool "{}"'.format(member.get('pool_name'))
            if member.get('vdev_name'):
                where = '{} in {}'.format(member['vdev_name'], where)
            return 'member of {}'.format(where)
        if part.get('type') == 'freebsd-boot':
            return 'boot code, not a filesystem'
        if part.get('type') == 'freebsd-swap':
            return 'swap, not active'
        mode = part.get('mode')
        if mode and mode != 'r0w0e0':
            return 'in use, open {}'.format(mode)
        return 'not in use'

    def json_report(self) -> Dict[str, Any]:
        output: Dict[str, Any] = {k.strip('_'): v for k, v in vars(self).items()}
        return output

