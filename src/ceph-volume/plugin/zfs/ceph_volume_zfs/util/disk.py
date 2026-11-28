import json
import re

from ceph_volume.util.disk import human_readable_size
from ceph_volume import process
from ceph_volume import sys_info

report_template = """
{geomname:<21} {mediasize:<16} {rotational!s:<7} {available:<6} {descr}  {reason}"""


def camcontrol_devlist_parser():
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


def get_camcontrol_identify(diskname):
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


def get_geom_disk(diskname):
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


def natural_key(name):
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


def _part_index(part):
    """
    Sort key for one gpart table: p4 before p5 and s1 before s2, by
    index rather than by the start sector gpart orders its output on.
    """
    m = re.search(r'[ps](\d+)$', part['name'])
    return int(m.group(1)) if m else 0


def gpart_show_all():
    """
    Every gpart table on the system, from one `gpart show -p`, keyed by
    the geom the table sits on:

      {'ada0': {'scheme': 'GPT', 'raw': [...],
                 'partitions': [{'name': 'ada0p1', 'type': ..., ...}]},
       'da0s1': {'scheme': 'BSD', ...}}

    Called once per run.  Asking gpart per disk instead means a failed
    command for every disk without a table, and ceph_volume.process
    logs each of those at WARNING: a host whose pool members are whole
    disks recorded a "No such geom" error for every one of them.  This
    also picks up nested tables for free, since `gpart show` with no
    geom named lists those as blocks of their own.

    The -p flag makes gpart print full partition device names (ada0p1)
    instead of bare index numbers, which is what we need to
    cross-reference against `mount -p` output.  Free-space gaps are
    skipped: gpart reports those with no partition name, they are not
    real partitions.
    """
    out, err, rc = process.call(['/sbin/gpart', 'show', '-p'],
                                verbose_on_failure=False)
    tables = {}
    if rc != 0 or not out:
        return tables
    line_re = re.compile(r'^\s*\d+\s+\d+\s+(\S+)\s+(\S+)\s+\(([^)]+)\)\s*$')
    current = None
    for line in out:
        stripped = line.strip()
        if stripped.startswith('=>'):
            # "=>       63  976773105    da0  MBR  (466G)": the geom and
            # then the scheme, both before the parenthesized size
            fields = stripped.split()
            if len(fields) < 3:
                current = None
                continue
            current = fields[-3]
            tables[current] = {'scheme': fields[-2], 'partitions': [],
                               'raw': [line]}
            continue
        if current is None:
            continue
        tables[current]['raw'].append(line)
        m = line_re.match(line)
        if not m:
            continue
        name, ptype, human = m.groups()
        if name == '-' or not name.startswith(current):
            # unallocated / free space region, not a real partition
            continue
        tables[current]['partitions'].append(
            {'name': name, 'type': ptype, 'size_human': human})
    for table in tables.values():
        table['partitions'].sort(key=_part_index)
    return tables


def get_partitions(diskname, tables=None):
    """
    Every partition on a disk, as a list of dicts:
    [{'name': 'ada0p1', 'type': 'freebsd-boot', 'size_human': '512K'},
    ...].

    An MBR slice can hold a BSD disklabel of its own, and
    `gpart show -p da0` does not descend into it: it reports da0s1 but
    not the da0s1a inside it, so a filesystem or a pool on that label
    would never be matched against the disk.  Each slice of an MBR or
    EBR table is therefore asked for its own table, and what it holds
    follows it in the list, carrying 'parent' with the slice name.
    GPT is only allowed at the outermost level and BSD does not nest,
    both by default, so neither is descended into.

    Returns an empty list if the disk has no partition table at all.
    """
    if tables is None:
        tables = gpart_show_all()
    table = tables.get(diskname) or {}
    scheme = table.get('scheme')
    result = []
    # copy, so that the per-disk keys added below never land on the
    # shared tables dict
    for part in table.get('partitions', []):
        result.append(dict(part))
        if scheme not in ('MBR', 'EBR'):
            continue
        nested = tables.get(part['name']) or {}
        for sub in nested.get('partitions', []):
            sub = dict(sub)
            sub['parent'] = part['name']
            result.append(sub)
    # `gpart show` does not print the label, the raw uuid or whether the
    # partition is open; `gpart list` does. Add them when available.
    containers = [diskname]
    containers.extend(sorted(set(p['parent'] for p in result if 'parent' in p)))
    details = {}
    for container in containers:
        details.update(get_partition_details(container))
    for part in result:
        part.update(details.get(part['name'], {}))
    return result


def get_partition_details(diskname):
    """
    Runs `gpart list <diskname>` and returns, per partition, what
    `gpart show` does not print: the GPT label, the raw uuid (what a
    gptid/<uuid> device name is made of) and the GEOM open mode:

      {'da4p3': {'label': 'zfs0', 'rawuuid': 'fa257269-...', 'mode': 'r1w1e2'}}

    The mode is how GEOM says that something has the partition open, for
    instance an imported zpool. r0w0e0 is idle; anything else is in use.
    Returns an empty dict when there is no partition table.
    """
    command = ['/sbin/gpart', 'list', diskname]
    out, err, rc = process.call(command, verbose_on_failure=False)
    details = {}
    if rc != 0 or not out:
        return details
    in_providers = False
    current = None
    for line in out:
        text = line.strip()
        if text == 'Providers:':
            in_providers = True
            current = None
            continue
        if text == 'Consumers:':
            in_providers = False
            current = None
            continue
        if not in_providers:
            continue
        m = re.match(r'^\d+\.\s+Name:\s+(\S+)$', text)
        if m:
            current = details.setdefault(m.group(1), {})
            continue
        if current is None:
            continue
        m = re.match(r'^(label|rawuuid|Mode):\s+(\S+)$', text)
        if m:
            key, value = m.groups()
            if key == 'Mode':
                current['mode'] = value
            elif value != '(null)':
                current[key] = value
    return details


def get_glabel_aliases():
    """
    Every GEOM label on the system, from one `glabel status`:
    {'gpt/zfs0': 'ada2p3', 'gptid/fa257269-...': 'da4p3', ...}.

    This is the same for every disk, so get_disks() reads it once and
    hands it to get_label_aliases() per disk.
    """
    aliases = {}
    out, err, rc = process.call(['/sbin/glabel', 'status'], verbose_on_failure=False)
    if rc != 0:
        return aliases
    for line in out:
        fields = line.split()
        if len(fields) >= 3 and fields[0] != 'Name':
            aliases[fields[0]] = fields[2]
    return aliases


def get_label_aliases(partitions=None, glabel=None):
    """
    Maps GEOM label names (gpt/zfs0, gptid/fa257269-..., diskid/DISK-...)
    to the device they stand for ('da4p3'). zpool, mount and swap often
    use these names, so comparing only with /dev/daNpM misses them.

    Built from `glabel status`, plus the label and raw uuid of the given
    partitions for the cases glabel does not list.  Pass `glabel` from
    get_glabel_aliases() to avoid running glabel again.
    """
    aliases = dict(glabel) if glabel is not None else get_glabel_aliases()
    for part in partitions or []:
        if part.get('label'):
            aliases.setdefault('gpt/' + part['label'], part['name'])
        if part.get('rawuuid'):
            aliases.setdefault('gptid/' + part['rawuuid'], part['name'])
    return aliases


def resolve_device(path, aliases):
    """'/dev/gptid/fa257269-...' -> 'da4p3'; plain names are returned as they are."""
    name = re.sub(r'^/dev/', '', path)
    return aliases.get(name, name)


# A GEOM transform appends its own suffix to the provider it sits on, and
# that is the name a mount or a pool then uses: geli (.eli), gjournal
# (.journal), gnop (.nop).  They can be stacked.
_GEOM_SUFFIX_RE = re.compile(r'(?:\.(?:eli|journal|nop))+$')


def _device_on_disk(name, diskname):
    """
    'da4p3' -> 'da4p3' when it is the disk or something on it, else None.

    What a partitioning scheme can put after the disk name on FreeBSD:
      p<n>      GPT partition                        da4p3
      s<n>      MBR slice, or an EBR one past s4     da4s1, da4s5
      s<n><l>   BSD disklabel inside a slice         da4s1a
      <l>       BSD disklabel on the raw disk        da4a
    gpart's BSD scheme takes up to 20 entries, so the disklabel letter
    runs a-t and not just a-h.  APM, LDM and BSD64 are left out: they
    describe Apple, Windows NT and DragonFly disks.

    Known gap: kern.geom.part.separator inserts a separator between the
    disk name and the partition name, so with that tunable set the names
    become da4.p3 and nothing matches here.
    """
    name = _GEOM_SUFFIX_RE.sub('', name)
    m = re.match(r'^(' + re.escape(diskname) + r'(?:p\d+|s\d+[a-t]?|[a-t])?)$',
                 name)
    return m.group(1) if m else None


def get_gpart_info(diskname, tables=None):
    """
    The partition scheme of one disk, from the tables gpart_show_all()
    prepared, plus the partition list from get_partitions().  Returns a
    dict with:
      - 'has_partitions': bool
      - 'scheme': 'GPT'/'MBR'/None
      - 'empty': bool -- True if there's no partition table AND no
        partitions at all (the disk is genuinely blank)
      - 'partitions': list of partition dicts (see get_partitions)
      - 'raw': raw `gpart show` output lines, for reference/logging

    A disk with an existing partition table is not safe to blindly
    hand to `prepare` without an explicit zap first.
    """
    if tables is None:
        tables = gpart_show_all()
    table = tables.get(diskname) or {}
    info = {
        'has_partitions': False,
        'scheme': table.get('scheme'),
        'empty': True,
        'partitions': [],
        'raw': table.get('raw', []),
    }
    if not table:
        # no table for this geom at all: the disk is genuinely blank
        return info
    if info['scheme']:
        info['has_partitions'] = True
        info['empty'] = False
    partitions = get_partitions(diskname, tables=tables)
    info['partitions'] = partitions
    if partitions:
        info['empty'] = False
        info['has_partitions'] = True
    return info


def _vdev_type(name):
    """'raidz2-0' -> 'raidz2', 'mirror-1' -> 'mirror', anything else as it is."""
    if 'raidz' in name:
        return name.split('-')[0]
    if 'mirror' in name:
        return 'mirror'
    return name


def _collect_leaf_vdevs(vdevs, pool_name, parent_name, leaves):
    """
    Walks the nested 'vdevs' of `zpool status -j` (root -> mirror/raidz ->
    leaf disks) and collects every leaf, with the group it sits in.
    """
    for name, vdev in vdevs.items():
        children = vdev.get('vdevs')
        if children:
            _collect_leaf_vdevs(children, pool_name, name, leaves)
        elif vdev.get('path'):
            grouped = parent_name and parent_name != pool_name
            leaves.append({
                'pool_name': pool_name,
                'vdev_name': parent_name if grouped else None,
                'vdev_type': _vdev_type(parent_name) if grouped else 'stripe',
                'path': vdev['path'],
            })


def _zpool_leaf_vdevs_plaintext():
    """
    Fallback for `zpool status -j` (OpenZFS < 2.3, e.g. ZFS 2.2.x on
    FreeBSD 14): reads `zpool status -P`, where a leaf vdev is a line that
    starts with a full device path. Indentation tells a leaf of a group
    (mirror-0, raidz2-0, logs, cache, spares) from a direct child of the
    pool.
    """
    command = ['/sbin/zpool', 'status', '-P']
    out, err, rc = process.call(command, verbose_on_failure=False)
    leaves = []
    if rc != 0 or not out:
        return leaves
    pool = None
    group = None
    group_indent = -1
    for line in out:
        stripped = line.strip()
        indent = len(line) - len(line.lstrip())
        m = re.match(r'^pool:\s+(\S+)', stripped)
        if m:
            pool = m.group(1)
            group = None
            group_indent = -1
            continue
        # A section header can be the only thing on its line, so do not
        # require anything after it: a cache device was being reported as
        # a plain stripe member because "cache" never matched.
        m = re.match(r'^(raidz\d*-\d+|mirror-\d+|logs|cache|spares|dedup|special)'
                     r'(?:\s|$)', stripped)
        if m:
            group = m.group(1)
            group_indent = indent
            continue
        fields = stripped.split()
        if not pool or not fields or not fields[0].startswith('/'):
            continue
        grouped = group is not None and indent > group_indent
        leaves.append({
            'pool_name': pool,
            'vdev_name': group if grouped else None,
            'vdev_type': _vdev_type(group) if grouped else 'stripe',
            'path': fields[0],
        })
    return leaves


def zpool_supports_json():
    """
    True when `zpool status -j` exists, which is OpenZFS 2.3 and newer.

    Running `zpool status -j` and reading the exit status would be the
    obvious probe, but ceph_volume.process logs every command that
    fails at WARNING and offers no way to turn that off, so a host on
    2.2 would record a zpool usage error on every single run.  Read the
    version instead: `zpool version` prints a first line of the form
    "zfs-2.2.6-FreeBSD_g33174af15".
    """
    out, err, rc = process.call(['/sbin/zpool', 'version'],
                                verbose_on_failure=False)
    if rc != 0 or not out:
        return False
    m = re.match(r'^zfs-(\d+)\.(\d+)', out[0].strip())
    if not m:
        return False
    return (int(m.group(1)), int(m.group(2))) >= (2, 3)


def get_zpool_leaf_vdevs():
    """
    Every leaf vdev of every imported pool:
    [{'pool_name': 'zroot', 'vdev_name': 'mirror-0', 'vdev_type': 'mirror',
      'path': '/dev/gptid/fa257269-...'}, ...]
    vdev_name is None for a plain stripe member.
    """
    if not zpool_supports_json():
        return _zpool_leaf_vdevs_plaintext()
    command = ['/sbin/zpool', 'status', '-j']
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc == 0 and out:
        try:
            data = json.loads(''.join(out))
        except (ValueError, TypeError):
            data = None
        if data is not None:
            leaves = []
            for pool_name, pool in data.get('pools', {}).items():
                # 'vdevs' holds one root vdev named after the pool, and
                # every group hangs below it, so the walk from here finds
                # anything nested, a logs or cache group included.
                _collect_leaf_vdevs(pool.get('vdevs') or {}, pool_name,
                                    pool_name, leaves)
                # A pool that has them also carries these beside 'vdevs',
                # keyed either by a group name holding nested vdevs or by
                # the device itself. Pass the key as the parent so a
                # device sitting directly under one is reported as part of
                # that class rather than as a bare stripe member. Absent
                # keys cost nothing, and which of these OpenZFS actually
                # emits has not been seen on a live pool yet.
                for key in ('logs', 'log', 'cache', 'l2cache', 'spares',
                            'dedup', 'special'):
                    _collect_leaf_vdevs(pool.get(key) or {}, pool_name,
                                        key, leaves)
            return leaves
    return _zpool_leaf_vdevs_plaintext()


def get_zpool_membership(diskname, partitions=None, aliases=None, leaves=None):
    """
    Checks whether this disk (or any gpart partition on it) is a member of
    an imported zpool. A pool names its devices as it was created: by
    /dev/daNpM, but just as often by label (gpt/zfs0) or by gptid
    (gptid/<uuid>), so every vdev path is resolved to the device it stands
    for before it is compared with this disk.

    Returns a dict:
      {'in_pool': bool, 'pool_name': str or None, 'vdev_name': str or None,
       'vdev_type': str or None,
       'partitions': {'da4p3': {'pool_name': ..., 'vdev_name': ..., 'vdev_type': ...}}}
    The first four describe the first match; 'partitions' has every
    partition (or the bare disk) that is a member.
    """
    result = {'in_pool': False, 'pool_name': None, 'vdev_name': None,
              'vdev_type': None, 'partitions': {}}
    if leaves is None:
        leaves = get_zpool_leaf_vdevs()
    if not leaves:
        return result
    if partitions is None:
        partitions = get_partitions(diskname)
    if aliases is None:
        aliases = get_label_aliases(partitions)
    for leaf in leaves:
        matched = _device_on_disk(resolve_device(leaf['path'], aliases), diskname)
        if not matched:
            continue
        member = {'pool_name': leaf['pool_name'], 'vdev_name': leaf['vdev_name'],
                  'vdev_type': leaf['vdev_type']}
        result['partitions'][matched] = member
        if not result['in_pool']:
            result.update(in_pool=True, **member)
    return result


def get_mount_entries():
    """
    Every mounted device with where it is mounted, from one `mount -p`:
    [('/dev/gpt/data', '/mnt/data'), ...].

    The output is the same whichever disk is being looked at, so
    get_disks() reads it once and hands it to get_mount_info().
    """
    out, err, rc = process.call(['/sbin/mount', '-p'])
    entries = []
    if rc != 0:
        return entries
    for line in out:
        fields = line.split()
        if not fields or not fields[0].startswith('/dev/'):
            continue
        entries.append((fields[0], fields[1] if len(fields) > 1 else None))
    return entries


def get_swap_entries():
    """
    Every active swap device, from one `swapctl -l`.  Read once per run
    for the same reason as get_mount_entries().
    """
    out, err, rc = process.call(['/sbin/swapctl', '-l'])
    entries = []
    if rc != 0 or not out:
        return entries
    for line in out[1:]:  # first line is the header: "Device  1K-blocks  Used"
        fields = line.split()
        if not fields or not fields[0].startswith('/dev/'):
            continue
        entries.append(fields[0])
    return entries


def get_mount_info(diskname, partitions=None, aliases=None, entries=None):
    """
    Cross-references `mount -p` (parseable output) against this disk's
    partitions to catch cases where something on the disk is actively
    mounted, even outside ZFS/Ceph's awareness (e.g. a stray UFS
    partition from a previous life of this drive).

    Reports per-partition, not just a disk-level yes/no -- pass the
    partition list from get_partitions() so every partition gets an
    entry (mounted or not), not just the ones that happen to be
    mounted.

    Returns a dict:
      {
        'mounted': bool,               # true if ANY partition (or the
                                        # bare disk device) is mounted
        'mountpoints': [str, ...],     # flat list, for quick display
        'by_partition': {              # per-partition detail
            'ada0p1': {'mounted': True, 'mountpoint': '/'},
            'ada0p2': {'mounted': False, 'mountpoint': None},
        },
      }

    A mount by label (/dev/gpt/somelabel) or by gptid (/dev/gptid/...)
    is resolved to the partition it stands for first.
    """
    result = {'mounted': False, 'mountpoints': [], 'by_partition': {}}
    partitions = partitions or []
    for part in partitions:
        result['by_partition'][part['name']] = {'mounted': False, 'mountpoint': None}
    if entries is None:
        entries = get_mount_entries()
    if aliases is None:
        aliases = get_label_aliases(partitions)
    for device, mountpoint in entries:
        matched_name = _device_on_disk(resolve_device(device, aliases), diskname)
        if not matched_name:
            continue
        result['mounted'] = True
        if mountpoint:
            result['mountpoints'].append(mountpoint)
        if matched_name in result['by_partition']:
            result['by_partition'][matched_name] = {
                'mounted': True,
                'mountpoint': mountpoint,
            }
        else:
            # mounted device that wasn't in the partitions list -- e.g.
            # the bare disk device itself mounted directly, no
            # partition table involved
            result['by_partition'][matched_name] = {
                'mounted': True,
                'mountpoint': mountpoint,
            }
    return result


def get_swap_info(diskname, partitions=None, aliases=None, entries=None):
    """
    Cross-references `swapctl -l` against this disk's partitions to
    catch active swap devices. Swap is NOT visible via `mount -p` --
    it's activated with swapon/swapctl, not mounted as a filesystem --
    so this is a separate check from get_mount_info(), needed for the
    same reason: destroying the partition table under an active swap
    device is exactly the kind of thing zap must refuse before ever
    reaching gpart destroy (which may itself refuse with a generic
    "Device busy", but that's the OS catching it late, after we've
    already told the user what we're about to destroy -- this check
    catches it upfront with a clear reason).

    Returns a dict:
      {
        'active': bool,               # true if ANY partition (or the
                                       # bare disk) is active swap
        'devices': [str, ...],        # flat list of active swap
                                       # device paths on this disk
        'by_partition': {
            'ada0p1': {'active': True},
            'ada0p2': {'active': False},
        },
      }
    """
    result = {'active': False, 'devices': [], 'by_partition': {}}
    partitions = partitions or []
    for part in partitions:
        result['by_partition'][part['name']] = {'active': False}
    if entries is None:
        entries = get_swap_entries()
    if aliases is None:
        aliases = get_label_aliases(partitions)
    for device in entries:
        matched_name = _device_on_disk(resolve_device(device, aliases), diskname)
        if not matched_name:
            continue
        result['active'] = True
        result['devices'].append(device)
        result['by_partition'][matched_name] = {'active': True}
    return result


def get_zpool_ceph_properties(pool_name):
    """
    Reads back the `ceph:*` user properties that
    ceph_volume_zfs.objectstore.Zfs._tag_zpool() sets on pools it
    creates. Used to positively identify a pool as one this plugin
    made before allowing anything destructive to touch it.

    Returns a dict of the ceph:* properties found (without the
    'ceph:' prefix), empty if none / pool doesn't exist.

    A pool is considered managed by this plugin only if it carries
    'managed_by' == 'ceph-volume-zfs'. Anything else -- an unrelated
    user pool, zroot -- has no such property and must never be
    treated as ours.
    """
    command = ['/sbin/zpool', 'get', '-H', '-o', 'property,value', 'all', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    properties = {}
    if rc != 0 or not out:
        return properties
    for line in out:
        fields = line.split('\t')
        if len(fields) < 2:
            fields = line.split()
        if len(fields) < 2:
            continue
        key, value = fields[0], fields[1]
        if key.startswith('ceph:'):
            properties[key[len('ceph:'):]] = value
    return properties


def zpool_is_ceph_managed(pool_name):
    """
    True only if the pool carries ceph:managed_by=ceph-volume-zfs,
    i.e. this plugin created it. Everything else is somebody else's
    pool.
    """
    props = get_zpool_ceph_properties(pool_name)
    return props.get('managed_by') == 'ceph-volume-zfs'


def list_zpools():
    """
    Returns a list of all zpool names on the system (imported pools
    only -- exported pools aren't visible to `zpool list`).
    """
    command = ['/sbin/zpool', 'list', '-H', '-o', 'name']
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc != 0 or not out:
        return []
    return [line.strip() for line in out if line.strip()]


def get_zvols(pool_name):
    """
    Returns the zvols (ZFS volumes) inside a pool, as a list of
    dicts: [{'name': 'osd-block-<fsid>', 'dataset':
    '<pool>/osd-block-<fsid>', 'size': '<bytes>'}, ...].
    """
    command = ['/sbin/zfs', 'list', '-H', '-p', '-t', 'volume',
               '-o', 'name,volsize', '-r', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    zvols = []
    if rc != 0 or not out:
        return zvols
    for line in out:
        fields = line.split('\t')
        if len(fields) < 2:
            fields = line.split()
        if len(fields) < 2:
            continue
        dataset, volsize = fields[0], fields[1]
        zvols.append({
            'dataset': dataset,
            'name': dataset.split('/', 1)[-1],
            'size': volsize,
        })
    return zvols


def get_zvol_ceph_properties(dataset):
    """
    Reads back the ceph:* user properties set on a zvol by
    ceph_volume_zfs.objectstore.Zfs._tag_zvol(). Returns a dict
    keyed without the 'ceph:' prefix.
    """
    command = ['/sbin/zfs', 'get', '-H', '-o', 'property,value', 'all', dataset]
    out, err, rc = process.call(command, verbose_on_failure=False)
    properties = {}
    if rc != 0 or not out:
        return properties
    for line in out:
        fields = line.split('\t')
        if len(fields) < 2:
            fields = line.split()
        if len(fields) < 2:
            continue
        key, value = fields[0], fields[1]
        if key.startswith('ceph:'):
            properties[key[len('ceph:'):]] = value
    return properties


def _collect_vdev_paths(vdevs, paths):
    """
    Recursively collects leaf vdev 'path' values from the nested
    'vdevs' structure of `zpool status -j`.
    """
    for name, vdev in vdevs.items():
        path = vdev.get('path')
        children = vdev.get('vdevs')
        if children:
            _collect_vdev_paths(children, paths)
        elif path:
            paths.append({
                'path': path,
                'state': vdev.get('state', ''),
                'vdev_type': vdev.get('vdev_type', ''),
            })


def _get_zpool_vdevs_plaintext(pool_name):
    """
    Plaintext fallback for get_zpool_vdevs() on OpenZFS < 2.3.
    Parses `zpool status -P` output to extract vdev device paths.
    """
    command = ['/sbin/zpool', 'status', '-P', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    paths = []
    if rc != 0 or not out:
        return paths
    in_config = False
    for line in out:
        stripped = line.strip()
        if stripped.startswith('config:'):
            in_config = True
            continue
        if stripped.startswith('errors:'):
            in_config = False
            continue
        if not in_config:
            continue
        # leaf vdev lines look like:  /dev/ada0p3   ONLINE   0   0   0
        m = re.match(r'^(/dev/\S+)\s+(\w+)', stripped)
        if m:
            paths.append({
                'path': m.group(1),
                'state': m.group(2),
                'vdev_type': 'disk',
            })
    return paths


def get_zpool_vdevs(pool_name):
    """
    Returns the physical devices backing a pool, as a list of dicts:
    [{'path': '/dev/ada0', 'state': 'ONLINE', 'vdev_type': 'disk'}].

    Tries `zpool status -j` (OpenZFS >= 2.3) first, falls back to
    plaintext parsing on older versions.
    """
    if not zpool_supports_json():
        return _get_zpool_vdevs_plaintext(pool_name)
    command = ['/sbin/zpool', 'status', '-j', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    paths = []
    if rc != 0:
        return _get_zpool_vdevs_plaintext(pool_name)
    if not out:
        return paths
    try:
        data = json.loads(''.join(out))
    except (ValueError, TypeError):
        return paths
    pool = data.get('pools', {}).get(pool_name, {})
    _collect_vdev_paths(pool.get('vdevs', {}), paths)
    return paths


def list_ceph_zpools():
    """
    Discovery entry point for `ceph-volume zfs list` (and, later,
    activate): enumerates every imported zpool and returns only those
    this plugin created, identified by
    ceph:managed_by=ceph-volume-zfs.

    Returns a list of dicts, one per Ceph-managed pool:
      {
        'pool': 'ceph-osd-5',
        'properties': {...},        # pool-level ceph:* properties
        'vdevs': [                  # physical devices backing the pool
            {'path': '/dev/ada0', 'state': 'ONLINE', 'vdev_type': 'disk'},
        ],
        'zvols': [                  # each zvol with its own ceph:* props
            {'name': ..., 'dataset': ..., 'size': ...,
             'device': '/dev/zvol/<dataset>', 'properties': {...}},
        ],
      }
    """
    results = []
    for pool in list_zpools():
        pool_props = get_zpool_ceph_properties(pool)
        if pool_props.get('managed_by') != 'ceph-volume-zfs':
            continue
        zvols = []
        for zvol in get_zvols(pool):
            zvol['device'] = '/dev/zvol/{}'.format(zvol['dataset'])
            zvol['properties'] = get_zvol_ceph_properties(zvol['dataset'])
            zvols.append(zvol)
        results.append({
            'pool': pool,
            'properties': pool_props,
            'vdevs': get_zpool_vdevs(pool),
            'zvols': zvols,
        })
    return results


def list_kern_disks():
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


def get_disks():
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

    def __init__(self, path=None):
        if not sys_info.devices:
            sys_info.devices = get_disks()
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

    def pretty_report(self, all=True):
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

    def verbose_report(self):
        """
        Detailed report for -v: one block per disk with its status, the
        reasons it is not available, its partitions and mount state.
        """
        order = sorted(self.disks, key=natural_key)
        return '\n\n'.join(self.disks[disk].describe() for disk in order)

    def json_report(self):
        output = []
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

    def __init__(self, path):
        self.abspath = path
        self.path = path
        self.reject_reasons = []
        self.available = True
        self.sys_api = sys_info.devices.get(path, {})
        self._evaluate_availability()

    def _evaluate_availability(self):
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
                has_usable_size = int(mediasize) > 0
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
    def _rotational(value):
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
    def _safe_int(value, default=0):
        """
        geom fields are sometimes non-numeric strings, e.g.
        'Mediasize: Unknown' on an optical drive with no media
        inserted. Fall back to `default` instead of raising.
        """
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def report(self):
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

    def describe(self):
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

    def _partition_state(self, part):
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

    def json_report(self):
        output = {k.strip('_'): v for k, v in vars(self).items()}
        return output

