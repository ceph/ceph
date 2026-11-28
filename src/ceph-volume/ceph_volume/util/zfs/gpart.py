"""GEOM partition and label helpers for FreeBSD (gpart, glabel)."""

import re
from typing import Any, Dict, List, Optional

from ceph_volume import process


# A GEOM transform appends its own suffix to the provider it sits on, and
# that is the name a mount or a pool then uses: geli (.eli), gjournal
# (.journal), gnop (.nop).  They can be stacked.
_GEOM_SUFFIX_RE = re.compile(r'(?:\.(?:eli|journal|nop))+$')


# One partition as get_partitions() returns it; every value is a string.
Partition = Dict[str, str]
# gpart_show_all(): geom -> {scheme: str, raw: [str], partitions: [Partition]}
GpartTables = Dict[str, Dict[str, Any]]


def part_index(part: Partition) -> int:
    """
    Sort key for one gpart table: p4 before p5 and s1 before s2, by
    index rather than by the start sector gpart orders its output on.
    """
    m = re.search(r'[ps](\d+)$', part['name'])
    return int(m.group(1)) if m else 0


def gpart_show_all() -> GpartTables:
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
    tables: GpartTables = {}
    if rc != 0 or not out:
        return tables
    line_re = re.compile(r'^\s*\d+\s+\d+\s+(\S+)\s+(\S+)\s+\(([^)]+)\)\s*$')
    current: Optional[str] = None
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
        table['partitions'].sort(key=part_index)
    return tables


def get_partitions(diskname: str,
                   tables: Optional[GpartTables] = None) -> List[Partition]:
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
    result: List[Partition] = []
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
    details: Dict[str, Dict[str, str]] = {}
    for container in containers:
        details.update(get_partition_details(container))
    for part in result:
        part.update(details.get(part['name'], {}))
    return result


def get_partition_details(diskname: str) -> Dict[str, Dict[str, str]]:
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
    details: Dict[str, Dict[str, str]] = {}
    if rc != 0 or not out:
        return details
    in_providers = False
    current: Optional[Dict[str, str]] = None
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


def get_glabel_aliases() -> Dict[str, str]:
    """
    Every GEOM label on the system, from one `glabel status`:
    {'gpt/zfs0': 'ada2p3', 'gptid/fa257269-...': 'da4p3', ...}.

    This is the same for every disk, so get_disks() reads it once and
    hands it to get_label_aliases() per disk.
    """
    aliases: Dict[str, str] = {}
    out, err, rc = process.call(['/sbin/glabel', 'status'], verbose_on_failure=False)
    if rc != 0:
        return aliases
    for line in out:
        fields = line.split()
        if len(fields) >= 3 and fields[0] != 'Name':
            aliases[fields[0]] = fields[2]
    return aliases


def get_label_aliases(partitions: Optional[List[Partition]] = None,
                      glabel: Optional[Dict[str, str]] = None) -> Dict[str, str]:
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


def resolve_device(path: str, aliases: Dict[str, str]) -> str:
    """'/dev/gptid/fa257269-...' -> 'da4p3'; plain names are returned as they are."""
    name = re.sub(r'^/dev/', '', path)
    return aliases.get(name, name)


def device_on_disk(name: str, diskname: str) -> Optional[str]:
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


def get_gpart_info(diskname: str,
                   tables: Optional[GpartTables] = None) -> Dict[str, Any]:
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
    info: Dict[str, Any] = {
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
