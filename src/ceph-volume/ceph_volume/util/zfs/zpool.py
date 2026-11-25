"""zpool helpers: vdev discovery, membership and ceph:* properties."""

import json
import logging
import re
from typing import Any, Dict, List, Optional

from ceph_volume import process

logger = logging.getLogger(__name__)
from ceph_volume.util.zfs.zvol import get_zvol_ceph_properties, get_zvols
from ceph_volume.util.zfs.gpart import (
    Partition,
    device_on_disk,
    get_label_aliases,
    get_partitions,
    resolve_device,
)


# One leaf vdev: pool_name, vdev_name (None for a stripe member),
# vdev_type and path.
LeafVdev = Dict[str, Any]
# One device backing a pool: path, state and vdev_type.
VdevPath = Dict[str, str]


def vdev_type(name: str) -> str:
    """'raidz2-0' -> 'raidz2', 'mirror-1' -> 'mirror', anything else as it is."""
    if 'raidz' in name:
        return name.split('-')[0]
    if 'mirror' in name:
        return 'mirror'
    return name


def collect_leaf_vdevs(vdevs: Dict[str, Any], pool_name: str,
                       parent_name: str, leaves: List[LeafVdev]) -> None:
    """
    Walks the nested 'vdevs' of `zpool status -j` (root -> mirror/raidz ->
    leaf disks) and collects every leaf, with the group it sits in.
    """
    for name, vdev in vdevs.items():
        children = vdev.get('vdevs')
        if children:
            collect_leaf_vdevs(children, pool_name, name, leaves)
        elif vdev.get('path'):
            grouped = parent_name and parent_name != pool_name
            leaves.append({
                'pool_name': pool_name,
                'vdev_name': parent_name if grouped else None,
                'vdev_type': vdev_type(parent_name) if grouped else 'stripe',
                'path': vdev['path'],
            })


def zpool_leaf_vdevs_plaintext() -> List[LeafVdev]:
    """
    Fallback for `zpool status -j` (OpenZFS < 2.3, e.g. ZFS 2.2.x on
    FreeBSD 14): reads `zpool status -P`, where a leaf vdev is a line that
    starts with a full device path. Indentation tells a leaf of a group
    (mirror-0, raidz2-0, logs, cache, spares) from a direct child of the
    pool.
    """
    command = ['/sbin/zpool', 'status', '-P']
    out, err, rc = process.call(command, verbose_on_failure=False)
    leaves: List[LeafVdev] = []
    if rc != 0 or not out:
        return leaves
    pool: Optional[str] = None
    group: Optional[str] = None
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
        member_of = group if group is not None and indent > group_indent else None
        leaves.append({
            'pool_name': pool,
            'vdev_name': member_of,
            'vdev_type': vdev_type(member_of) if member_of else 'stripe',
            'path': fields[0],
        })
    return leaves


def zpool_supports_json() -> bool:
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


def get_zpool_leaf_vdevs() -> List[LeafVdev]:
    """
    Every leaf vdev of every imported pool:
    [{'pool_name': 'zroot', 'vdev_name': 'mirror-0', 'vdev_type': 'mirror',
      'path': '/dev/gptid/fa257269-...'}, ...]
    vdev_name is None for a plain stripe member.
    """
    if not zpool_supports_json():
        return zpool_leaf_vdevs_plaintext()
    command = ['/sbin/zpool', 'status', '-j']
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc == 0 and out:
        try:
            data = json.loads(''.join(out))
        except (ValueError, TypeError):
            data = None
        if data is not None:
            leaves: List[LeafVdev] = []
            for pool_name, pool in data.get('pools', {}).items():
                # 'vdevs' holds one root vdev named after the pool, and
                # every group hangs below it, so the walk from here finds
                # anything nested, a logs or cache group included.
                collect_leaf_vdevs(pool.get('vdevs') or {}, pool_name,
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
                    collect_leaf_vdevs(pool.get(key) or {}, pool_name,
                                        key, leaves)
            return leaves
    return zpool_leaf_vdevs_plaintext()


def get_zpool_membership(diskname: str,
                         partitions: Optional[List[Partition]] = None,
                         aliases: Optional[Dict[str, str]] = None,
                         leaves: Optional[List[LeafVdev]] = None) -> Dict[str, Any]:
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
    result: Dict[str, Any] = {'in_pool': False, 'pool_name': None, 'vdev_name': None,
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
        matched = device_on_disk(resolve_device(leaf['path'], aliases), diskname)
        if not matched:
            continue
        member = {'pool_name': leaf['pool_name'], 'vdev_name': leaf['vdev_name'],
                  'vdev_type': leaf['vdev_type']}
        result['partitions'][matched] = member
        if not result['in_pool']:
            result.update(in_pool=True, **member)
    return result


def get_zpool_ceph_properties(pool_name: str) -> Dict[str, str]:
    """
    Reads back the `ceph:*` user properties that
    ceph_volume.objectstore.zfs.Zfs._tag_zpool() sets on pools it
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
    properties: Dict[str, str] = {}
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


def zpool_is_ceph_managed(pool_name: str) -> bool:
    """
    True only if the pool carries ceph:managed_by=ceph-volume-zfs,
    i.e. this plugin created it. Everything else is somebody else's
    pool.
    """
    props = get_zpool_ceph_properties(pool_name)
    return props.get('managed_by') == 'ceph-volume-zfs'


def list_zpools() -> List[str]:
    """
    Returns a list of all zpool names on the system (imported pools
    only -- exported pools aren't visible to `zpool list`).
    """
    command = ['/sbin/zpool', 'list', '-H', '-o', 'name']
    out, err, rc = process.call(command, verbose_on_failure=False)
    if rc != 0 or not out:
        return []
    return [line.strip() for line in out if line.strip()]


def collect_vdev_paths(vdevs: Dict[str, Any], paths: List[VdevPath]) -> None:
    """
    Recursively collects leaf vdev 'path' values from the nested
    'vdevs' structure of `zpool status -j`.
    """
    for name, vdev in vdevs.items():
        path = vdev.get('path')
        children = vdev.get('vdevs')
        if children:
            collect_vdev_paths(children, paths)
        elif path:
            paths.append({
                'path': path,
                'state': vdev.get('state', ''),
                'vdev_type': vdev.get('vdev_type', ''),
            })


def get_zpool_vdevs_plaintext(pool_name: str) -> List[VdevPath]:
    """
    Plaintext fallback for get_zpool_vdevs() on OpenZFS < 2.3.
    Parses `zpool status -P` output to extract vdev device paths.
    """
    command = ['/sbin/zpool', 'status', '-P', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    paths: List[VdevPath] = []
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


def get_zpool_vdevs(pool_name: str) -> List[VdevPath]:
    """
    Returns the physical devices backing a pool, as a list of dicts:
    [{'path': '/dev/ada0', 'state': 'ONLINE', 'vdev_type': 'disk'}].

    Tries `zpool status -j` (OpenZFS >= 2.3) first, falls back to
    plaintext parsing on older versions.
    """
    if not zpool_supports_json():
        return get_zpool_vdevs_plaintext(pool_name)
    command = ['/sbin/zpool', 'status', '-j', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    paths: List[VdevPath] = []
    if rc != 0:
        return get_zpool_vdevs_plaintext(pool_name)
    if not out:
        return paths
    try:
        data = json.loads(''.join(out))
    except (ValueError, TypeError):
        return paths
    pool = data.get('pools', {}).get(pool_name, {})
    collect_vdev_paths(pool.get('vdevs', {}), paths)
    return paths


def list_ceph_zpools() -> List[Dict[str, Any]]:
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
    results: List[Dict[str, Any]] = []
    for pool in list_zpools():
        pool_props = get_zpool_ceph_properties(pool)
        if pool_props.get('managed_by') != 'ceph-volume-zfs':
            continue
        zvols: List[Dict[str, Any]] = []
        for vol in get_zvols(pool):
            zvol: Dict[str, Any] = dict(vol)
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


def tag_pool(pool_name: str, osd_id: str, osd_fsid: str,
             device_type: str = 'block',
             zvol_name: Optional[str] = None) -> None:
    """
    Sets a minimal set of ceph:* user properties on a zpool, so
    ``ceph-volume zfs list``/``activate`` can find Ceph-managed pools
    directly via ``zpool get``.
    """
    properties: Dict[str, str] = {
        'ceph:osd_id': str(osd_id),
        'ceph:osd_fsid': str(osd_fsid),
        'ceph:managed_by': 'ceph-volume-zfs',
        'ceph:type': device_type,
    }
    if device_type == 'block' and zvol_name:
        properties['ceph:zvol_name'] = zvol_name
    set_properties(pool_name, properties)


def find_osd(osd_id: Optional[str] = None,
             osd_fsid: Optional[str] = None,
             default_cluster: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Locates a prepared OSD by scanning Ceph-managed zpools for
    matching ``ceph:osd_id`` / ``ceph:osd_fsid`` properties.  Returns
    a dict with the block/db/wal device paths, or ``None``.

    This is the ZFS counterpart to LVM's tag lookup: everything
    needed to reconstruct the OSD directory lives in the ZFS
    properties ``prepare()`` wrote, so no external state is required.

    *default_cluster* is used as the cluster name when the pool does
    not carry one (falls back to ``ceph_volume.conf.cluster``).
    """
    from ceph_volume import conf
    for entry in list_ceph_zpools():
        props = entry['properties']
        if osd_id is not None and str(props.get('osd_id')) != str(osd_id):
            continue
        if osd_fsid is not None and props.get('osd_fsid') != osd_fsid:
            continue
        found: Dict[str, Any] = {
            'osd_id': props.get('osd_id'),
            'osd_fsid': props.get('osd_fsid'),
            'cluster_name': props.get('cluster_name') or default_cluster or conf.cluster,
            'block': None,
            'db': None,
            'wal': None,
        }
        for zvol in entry['zvols']:
            ztype = zvol['properties'].get('type')
            if ztype in ('block', 'db', 'wal'):
                found[ztype] = zvol['device']
            for other in ('db', 'wal'):
                linked = zvol['properties'].get('{}_device'.format(other))
                if linked and not found[other]:
                    found[other] = linked
        if found['block']:
            return found
    return None


def set_properties(pool_name: str, properties: Dict[str, str]) -> None:
    """
    Sets ZFS user properties on a zpool.
    Keys with an empty or None value are skipped.
    """
    for key, value in properties.items():
        if not value:
            continue
        logger.debug('Setting %s=%s on zpool %s', key, value, pool_name)
        process.run(['/sbin/zpool', 'set', '{}={}'.format(key, value), pool_name])


def pool_fraction_size(pool_name: str, fraction: float = 0.95) -> str:
    """
    Returns *fraction* of the pool's available bytes as an absolute
    size string suitable for ``zfs create -V``.
    Must be called after the pool exists.
    """
    out, err, rc = process.call(
        ['/sbin/zfs', 'get', '-Hp', '-o', 'value', 'available', pool_name]
    )
    if rc != 0 or not out:
        raise RuntimeError(
            'Could not determine available space in zpool {}'.format(pool_name)
        )
    return str(int(int(out[0].strip()) * fraction))
