"""Mount table and swap helpers for FreeBSD."""

from typing import Any, Dict, List, Optional, Tuple

from ceph_volume import process
from ceph_volume.util.zfs.gpart import (
    Partition,
    device_on_disk,
    get_label_aliases,
    resolve_device,
)


# One line of "mount -p": (device, mountpoint or None)
MountEntry = Tuple[str, Optional[str]]


def get_mount_entries() -> List[MountEntry]:
    """
    Every mounted device with where it is mounted, from one `mount -p`:
    [('/dev/gpt/data', '/mnt/data'), ...].

    The output is the same whichever disk is being looked at, so
    get_disks() reads it once and hands it to get_mount_info().
    """
    out, err, rc = process.call(['/sbin/mount', '-p'])
    entries: List[MountEntry] = []
    if rc != 0:
        return entries
    for line in out:
        fields = line.split()
        if not fields or not fields[0].startswith('/dev/'):
            continue
        entries.append((fields[0], fields[1] if len(fields) > 1 else None))
    return entries


def get_swap_entries() -> List[str]:
    """
    Every active swap device, from one `swapctl -l`.  Read once per run
    for the same reason as get_mount_entries().
    """
    out, err, rc = process.call(['/sbin/swapctl', '-l'])
    entries: List[str] = []
    if rc != 0 or not out:
        return entries
    for line in out[1:]:  # first line is the header: "Device  1K-blocks  Used"
        fields = line.split()
        if not fields or not fields[0].startswith('/dev/'):
            continue
        entries.append(fields[0])
    return entries


def get_mount_info(diskname: str,
                   partitions: Optional[List[Partition]] = None,
                   aliases: Optional[Dict[str, str]] = None,
                   entries: Optional[List[MountEntry]] = None) -> Dict[str, Any]:
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
    result: Dict[str, Any] = {'mounted': False, 'mountpoints': [], 'by_partition': {}}
    partitions = partitions or []
    for part in partitions:
        result['by_partition'][part['name']] = {'mounted': False, 'mountpoint': None}
    if entries is None:
        entries = get_mount_entries()
    if aliases is None:
        aliases = get_label_aliases(partitions)
    for device, mountpoint in entries:
        matched_name = device_on_disk(resolve_device(device, aliases), diskname)
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


def get_swap_info(diskname: str,
                  partitions: Optional[List[Partition]] = None,
                  aliases: Optional[Dict[str, str]] = None,
                  entries: Optional[List[str]] = None) -> Dict[str, Any]:
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
    result: Dict[str, Any] = {'active': False, 'devices': [], 'by_partition': {}}
    partitions = partitions or []
    for part in partitions:
        result['by_partition'][part['name']] = {'active': False}
    if entries is None:
        entries = get_swap_entries()
    if aliases is None:
        aliases = get_label_aliases(partitions)
    for device in entries:
        matched_name = device_on_disk(resolve_device(device, aliases), diskname)
        if not matched_name:
            continue
        result['active'] = True
        result['devices'].append(device)
        result['by_partition'][matched_name] = {'active': True}
    return result
