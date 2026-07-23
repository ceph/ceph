import glob
import os

_SYS_BLOCK = '/sys/block'


def block_device_fact_sysfs_rel_path(sysdir: str, rel: str) -> str:
    """
    Resolve the sysfs path (relative to `sysdir`, typically `/sys/block/<devname>/`) for
    block device metadata files such as vendor/model/rev.

    If the usual path `<sysdir>/<rel>` exists, `rel` is returned.
    Otherwise, for nvme style layouts, looks for `<sysdir>/device/nvme*/device/<basename>`
    and returns the matching path relative to `sysdir` when exactly one match exists.
    """
    if rel not in ('device/vendor', 'device/model', 'device/rev'):
        return rel
    sysdir = os.path.normpath(sysdir)
    default_path = os.path.join(sysdir, rel)
    if os.path.exists(default_path):
        return rel
    basename = os.path.basename(rel)
    pattern = os.path.join(sysdir, 'device', 'nvme*', 'device', basename)
    matches = sorted(glob.glob(pattern))
    if len(matches) == 1:
        return os.path.relpath(matches[0], sysdir)
    return rel


def is_whole_nvme_namespace_name(basename: str, _sys_block_path: str = _SYS_BLOCK) -> bool:
    """
    True if `basename` is a whole nvme namespace (example: nvme0n1), not a partition.

    Uses sysfs only: partitions expose a `partition` file; whole NVMe
    namespaces have a controller entry under `device/nvme*`.
    """
    sysdir = os.path.join(_sys_block_path, basename)
    if not os.path.isdir(sysdir):
        return False
    if os.path.exists(os.path.join(sysdir, 'partition')):
        return False
    return bool(glob.glob(os.path.join(sysdir, 'device', 'nvme*')))
