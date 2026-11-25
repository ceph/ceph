"""ZFS volume (zvol) helpers: listing, reading and setting ceph:* properties."""

import logging
from typing import Dict, List, Optional

from ceph_volume import process

logger = logging.getLogger(__name__)


def get_zvols(pool_name: str) -> List[Dict[str, str]]:
    """
    Returns the zvols (ZFS volumes) inside a pool, as a list of
    dicts: [{'name': 'osd-block-<fsid>', 'dataset':
    '<pool>/osd-block-<fsid>', 'size': '<bytes>'}, ...].
    """
    command = ['/sbin/zfs', 'list', '-H', '-p', '-t', 'volume',
               '-o', 'name,volsize', '-r', pool_name]
    out, err, rc = process.call(command, verbose_on_failure=False)
    zvols: List[Dict[str, str]] = []
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


def get_zvol_ceph_properties(dataset: str) -> Dict[str, str]:
    """
    Reads back the ceph:* user properties set on a zvol by
    ceph_volume.objectstore.zfs.Zfs._tag_zvol(). Returns a dict
    keyed without the 'ceph:' prefix.
    """
    command = ['/sbin/zfs', 'get', '-H', '-o', 'property,value', 'all', dataset]
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


def set_properties(dataset: str, properties: Dict[str, str]) -> None:
    """
    Sets ZFS user properties on a dataset (zvol or filesystem).
    Keys with an empty or None value are skipped.
    """
    for key, value in properties.items():
        if not value:
            continue
        logger.debug('Setting %s=%s on %s', key, value, dataset)
        process.run(['/sbin/zfs', 'set', '{}={}'.format(key, value), dataset])


def tag_zvol(dataset: str, osd_id: str, osd_fsid: str,
             cluster_name: str, cluster_fsid: str,
             objectstore: str, crush_device_class: str,
             device_type: str = 'block',
             metadata_devices: Optional[Dict[str, str]] = None,
             block_zvol_path: Optional[str] = None) -> None:
    """
    Sets ceph:* user properties on a zvol -- the metadata store
    equivalent to LVM tags.

    *metadata_devices* maps ``'db'``/``'wal'`` to their zvol dataset
    paths (used for cross-links when *device_type* is ``'block'``).
    *block_zvol_path* is the ``/dev/zvol/...`` path of the main block
    zvol (used for cross-links when tagging a db or wal zvol).
    """
    properties: Dict[str, str] = {
        'ceph:osd_id': str(osd_id),
        'ceph:osd_fsid': str(osd_fsid),
        'ceph:cluster_name': str(cluster_name),
        'ceph:cluster_fsid': cluster_fsid,
        'ceph:type': device_type,
        'ceph:objectstore': str(objectstore),
        'ceph:crush_device_class': crush_device_class or '',
    }
    if device_type == 'block' and metadata_devices:
        for other in ('db', 'wal'):
            other_dataset = metadata_devices.get(other)
            if other_dataset:
                properties['ceph:{}_device'.format(other)] = \
                    '/dev/zvol/{}'.format(other_dataset)
    elif device_type != 'block' and block_zvol_path:
        properties['ceph:block_device'] = block_zvol_path
    set_properties(dataset, properties)


def create_cmd(size: str, dataset: str, thin: bool = False) -> List[str]:
    """
    Builds a ``zfs create -V`` command for a zvol.
    Pass *thin=True* for a sparse (-s) zvol.
    """
    cmd = ['/sbin/zfs', 'create']
    if thin:
        cmd.append('-s')
    cmd.extend(['-V', size, dataset])
    return cmd
