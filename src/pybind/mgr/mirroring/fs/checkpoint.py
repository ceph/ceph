import errno
import os
import time
from datetime import datetime

import cephfs

from .exception import MirrorException

# Snapshot metadata keys for mirroring checkpoints (must match Checkpoint.h).
CHECKPOINT_STATUS_KEY = 'cephfs.mirror.checkpoint.status'
CHECKPOINT_CREATED_AT_KEY = 'cephfs.mirror.checkpoint.created_at'
CHECKPOINT_UPDATED_AT_KEY = 'cephfs.mirror.checkpoint.updated_at'
CHECKPOINT_ERROR_MSG_KEY = 'cephfs.mirror.checkpoint.error_msg'
CHECKPOINT_METADATA_KEYS = (
    CHECKPOINT_STATUS_KEY,
    CHECKPOINT_CREATED_AT_KEY,
    CHECKPOINT_UPDATED_AT_KEY,
    CHECKPOINT_ERROR_MSG_KEY,
)
CHECKPOINT_STATUS_CREATED = 0
CHECKPOINT_STATUS_COMPLETE = 1
CHECKPOINT_STATUS_FAILED = 2
CHECKPOINT_STATUS_NAMES = {
    CHECKPOINT_STATUS_CREATED: 'created',
    CHECKPOINT_STATUS_COMPLETE: 'complete',
    CHECKPOINT_STATUS_FAILED: 'failed',
}


def get_checkpoint_epoch():
    """Get current time as UNIX epoch timestamp (9 decimal places, matches C++ %.9f)."""
    return f'{time.time():.9f}'


def format_epoch_timestamp(epoch_str):
    """Format UNIX epoch timestamp for display in ISO format with timezone."""
    try:
        epoch = float(epoch_str)
        dt = datetime.fromtimestamp(epoch).astimezone()
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    except (ValueError, OSError):
        return epoch_str


def snap_path(dir_path, snap_dir, snap_name):
    return os.path.join(dir_path, snap_dir, snap_name)


def checkpoint_status_to_string(status_val):
    try:
        return CHECKPOINT_STATUS_NAMES[int(status_val)]
    except (ValueError, KeyError):
        return 'unknown'


def checkpoint_from_snap(snap_id, snap_name, metadata):
    created_at = metadata.get(CHECKPOINT_CREATED_AT_KEY, '')
    updated_at = metadata.get(CHECKPOINT_UPDATED_AT_KEY, '')

    cp = {
        'snap_id': snap_id,
        'snap_name': snap_name,
        'status': checkpoint_status_to_string(
            metadata.get(CHECKPOINT_STATUS_KEY, CHECKPOINT_STATUS_CREATED)),
        'created_at': format_epoch_timestamp(created_at) if created_at else '',
        'updated_at': format_epoch_timestamp(updated_at) if updated_at else '',
    }
    if CHECKPOINT_ERROR_MSG_KEY in metadata:
        cp['error_msg'] = metadata[CHECKPOINT_ERROR_MSG_KEY]
    return cp


def is_checkpointed(metadata):
    return CHECKPOINT_STATUS_KEY in metadata


class Checkpoint:
    def __init__(self, get_snapdir):
        self.get_snapdir = get_snapdir

    def list_directory_snapshots(self, fsh, dir_path):
        snap_dir = self.get_snapdir()
        snap_names = []
        try:
            with fsh.opendir(os.path.join(dir_path, snap_dir)) as d_handle:
                ent = fsh.readdir(d_handle)
                while ent:
                    name = ent.d_name.decode('utf-8')
                    if name not in ('.', '..') and not name.startswith('_'):
                        snap_names.append(name)
                    ent = fsh.readdir(d_handle)
        except cephfs.Error as e:
            if e.errno == errno.ENOENT:
                return []
            raise MirrorException(-e.errno, f'failed to list snapshots: {e}')
        return snap_names

    def snap_info(self, fsh, dir_path, snap_name):
        path = snap_path(dir_path, self.get_snapdir(), snap_name)
        try:
            return fsh.snap_info(path)
        except cephfs.Error as e:
            raise MirrorException(-e.errno,
                                  f"snapshot '{snap_name}' not found under {dir_path}")

    def get_latest_snap(self, fsh, dir_path):
        latest_id = 0
        latest_name = None
        latest_info = None

        for snap_name in self.list_directory_snapshots(fsh, dir_path):
            info = self.snap_info(fsh, dir_path, snap_name)
            if info['id'] > latest_id:
                latest_id = info['id']
                latest_name = snap_name
                latest_info = info

        if latest_name is None:
            raise MirrorException(-errno.ENOENT, f'no snapshots found under {dir_path}')
        return latest_name, latest_info

    def write_metadata(self, fsh, dir_path, snap_name, snap_info):
        if is_checkpointed(snap_info.get('metadata', {})):
            raise MirrorException(-errno.EEXIST, 'checkpoint already exists for snapshot')

        path = snap_path(dir_path, self.get_snapdir(), snap_name)
        now = get_checkpoint_epoch()
        to_write = {
            CHECKPOINT_STATUS_KEY: str(CHECKPOINT_STATUS_CREATED),
            CHECKPOINT_CREATED_AT_KEY: now,
            CHECKPOINT_UPDATED_AT_KEY: now,
        }

        op = cephfs.CEPH_SNAP_MD_OP_CREATE | cephfs.CEPH_SNAP_MD_OP_EXCL
        for key, val in to_write.items():
            fsh.do_snap_md_op(path, key, val, op)

    def remove_metadata(self, fsh, dir_path, snap_name, snap_info):
        metadata = snap_info.get('metadata', {})
        if not is_checkpointed(metadata):
            raise MirrorException(-errno.ENOENT, 'checkpoint not found for snapshot')

        path = snap_path(dir_path, self.get_snapdir(), snap_name)
        for key in CHECKPOINT_METADATA_KEYS:
            if key in metadata:
                fsh.do_snap_md_op(path, key, '', cephfs.CEPH_SNAP_MD_OP_REMOVE)
