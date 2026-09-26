import os
import errno
import logging

from ceph.deployment.service_spec import ServiceSpec, PlacementSpec

import cephfs
import orchestrator

from .exception import VolumeException

log = logging.getLogger(__name__)

def create_pool(mgr, pool_name, **extra_args):
    # create the given pool
    command = extra_args
    command.update({'prefix': 'osd pool create', 'pool': pool_name})
    return mgr.mon_command(command)

def remove_pool(mgr, pool_name):
    command = {'prefix': 'osd pool rm', 'pool': pool_name, 'pool2': pool_name,
               'yes_i_really_really_mean_it': True}
    return mgr.mon_command(command)

def rename_pool(mgr, pool_name, new_pool_name):
    command = {'prefix': 'osd pool rename', 'srcpool': pool_name,
               'destpool': new_pool_name}
    return mgr.mon_command(command)

def create_filesystem(mgr, fs_name, metadata_pool, data_pool):
    command = {'prefix': 'fs new', 'fs_name': fs_name, 'metadata': metadata_pool,
               'data': data_pool}
    return mgr.mon_command(command)

def remove_filesystem(mgr, fs_name):
    command = {'prefix': 'fs fail', 'fs_name': fs_name}
    r, outb, outs = mgr.mon_command(command)
    if r != 0:
        return r, outb, outs

    command = {'prefix': 'fs rm', 'fs_name': fs_name, 'yes_i_really_mean_it': True}
    return mgr.mon_command(command)

def rename_filesystem(mgr, fs_name, new_fs_name):
    command = {'prefix': 'fs rename', 'fs_name': fs_name, 'new_fs_name': new_fs_name,
               'yes_i_really_mean_it': True}
    return mgr.mon_command(command)

def create_mds(mgr, fs_name, placement):
    try:
        spec = ServiceSpec(service_type='mds',
                           service_id=fs_name,
                           placement=PlacementSpec.from_string(placement))
    except Exception as e:
        return -errno.EINVAL, "", str(e)
    try:
        completion = mgr.apply([spec], no_overwrite=True)
        orchestrator.raise_if_exception(completion)
    except (ImportError, orchestrator.OrchestratorError):
        return 0, "", "Volume created successfully (no MDS daemons created)"
    except Exception as e:
        # Don't let detailed orchestrator exceptions (python backtraces)
        # bubble out to the user
        log.exception("Failed to create MDS daemons")
        return -errno.EINVAL, "", str(e)
    return 0, "", ""

def volume_exists(mgr, fs_name):
    fs_map = mgr.get('fs_map')
    for fs in fs_map['filesystems']:
        if fs['mdsmap']['fs_name'] == fs_name:
            return True
    return False

def listdir(fs, dirpath, filter_entries=None, filter_files=True):
    """
    Get the directory entries for a given path. List only dirs if 'filter_files' is True.
    Don't list the entries passed in 'filter_entries'

    :rtype: list of bytes
    """
    entries = []
    if filter_entries is None:
        filter_entries = [b".", b".."]
    else:
        filter_entries.extend([b".", b".."])
    try:
        with fs.opendir(dirpath) as dir_handle:
            d = fs.readdir(dir_handle)
            while d:
                if (d.d_name not in filter_entries):
                    if not filter_files:
                        entries.append(d.d_name)
                    elif d.is_dir():
                        entries.append(d.d_name)
                d = fs.readdir(dir_handle)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])
    return entries


def list_files(fs, path):
    return listdir(fs, path, filter_files=False)


def is_dir_empty(fs, path):
    try:
        with fs.opendir(path) as dir_handle:
            d = fs.readdir(dir_handle)
            while d:
                if not d.d_name in (b'.', b'..'):
                    return False
                d = fs.readdir(dir_handle)
    except cephfs.Error as e:
        raise VolumeException(exception=e)

    return True


def has_subdir(fs, dirpath, filter_entries=None):
    """
    Check the presence of directory (only dirs) for a given path
    """
    res = False
    if filter_entries is None:
        filter_entries = [b".", b".."]
    else:
        filter_entries.extend([b".", b".."])
    try:
        with fs.opendir(dirpath) as dir_handle:
            d = fs.readdir(dir_handle)
            while d:
                if (d.d_name not in filter_entries) and d.is_dir():
                    res = True
                    break
                d = fs.readdir(dir_handle)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])
    return res

def is_inherited_snap(snapname):
    """
    Returns True if the snapname is inherited else False
    """
    return snapname.startswith("_")


def list_snaps(fs, vol_spec, snapdirpath, filter_inherited_snaps=True):
    """
    Get the snap names from a given snap directory path

    :returns: list of snap names
    """
    if os.path.basename(snapdirpath) != vol_spec.snap_base_dir.encode('utf-8'):
        raise VolumeException(-errno.EINVAL, "Not a snap directory: {0}".format(snapdirpath))
    snaps = []
    try:
        with fs.opendir(snapdirpath) as dir_handle:
            d = fs.readdir(dir_handle)
            while d:
                if (d.d_name not in (b".", b"..")) and d.is_dir():
                    d_name = d.d_name.decode('utf-8')
                    if not is_inherited_snap(d_name):
                        snaps.append(d.d_name)
                    elif is_inherited_snap(d_name) and not filter_inherited_snaps:
                        snaps.append(d.d_name)
                d = fs.readdir(dir_handle)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])
    return snaps


def list_one_entry_at_a_time(fs, dirpath):
    """
    Get a directory entry (one entry a time)
    """
    try:
        with fs.opendir(dirpath) as dir_handle:
            d = fs.readdir(dir_handle)
            while d:
                if d.d_name not in (b".", b".."):
                    yield d
                d = fs.readdir(dir_handle)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])

def copy_file(fs, src, dst, mode, cancel_check=None):
    """
    Copy a regular file from @src to @dst. @dst is overwritten if it exists.
    """
    src_fd = dst_fd = None
    try:
        src_fd = fs.open(src, os.O_RDONLY)
        dst_fd = fs.open(dst, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode)
    except cephfs.Error as e:
        if src_fd is not None:
            fs.close(src_fd)
        if dst_fd is not None:
            fs.close(dst_fd)
        raise VolumeException(-e.args[0], e.args[1])

    IO_SIZE = 8 * 1024 * 1024
    try:
        while True:
            if cancel_check and cancel_check():
                raise VolumeException(-errno.EINTR, "copy operation interrupted")
            data = fs.read(src_fd, -1, IO_SIZE)
            if not len(data):
                break
            written = 0
            while written < len(data):
                written += fs.write(dst_fd, data[written:], -1)
        fs.fsync(dst_fd, 0)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])
    finally:
        fs.close(src_fd)
        fs.close(dst_fd)

def get_ancestor_xattr(fs, path, attr):
    """
    Helper for reading layout information: if this xattr is missing
    on the requested path, keep checking parents until we find it.
    """
    try:
        return fs.getxattr(path, attr).decode('utf-8')
    except cephfs.NoData as e:
        if path == "/":
            raise VolumeException(-e.args[0], e.args[1])
        else:
            return get_ancestor_xattr(fs, os.path.split(path)[0], attr)


def get_all_xattrs(fs, path):
    '''
    Get/return all xattrs present on the given path.

    :returns: dict of xattr key and values
    '''
    num_of_keys, keys = fs.listxattr(path)
    if not keys:
        return
    keys = keys.split('\x00')
    assert len(keys) == num_of_keys

    sv_xattrs = {}
    for xattr in keys:
        if not xattr:
            continue
        val = fs.getxattr(path, xattr)
        if val:
            sv_xattrs[xattr] = val

    return sv_xattrs


def set_all_xattrs(fs, path, path_xattrs):
    '''
    Set all passed xattrs on the given path

    :param xattrs: dict of xattr key and values
    '''
    for xattr, val in path_xattrs.items():
        if not val:
            continue
        fs.setxattr(path, xattr, val)


def create_base_dir(fs, path, mode):
    """
    Create volspec base/group directory if it doesn't exist
    """
    try:
        fs.stat(path)
    except cephfs.Error as e:
        if e.args[0] == errno.ENOENT:
            fs.mkdirs(path, mode)
        else:
            raise VolumeException(-e.args[0], e.args[1])


def statx(fs, path, fields=None, follow_symlink=True):
    '''
    Convenient wrapper around libcephfs's statx().

    :param path: path to be statx'ed
    :param fields: stat buffer fields to be fetched
    :returns: bool or list. list if fields were passed, otherwise bool
    '''
    mask = 0
    if fields:
        if 'uid' in fields:
            mask = cephfs.CEPH_STATX_UID
        if 'gid' in fields:
            mask = mask | cephfs.CEPH_STATX_GID
        if 'mode' in fields:
            mask = mask | cephfs.CEPH_STATX_MODE

    flags = 0
    flags = flags | cephfs.AT_STATX_SYNC_AS_STAT
    if follow_symlink:
        flags = flags | cephfs.AT_SYMLINK_NOFOLLOW

    # sxb = statx buffer
    sxb = fs.statx(path, mask, flags)

    if mask == 0:
        # no fields were fetched, only presence was checked
        return True

    sxb_fields = []
     if 'uid' in fields:
        sxb_fields.append(int(sxb['uid']))
     if 'gid' in fields:
        sxb_fields.append(int(sxb['gid']))
    if 'mode' in fields:
        sxb_fields.append(int(sxb['mode']))

    return sxb_fields


def path_exists(fs, path, follow_symlink=True):
    try:
        # prefer statx over stat has it passes much lesser data on n/w
        statx(fs, path, None, follow_symlink)
    except cephfs.PermissionError:
        raise
    except:
        return False
    else:
        return True
