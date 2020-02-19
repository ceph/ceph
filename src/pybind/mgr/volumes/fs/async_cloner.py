import os
import stat
import time
import errno
import logging
from contextlib import contextmanager

import cephfs

from .async_job import AsyncJobs
from .exception import IndexException, MetadataMgrException, OpSmException, VolumeException
from .fs_util import copy_file
from .operations.op_sm import OpSm
from .operations.resolver import resolve
from .operations.volume import open_volume, open_volume_lockless
from .operations.group import open_group
from .operations.subvolume import open_subvol
from .operations.clone_index import open_clone_index

log = logging.getLogger(__name__)

# helper for fetching a clone entry for a given volume
def get_next_clone_entry(volume_client, volname, running_jobs):
    log.debug("fetching clone entry for volume '{0}'".format(volname))

    try:
        with open_volume_lockless(volume_client, volname) as fs_handle:
            try:
                with open_clone_index(fs_handle, volume_client.volspec) as clone_index:
                    job = clone_index.get_oldest_clone_entry(running_jobs)
                    return 0, job
            except IndexException as ve:
                if ve.errno == -errno.ENOENT:
                    return 0, None
                raise ve
    except VolumeException as ve:
        log.error("error fetching clone entry for volume '{0}' ({1})".format(volname), ve)
        return ve.errno, None
    return ret

@contextmanager
def open_at_volume(volume_client, volname, groupname, subvolname, need_complete=False, expected_types=[]):
    with open_volume(volume_client, volname) as fs_handle:
        with open_group(fs_handle, volume_client.volspec, groupname) as group:
            with open_subvol(fs_handle, volume_client.volspec, group, subvolname,
                             need_complete, expected_types) as subvolume:
                yield subvolume

@contextmanager
def open_at_group(volume_client, fs_handle, groupname, subvolname, need_complete=False, expected_types=[]):
    with open_group(fs_handle, volume_client.volspec, groupname) as group:
        with open_subvol(fs_handle, volume_client.volspec, group, subvolname,
                         need_complete, expected_types) as subvolume:
            yield subvolume

def get_clone_state(volume_client, volname, groupname, subvolname):
    with open_at_volume(volume_client, volname, groupname, subvolname) as subvolume:
        return subvolume.state

def set_clone_state(volume_client, volname, groupname, subvolname, state):
    with open_at_volume(volume_client, volname, groupname, subvolname) as subvolume:
        subvolume.state = (state, True)

def get_clone_source(clone_subvolume):
    source = clone_subvolume._get_clone_source()
    return (source['volume'], source.get('group', None), source['subvolume'], source['snapshot'])

def handle_clone_pending(volume_client, volname, index, groupname, subvolname, should_cancel):
    try:
        next_state = OpSm.get_next_state("clone", "pending", 0)
    except OpSmException as oe:
        raise VolumeException(oe.error, oe.error_str)
    return (next_state, False)

def bulk_copy(fs_handle, source_path, dst_path, should_cancel):
    """
    bulk copy data from source to destination -- only directories, symlinks
    and regular files are synced. note that @should_cancel is not used right
    now but would be required when implementing cancelation for in-progress
    clone operations.
    """
    log.info("copying data from {0} to {1}".format(source_path, dst_path))
    def cptree(src_root_path, dst_root_path):
        log.debug("cptree: {0} -> {1}".format(src_root_path, dst_root_path))
        try:
            with fs_handle.opendir(src_root_path) as dir_handle:
                d = fs_handle.readdir(dir_handle)
                while d:
                    if d.d_name not in (b".", b".."):
                        log.debug("d={0}".format(d))
                        d_full_src = os.path.join(src_root_path, d.d_name)
                        d_full_dst = os.path.join(dst_root_path, d.d_name)
                        st = fs_handle.lstat(d_full_src)
                        mo = st.st_mode & ~stat.S_IFMT(st.st_mode)
                        if stat.S_ISDIR(st.st_mode):
                            log.debug("cptree: (DIR) {0}".format(d_full_src))
                            try:
                                fs_handle.mkdir(d_full_dst, mo)
                                fs_handle.chown(d_full_dst, int(st.st_uid), int(st.st_gid))
                            except cephfs.Error as e:
                                if not e.args[0] == errno.EEXIST:
                                    raise
                            cptree(d_full_src, d_full_dst)
                        elif stat.S_ISLNK(st.st_mode):
                            log.debug("cptree: (SYMLINK) {0}".format(d_full_src))
                            target = fs_handle.readlink(d_full_src, 4096)
                            try:
                                fs_handle.symlink(target[:st.st_size], d_full_dst)
                            except cephfs.Error as e:
                                if not e.args[0] == errno.EEXIST:
                                    raise
                        elif stat.S_ISREG(st.st_mode):
                            log.debug("cptree: (REG) {0}".format(d_full_src))
                            copy_file(fs_handle, d_full_src, d_full_dst, mo, int(st.st_uid), int(st.st_gid))
                        else:
                            log.warn("cptree: (IGNORE) {0}".format(d_full_src))
                    d = fs_handle.readdir(dir_handle)
        except cephfs.Error as e:
            if not e.args[0] == errno.ENOENT:
                raise VolumeException(-e.args[0], e.args[1])
    cptree(source_path, dst_path)

def do_clone(volume_client, volname, groupname, subvolname, should_cancel):
    with open_volume_lockless(volume_client, volname) as fs_handle:
        with open_at_group(volume_client, fs_handle, groupname, subvolname) as clone_subvolume:
            s_volname, s_groupname, s_subvolname, s_snapname = get_clone_source(clone_subvolume)
            with open_at_group(volume_client, fs_handle, s_groupname, s_subvolname) as source_subvolume:
                src_path = source_subvolume.snapshot_path(s_snapname)
                dst_path = clone_subvolume.path
                bulk_copy(fs_handle, src_path, dst_path, should_cancel)

def handle_clone_in_progress(volume_client, volname, index, groupname, subvolname, should_cancel):
    try:
        do_clone(volume_client, volname, groupname, subvolname, should_cancel)
        next_state = OpSm.get_next_state("clone", "in-progress", 0)
    except VolumeException as ve:
        # jump to failed state
        next_state = OpSm.get_next_state("clone", "in-progress", -1)
    except OpSmException as oe:
        raise VolumeException(oe.error, oe.error_str)
    return (next_state, False)

def handle_clone_failed(volume_client, volname, index, groupname, subvolname, should_cancel):
    try:
        # detach source but leave the clone section intact for later inspection
        with open_volume(volume_client, volname) as fs_handle:
            with open_at_group(volume_client, fs_handle, groupname, subvolname) as clone_subvolume:
                s_volname, s_groupname, s_subvolname, s_snapname = get_clone_source(clone_subvolume)
                with open_at_group(volume_client, fs_handle, s_groupname, s_subvolname) as source_subvolume:
                    source_subvolume.detach_snapshot(s_snapname, index)
    except (MetadataMgrException, VolumeException) as e:
        log.error("failed to detach clone from snapshot: {0}".format(e))
    return (None, True)

def handle_clone_complete(volume_client, volname, index, groupname, subvolname, should_cancel):
    try:
        with open_volume(volume_client, volname) as fs_handle:
            with open_at_group(volume_client, fs_handle, groupname, subvolname) as clone_subvolume:
                s_volname, s_groupname, s_subvolname, s_snapname = get_clone_source(clone_subvolume)
                with open_at_group(volume_client, fs_handle, s_groupname, s_subvolname) as source_subvolume:
                    source_subvolume.detach_snapshot(s_snapname, index)
                    clone_subvolume.remove_clone_source(flush=True)
    except (MetadataMgrException, VolumeException) as e:
        log.error("failed to detach clone from snapshot: {0}".format(e))
    return (None, True)

def start_clone_sm(volume_client, volname, index, groupname, subvolname, state_table, should_cancel):
    finished = False
    current_state = None
    try:
        current_state = get_clone_state(volume_client, volname, groupname, subvolname)
        log.debug("cloning ({0}, {1}, {2}) -- starting state \"{3}\"".format(volname, groupname, subvolname, current_state))
        while not finished:
            handler = state_table.get(current_state, None)
            if not handler:
                raise VolumeException(-errno.EINVAL, "invalid clone state: \"{0}\"".format(current_state))
            (next_state, finished) = handler(volume_client, volname, index, groupname, subvolname, should_cancel)
            if next_state:
                log.debug("({0}, {1}, {2}) transition state [\"{3}\" => \"{4}\"]".format(volname, groupname, subvolname,\
                                                                                         current_state, next_state))
                set_clone_state(volume_client, volname, groupname, subvolname, next_state)
                current_state = next_state
    except VolumeException as ve:
        log.error("clone failed for ({0}, {1}, {2}) (current_state: {3}, reason: {4})".format(volname, groupname,\
                                                                                             subvolname, current_state, ve))

def clone(volume_client, volname, index, clone_path, state_table, should_cancel):
    log.info("cloning to subvolume path: {0}".format(clone_path))
    resolved = resolve(volume_client.volspec, clone_path)

    groupname  = resolved[0]
    subvolname = resolved[1]
    log.debug("resolved to [group: {0}, subvolume: {1}]".format(groupname, subvolname))

    try:
        log.info("starting clone: ({0}, {1}, {2})".format(volname, groupname, subvolname))
        start_clone_sm(volume_client, volname, index, groupname, subvolname, state_table, should_cancel)
        log.info("finished clone: ({0}, {1}, {2})".format(volname, groupname, subvolname))
    except VolumeException as ve:
        log.error("clone failed for ({0}, {1}, {2}), reason: {3}".format(volname, groupname, subvolname, ve))

class Cloner(AsyncJobs):
    """
    Asynchronous cloner: pool of threads to copy data from a snapshot to a subvolume.
    this relies on a simple state machine (which mimics states from OpSm class) as
    the driver. file types supported are directories, symbolic links and regular files.
    """
    def __init__(self, volume_client, tp_size):
        self.vc = volume_client
        self.state_table = {
            'pending'     : handle_clone_pending,
            'in-progress' : handle_clone_in_progress,
            'complete'    : handle_clone_complete,
            'failed'      : handle_clone_failed
        }
        super(Cloner, self).__init__(volume_client, "cloner", tp_size)

    def get_next_job(self, volname, running_jobs):
        return get_next_clone_entry(self.vc, volname, running_jobs)

    def execute_job(self, volname, job, should_cancel):
        clone(self.vc, volname, job[0].decode('utf-8'), job[1].decode('utf-8'), self.state_table, should_cancel)
