import os
import stat
import time
import errno
import logging
from contextlib import contextmanager
from typing import Optional

import cephfs
from mgr_util import lock_timeout_log

from .async_job import AsyncJobs
from .exception import IndexException, MetadataMgrException, OpSmException, VolumeException
from .fs_util import copy_file
from .operations.versions.op_sm import SubvolumeOpSm
from .operations.versions.subvolume_attrs import SubvolumeTypes, SubvolumeStates, SubvolumeActions
from .operations.resolver import resolve
from .operations.volume import open_volume, open_volume_lockless
from .operations.group import open_group
from .operations.subvolume import open_subvol
from .operations.clone_index import open_clone_index
from .operations.template import SubvolumeOpType

log = logging.getLogger(__name__)

# helper for fetching a clone entry for a given volume
def get_next_clone_entry(fs_client, volspec, volname, running_jobs):
    log.debug("fetching clone entry for volume '{0}'".format(volname))

    try:
        with open_volume_lockless(fs_client, volname) as fs_handle:
            try:
                with open_clone_index(fs_handle, volspec) as clone_index:
                    job = clone_index.get_oldest_clone_entry(running_jobs)
                    return 0, job
            except IndexException as ve:
                if ve.errno == -errno.ENOENT:
                    return 0, None
                raise ve
    except VolumeException as ve:
        log.error("error fetching clone entry for volume '{0}' ({1})".format(volname, ve))
        return ve.errno, None

@contextmanager
def open_at_volume(fs_client, volspec, volname, groupname, subvolname, op_type):
    with open_volume(fs_client, volname) as fs_handle:
        with open_group(fs_handle, volspec, groupname) as group:
            with open_subvol(fs_client.mgr, fs_handle, volspec, group, subvolname, op_type) as subvolume:
                yield subvolume

@contextmanager
def open_at_group(fs_client, fs_handle, volspec, groupname, subvolname, op_type):
    with open_group(fs_handle, volspec, groupname) as group:
        with open_subvol(fs_client.mgr, fs_handle, volspec, group, subvolname, op_type) as subvolume:
            yield subvolume

@contextmanager
def open_at_group_unique(fs_client, fs_handle, volspec, s_groupname, s_subvolname, c_subvolume, c_groupname, c_subvolname, op_type):
    # if a snapshot of a retained subvolume is being cloned to recreate the same subvolume, return
    # the clone subvolume as the source subvolume
    if s_groupname == c_groupname and s_subvolname == c_subvolname:
        yield c_subvolume
    else:
        with open_at_group(fs_client, fs_handle, volspec, s_groupname, s_subvolname, op_type) as s_subvolume:
            yield s_subvolume


@contextmanager
def open_clone_subvolume_pair(fs_client, fs_handle, volspec, volname, groupname, subvolname):
    with open_at_group(fs_client, fs_handle, volspec, groupname, subvolname, SubvolumeOpType.CLONE_INTERNAL) as clone_subvolume:
        s_volname, s_groupname, s_subvolname, s_snapname = get_clone_source(clone_subvolume)
        if groupname == s_groupname and subvolname == s_subvolname:
            # use the same subvolume to avoid metadata overwrites
            yield (clone_subvolume, clone_subvolume, s_snapname)
        else:
            with open_at_group(fs_client, fs_handle, volspec, s_groupname, s_subvolname, SubvolumeOpType.CLONE_SOURCE) as source_subvolume:
                yield (clone_subvolume, source_subvolume, s_snapname)

def get_clone_state(fs_client, volspec, volname, groupname, subvolname):
    with open_at_volume(fs_client, volspec, volname, groupname, subvolname, SubvolumeOpType.CLONE_INTERNAL) as subvolume:
        return subvolume.state

def set_clone_state(fs_client, volspec, volname, groupname, subvolname, state):
    with open_at_volume(fs_client, volspec, volname, groupname, subvolname, SubvolumeOpType.CLONE_INTERNAL) as subvolume:
        subvolume.state = (state, True)

def get_clone_source(clone_subvolume):
    source = clone_subvolume._get_clone_source()
    return (source['volume'], source.get('group', None), source['subvolume'], source['snapshot'])

def get_next_state_on_error(errnum):
    if errnum == -errno.EINTR:
        next_state = SubvolumeOpSm.transition(SubvolumeTypes.TYPE_CLONE,
                                              SubvolumeStates.STATE_INPROGRESS,
                                              SubvolumeActions.ACTION_CANCELLED)
    else:
        # jump to failed state, on all other errors
        next_state = SubvolumeOpSm.transition(SubvolumeTypes.TYPE_CLONE,
                                              SubvolumeStates.STATE_INPROGRESS,
                                              SubvolumeActions.ACTION_FAILED)
    return next_state

def handle_clone_pending(fs_client, volspec, volname, index, groupname, subvolname, should_cancel):
    try:
        if should_cancel():
            next_state = SubvolumeOpSm.transition(SubvolumeTypes.TYPE_CLONE,
                                                  SubvolumeStates.STATE_PENDING,
                                                  SubvolumeActions.ACTION_CANCELLED)
            update_clone_failure_status(fs_client, volspec, volname, groupname, subvolname,
                                        VolumeException(-errno.EINTR, "user interrupted clone operation"))
        else:
            next_state = SubvolumeOpSm.transition(SubvolumeTypes.TYPE_CLONE,
                                                  SubvolumeStates.STATE_PENDING,
                                                  SubvolumeActions.ACTION_SUCCESS)
    except OpSmException as oe:
        raise VolumeException(oe.errno, oe.error_str)
    return (next_state, False)

def sync_attrs(fs_handle, target_path, source_statx):
    try:
        fs_handle.lchown(target_path, source_statx["uid"], source_statx["gid"])
        fs_handle.lutimes(target_path, (time.mktime(source_statx["atime"].timetuple()),
                                        time.mktime(source_statx["mtime"].timetuple())))
        fs_handle.lchmod(target_path, source_statx["mode"])
    except cephfs.Error as e:
        log.warning("error synchronizing attrs for {0} ({1})".format(target_path, e))
        raise e

def bulk_copy(fs_handle, source_path, dst_path, should_cancel):
    """
    bulk copy data from source to destination -- only directories, symlinks
    and regular files are synced.
    """
    log.info("copying data from {0} to {1}".format(source_path, dst_path))
    def cptree(src_root_path, dst_root_path):
        log.debug("cptree: {0} -> {1}".format(src_root_path, dst_root_path))
        try:
            with fs_handle.opendir(src_root_path) as dir_handle:
                d = fs_handle.readdir(dir_handle)
                while d and not should_cancel():
                    if d.d_name not in (b".", b".."):
                        log.debug("d={0}".format(d))
                        d_full_src = os.path.join(src_root_path, d.d_name)
                        d_full_dst = os.path.join(dst_root_path, d.d_name)
                        stx = fs_handle.statx(d_full_src, cephfs.CEPH_STATX_MODE  |
                                                          cephfs.CEPH_STATX_UID   |
                                                          cephfs.CEPH_STATX_GID   |
                                                          cephfs.CEPH_STATX_ATIME |
                                                          cephfs.CEPH_STATX_MTIME |
                                                          cephfs.CEPH_STATX_SIZE,
                                                          cephfs.AT_SYMLINK_NOFOLLOW)
                        handled = True
                        mo = stx["mode"] & ~stat.S_IFMT(stx["mode"])
                        if stat.S_ISDIR(stx["mode"]):
                            log.debug("cptree: (DIR) {0}".format(d_full_src))
                            try:
                                fs_handle.mkdir(d_full_dst, mo)
                            except cephfs.Error as e:
                                if not e.args[0] == errno.EEXIST:
                                    raise
                            cptree(d_full_src, d_full_dst)
                        elif stat.S_ISLNK(stx["mode"]):
                            log.debug("cptree: (SYMLINK) {0}".format(d_full_src))
                            target = fs_handle.readlink(d_full_src, 4096)
                            try:
                                fs_handle.symlink(target[:stx["size"]], d_full_dst)
                            except cephfs.Error as e:
                                if not e.args[0] == errno.EEXIST:
                                    raise
                        elif stat.S_ISREG(stx["mode"]):
                            log.debug("cptree: (REG) {0}".format(d_full_src))
                            copy_file(fs_handle, d_full_src, d_full_dst, mo, cancel_check=should_cancel)
                        else:
                            handled = False
                            log.warning("cptree: (IGNORE) {0}".format(d_full_src))
                        if handled:
                            sync_attrs(fs_handle, d_full_dst, stx)
                    d = fs_handle.readdir(dir_handle)
                stx_root = fs_handle.statx(src_root_path, cephfs.CEPH_STATX_ATIME |
                                                          cephfs.CEPH_STATX_MTIME,
                                                          cephfs.AT_SYMLINK_NOFOLLOW)
                fs_handle.lutimes(dst_root_path, (time.mktime(stx_root["atime"].timetuple()),
                                                  time.mktime(stx_root["mtime"].timetuple())))
        except cephfs.Error as e:
            if not e.args[0] == errno.ENOENT:
                raise VolumeException(-e.args[0], e.args[1])
    cptree(source_path, dst_path)
    if should_cancel():
        raise VolumeException(-errno.EINTR, "user interrupted clone operation")

def set_quota_on_clone(fs_handle, clone_volumes_pair):
    src_path = clone_volumes_pair[1].snapshot_data_path(clone_volumes_pair[2])
    dst_path = clone_volumes_pair[0].path
    quota: Optional[int] = None
    try:
        quota = int(fs_handle.getxattr(src_path, 'ceph.quota.max_bytes').decode('utf-8'))
    except cephfs.NoData:
        pass

    if quota is not None:
        try:
            fs_handle.setxattr(dst_path, 'ceph.quota.max_bytes', str(quota).encode('utf-8'), 0)
        except cephfs.InvalidValue:
            raise VolumeException(-errno.EINVAL, "invalid size specified: '{0}'".format(quota))
        except cephfs.Error as e:
             raise VolumeException(-e.args[0], e.args[1])

    quota_files: Optional[int] = None
    try:
        quota_files = int(fs_handle.getxattr(src_path, 'ceph.quota.max_files').decode('utf-8'))
    except cephfs.NoData:
        pass

    if quota_files is not None:
        try:
            fs_handle.setxattr(dst_path, 'ceph.quota.max_files', str(quota_files).encode('utf-8'), 0)
        except cephfs.InvalidValue:
            raise VolumeException(-errno.EINVAL, "invalid file count specified: '{0}'".format(quota_files))
        except cephfs.Error as e:
             raise VolumeException(-e.args[0], e.args[1])

def do_clone(fs_client, volspec, volname, groupname, subvolname, should_cancel):
    with open_volume_lockless(fs_client, volname) as fs_handle:
        with open_clone_subvolume_pair(fs_client, fs_handle, volspec, volname,
                                       groupname, subvolname) \
            as (subvol0, subvol1, subvol2):
            src_path = subvol1.snapshot_data_path(subvol2)
            dst_path = subvol0.path
            # XXX: this is where cloning (of subvolume's snapshots) actually
            # happens.
            bulk_copy(fs_handle, src_path, dst_path, should_cancel)
            set_quota_on_clone(fs_handle, (subvol0, subvol1, subvol2))

def update_clone_failure_status(fs_client, volspec, volname, groupname, subvolname, ve):
    with open_volume_lockless(fs_client, volname) as fs_handle:
        with open_clone_subvolume_pair(fs_client, fs_handle, volspec, volname,
                                       groupname, subvolname) \
            as (subvol0, subvol1, subvol2) :
            if ve.errno == -errno.EINTR:
                subvol0.add_clone_failure(-ve.errno, "user interrupted clone operation")
            else:
                subvol0.add_clone_failure(-ve.errno, ve.error_str)

def log_clone_failure(volname, groupname, subvolname, ve):
    if ve.errno == -errno.EINTR:
        log.info("Clone cancelled: ({0}, {1}, {2})".format(volname, groupname, subvolname))
    elif ve.errno == -errno.EDQUOT:
        log.error("Clone failed: ({0}, {1}, {2}, reason -> Disk quota exceeded)".format(volname, groupname, subvolname))
    else:
        log.error("Clone failed: ({0}, {1}, {2}, reason -> {3})".format(volname, groupname, subvolname, ve))

def handle_clone_in_progress(fs_client, volspec, volname, index, groupname, subvolname, should_cancel):
    try:
        do_clone(fs_client, volspec, volname, groupname, subvolname, should_cancel)
        next_state = SubvolumeOpSm.transition(SubvolumeTypes.TYPE_CLONE,
                                              SubvolumeStates.STATE_INPROGRESS,
                                              SubvolumeActions.ACTION_SUCCESS)
    except VolumeException as ve:
        update_clone_failure_status(fs_client, volspec, volname, groupname, subvolname, ve)
        log_clone_failure(volname, groupname, subvolname, ve)
        next_state = get_next_state_on_error(ve.errno)
    except OpSmException as oe:
        raise VolumeException(oe.errno, oe.error_str)
    return (next_state, False)

def handle_clone_failed(fs_client, volspec, volname, index, groupname, subvolname, should_cancel):
    try:
        with open_volume(fs_client, volname) as fs_handle:
            # detach source but leave the clone section intact for later inspection
            with open_clone_subvolume_pair(fs_client, fs_handle, volspec,
                                           volname, groupname, subvolname) \
                as (subvol0, subvol1, subvol2):
                subvol1.detach_snapshot(subvol2, index)
    except (MetadataMgrException, VolumeException) as e:
        log.error("failed to detach clone from snapshot: {0}".format(e))
    return (None, True)

def handle_clone_complete(fs_client, volspec, volname, index, groupname, subvolname, should_cancel):
    try:
        with open_volume(fs_client, volname) as fs_handle:
            with open_clone_subvolume_pair(fs_client, fs_handle, volspec,
                                           volname, groupname, subvolname) \
                as (subvol0, subvol1, subvol2):
                subvol1.detach_snapshot(subvol2, index)
                subvol0.remove_clone_source(flush=True)
    except (MetadataMgrException, VolumeException) as e:
        log.error("failed to detach clone from snapshot: {0}".format(e))
    return (None, True)

def start_clone_sm(fs_client, volspec, volname, index, groupname, subvolname, state_table, should_cancel, snapshot_clone_delay):
    finished = False
    current_state = None
    try:
        current_state = get_clone_state(fs_client, volspec, volname, groupname, subvolname)
        log.debug("cloning ({0}, {1}, {2}) -- starting state \"{3}\"".format(volname, groupname, subvolname, current_state))
        if current_state == SubvolumeStates.STATE_PENDING:
            time.sleep(snapshot_clone_delay)
            log.info("Delayed cloning ({0}, {1}, {2}) -- by {3} seconds".format(volname, groupname, subvolname, snapshot_clone_delay))
        while not finished:
            # XXX: this is where request operation is mapped to relevant
            # function.
            handler = state_table.get(current_state, None)
            if not handler:
                raise VolumeException(-errno.EINVAL, "invalid clone state: \"{0}\"".format(current_state))
            # XXX: this is where the requested operation for subvolume's
            # snapshot clone is performed. the function for the request
            # operation is run through "handler".
            (next_state, finished) = handler(fs_client, volspec, volname, index, groupname, subvolname, should_cancel)
            if next_state:
                log.debug("({0}, {1}, {2}) transition state [\"{3}\" => \"{4}\"]".format(volname, groupname, subvolname,\
                                                                                         current_state, next_state))
                set_clone_state(fs_client, volspec, volname, groupname, subvolname, next_state)
                current_state = next_state
    except (MetadataMgrException, VolumeException) as e:
        log.error(f"clone failed for ({volname}, {groupname}, {subvolname}) "
                  f"(current_state: {current_state}, reason: {e} {os.strerror(-e.args[0])})")
        raise

def clone(fs_client, volspec, volname, index, clone_path, state_table, should_cancel, snapshot_clone_delay):
    log.info("cloning to subvolume path: {0}".format(clone_path))
    resolved = resolve(volspec, clone_path)

    groupname  = resolved[0]
    subvolname = resolved[1]
    log.debug("resolved to [group: {0}, subvolume: {1}]".format(groupname, subvolname))

    try:
        log.info("starting clone: ({0}, {1}, {2})".format(volname, groupname, subvolname))
        start_clone_sm(fs_client, volspec, volname, index, groupname, subvolname, state_table, should_cancel, snapshot_clone_delay)
        log.info("finished clone: ({0}, {1}, {2})".format(volname, groupname, subvolname))
    except (MetadataMgrException, VolumeException) as e:
        log.error(f"clone failed for ({volname}, {groupname}, {subvolname}), reason: {e} {os.strerror(-e.args[0])}")

class Cloner(AsyncJobs):
    """
    Asynchronous cloner: pool of threads to copy data from a snapshot to a subvolume.
    this relies on a simple state machine (which mimics states from SubvolumeOpSm class) as
    the driver. file types supported are directories, symbolic links and regular files.
    """
    def __init__(self, volume_client, tp_size, snapshot_clone_delay, clone_no_wait):
        self.vc = volume_client
        self.snapshot_clone_delay = snapshot_clone_delay
        self.snapshot_clone_no_wait = clone_no_wait
        self.state_table = {
            SubvolumeStates.STATE_PENDING      : handle_clone_pending,
            SubvolumeStates.STATE_INPROGRESS   : handle_clone_in_progress,
            SubvolumeStates.STATE_COMPLETE     : handle_clone_complete,
            SubvolumeStates.STATE_FAILED       : handle_clone_failed,
            SubvolumeStates.STATE_CANCELED     : handle_clone_failed,
        }
        super(Cloner, self).__init__(volume_client, "cloner", tp_size)

    def reconfigure_max_concurrent_clones(self, tp_size):
        return super(Cloner, self).reconfigure_max_async_threads(tp_size)

    def reconfigure_snapshot_clone_delay(self, timeout):
        self.snapshot_clone_delay = timeout

    def reconfigure_reject_clones(self, clone_no_wait):
        self.snapshot_clone_no_wait = clone_no_wait

    def is_clone_cancelable(self, clone_state):
        return not (SubvolumeOpSm.is_complete_state(clone_state) or SubvolumeOpSm.is_failed_state(clone_state))

    def get_clone_tracking_index(self, fs_handle, clone_subvolume):
        with open_clone_index(fs_handle, self.vc.volspec) as index:
            return index.find_clone_entry_index(clone_subvolume.base_path)

    def _cancel_pending_clone(self, fs_handle, clone_subvolume, clone_subvolname, clone_groupname, status, track_idx):
        clone_state = SubvolumeStates.from_value(status['state'])
        assert self.is_clone_cancelable(clone_state)

        s_groupname = status['source'].get('group', None)
        s_subvolname = status['source']['subvolume']
        s_snapname = status['source']['snapshot']

        with open_at_group_unique(self.fs_client, fs_handle, self.vc.volspec, s_groupname, s_subvolname, clone_subvolume,
                                  clone_groupname, clone_subvolname, SubvolumeOpType.CLONE_SOURCE) as s_subvolume:
            next_state = SubvolumeOpSm.transition(SubvolumeTypes.TYPE_CLONE,
                                                  clone_state,
                                                  SubvolumeActions.ACTION_CANCELLED)
            clone_subvolume.state = (next_state, True)
            clone_subvolume.add_clone_failure(errno.EINTR, "user interrupted clone operation")
            s_subvolume.detach_snapshot(s_snapname, track_idx.decode('utf-8'))

    def cancel_job(self, volname, job):
        """
        override base class `cancel_job`. interpret @job as (clone, group) tuple.
        """
        clonename = job[0]
        groupname = job[1]
        track_idx = None

        try:
            with open_volume(self.fs_client, volname) as fs_handle:
                with open_group(fs_handle, self.vc.volspec, groupname) as group:
                    with open_subvol(self.fs_client.mgr, fs_handle, self.vc.volspec, group, clonename, SubvolumeOpType.CLONE_CANCEL) as clone_subvolume:
                        status = clone_subvolume.status
                        clone_state = SubvolumeStates.from_value(status['state'])
                        if not self.is_clone_cancelable(clone_state):
                            raise VolumeException(-errno.EINVAL, "cannot cancel -- clone finished (check clone status)")
                        track_idx = self.get_clone_tracking_index(fs_handle, clone_subvolume)
                        if not track_idx:
                            log.warning("cannot lookup clone tracking index for {0}".format(clone_subvolume.base_path))
                            raise VolumeException(-errno.EINVAL, "error canceling clone")
                        clone_job = (track_idx, clone_subvolume.base_path)
                        jobs = [j[0] for j in self.jobs[volname]]
                        with lock_timeout_log(self.lock):
                            if SubvolumeOpSm.is_init_state(SubvolumeTypes.TYPE_CLONE, clone_state) and not clone_job in jobs:
                                logging.debug("Cancelling pending job {0}".format(clone_job))
                                # clone has not started yet -- cancel right away.
                                self._cancel_pending_clone(fs_handle, clone_subvolume, clonename, groupname, status, track_idx)
                                return
            # cancelling an on-going clone would persist "canceled" state in subvolume metadata.
            # to persist the new state, async cloner accesses the volume in exclusive mode.
            # accessing the volume in exclusive mode here would lead to deadlock.
            assert track_idx is not None
            with lock_timeout_log(self.lock):
                with open_volume_lockless(self.fs_client, volname) as fs_handle:
                    with open_group(fs_handle, self.vc.volspec, groupname) as group:
                        with open_subvol(self.fs_client.mgr, fs_handle, self.vc.volspec, group, clonename, SubvolumeOpType.CLONE_CANCEL) as clone_subvolume:
                            if not self._cancel_job(volname, (track_idx, clone_subvolume.base_path)):
                                raise VolumeException(-errno.EINVAL, "cannot cancel -- clone finished (check clone status)")
        except (IndexException, MetadataMgrException) as e:
            log.error("error cancelling clone {0}: ({1})".format(job, e))
            raise VolumeException(-errno.EINVAL, "error canceling clone")

    def get_next_job(self, volname, running_jobs):
        return get_next_clone_entry(self.fs_client, self.vc.volspec, volname, running_jobs)

    def execute_job(self, volname, job, should_cancel):
        clone(self.fs_client, self.vc.volspec, volname, job[0].decode('utf-8'), job[1].decode('utf-8'), self.state_table, should_cancel, self.snapshot_clone_delay)
