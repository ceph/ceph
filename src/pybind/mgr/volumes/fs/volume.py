import json
import errno
import logging

import cephfs

from mgr_util import CephfsClient

from .fs_util import listdir

from .operations.volume import create_volume, \
    delete_volume, list_volumes, open_volume
from .operations.group import open_group, create_group, remove_group
from .operations.subvolume import open_subvol, create_subvol, remove_subvol, \
    create_clone

from .vol_spec import VolSpec
from .exception import VolumeException
from .async_cloner import Cloner
from .purge_queue import ThreadPoolPurgeQueueMixin

log = logging.getLogger(__name__)


def octal_str_to_decimal_int(mode):
    try:
        return int(mode, 8)
    except ValueError:
        raise VolumeException(-errno.EINVAL, "Invalid mode '{0}'".format(mode))


def name_to_json(names):
    """
    convert the list of names to json
    """
    namedict = []
    for i in range(len(names)):
        namedict.append({'name': names[i].decode('utf-8')})
    return json.dumps(namedict, indent=4, sort_keys=True)


class VolumeClient(CephfsClient):
    def __init__(self, mgr):
        super().__init__(mgr)
        # volume specification
        self.volspec = VolSpec(mgr.rados.conf_get('client_snapdir'))
        # TODO: make thread pool size configurable
        self.cloner = Cloner(self, 4)
        self.purge_queue = ThreadPoolPurgeQueueMixin(self, 4)
        # on startup, queue purge job for available volumes to kickstart
        # purge for leftover subvolume entries in trash. note that, if the
        # trash directory does not exist or if there are no purge entries
        # available for a volume, the volume is removed from the purge
        # job list.
        fs_map = self.mgr.get('fs_map')
        for fs in fs_map['filesystems']:
            self.cloner.queue_job(fs['mdsmap']['fs_name'])
            self.purge_queue.queue_job(fs['mdsmap']['fs_name'])

    def shutdown(self):
        # Overrides CephfsClient.shutdown()
        log.info("shutting down")
        # first, note that we're shutting down
        self.stopping.set()
        # second, ask purge threads to quit
        self.purge_queue.cancel_all_jobs()
        # third, delete all libcephfs handles from connection pool
        self.connection_pool.del_all_handles()

    def cluster_log(self, msg, lvl=None):
        """
        log to cluster log with default log level as WARN.
        """
        if not lvl:
            lvl = self.mgr.CLUSTER_LOG_PRIO_WARN
        self.mgr.cluster_log("cluster", lvl, msg)

    def volume_exception_to_retval(self, ve):
        """
        return a tuple representation from a volume exception
        """
        return ve.to_tuple()

    ### volume operations -- create, rm, ls

    def create_fs_volume(self, volname, placement):
        if self.is_stopping():
            return -errno.ESHUTDOWN, "", "shutdown in progress"
        return create_volume(self.mgr, volname, placement)

    def delete_fs_volume(self, volname, confirm):
        if self.is_stopping():
            return -errno.ESHUTDOWN, "", "shutdown in progress"

        if confirm != "--yes-i-really-mean-it":
            return -errno.EPERM, "", "WARNING: this will *PERMANENTLY DESTROY* all data " \
                "stored in the filesystem '{0}'. If you are *ABSOLUTELY CERTAIN* " \
                "that is what you want, re-issue the command followed by " \
                "--yes-i-really-mean-it.".format(volname)

        self.purge_queue.cancel_jobs(volname)
        self.connection_pool.del_fs_handle(volname, wait=True)
        return delete_volume(self.mgr, volname)

    def list_fs_volumes(self):
        if self.stopping.is_set():
            return -errno.ESHUTDOWN, "", "shutdown in progress"
        volumes = list_volumes(self.mgr)
        return 0, json.dumps(volumes, indent=4, sort_keys=True), ""

    ### subvolume operations

    def _create_subvolume(self, fs_handle, volname, group, subvolname, **kwargs):
        size       = kwargs['size']
        pool       = kwargs['pool_layout']
        uid        = kwargs['uid']
        gid        = kwargs['gid']
        mode       = kwargs['mode']
        isolate_nspace = kwargs['namespace_isolated']

        oct_mode = octal_str_to_decimal_int(mode)
        try:
            create_subvol(
                fs_handle, self.volspec, group, subvolname, size, isolate_nspace, pool, oct_mode, uid, gid)
        except VolumeException as ve:
            # kick the purge threads for async removal -- note that this
            # assumes that the subvolume is moved to trashcan for cleanup on error.
            self.purge_queue.queue_job(volname)
            raise ve

    def create_subvolume(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        size       = kwargs['size']
        pool       = kwargs['pool_layout']
        uid        = kwargs['uid']
        gid        = kwargs['gid']
        isolate_nspace = kwargs['namespace_isolated']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    try:
                        with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                            # idempotent creation -- valid. Attributes set is supported.
                            uid = uid if uid else subvolume.uid
                            gid = gid if gid else subvolume.gid
                            subvolume.set_attrs(subvolume.path, size, isolate_nspace, pool, uid, gid)
                    except VolumeException as ve:
                        if ve.errno == -errno.ENOENT:
                            self._create_subvolume(fs_handle, volname, group, subvolname, **kwargs)
                        else:
                            raise
        except VolumeException as ve:
            # volume/group does not exist or subvolume creation failed
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        force      = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    remove_subvol(fs_handle, self.volspec, group, subvolname, force)
                    # kick the purge threads for async removal -- note that this
                    # assumes that the subvolume is moved to trash can.
                    # TODO: make purge queue as singleton so that trash can kicks
                    # the purge threads on dump.
                    self.purge_queue.queue_job(volname)
        except VolumeException as ve:
            if ve.errno == -errno.EAGAIN:
                ve = VolumeException(ve.errno, ve.error_str + " (use --force to override)")
                ret = self.volume_exception_to_retval(ve)
            elif not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def resize_subvolume(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        newsize    = kwargs['new_size']
        noshrink   = kwargs['no_shrink']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        nsize, usedbytes = subvolume.resize(newsize, noshrink)
                        ret = 0, json.dumps(
                            [{'bytes_used': usedbytes},{'bytes_quota': nsize},
                             {'bytes_pcent': "undefined" if nsize == 0 else '{0:.2f}'.format((float(usedbytes) / nsize) * 100.0)}],
                            indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def subvolume_getpath(self, **kwargs):
        ret        = None
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        subvolpath = subvolume.path
                        ret = 0, subvolpath.decode("utf-8"), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def subvolume_info(self, **kwargs):
        ret        = None
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        mon_addr_lst = []
                        mon_map_mons = self.mgr.get('mon_map')['mons']
                        for mon in mon_map_mons:
                            ip_port = mon['addr'].split("/")[0]
                            mon_addr_lst.append(ip_port)

                        subvol_info_dict = subvolume.info()
                        subvol_info_dict["mon_addrs"] = mon_addr_lst
                        ret = 0, json.dumps(subvol_info_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolumes(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    subvolumes = group.list_subvolumes()
                    ret = 0, name_to_json(subvolumes), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    ### subvolume snapshot

    def create_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        subvolume.create_snapshot(snapname)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']
        force      = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        subvolume.remove_snapshot(snapname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def subvolume_snapshot_info(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        snap_info_dict = subvolume.snapshot_info(snapname)
                        ret = 0, json.dumps(snap_info_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
                ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolume_snapshots(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        snapshots = subvolume.list_snapshots()
                        ret = 0, name_to_json(snapshots), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def protect_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        subvolume.protect_snapshot(snapname)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def unprotect_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        subvolume.unprotect_snapshot(snapname)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def _prepare_clone_subvolume(self, fs_handle, volname, subvolume, snapname, target_group, target_subvolname, target_pool):
        create_clone(fs_handle, self.volspec, target_group, target_subvolname, target_pool, volname, subvolume, snapname)
        with open_subvol(fs_handle, self.volspec, target_group, target_subvolname, need_complete=False) as target_subvolume:
            try:
                subvolume.attach_snapshot(snapname, target_subvolume)
                self.cloner.queue_job(volname)
            except VolumeException as ve:
                try:
                    target_subvolume.remove()
                    self.purge_queue.queue_job(volname)
                except Exception as e:
                    log.warning("failed to cleanup clone subvolume '{0}' ({1})".format(target_subvolname, e))
                raise ve

    def _clone_subvolume_snapshot(self, fs_handle, volname, subvolume, **kwargs):
        snapname          = kwargs['snap_name']
        target_pool       = kwargs['pool_layout']
        target_subvolname = kwargs['target_sub_name']
        target_groupname  = kwargs['target_group_name']

        if not snapname.encode('utf-8') in subvolume.list_snapshots():
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
        if not subvolume.is_snapshot_protected(snapname):
            raise VolumeException(-errno.EINVAL, "snapshot '{0}' is not protected".format(snapname))

        # TODO: when the target group is same as source, reuse group object.
        with open_group(fs_handle, self.volspec, target_groupname) as target_group:
            try:
                with open_subvol(fs_handle, self.volspec, target_group, target_subvolname, need_complete=False):
                    raise VolumeException(-errno.EEXIST, "subvolume '{0}' exists".format(target_subvolname))
            except VolumeException as ve:
                if ve.errno == -errno.ENOENT:
                    self._prepare_clone_subvolume(fs_handle, volname, subvolume, snapname,
                                                  target_group, target_subvolname, target_pool)
                else:
                    raise

    def clone_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, subvolname) as subvolume:
                        self._clone_subvolume_snapshot(fs_handle, volname, subvolume, **kwargs)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def clone_status(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        clonename = kwargs['clone_name']
        groupname = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(fs_handle, self.volspec, group, clonename,
                                     need_complete=False, expected_types=["clone"]) as subvolume:
                        ret = 0, json.dumps({'status' : subvolume.status}, indent=2), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def clone_cancel(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        clonename = kwargs['clone_name']
        groupname = kwargs['group_name']

        try:
            self.cloner.cancel_job(volname, (clonename, groupname))
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    ### group operations

    def create_subvolume_group(self, **kwargs):
        ret       = 0, "", ""
        volname    = kwargs['vol_name']
        groupname = kwargs['group_name']
        pool      = kwargs['pool_layout']
        uid       = kwargs['uid']
        gid       = kwargs['gid']
        mode      = kwargs['mode']

        try:
            with open_volume(self, volname) as fs_handle:
                try:
                    with open_group(fs_handle, self.volspec, groupname):
                        # idempotent creation -- valid.
                        pass
                except VolumeException as ve:
                    if ve.errno == -errno.ENOENT:
                        oct_mode = octal_str_to_decimal_int(mode)
                        create_group(fs_handle, self.volspec, groupname, pool, oct_mode, uid, gid)
                    else:
                        raise
        except VolumeException as ve:
            # volume does not exist or subvolume group creation failed
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_group(self, **kwargs):
        ret       = 0, "", ""
        volname    = kwargs['vol_name']
        groupname = kwargs['group_name']
        force     = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                remove_group(fs_handle, self.volspec, groupname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def getpath_subvolume_group(self, **kwargs):
        volname    = kwargs['vol_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    return 0, group.path.decode('utf-8'), ""
        except VolumeException as ve:
            return self.volume_exception_to_retval(ve)

    def list_subvolume_groups(self, **kwargs):
        volname = kwargs['vol_name']
        ret     = 0, '[]', ""
        try:
            with open_volume(self, volname) as fs_handle:
                groups = listdir(fs_handle, self.volspec.base_dir)
                ret = 0, name_to_json(groups), ""
        except VolumeException as ve:
            if not ve.errno == -errno.ENOENT:
                ret = self.volume_exception_to_retval(ve)
        return ret

    ### group snapshot

    def create_subvolume_group_snapshot(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        snapname  = kwargs['snap_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    group.create_snapshot(snapname)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_group_snapshot(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        snapname  = kwargs['snap_name']
        force     = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    group.remove_snapshot(snapname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolume_group_snapshots(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    snapshots = group.list_snapshots()
                    ret = 0, name_to_json(snapshots), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret
