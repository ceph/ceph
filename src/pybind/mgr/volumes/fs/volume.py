import json
import errno
import logging
import mgr_util
import inspect
import functools
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import cephfs

from ceph.fs.earmarking import CephFSVolumeEarmarking, EarmarkException

from mgr_util import CephfsClient

from .fs_util import listdir, has_subdir
from .stats_util import get_stats

from .operations.group import open_group, create_group, remove_group, \
    open_group_unique, set_group_attrs
from .operations.volume import create_volume, delete_volume, rename_volume, \
    list_volumes, open_volume, get_pool_names, get_pool_ids, \
    get_pending_subvol_deletions_count, get_all_pending_clones_count
from .operations.subvolume import open_subvol, create_subvol, remove_subvol, \
    create_clone, open_subvol_in_group

from .vol_spec import VolSpec
from .exception import VolumeException, ClusterError, ClusterTimeout, \
    EvictionError
from .async_cloner import Cloner
from .purge_queue import ThreadPoolPurgeQueueMixin
from .operations.template import SubvolumeOpType
from .stats_util import CloneProgressReporter

if TYPE_CHECKING:
    from volumes import Module

log = logging.getLogger(__name__)

ALLOWED_ACCESS_LEVELS = ('r', 'rw')


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


class VolumeClient(CephfsClient["Module"]):
    def __init__(self, mgr):
        super().__init__(mgr)
        # volume specification
        self.volspec = VolSpec(mgr.rados.conf_get('client_snapdir'))
        self.cloner = Cloner(self, self.mgr.max_concurrent_clones, self.mgr.snapshot_clone_delay,
                             self.mgr.snapshot_clone_no_wait)
        self.clone_progress_reporter = CloneProgressReporter(self,
                                                             self.volspec)
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
        # stop clones
        self.cloner.shutdown()
        # stop purge threads
        self.purge_queue.shutdown()
        # last, delete all libcephfs handles from connection pool
        self.connection_pool.del_all_connections()

    def cluster_log(self, msg, lvl=None):
        """
        log to cluster log with default log level as WARN.
        """
        if not lvl:
            lvl = self.mgr.ClusterLogPrio.WARN
        self.mgr.cluster_log("cluster", lvl, msg)

    def volume_exception_to_retval(self_or_method: Any, ve: Optional[VolumeException] = None):
        """
        return a tuple representation from a volume exception
        OR wrap the decorated method into a try:catch:
        that will convert VolumeException to the tuple
        """
        if ve is None and callable(self_or_method):
            # used as a decorator
            method: Callable = self_or_method
            @functools.wraps(method)
            def wrapper(self, *args, **kwargs):
                try:
                    return method(self, *args, **kwargs)
                except VolumeException as ve:
                    return self.volume_exception_to_retval(ve)
            return wrapper
        elif ve is not None:
            # used as a method on self with a VolumeException argument
            return ve.to_tuple()
        else:
            # shouldn't get here, bad call
            assert(ve is not None)

    ### volume operations -- create, rm, ls

    def create_fs_volume(self, volname, placement):
        return create_volume(self.mgr, volname, placement)

    def delete_fs_volume(self, volname, confirm):
        if confirm != "--yes-i-really-mean-it":
            return -errno.EPERM, "", "WARNING: this will *PERMANENTLY DESTROY* all data " \
                "stored in the filesystem '{0}'. If you are *ABSOLUTELY CERTAIN* " \
                "that is what you want, re-issue the command followed by " \
                "--yes-i-really-mean-it.".format(volname)

        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config get',
            'key': 'mon_allow_pool_delete',
            'who': 'mon',
            'format': 'json',
        })
        mon_allow_pool_delete = json.loads(out)
        if not mon_allow_pool_delete:
            return -errno.EPERM, "", "pool deletion is disabled; you must first " \
                "set the mon_allow_pool_delete config option to true before volumes " \
                "can be deleted"

        metadata_pool, data_pools = get_pool_names(self.mgr, volname)
        if not metadata_pool:
            return -errno.ENOENT, "", "volume {0} doesn't exist".format(volname)
        self.purge_queue.cancel_jobs(volname)
        self.connection_pool.del_connections(volname, wait=True)
        return delete_volume(self.mgr, volname, metadata_pool, data_pools)

    def list_fs_volumes(self):
        volnames = list_volumes(self.mgr)
        # since we report in json format, make a dict of volnames.
        volumes = [{'name': vn} for vn in volnames]
        return 0, json.dumps(volumes, indent=4, sort_keys=True), ""

    def rename_fs_volume(self, volname, newvolname, sure):
        if not sure:
            return (
                -errno.EPERM, "",
                "WARNING: This will rename the filesystem and possibly its "
                "pools. It is a potentially disruptive operation, clients' "
                "cephx credentials need reauthorized to access the file system "
                "and its pools with the new name. Add --yes-i-really-mean-it "
                "if you are sure you wish to continue.")

        return rename_volume(self.mgr, volname, newvolname)

    def volume_info(self, **kwargs):
        ret     = None
        volname = kwargs['vol_name']
        human_readable    = kwargs['human_readable']

        try:
            with open_volume(self, volname) as fs_handle:
                path = self.volspec.base_dir
                vol_info_dict = {}
                try:
                    st = fs_handle.statx(path.encode('utf-8'), cephfs.CEPH_STATX_SIZE,
                                         cephfs.AT_SYMLINK_NOFOLLOW)

                    usedbytes = st['size']
                    vol_info_dict = get_pending_subvol_deletions_count(fs_handle, path)
                    if human_readable:
                        vol_info_dict['used_size'] = mgr_util.format_bytes(int(usedbytes), 5)
                    else:
                        vol_info_dict['used_size'] = int(usedbytes)
                except cephfs.Error as e:
                    if e.args[0] == errno.ENOENT:
                        pass
                df = self.mgr.get("df")
                pool_stats = dict([(p['id'], p['stats']) for p in df['pools']])
                osdmap = self.mgr.get("osd_map")
                pools = dict([(p['pool'], p) for p in osdmap['pools']])
                metadata_pool_id, data_pool_ids = get_pool_ids(self.mgr, volname)
                vol_info_dict["pools"] = {"metadata": [], "data": []}
                for pool_id in [metadata_pool_id] + data_pool_ids:
                    if pool_id == metadata_pool_id:
                        pool_type = "metadata"
                    else:
                        pool_type = "data"
                    if human_readable:
                        vol_info_dict["pools"][pool_type].append({
                                        'name': pools[pool_id]['pool_name'],
                                        'used': mgr_util.format_bytes(pool_stats[pool_id]['bytes_used'], 5),
                                        'avail': mgr_util.format_bytes(pool_stats[pool_id]['max_avail'], 5)})
                    else:
                        vol_info_dict["pools"][pool_type].append({
                                        'name': pools[pool_id]['pool_name'],
                                        'used': pool_stats[pool_id]['bytes_used'],
                                        'avail': pool_stats[pool_id]['max_avail']})

                mon_addr_lst = []
                mon_map_mons = self.mgr.get('mon_map')['mons']
                for mon in mon_map_mons:
                    ip_port = mon['addr'].split("/")[0]
                    mon_addr_lst.append(ip_port)
                vol_info_dict["mon_addrs"] = mon_addr_lst
                ret = 0, json.dumps(vol_info_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    ### subvolume operations

    def _create_subvolume(self, fs_handle, volname, group, subvolname, **kwargs):
        size       = kwargs['size']
        pool       = kwargs['pool_layout']
        uid        = kwargs['uid']
        gid        = kwargs['gid']
        mode       = kwargs['mode']
        isolate_nspace = kwargs['namespace_isolated']
        earmark    = kwargs['earmark'] or ''  # if not set, default to empty string --> no earmark

        oct_mode = octal_str_to_decimal_int(mode)

        try:
            create_subvol(
                self.mgr, fs_handle, self.volspec, group, subvolname, size, isolate_nspace, pool, oct_mode, uid, gid, earmark)
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
        mode       = kwargs['mode']
        isolate_nspace = kwargs['namespace_isolated']
        earmark    = kwargs['earmark'] or ''  # if not set, default to empty string --> no earmark

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    try:
                        with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.CREATE) as subvolume:
                            # idempotent creation -- valid. Attributes set is supported.
                            attrs = {
                                'uid': uid if uid else subvolume.uid,
                                'gid': gid if gid else subvolume.gid,
                                'mode': octal_str_to_decimal_int(mode),
                                'data_pool': pool,
                                'pool_namespace': subvolume.namespace if isolate_nspace else None,
                                'quota': size,
                                'earmark': earmark
                            }
                            subvolume.set_attrs(subvolume.path, attrs)
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
        ret         = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        groupname   = kwargs['group_name']
        force       = kwargs['force']
        retainsnaps = kwargs['retain_snapshots']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    remove_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, force, retainsnaps)
                    # kick the purge threads for async removal -- note that this
                    # assumes that the subvolume is moved to trash can.
                    # TODO: make purge queue as singleton so that trash can kicks
                    # the purge threads on dump.
                    self.purge_queue.queue_job(volname)
        except VolumeException as ve:
            if ve.errno == -errno.EAGAIN and not force:
                ve = VolumeException(ve.errno, ve.error_str + " (use --force to override)")
                ret = self.volume_exception_to_retval(ve)
            elif not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def authorize_subvolume(self, **kwargs):
        ret = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        authid      = kwargs['auth_id']
        groupname   = kwargs['group_name']
        accesslevel = kwargs['access_level']
        tenant_id   = kwargs['tenant_id']
        allow_existing_id = kwargs['allow_existing_id']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.ALLOW_ACCESS) as subvolume:
                        key = subvolume.authorize(authid, accesslevel, tenant_id, allow_existing_id)
                        ret = 0, key, ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def deauthorize_subvolume(self, **kwargs):
        ret = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        authid      = kwargs['auth_id']
        groupname   = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.DENY_ACCESS) as subvolume:
                        subvolume.deauthorize(authid)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def authorized_list(self, **kwargs):
        ret = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        groupname   = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.AUTH_LIST) as subvolume:
                        auths = subvolume.authorized_list()
                        ret = 0, json.dumps(auths, indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def evict(self, **kwargs):
        ret = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        authid      = kwargs['auth_id']
        groupname   = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.EVICT) as subvolume:
                        subvolume.evict(volname, authid)
                        ret = 0, "", ""
        except (VolumeException, ClusterTimeout, ClusterError, EvictionError) as e:
            if isinstance(e, VolumeException):
                ret = self.volume_exception_to_retval(e)
            elif isinstance(e, ClusterTimeout):
                ret = -errno.ETIMEDOUT , "", "Timedout trying to talk to ceph cluster"
            elif isinstance(e, ClusterError):
                ret = e._result_code , "", e._result_str
            elif isinstance(e, EvictionError):
                ret = -errno.EINVAL, "", str(e)
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
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.RESIZE) as subvolume:
                        nsize, usedbytes = subvolume.resize(newsize, noshrink)
                        ret = 0, json.dumps(
                            [{'bytes_used': usedbytes},{'bytes_quota': nsize},
                             {'bytes_pcent': "undefined" if nsize == 0 else '{0:.2f}'.format((float(usedbytes) / nsize) * 100.0)}],
                            indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def subvolume_pin(self, **kwargs):
        ret         = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        pin_type    = kwargs['pin_type']
        pin_setting = kwargs['pin_setting']
        groupname   = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.PIN) as subvolume:
                        subvolume.pin(pin_type, pin_setting)
                        ret = 0, json.dumps({}), ""
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
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.GETPATH) as subvolume:
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
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.INFO) as subvolume:
                        mon_addr_lst = []
                        mon_map_mons = self.mgr.get('mon_map')['mons']
                        for mon in mon_map_mons:
                            ip_port = mon['addr'].split("/")[0]
                            mon_addr_lst.append(ip_port)

                        subvol_info_dict = subvolume.info()
                        subvol_info_dict["mon_addrs"] = mon_addr_lst
                        subvol_info_dict["flavor"] = subvolume.VERSION
                        ret = 0, json.dumps(subvol_info_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @volume_exception_to_retval
    def quiesce(self, cmd):
        volname    = cmd['vol_name']
        default_group_name  = cmd.get('group_name', None)
        roots = []
        leader_gid = cmd.get('with_leader', None)

        with open_volume(self, volname) as fs_handle:
            if leader_gid is None:
                fscid = fs_handle.get_fscid()
                leader_gid = self.mgr.get_quiesce_leader_gid(fscid)
                if leader_gid is None:
                    return -errno.ENOENT, "", "Couldn't resolve the quiesce leader for volume %s (%s)" % (volname, fscid)

            if cmd.get('leader', False):
                return (
                    0,
                    "mds.%d" % leader_gid,
                    "Resolved the quiesce leader for volume '{volname}' as gid {gid}".format(volname=volname, gid=leader_gid)
                )


            for member in cmd.get('members', []):
                try:
                    member_parts = urlsplit(member)
                except ValueError as ve:
                    return -errno.EINVAL, "", str(ve)
                group_name = default_group_name

                *maybe_group_name, subvol_name = member_parts.path.strip('/').split('/')
                if len(maybe_group_name) > 1:
                    return -errno.EINVAL, "", "The `<group>/<subvol>` member syntax is accepted with no more than one group"
                elif len(maybe_group_name) == 1:
                    group_name = maybe_group_name[0]

                with open_group(fs_handle, self.volspec, group_name) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvol_name, SubvolumeOpType.GETPATH) as subvol:
                        member_parts = member_parts._replace(path=subvol.path.decode('utf-8'))
                        roots.append(urlunsplit(member_parts))
        
        cmd['roots'] = roots
        cmd['prefix'] = 'quiesce db'

        return self.mgr.tell_quiesce_leader(leader_gid, cmd)

    def set_user_metadata(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        keyname    = kwargs['key_name']
        value      = kwargs['value']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.USER_METADATA_SET) as subvolume:
                        subvolume.set_user_metadata(keyname, value)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def get_user_metadata(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        keyname    = kwargs['key_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.USER_METADATA_GET) as subvolume:
                        value = subvolume.get_user_metadata(keyname)
                        ret = 0, value, ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def list_user_metadata(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.USER_METADATA_LIST) as subvolume:
                        subvol_metadata_dict = subvolume.list_user_metadata()
                        ret = 0, json.dumps(subvol_metadata_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_user_metadata(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        keyname    = kwargs['key_name']
        force      = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.USER_METADATA_REMOVE) as subvolume:
                        subvolume.remove_user_metadata(keyname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
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

    def subvolume_exists(self, **kwargs):
        volname = kwargs['vol_name']
        groupname = kwargs['group_name']
        ret = 0, "", ""
        volume_exists = False

        try:
            with open_volume(self, volname) as fs_handle:
                volume_exists = True
                with open_group(fs_handle, self.volspec, groupname) as group:
                    res = group.has_subvolumes()
                    if res:
                        ret = 0, "subvolume exists", ""
                    else:
                        ret = 0, "no subvolume exists", ""
        except VolumeException as ve:
            if volume_exists and ve.errno == -errno.ENOENT:
                ret = 0, "no subvolume exists", ""
            else:
                ret = self.volume_exception_to_retval(ve)
        return ret

    def get_earmark(self, **kwargs) -> Tuple[int, Optional[str], str]:
        ret: Tuple[int, Optional[str], str] = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.EARMARK_GET) as subvolume:
                        log.info("Getting earmark for subvolume %s", subvolume.path)
                        fs_earmark = CephFSVolumeEarmarking(fs_handle, subvolume.path)
                        earmark = fs_earmark.get_earmark()
                        ret = 0, earmark, ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        except EarmarkException as ee:
            log.error(f"Earmark error occurred: {ee}")
            ret = ee.to_tuple()
        return ret

    def set_earmark(self, **kwargs):  # type: ignore
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname = kwargs['group_name']
        earmark   = kwargs['earmark']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.EARMARK_SET) as subvolume:
                        log.info("Setting earmark %s for subvolume %s", earmark, subvolume.path)
                        fs_earmark = CephFSVolumeEarmarking(fs_handle, subvolume.path)
                        fs_earmark.set_earmark(earmark)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        except EarmarkException as ee:
            log.error(f"Earmark error occurred: {ee}")
            ret = ee.to_tuple()  # type: ignore
        return ret

    def clear_earmark(self, **kwargs):  # type: ignore
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.EARMARK_CLEAR) as subvolume:
                        log.info("Removing earmark for subvolume %s", subvolume.path)
                        fs_earmark = CephFSVolumeEarmarking(fs_handle, subvolume.path)
                        fs_earmark.clear_earmark()
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        except EarmarkException as ee:
            log.error(f"Earmark error occurred: {ee}")
            ret = ee.to_tuple()  # type: ignore
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
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_CREATE) as subvolume:
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
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_REMOVE) as subvolume:
                        subvolume.remove_snapshot(snapname, force)
        except VolumeException as ve:
            # ESTALE serves as an error to state that subvolume is currently stale due to internal removal and,
            # we should tickle the purge jobs to purge the same
            if ve.errno == -errno.ESTALE:
                self.purge_queue.queue_job(volname)
            elif not (ve.errno == -errno.ENOENT and force):
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
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_INFO) as subvolume:
                        snap_info_dict = subvolume.snapshot_info(snapname)
                        ret = 0, json.dumps(snap_info_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
                ret = self.volume_exception_to_retval(ve)
        return ret

    def set_subvolume_snapshot_metadata(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']
        keyname    = kwargs['key_name']
        value      = kwargs['value']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_METADATA_SET) as subvolume:
                        if not snapname.encode('utf-8') in subvolume.list_snapshots():
                            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
                        subvolume.set_snapshot_metadata(snapname, keyname, value)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def get_subvolume_snapshot_metadata(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']
        keyname    = kwargs['key_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_METADATA_GET) as subvolume:
                        if not snapname.encode('utf-8') in subvolume.list_snapshots():
                            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
                        value = subvolume.get_snapshot_metadata(snapname, keyname)
                        ret = 0, value, ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolume_snapshot_metadata(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_METADATA_LIST) as subvolume:
                        if not snapname.encode('utf-8') in subvolume.list_snapshots():
                            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
                        snap_metadata_dict = subvolume.list_snapshot_metadata(snapname)
                        ret = 0, json.dumps(snap_metadata_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_snapshot_metadata(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']
        keyname    = kwargs['key_name']
        force      = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_METADATA_REMOVE) as subvolume:
                        if not snapname.encode('utf-8') in subvolume.list_snapshots():
                            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
                        subvolume.remove_snapshot_metadata(snapname, keyname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
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
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_LIST) as subvolume:
                        snapshots = subvolume.list_snapshots()
                        ret = 0, name_to_json(snapshots), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def protect_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", "Deprecation warning: 'snapshot protect' call is deprecated and will be removed in a future release"
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_PROTECT):
                        log.warning("snapshot protect call is deprecated and will be removed in a future release")
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def unprotect_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", "Deprecation warning: 'snapshot unprotect' call is deprecated and will be removed in a future release"
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_UNPROTECT):
                        log.warning("snapshot unprotect call is deprecated and will be removed in a future release")
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def _prepare_clone_subvolume(self, fs_handle, volname, s_subvolume, s_snapname, t_group, t_subvolname, **kwargs):
        t_pool              = kwargs['pool_layout']
        s_subvolname        = kwargs['sub_name']
        s_groupname         = kwargs['group_name']
        t_groupname         = kwargs['target_group_name']

        create_clone(self.mgr, fs_handle, self.volspec, t_group, t_subvolname, t_pool, volname, s_subvolume, s_snapname)
        with open_subvol(self.mgr, fs_handle, self.volspec, t_group, t_subvolname, SubvolumeOpType.CLONE_INTERNAL) as t_subvolume:
            try:
                if t_groupname == s_groupname and t_subvolname == s_subvolname:
                    t_subvolume.attach_snapshot(s_snapname, t_subvolume)
                else:
                    s_subvolume.attach_snapshot(s_snapname, t_subvolume)
                self.cloner.queue_job(volname)
                self.clone_progress_reporter.initiate_reporting()
            except VolumeException as ve:
                try:
                    t_subvolume.remove()
                    self.purge_queue.queue_job(volname)
                except Exception as e:
                    log.warning("failed to cleanup clone subvolume '{0}' ({1})".format(t_subvolname, e))
                raise ve

    def _clone_subvolume_snapshot(self, fs_handle, volname, s_group, s_subvolume, **kwargs):
        s_snapname          = kwargs['snap_name']
        target_subvolname   = kwargs['target_sub_name']
        target_groupname    = kwargs['target_group_name']
        s_groupname         = kwargs['group_name']

        if not s_snapname.encode('utf-8') in s_subvolume.list_snapshots():
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(s_snapname))

        with open_group_unique(fs_handle, self.volspec, target_groupname, s_group, s_groupname) as target_group:
            try:
                with open_subvol(self.mgr, fs_handle, self.volspec, target_group, target_subvolname, SubvolumeOpType.CLONE_CREATE):
                    raise VolumeException(-errno.EEXIST, "subvolume '{0}' exists".format(target_subvolname))
            except VolumeException as ve:
                if ve.errno == -errno.ENOENT:
                    self._prepare_clone_subvolume(fs_handle, volname, s_subvolume, s_snapname,
                                                  target_group, target_subvolname, **kwargs)
                else:
                    raise

    def clone_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        s_subvolname = kwargs['sub_name']
        s_groupname  = kwargs['group_name']

        try:
            if self.mgr.snapshot_clone_no_wait and \
               get_all_pending_clones_count(self, self.mgr, self.volspec) >= self.mgr.max_concurrent_clones:
                raise(VolumeException(-errno.EAGAIN, "all cloner threads are busy, please try again later"))
            
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, s_groupname) as s_group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, s_group, s_subvolname, SubvolumeOpType.CLONE_SOURCE) as s_subvolume:
                        self._clone_subvolume_snapshot(fs_handle, volname, s_group, s_subvolume, **kwargs)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def _get_clone_src_path(self, vol_handle, dst_group, dst_subvol):
        src_subvol_details = dst_subvol._get_clone_source()
        # We exercise op type checks on subvolume but not on subvolumegroups and we don't allow
        # internal directories (including "_nogroup" to be opened). To do that we need to pass
        # None (instead of "_nogroup") as value of "groupname" which is a parameter accepted by
        # Group.__init__(). We could've allowed opening "_nogroup" but moving forward with
        # current convention.
        src_group_name = src_subvol_details.get('group', None)
        src_subvol_name = src_subvol_details['subvolume']
        src_snap_name = src_subvol_details['snapshot']

        try:
            if src_group_name != dst_group.groupname:
                with open_subvol_in_group(self.mgr, vol_handle, self.volspec,
                                          src_group_name, src_subvol_name,
                                          SubvolumeOpType.CLONE_SOURCE) as src_subvol:
                    src_path = src_subvol.snapshot_data_path(src_snap_name).decode('utf-8')

            else:
                with open_subvol(self.mgr, vol_handle, self.volspec, dst_group,
                                 src_subvol_name, SubvolumeOpType.CLONE_SOURCE) as \
                                 src_subvol:
                    src_path = src_subvol.snapshot_data_path(src_snap_name).decode('utf-8')
        except VolumeException as e:
            if e.errno != -errno.ENOENT:
                raise

            log.debug(f'snapshot "{src_snap_name}" which is/was being cloned to create subvolume '
                      '"{dst_subvol.subvolname}" has become missing. skipping adding progress '
                      'report to "clone status" output and, likely, cloning will fail.')
            src_path = None

        return src_path

    def _get_clone_progress_report(self, vol_handle, dst_group, dst_subvol):
        dst_path = dst_subvol.base_path.decode('utf-8')
        src_path = self._get_clone_src_path(vol_handle, dst_group, dst_subvol)
        if not src_path:
            return None

        stats = get_stats(src_path, dst_path, vol_handle)
        stats['percentage cloned'] = str(stats['percentage cloned']) + '%'
        return stats

    def _get_clone_status(self, vol_handle, group, subvol):
        status = subvol.status
        if status['state'] == 'in-progress':
            stats = self._get_clone_progress_report(vol_handle, group, subvol)
            if stats:
                status.update({'progress_report': stats})

        return json.dumps({'status' : status}, indent=2)

    def clone_status(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        clonename = kwargs['clone_name']
        groupname = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, clonename, SubvolumeOpType.CLONE_STATUS) as subvolume:
                        status = self._get_clone_status(fs_handle, group, subvolume)
                        ret = 0, status, ""
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
        size       = kwargs['size']
        pool      = kwargs['pool_layout']
        uid       = kwargs['uid']
        gid       = kwargs['gid']
        mode      = kwargs['mode']

        try:
            with open_volume(self, volname) as fs_handle:
                try:
                    with open_group(fs_handle, self.volspec, groupname) as group:
                        # idempotent creation -- valid.
                        attrs = {
                            'uid': uid,
                            'gid': gid,
                            'mode': octal_str_to_decimal_int(mode),
                            'data_pool': pool,
                            'quota': size
                        }
                        set_group_attrs(fs_handle, group.path, attrs)
                except VolumeException as ve:
                    if ve.errno == -errno.ENOENT:
                        oct_mode = octal_str_to_decimal_int(mode)
                        create_group(fs_handle, self.volspec, groupname, size, pool, oct_mode, uid, gid)
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

    def subvolumegroup_info(self, **kwargs):
        ret        = None
        volname    = kwargs['vol_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                        mon_addr_lst = []
                        mon_map_mons = self.mgr.get('mon_map')['mons']
                        for mon in mon_map_mons:
                            ip_port = mon['addr'].split("/")[0]
                            mon_addr_lst.append(ip_port)

                        group_info_dict = group.info()
                        group_info_dict["mon_addrs"] = mon_addr_lst
                        ret = 0, json.dumps(group_info_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def resize_subvolume_group(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        groupname  = kwargs['group_name']
        newsize    = kwargs['new_size']
        noshrink   = kwargs['no_shrink']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                        nsize, usedbytes = group.resize(newsize, noshrink)
                        ret = 0, json.dumps(
                            [{'bytes_used': usedbytes},{'bytes_quota': nsize},
                             {'bytes_pcent': "undefined" if nsize == 0 else '{0:.2f}'.format((float(usedbytes) / nsize) * 100.0)}],
                            indent=4, sort_keys=True), ""
        except VolumeException as ve:
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
        volume_exists = False
        try:
            with open_volume(self, volname) as fs_handle:
                volume_exists = True
                groups = listdir(fs_handle, self.volspec.base_dir, filter_entries=[dir.encode('utf-8') for dir in self.volspec.INTERNAL_DIRS])
                ret = 0, name_to_json(groups), ""
        except VolumeException as ve:
            if not ve.errno == -errno.ENOENT or not volume_exists:
                ret = self.volume_exception_to_retval(ve)
        return ret

    def pin_subvolume_group(self, **kwargs):
        ret           = 0, "", ""
        volname       = kwargs['vol_name']
        groupname     = kwargs['group_name']
        pin_type      = kwargs['pin_type']
        pin_setting   = kwargs['pin_setting']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    group.pin(pin_type, pin_setting)
                    ret = 0, json.dumps({}), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def subvolume_group_exists(self, **kwargs):
        volname = kwargs['vol_name']
        ret = 0, "", ""
        volume_exists = False

        try:
            with open_volume(self, volname) as fs_handle:
                volume_exists = True
                res = has_subdir(fs_handle, self.volspec.base_dir, filter_entries=[
                                 dir.encode('utf-8') for dir in self.volspec.INTERNAL_DIRS])
                if res:
                    ret = 0, "subvolumegroup exists", ""
                else:
                    ret = 0, "no subvolumegroup exists", ""
        except VolumeException as ve:
            if volume_exists and ve.errno == -errno.ENOENT:
                ret = 0, "no subvolumegroup exists", ""
            else:
                ret = self.volume_exception_to_retval(ve)
        return ret

    ### group snapshot

    def create_subvolume_group_snapshot(self, **kwargs):
        ret       = -errno.ENOSYS, "", "subvolume group snapshots are not supported"
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        # snapname  = kwargs['snap_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname):
                    # as subvolumes are marked with the vxattr ceph.dir.subvolume deny snapshots
                    # at the subvolume group (see: https://tracker.ceph.com/issues/46074)
                    # group.create_snapshot(snapname)
                    pass
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
