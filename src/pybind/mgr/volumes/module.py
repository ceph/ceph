from threading import Event
import errno
import json
try:
    import queue as Queue
except ImportError:
    import Queue

import cephfs
from mgr_module import MgrModule
import orchestrator

from .fs.subvolume import SubvolumePath, SubvolumeClient
from .fs.volume import VolumeClient

class PurgeJob(object):
    def __init__(self, volume_fscid, subvolume_path):
        """
        Purge tasks work in terms of FSCIDs, so that if we process
        a task later when a volume was deleted and recreated with
        the same name, we can correctly drop the task that was
        operating on the original volume.
        """
        self.fscid = volume_fscid
        self.subvolume_path = subvolume_path


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    COMMANDS = [
        {
            'cmd': 'fs volume ls',
            'desc': "List volumes",
            'perm': 'r'
        },
        {
            'cmd': 'fs volume create '
                   'name=name,type=CephString '
                   'name=size,type=CephString,req=false ',
            'desc': "Create a CephFS volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs volume rm '
                   'name=vol_name,type=CephString',
            'desc': "Delete a CephFS volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup create '
                   'name=vol_name,type=CephString '
                   'name=group_name,type=CephString ',
            'desc': "Create a CephFS subvolume group in a volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup rm '
                   'name=vol_name,type=CephString '
                   'name=group_name,type=CephString '
                   'name=force,type=CephBool,req=false ',
            'desc': "Delete a CephFS subvolume group in a volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume create '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=size,type=CephInt,req=false '
                   'name=group_name,type=CephString,req=false ',
            'desc': "Create a CephFS subvolume in a volume, and optionally, "
                    "with a specific size (in bytes) and in a specific "
                    "subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume rm '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=group_name,type=CephString,req=false '
                   'name=force,type=CephBool,req=false ',
            'desc': "Delete a CephFS subvolume in a volume, and optionally, "
                    "in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume getpath '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "Get the mountpath of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup snapshot create '
                   'name=vol_name,type=CephString '
                   'name=group_name,type=CephString '
                   'name=snap_name,type=CephString ',
            'desc': "Create a snapshot of a CephFS subvolume group in a volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup snapshot rm '
                   'name=vol_name,type=CephString '
                   'name=group_name,type=CephString '
                   'name=snap_name,type=CephString '
                   'name=force,type=CephBool,req=false ',
                   'desc': "Delete a snapshot of a CephFS subvolume group in a volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot create '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=snap_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "Create a snapshot of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot rm '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=snap_name,type=CephString '
                   'name=group_name,type=CephString,req=false '
                   'name=force,type=CephBool,req=false ',
            'desc': "Delete a snapshot of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },

        # volume ls [recursive]
        # subvolume ls <volume>
        # volume authorize/deauthorize
        # subvolume authorize/deauthorize

        # volume describe (free space, etc)
        # volume auth list (vc.get_authorized_ids)

        # snapshots?

        # FIXME: we're doing CephFSVolumeClient.recover on every
        # path where we instantiate and connect a client.  Perhaps
        # keep clients alive longer, or just pass a "don't recover"
        # flag in if it's the >1st time we connected a particular
        # volume in the lifetime of this module instance.
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self._initialized = Event()
        self.vc = VolumeClient(self)

        self._background_jobs = Queue.Queue()

    def serve(self):
        # TODO: discover any subvolumes pending purge, and enqueue
        # them in background_jobs at startup

        # TODO: consume background_jobs
        #   skip purge jobs if their fscid no longer exists

        # TODO: on volume delete, cancel out any background jobs that
        # affect subvolumes within that volume.

        # ... any background init needed?  Can get rid of this
        # and _initialized if not
        self._initialized.set()

    def handle_command(self, inbuf, cmd):
        self._initialized.wait()

        handler_name = "_cmd_" + cmd['prefix'].replace(" ", "_")
        try:
            handler = getattr(self, handler_name)
        except AttributeError:
            return -errno.EINVAL, "", "Unknown command"

        return handler(inbuf, cmd)

    def _cmd_fs_volume_create(self, inbuf, cmd):
        # TODO: validate name against any rules for pool/fs names
        # (...are there any?)
        vol_id = cmd['name']
        size = cmd.get('size', None)

        return self.vc.create_volume(vol_id, size)

    def _cmd_fs_volume_rm(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        return self.vc.delete_volume(vol_name)

    def _cmd_fs_volume_ls(self, inbuf, cmd):
        return self.vc.list_volumes()

    def _cmd_fs_subvolumegroup_create(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        vol_name = cmd['vol_name']
        group_name = cmd['group_name']

        if not self._volume_exists(vol_name):
            return -errno.ENOENT, "", \
                   "Volume '{0}' not found, create it with `ceph fs volume create` " \
                   "before trying to create subvolume groups".format(vol_name)

        # TODO: validate that subvol size fits in volume size

        with SubvolumeClient(self, fs_name=vol_name) as svc:
            svc.create_group(group_name)

        return 0, "", ""

    def _cmd_fs_subvolumegroup_rm(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        vol_name = cmd['vol_name']
        group_name = cmd['group_name']

        force = cmd.get('force', False)

        if not self._volume_exists(vol_name):
            if force:
                return 0, "", ""
            else:
                return -errno.ENOENT, "", \
                       "Volume '{0}' not found, cannot remove subvolume group '{0}'".format(vol_name, group_name)

        with SubvolumeClient(self, fs_name=vol_name) as svc:
            # TODO: check whether there are no subvolumes in the group
            try:
                svc.delete_group(group_name)
            except cephfs.ObjectNotFound:
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                           "Subvolume group '{0}' not found, cannot remove it".format(group_name)


        return 0, "", ""

    def _cmd_fs_subvolume_create(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        vol_name = cmd['vol_name']
        sub_name = cmd['sub_name']

        size = cmd.get('size', None)
        group_name = cmd.get('group_name', None)

        if not self._volume_exists(vol_name):
            return -errno.ENOENT, "", \
                   "Volume '{0}' not found, create it with `ceph fs volume create` " \
                   "before trying to create subvolumes".format(vol_name)

        # TODO: validate that subvol size fits in volume size

        with SubvolumeClient(self, fs_name=vol_name) as svc:
            if group_name and not svc.get_group_path(group_name):
                return -errno.ENOENT, "", \
                    "Subvolume group '{0}' not found, create it with `ceph fs subvolumegroup create` " \
                    "before trying to create subvolumes".format(group_name)
            svp = SubvolumePath(group_name, sub_name)
            svc.create_subvolume(svp, size)

        return 0, "", ""

    def _cmd_fs_subvolume_rm(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        vol_name = cmd['vol_name']
        sub_name = cmd['sub_name']

        force = cmd.get('force', False)
        group_name = cmd.get('group_name', None)

        fs = self._volume_get_fs(vol_name)
        if fs is None:
            if force:
                return 0, "", ""
            else:
                return -errno.ENOENT, "", \
                       "Volume '{0}' not found, cannot remove subvolume '{1}'".format(vol_name, sub_name)

        vol_fscid = fs['id']

        with SubvolumeClient(self, fs_name=vol_name) as svc:
            if group_name and not svc.get_group_path(group_name):
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                           "Subvolume group '{0}' not found, cannot remove subvolume '{1}'".format(group_name, sub_name)
            svp = SubvolumePath(group_name, sub_name)
            try:
                svc.delete_subvolume(svp)
            except cephfs.ObjectNotFound:
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                           "Subvolume '{0}' not found, cannot remove it".format(sub_name)
            svc.purge_subvolume(svp)

        # TODO: purge subvolume asynchronously
        # self._background_jobs.put(PurgeJob(vol_fscid, svp))

        return 0, "", ""

    def _cmd_fs_subvolume_getpath(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        sub_name = cmd['sub_name']

        group_name = cmd.get('group_name', None)

        if not self._volume_exists(vol_name):
            return -errno.ENOENT, "", "Volume '{0}' not found".format(vol_name)

        with SubvolumeClient(self, fs_name=vol_name) as svc:
            if group_name and not svc.get_group_path(group_name):
                return -errno.ENOENT, "", \
                    "Subvolume group '{0}' not found".format(group_name)
            svp = SubvolumePath(group_name, sub_name)
            path = svc.get_subvolume_path(svp)
            if not path:
                return -errno.ENOENT, "", \
                       "Subvolume '{0}' not found".format(sub_name)
        return 0, path, ""

    def _cmd_fs_subvolumegroup_snapshot_create(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        group_name = cmd['group_name']
        snap_name = cmd['snap_name']

        if not self._volume_exists(vol_name):
            return -errno.ENOENT, "", \
                   "Volume '{0}' not found, cannot create snapshot '{1}'".format(vol_name, snap_name)

        with SubvolumeClient(self, fs_name=vol_name) as svc:
            if group_name and not svc.get_group_path(group_name):
                return -errno.ENOENT, "", \
                    "Subvolume group '{0}' not found, cannot create snapshot '{1}'".format(group_name, snap_name)
            svc.create_group_snapshot(group_name, snap_name)

        return 0, "", ""

    def _cmd_fs_subvolumegroup_snapshot_rm(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        group_name = cmd['group_name']
        snap_name = cmd['snap_name']

        force = cmd.get('force', False)

        if not self._volume_exists(vol_name):
            if force:
                return 0, "", ""
            else:
                return -errno.ENOENT, "", \
                       "Volume '{0}' not found, cannot remove subvolumegroup snapshot '{1}'".format(vol_name, snap_name)

        with SubvolumeClient(self, fs_name=vol_name) as svc:
            if group_name and not svc.get_group_path(group_name):
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                           "Subvolume group '{0}' not found, cannot remove subvolumegroup snapshot '{1}'".format(group_name, snap_name)
            try:
                svc.delete_group_snapshot(group_name, snap_name)
            except cephfs.ObjectNotFound:
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                           "Subvolume group snapshot '{0}' not found, cannot remove it".format(snap_name)

        return 0, "", ""

    def _cmd_fs_subvolume_snapshot_create(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        sub_name = cmd['sub_name']
        snap_name = cmd['snap_name']

        group_name = cmd.get('group_name', None)

        if not self._volume_exists(vol_name):
            return -errno.ENOENT, "", \
                   "Volume '{0}' not found, cannot create snapshot '{1}'".format(vol_name, snap_name)

        with SubvolumeClient(self, fs_name=vol_name) as svc:
            if group_name and not svc.get_group_path(group_name):
                return -errno.ENOENT, "", \
                    "Subvolume group '{0}' not found, cannot create snapshot '{1}'".format(group_name, snap_name)
            svp = SubvolumePath(group_name, sub_name)
            if not svc.get_subvolume_path(svp):
                return -errno.ENOENT, "", \
                       "Subvolume '{0}' not found, cannot create snapshot '{1}'".format(sub_name, snap_name)
            svc.create_subvolume_snapshot(svp, snap_name)

        return 0, "", ""

    def _cmd_fs_subvolume_snapshot_rm(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        sub_name = cmd['sub_name']
        snap_name = cmd['snap_name']

        force = cmd.get('force', False)
        group_name = cmd.get('group_name', None)

        if not self._volume_exists(vol_name):
            if force:
                return 0, "", ""
            else:
                return -errno.ENOENT, "", \
                       "Volume '{0}' not found, cannot remove subvolume snapshot '{1}'".format(vol_name, snap_name)

        with SubvolumeClient(self, fs_name=vol_name) as svc:
            if group_name and not svc.get_group_path(group_name):
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                           "Subvolume group '{0}' not found, cannot remove subvolume snapshot '{1}'".format(group_name, snap_name)
            svp = SubvolumePath(group_name, sub_name)
            if not svc.get_subvolume_path(svp):
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                           "Subvolume '{0}' not found, cannot remove subvolume snapshot '{1}'".format(sub_name, snap_name)
            try:
                svc.delete_subvolume_snapshot(svp, snap_name)
            except cephfs.ObjectNotFound:
                if force:
                    return 0, "", ""
                else:
                    return -errno.ENOENT, "", \
                           "Subvolume snapshot '{0}' not found, cannot remove it".format(snap_name)

        return 0, "", ""
