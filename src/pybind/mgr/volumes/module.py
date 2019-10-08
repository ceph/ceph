from threading import Event
import errno
import json
try:
    import queue as Queue
except ImportError:
    import Queue

from mgr_module import MgrModule
import orchestrator

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
                   'name=name,type=CephString ',
            'desc': "Create a CephFS volume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs volume rm '
                   'name=vol_name,type=CephString '
                   'name=yes-i-really-mean-it,type=CephString,req=false ',
            'desc': "Delete a FS volume by passing --yes-i-really-mean-it flag",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolumegroup ls '
            'name=vol_name,type=CephString ',
            'desc': "List subvolumegroups",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolumegroup create '
                   'name=vol_name,type=CephString '
                   'name=group_name,type=CephString '
                   'name=pool_layout,type=CephString,req=false '
                   'name=mode,type=CephString,req=false ',
            'desc': "Create a CephFS subvolume group in a volume, and optionally, "
                    "with a specific data pool layout, and a specific numeric mode",
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
            'cmd': 'fs subvolume ls '
                   'name=vol_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "List subvolumes",
            'perm': 'r'
        },
        {
            'cmd': 'fs subvolume create '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=size,type=CephInt,req=false '
                   'name=group_name,type=CephString,req=false '
                   'name=pool_layout,type=CephString,req=false '
                   'name=mode,type=CephString,req=false ',
            'desc': "Create a CephFS subvolume in a volume, and optionally, "
                    "with a specific size (in bytes), a specific data pool layout, "
                    "a specific mode, and in a specific subvolume group",
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
            'cmd': 'fs subvolumegroup getpath '
                   'name=vol_name,type=CephString '
                   'name=group_name,type=CephString ',
            'desc': "Get the mountpath of a CephFS subvolume group in a volume",
            'perm': 'r'
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
            'cmd': 'fs subvolumegroup snapshot ls '
                   'name=vol_name,type=CephString '
                   'name=group_name,type=CephString ',
            'desc': "List subvolumegroup snapshots",
            'perm': 'r'
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
            'cmd': 'fs subvolume snapshot ls '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "List subvolume snapshots",
            'perm': 'r'
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
        return self.vc.create_volume(vol_id)

    def _cmd_fs_volume_rm(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        confirm = cmd.get('yes-i-really-mean-it', None)
        return self.vc.delete_volume(vol_name, confirm)

    def _cmd_fs_volume_ls(self, inbuf, cmd):
        return self.vc.list_volumes()

    def _cmd_fs_subvolumegroup_create(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        return self.vc.create_subvolume_group(
            None, vol_name=cmd['vol_name'], group_name=cmd['group_name'],
            pool_layout=cmd.get('pool_layout', None), mode=cmd.get('mode', '755'))

    def _cmd_fs_subvolumegroup_rm(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        return self.vc.remove_subvolume_group(None, vol_name=cmd['vol_name'],
                                              group_name=cmd['group_name'],
                                              force=cmd.get('force', False))

    def _cmd_fs_subvolumegroup_ls(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        return self.vc.list_subvolume_groups(None, vol_name=cmd['vol_name'])

    def _cmd_fs_subvolume_create(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        return self.vc.create_subvolume(None, vol_name=cmd['vol_name'],
                                        sub_name=cmd['sub_name'],
                                        group_name=cmd.get('group_name', None),
                                        size=cmd.get('size', None),
                                        pool_layout=cmd.get('pool_layout', None),
                                        mode=cmd.get('mode', '755'))

    def _cmd_fs_subvolume_rm(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        return self.vc.remove_subvolume(None, vol_name=cmd['vol_name'],
                                        sub_name=cmd['sub_name'],
                                        group_name=cmd.get('group_name', None),
                                        force=cmd.get('force', False))

    def _cmd_fs_subvolume_ls(self, inbuf, cmd):
        return self.vc.list_subvolumes(None, vol_name=cmd['vol_name'],
                                       group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolumegroup_getpath(self, inbuf, cmd):
        return self.vc.getpath_subvolume_group(
                None, vol_name=cmd['vol_name'], group_name=cmd['group_name'])

    def _cmd_fs_subvolume_getpath(self, inbuf, cmd):
        return self.vc.subvolume_getpath(None, vol_name=cmd['vol_name'],
                                         sub_name=cmd['sub_name'],
                                         group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolumegroup_snapshot_create(self, inbuf, cmd):
        return self.vc.create_subvolume_group_snapshot(None, vol_name=cmd['vol_name'],
                                                       group_name=cmd['group_name'],
                                                       snap_name=cmd['snap_name'])

    def _cmd_fs_subvolumegroup_snapshot_rm(self, inbuf, cmd):
        return self.vc.remove_subvolume_group_snapshot(None, vol_name=cmd['vol_name'],
                                                       group_name=cmd['group_name'],
                                                       snap_name=cmd['snap_name'],
                                                       force=cmd.get('force', False))

    def _cmd_fs_subvolumegroup_snapshot_ls(self, inbuf, cmd):
        return self.vc.list_subvolume_group_snapshots(None, vol_name=cmd['vol_name'],
                                                      group_name=cmd['group_name'])

    def _cmd_fs_subvolume_snapshot_create(self, inbuf, cmd):
        return self.vc.create_subvolume_snapshot(None, vol_name=cmd['vol_name'],
                                                 sub_name=cmd['sub_name'],
                                                 snap_name=cmd['snap_name'],
                                                 group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolume_snapshot_rm(self, inbuf, cmd):
        return self.vc.remove_subvolume_snapshot(None, vol_name=cmd['vol_name'],
                                                 sub_name=cmd['sub_name'],
                                                 snap_name=cmd['snap_name'],
                                                 group_name=cmd.get('group_name', None),
                                                 force=cmd.get('force', False))

    def _cmd_fs_subvolume_snapshot_ls(self, inbuf, cmd):
        return self.vc.list_subvolume_snapshots(None, vol_name=cmd['vol_name'],
                                                sub_name=cmd['sub_name'],
                                                group_name=cmd.get('group_name', None))
