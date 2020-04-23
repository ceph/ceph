import errno
import json

from mgr_module import MgrModule
import orchestrator

from .fs.volume import VolumeClient
from .fs.nfs import NFSCluster, FSExport

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
                   'name=placement,type=CephString,req=false ',
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
                   'name=uid,type=CephInt,req=false '
                   'name=gid,type=CephInt,req=false '
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
                   'name=uid,type=CephInt,req=false '
                   'name=gid,type=CephInt,req=false '
                   'name=mode,type=CephString,req=false '
                   'name=namespace_isolated,type=CephBool,req=false ',
            'desc': "Create a CephFS subvolume in a volume, and optionally, "
                    "with a specific size (in bytes), a specific data pool layout, "
                    "a specific mode, in a specific subvolume group and in separate "
                    "RADOS namespace",
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
            'cmd': 'fs subvolume info '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "Get the metadata of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'r'
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
            'cmd': 'fs subvolume snapshot info '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=snap_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "Get the metadata of a CephFS subvolume snapshot "
                    "and optionally, in a specific subvolume group",
            'perm': 'r'
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
        {
            'cmd': 'fs subvolume resize '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=new_size,type=CephString,req=true '
                   'name=group_name,type=CephString,req=false '
                   'name=no_shrink,type=CephBool,req=false ',
            'desc': "Resize a CephFS subvolume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot protect '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=snap_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "Protect snapshot of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot unprotect '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=snap_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "Unprotect a snapshot of a CephFS subvolume in a volume, "
                    "and optionally, in a specific subvolume group",
            'perm': 'rw'
        },
        {
            'cmd': 'fs subvolume snapshot clone '
                   'name=vol_name,type=CephString '
                   'name=sub_name,type=CephString '
                   'name=snap_name,type=CephString '
                   'name=target_sub_name,type=CephString '
                   'name=pool_layout,type=CephString,req=false '
                   'name=group_name,type=CephString,req=false '
                   'name=target_group_name,type=CephString,req=false ',
            'desc': "Clone a snapshot to target subvolume",
            'perm': 'rw'
        },
        {
            'cmd': 'fs clone status '
                   'name=vol_name,type=CephString '
                   'name=clone_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "Get status on a cloned subvolume.",
            'perm': 'r'
        },
        {
            'cmd': 'fs clone cancel '
                   'name=vol_name,type=CephString '
                   'name=clone_name,type=CephString '
                   'name=group_name,type=CephString,req=false ',
            'desc': "Cancel an pending or ongoing clone operation.",
            'perm': 'r'
        },
        {
            'cmd': 'nfs export create '
            'name=type,type=CephString '
            'name=fsname,type=CephString '
            'name=binding,type=CephString '
            'name=attach,type=CephString '
            'name=readonly,type=CephBool,req=false '
            'name=path,type=CephString,req=false ',
            'desc': "Create a cephfs export",
            'perm': 'rw'
        },
        {
            'cmd': 'fs nfs export delete '
                   'name=export_id,type=CephInt,req=true ',
            'desc': "Delete a cephfs export",
            'perm': 'rw'
        },
        {
            'cmd': 'nfs cluster create '
                   'name=type,type=CephString '
                   'name=clusterid,type=CephString '
                   'name=placement,type=CephString,req=false ',
            'desc': "Create an NFS Cluster",
            'perm': 'rw'
        },
        {
            'cmd': 'nfs cluster update '
                   'name=clusterid,type=CephString '
                   'name=placement,type=CephString ',
            'desc': "Updates an NFS Cluster",
            'perm': 'rw'
        },
        {
            'cmd': 'nfs cluster delete '
                   'name=clusterid,type=CephString ',
            'desc': "Deletes an NFS Cluster",
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
        self.vc = VolumeClient(self)
        self.fs_export = FSExport(self)

    def __del__(self):
        self.vc.shutdown()

    def shutdown(self):
        self.vc.shutdown()

    def handle_command(self, inbuf, cmd):
        handler_name = "_cmd_" + cmd['prefix'].replace(" ", "_")
        try:
            handler = getattr(self, handler_name)
        except AttributeError:
            return -errno.EINVAL, "", "Unknown command"

        return handler(inbuf, cmd)

    def _cmd_fs_volume_create(self, inbuf, cmd):
        vol_id = cmd['name']
        placement = cmd.get('placement', '')
        return self.vc.create_fs_volume(vol_id, placement)

    def _cmd_fs_volume_rm(self, inbuf, cmd):
        vol_name = cmd['vol_name']
        confirm = cmd.get('yes-i-really-mean-it', None)
        return self.vc.delete_fs_volume(vol_name, confirm)

    def _cmd_fs_volume_ls(self, inbuf, cmd):
        return self.vc.list_fs_volumes()

    def _cmd_fs_subvolumegroup_create(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        return self.vc.create_subvolume_group(
            vol_name=cmd['vol_name'], group_name=cmd['group_name'],
            pool_layout=cmd.get('pool_layout', None), mode=cmd.get('mode', '755'),
            uid=cmd.get('uid', None), gid=cmd.get('gid', None))

    def _cmd_fs_subvolumegroup_rm(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        return self.vc.remove_subvolume_group(vol_name=cmd['vol_name'],
                                              group_name=cmd['group_name'],
                                              force=cmd.get('force', False))

    def _cmd_fs_subvolumegroup_ls(self, inbuf, cmd):
        return self.vc.list_subvolume_groups(vol_name=cmd['vol_name'])

    def _cmd_fs_subvolume_create(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        return self.vc.create_subvolume(vol_name=cmd['vol_name'],
                                        sub_name=cmd['sub_name'],
                                        group_name=cmd.get('group_name', None),
                                        size=cmd.get('size', None),
                                        pool_layout=cmd.get('pool_layout', None),
                                        uid=cmd.get('uid', None),
                                        gid=cmd.get('gid', None),
                                        mode=cmd.get('mode', '755'),
                                        namespace_isolated=cmd.get('namespace_isolated', False))

    def _cmd_fs_subvolume_rm(self, inbuf, cmd):
        """
        :return: a 3-tuple of return code(int), empty string(str), error message (str)
        """
        return self.vc.remove_subvolume(vol_name=cmd['vol_name'],
                                        sub_name=cmd['sub_name'],
                                        group_name=cmd.get('group_name', None),
                                        force=cmd.get('force', False))

    def _cmd_fs_subvolume_ls(self, inbuf, cmd):
        return self.vc.list_subvolumes(vol_name=cmd['vol_name'],
                                       group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolumegroup_getpath(self, inbuf, cmd):
        return self.vc.getpath_subvolume_group(
            vol_name=cmd['vol_name'], group_name=cmd['group_name'])

    def _cmd_fs_subvolume_getpath(self, inbuf, cmd):
        return self.vc.subvolume_getpath(vol_name=cmd['vol_name'],
                                         sub_name=cmd['sub_name'],
                                         group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolume_info(self, inbuf, cmd):
        return self.vc.subvolume_info(vol_name=cmd['vol_name'],
                                      sub_name=cmd['sub_name'],
                                      group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolumegroup_snapshot_create(self, inbuf, cmd):
        return self.vc.create_subvolume_group_snapshot(vol_name=cmd['vol_name'],
                                                       group_name=cmd['group_name'],
                                                       snap_name=cmd['snap_name'])

    def _cmd_fs_subvolumegroup_snapshot_rm(self, inbuf, cmd):
        return self.vc.remove_subvolume_group_snapshot(vol_name=cmd['vol_name'],
                                                       group_name=cmd['group_name'],
                                                       snap_name=cmd['snap_name'],
                                                       force=cmd.get('force', False))

    def _cmd_fs_subvolumegroup_snapshot_ls(self, inbuf, cmd):
        return self.vc.list_subvolume_group_snapshots(vol_name=cmd['vol_name'],
                                                      group_name=cmd['group_name'])

    def _cmd_fs_subvolume_snapshot_create(self, inbuf, cmd):
        return self.vc.create_subvolume_snapshot(vol_name=cmd['vol_name'],
                                                 sub_name=cmd['sub_name'],
                                                 snap_name=cmd['snap_name'],
                                                 group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolume_snapshot_rm(self, inbuf, cmd):
        return self.vc.remove_subvolume_snapshot(vol_name=cmd['vol_name'],
                                                 sub_name=cmd['sub_name'],
                                                 snap_name=cmd['snap_name'],
                                                 group_name=cmd.get('group_name', None),
                                                 force=cmd.get('force', False))

    def _cmd_fs_subvolume_snapshot_info(self, inbuf, cmd):
        return self.vc.subvolume_snapshot_info(vol_name=cmd['vol_name'],
                                               sub_name=cmd['sub_name'],
                                               snap_name=cmd['snap_name'],
                                               group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolume_snapshot_ls(self, inbuf, cmd):
        return self.vc.list_subvolume_snapshots(vol_name=cmd['vol_name'],
                                                sub_name=cmd['sub_name'],
                                                group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolume_resize(self, inbuf, cmd):
        return self.vc.resize_subvolume(vol_name=cmd['vol_name'], sub_name=cmd['sub_name'],
                                        new_size=cmd['new_size'], group_name=cmd.get('group_name', None),
                                        no_shrink=cmd.get('no_shrink', False))

    def _cmd_fs_subvolume_snapshot_protect(self, inbuf, cmd):
        return self.vc.protect_subvolume_snapshot(vol_name=cmd['vol_name'], sub_name=cmd['sub_name'],
                                                  snap_name=cmd['snap_name'], group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolume_snapshot_unprotect(self, inbuf, cmd):
        return self.vc.unprotect_subvolume_snapshot(vol_name=cmd['vol_name'], sub_name=cmd['sub_name'],
                                                    snap_name=cmd['snap_name'], group_name=cmd.get('group_name', None))

    def _cmd_fs_subvolume_snapshot_clone(self, inbuf, cmd):
        return self.vc.clone_subvolume_snapshot(
            vol_name=cmd['vol_name'], sub_name=cmd['sub_name'], snap_name=cmd['snap_name'],
            group_name=cmd.get('group_name', None), pool_layout=cmd.get('pool_layout', None),
            target_sub_name=cmd['target_sub_name'], target_group_name=cmd.get('target_group_name', None))

    def _cmd_fs_clone_status(self, inbuf, cmd):
        return self.vc.clone_status(
            vol_name=cmd['vol_name'], clone_name=cmd['clone_name'],  group_name=cmd.get('group_name', None))

    def _cmd_fs_clone_cancel(self, inbuf, cmd):
        return self.vc.clone_cancel(
            vol_name=cmd['vol_name'], clone_name=cmd['clone_name'],  group_name=cmd.get('group_name', None))

    def _cmd_nfs_export_create(self, inbuf, cmd):
        #TODO Extend export creation for rgw.
        return self.fs_export.create_export(export_type=cmd['type'], fs_name=cmd['fsname'],
                pseudo_path=cmd['binding'], read_only=cmd.get('readonly', False),
                path=cmd.get('path', '/'), cluster_id=cmd.get('attach'))

    def _cmd_fs_nfs_export_delete(self, inbuf, cmd):
        return self.fs_export.delete_export(cmd['export_id'])

    def _cmd_nfs_cluster_create(self, inbuf, cmd):
        nfs_cluster_obj = NFSCluster(self, cmd['clusterid'])
        return nfs_cluster_obj.create_nfs_cluster(export_type=cmd['type'],
                                                  placement=cmd.get('placement', None))

    def _cmd_nfs_cluster_update(self, inbuf, cmd):
        nfs_cluster_obj = NFSCluster(self, cmd['clusterid'])
        return nfs_cluster_obj.update_nfs_cluster(placement=cmd['placement'])

    def _cmd_nfs_cluster_delete(self, inbuf, cmd):
        nfs_cluster_obj = NFSCluster(self, cmd['clusterid'])
        return nfs_cluster_obj.delete_nfs_cluster()
