import errno
import logging
import traceback
import threading

from mgr_module import MgrModule
from volumes.module import mgr_cmd_wrap
import orchestrator

from .fs.nfs import NFSCluster, FSExport

log = logging.getLogger(__name__)

goodchars = '[A-Za-z0-9-_.]'


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    COMMANDS = [
        {
            'cmd': 'nfs export create cephfs '
            'name=fsname,type=CephString '
            'name=clusterid,type=CephString '
            'name=binding,type=CephString '
            'name=readonly,type=CephBool,req=false '
            'name=path,type=CephString,req=false ',
            'desc': "Create a cephfs export",
            'perm': 'rw'
        },
        {
            'cmd': 'nfs export delete '
                   'name=clusterid,type=CephString '
                   'name=binding,type=CephString ',
            'desc': "Delete a cephfs export",
            'perm': 'rw'
        },
        {
            'cmd': 'nfs export ls '
                   'name=clusterid,type=CephString '
                   'name=detailed,type=CephBool,req=false ',
            'desc': "List exports of a NFS cluster",
            'perm': 'r'
        },
        {
            'cmd': 'nfs export get '
                   'name=clusterid,type=CephString '
                   'name=binding,type=CephString ',
            'desc': "Fetch a export of a NFS cluster given the pseudo path/binding",
            'perm': 'r'
        },
        {
            'cmd': 'nfs export update ',
            'desc': "Update an export of a NFS cluster by `-i <json_file>`",
            'perm': 'rw'
        },
        {
            'cmd': 'nfs cluster create '
                   f'name=clusterid,type=CephString,goodchars={goodchars} '
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
        {
            'cmd': 'nfs cluster ls ',
            'desc': "List NFS Clusters",
            'perm': 'r'
        },
        {
            'cmd': 'nfs cluster info '
                   'name=clusterid,type=CephString,req=false ',
            'desc': "Displays NFS Cluster info",
            'perm': 'r'
        },
        {
            'cmd': 'nfs cluster config set '
                   'name=clusterid,type=CephString ',
            'desc': "Set NFS-Ganesha config by `-i <config_file>`",
            'perm': 'rw'
        },
        {
            'cmd': 'nfs cluster config reset '
                   'name=clusterid,type=CephString ',
            'desc': "Reset NFS-Ganesha Config to default",
            'perm': 'rw'
        },
    ]

    MODULE_OPTIONS = []

    def __init__(self, *args, **kwargs):
        self.inited = False
        self.lock = threading.Lock()
        super(Module, self).__init__(*args, **kwargs)
        with self.lock:
            self.fs_export = FSExport(self)
            self.nfs = NFSCluster(self)
            self.inited = True

    def handle_command(self, inbuf, cmd):
        handler_name = "_cmd_" + cmd['prefix'].replace(" ", "_")
        try:
            handler = getattr(self, handler_name)
        except AttributeError:
            return -errno.EINVAL, "", "Unknown command"

        return handler(inbuf, cmd)

    @mgr_cmd_wrap
    def _cmd_nfs_export_create_cephfs(self, inbuf, cmd):
        #TODO Extend export creation for rgw.
        return self.fs_export.create_export(fs_name=cmd['fsname'], cluster_id=cmd['clusterid'],
                pseudo_path=cmd['binding'], read_only=cmd.get('readonly', False), path=cmd.get('path', '/'))

    @mgr_cmd_wrap
    def _cmd_nfs_export_delete(self, inbuf, cmd):
        return self.fs_export.delete_export(cluster_id=cmd['clusterid'], pseudo_path=cmd['binding'])

    @mgr_cmd_wrap
    def _cmd_nfs_export_ls(self, inbuf, cmd):
        return self.fs_export.list_exports(cluster_id=cmd['clusterid'], detailed=cmd.get('detailed', False))

    @mgr_cmd_wrap
    def _cmd_nfs_export_get(self, inbuf, cmd):
        return self.fs_export.get_export(cluster_id=cmd['clusterid'], pseudo_path=cmd['binding'])

    @mgr_cmd_wrap
    def _cmd_nfs_export_update(self, inbuf, cmd):
        # The export <json_file> is passed to -i and it's processing is handled by the Ceph CLI.
        return self.fs_export.update_export(export_config=inbuf)

    @mgr_cmd_wrap
    def _cmd_nfs_cluster_create(self, inbuf, cmd):
        return self.nfs.create_nfs_cluster(cluster_id=cmd['clusterid'],
                                           placement=cmd.get('placement', None))

    @mgr_cmd_wrap
    def _cmd_nfs_cluster_update(self, inbuf, cmd):
        return self.nfs.update_nfs_cluster(cluster_id=cmd['clusterid'], placement=cmd['placement'])

    @mgr_cmd_wrap
    def _cmd_nfs_cluster_delete(self, inbuf, cmd):
        return self.nfs.delete_nfs_cluster(cluster_id=cmd['clusterid'])

    @mgr_cmd_wrap
    def _cmd_nfs_cluster_ls(self, inbuf, cmd):
        return self.nfs.list_nfs_cluster()

    @mgr_cmd_wrap
    def _cmd_nfs_cluster_info(self, inbuf, cmd):
        return self.nfs.show_nfs_cluster_info(cluster_id=cmd.get('clusterid', None))

    def _cmd_nfs_cluster_config_set(self, inbuf, cmd):
        return self.nfs.set_nfs_cluster_config(cluster_id=cmd['clusterid'], nfs_config=inbuf)

    def _cmd_nfs_cluster_config_reset(self, inbuf, cmd):
        return self.nfs.reset_nfs_cluster_config(cluster_id=cmd['clusterid'])
