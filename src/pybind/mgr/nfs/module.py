import logging
import threading

from mgr_module import MgrModule, CLICommand
import orchestrator

from .export import ExportMgr
from .cluster import NFSCluster

log = logging.getLogger(__name__)


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS = []

    def __init__(self, *args, **kwargs):
        self.inited = False
        self.lock = threading.Lock()
        super(Module, self).__init__(*args, **kwargs)
        with self.lock:
            self.export_mgr = ExportMgr(self)
            self.nfs = NFSCluster(self)
            self.inited = True

    @CLICommand('nfs export create cephfs', perm='rw')
    def _cmd_nfs_export_create_cephfs(self, fsname: str, clusterid: str, binding: str,
                                      readonly: bool=False, path: str='/'):
        """Create a cephfs export"""
        # TODO Extend export creation for rgw.
        return self.export_mgr.create_export(fsal_type='cephfs', fs_name=fsname,
                                             cluster_id=clusterid, pseudo_path=binding,
                                             read_only=readonly, path=path)

    @CLICommand('nfs export delete', perm='rw')
    def _cmd_nfs_export_delete(self, clusterid: str, binding: str):
        """Delete a cephfs export"""
        return self.export_mgr.delete_export(cluster_id=clusterid, pseudo_path=binding)

    @CLICommand('nfs export ls', perm='r')
    def _cmd_nfs_export_ls(self, clusterid: str, detailed: bool=False):
        """List exports of a NFS cluster"""
        return self.export_mgr.list_exports(cluster_id=clusterid, detailed=detailed)

    @CLICommand('nfs export get', perm='r')
    def _cmd_nfs_export_get(self, clusterid: str, binding: str):
        """Fetch a export of a NFS cluster given the pseudo path/binding"""
        return self.export_mgr.get_export(cluster_id=clusterid, pseudo_path=binding)

    @CLICommand('nfs export update', perm='rw')
    def _cmd_nfs_export_update(self, inbuf: str):
        """Update an export of a NFS cluster by `-i <json_file>`"""
        # The export <json_file> is passed to -i and it's processing is handled by the Ceph CLI.
        return self.export_mgr.update_export(export_config=inbuf)

    @CLICommand('nfs cluster create', perm='rw')
    def _cmd_nfs_cluster_create(self, clusterid: str, placement: str=None):
        """Create an NFS Cluster"""
        return self.nfs.create_nfs_cluster(cluster_id=clusterid, placement=placement)

    @CLICommand('nfs cluster update', perm='rw')
    def _cmd_nfs_cluster_update(self, clusterid: str, placement: str):
        """Updates an NFS Cluster"""
        return self.nfs.update_nfs_cluster(cluster_id=clusterid, placement=placement)

    @CLICommand('nfs cluster delete', perm='rw')
    def _cmd_nfs_cluster_delete(self, clusterid: str):
        """Deletes an NFS Cluster"""
        return self.nfs.delete_nfs_cluster(cluster_id=clusterid)

    @CLICommand('nfs cluster ls', perm='r')
    def _cmd_nfs_cluster_ls(self):
        """List NFS Clusters"""
        return self.nfs.list_nfs_cluster()

    @CLICommand('nfs cluster info', perm='r')
    def _cmd_nfs_cluster_info(self, clusterid: str=None):
        """Displays NFS Cluster info"""
        return self.nfs.show_nfs_cluster_info(cluster_id=clusterid)

    @CLICommand('nfs cluster config set', perm='rw')
    def _cmd_nfs_cluster_config_set(self, clusterid: str, inbuf: str):
        """Set NFS-Ganesha config by `-i <config_file>`"""
        return self.nfs.set_nfs_cluster_config(cluster_id=clusterid, nfs_config=inbuf)

    @CLICommand('nfs cluster config reset', perm='rw')
    def _cmd_nfs_cluster_config_reset(self, clusterid: str):
        """Reset NFS-Ganesha Config to default"""
        return self.nfs.reset_nfs_cluster_config(cluster_id=clusterid)
