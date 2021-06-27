import logging
import threading
from typing import Tuple, Optional, List

from mgr_module import MgrModule, CLICommand, Option, CLICheckNonemptyFileInput
import orchestrator

from .export import ExportMgr
from .cluster import NFSCluster
from typing import Any

log = logging.getLogger(__name__)


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS: List[Option] = []

    def __init__(self, *args: str, **kwargs: Any) -> None:
        self.inited = False
        self.lock = threading.Lock()
        super(Module, self).__init__(*args, **kwargs)
        with self.lock:
            self.export_mgr = ExportMgr(self)
            self.nfs = NFSCluster(self)
            self.inited = True

    @CLICommand('nfs export create cephfs', perm='rw')
    def _cmd_nfs_export_create_cephfs(
            self,
            fsname: str,
            cluster_id: str,
            pseudo_path: str,
            path: Optional[str] = '/',
            readonly: Optional[bool] = False,
            client_addr: Optional[List[str]] = None,
            squash: str = 'none',
    ) -> Tuple[int, str, str]:
        """Create a CephFS export"""
        return self.export_mgr.create_export(fsal_type='cephfs', fs_name=fsname,
                                             cluster_id=cluster_id, pseudo_path=pseudo_path,
                                             read_only=readonly, path=path,
                                             squash=squash, addr=client_addr)

    @CLICommand('nfs export create rgw', perm='rw')
    def _cmd_rgw_export_create_rgw(
            self,
            bucket: str,
            cluster_id: str,
            pseudo_path: str,
            readonly: Optional[bool] = False,
            client_addr: Optional[List[str]] = None,
            realm: Optional[str] = None,
    ) -> Tuple[int, str, str]:
        """Create an RGW export"""
        return self.export_mgr.create_export(fsal_type='rgw', bucket=bucket,
                                             realm=realm,
                                             cluster_id=cluster_id, pseudo_path=pseudo_path,
                                             read_only=readonly, squash='none',
                                             addr=client_addr)

    @CLICommand('nfs export rm', perm='rw')
    def _cmd_nfs_export_rm(self, cluster_id: str, pseudo_path: str) -> Tuple[int, str, str]:
        """Remove a cephfs export"""
        return self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export delete', perm='rw')
    def _cmd_nfs_export_delete(self, cluster_id: str, pseudo_path: str) -> Tuple[int, str, str]:
        """Delete a cephfs export (DEPRECATED)"""
        return self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export ls', perm='r')
    def _cmd_nfs_export_ls(self, cluster_id: str, detailed: bool = False) -> Tuple[int, str, str]:
        """List exports of a NFS cluster"""
        return self.export_mgr.list_exports(cluster_id=cluster_id, detailed=detailed)

    @CLICommand('nfs export info', perm='r')
    def _cmd_nfs_export_info(self, cluster_id: str, pseudo_path: str) -> Tuple[int, str, str]:
        """Fetch a export of a NFS cluster given the pseudo path/binding"""
        return self.export_mgr.get_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export get', perm='r')
    def _cmd_nfs_export_get(self, cluster_id: str, pseudo_path: str) -> Tuple[int, str, str]:
        """Fetch a export of a NFS cluster given the pseudo path/binding (DEPRECATED)"""
        return self.export_mgr.get_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export apply', perm='rw')
    @CLICheckNonemptyFileInput(desc='Export JSON specification')
    def _cmd_nfs_export_apply(self, cluster_id: str, inbuf: str) -> Tuple[int, str, str]:
        """Create or update an export by `-i <json_file>`"""
        # The export <json_file> is passed to -i and it's processing
        # is handled by the Ceph CLI.
        return self.export_mgr.apply_export(cluster_id, export_config=inbuf)

    @CLICommand('nfs cluster create', perm='rw')
    def _cmd_nfs_cluster_create(self,
                                cluster_id: str,
                                placement: Optional[str] = None,
                                ingress: Optional[bool] = None,
                                virtual_ip: Optional[str] = None) -> Tuple[int, str, str]:
        """Create an NFS Cluster"""
        return self.nfs.create_nfs_cluster(cluster_id=cluster_id, placement=placement,
                                           virtual_ip=virtual_ip, ingress=ingress)

    @CLICommand('nfs cluster rm', perm='rw')
    def _cmd_nfs_cluster_rm(self, cluster_id: str) -> Tuple[int, str, str]:
        """Removes an NFS Cluster"""
        return self.nfs.delete_nfs_cluster(cluster_id=cluster_id)

    @CLICommand('nfs cluster delete', perm='rw')
    def _cmd_nfs_cluster_delete(self, cluster_id: str) -> Tuple[int, str, str]:
        """Removes an NFS Cluster (DEPRECATED)"""
        return self.nfs.delete_nfs_cluster(cluster_id=cluster_id)

    @CLICommand('nfs cluster ls', perm='r')
    def _cmd_nfs_cluster_ls(self) -> Tuple[int, str, str]:
        """List NFS Clusters"""
        return self.nfs.list_nfs_cluster()

    @CLICommand('nfs cluster info', perm='r')
    def _cmd_nfs_cluster_info(self, cluster_id: Optional[str] = None) -> Tuple[int, str, str]:
        """Displays NFS Cluster info"""
        return self.nfs.show_nfs_cluster_info(cluster_id=cluster_id)

    @CLICommand('nfs cluster config set', perm='rw')
    @CLICheckNonemptyFileInput(desc='NFS-Ganesha Configuration')
    def _cmd_nfs_cluster_config_set(self, cluster_id: str, inbuf: str) -> Tuple[int, str, str]:
        """Set NFS-Ganesha config by `-i <config_file>`"""
        return self.nfs.set_nfs_cluster_config(cluster_id=cluster_id, nfs_config=inbuf)

    @CLICommand('nfs cluster config reset', perm='rw')
    def _cmd_nfs_cluster_config_reset(self, cluster_id: str) -> Tuple[int, str, str]:
        """Reset NFS-Ganesha Config to default"""
        return self.nfs.reset_nfs_cluster_config(cluster_id=cluster_id)
