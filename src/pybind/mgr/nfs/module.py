import logging
import threading
from typing import Tuple, Optional, List, Dict, Any
import yaml

from mgr_module import MgrModule, CLICommand, Option, CLICheckNonemptyFileInput
import object_format
import orchestrator
from orchestrator.module import IngressType
from mgr_util import CephFSEarmarkResolver

from .export import ExportMgr, AppliedExportResults
from .cluster import NFSCluster
from .utils import available_clusters
from .qos_conf import QOSType, QOSBandwidthControl, UserQoSType

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
    @object_format.Responder()
    def _cmd_nfs_export_create_cephfs(
            self,
            cluster_id: str,
            pseudo_path: str,
            fsname: str,
            path: Optional[str] = '/',
            readonly: Optional[bool] = False,
            client_addr: Optional[List[str]] = None,
            squash: str = 'none',
            sectype: Optional[List[str]] = None,
            cmount_path: Optional[str] = "/"
    ) -> Dict[str, Any]:
        """Create a CephFS export"""
        earmark_resolver = CephFSEarmarkResolver(self)
        return self.export_mgr.create_export(
            fsal_type='cephfs',
            fs_name=fsname,
            cluster_id=cluster_id,
            pseudo_path=pseudo_path,
            read_only=readonly,
            path=path,
            squash=squash,
            addr=client_addr,
            sectype=sectype,
            cmount_path=cmount_path,
            earmark_resolver=earmark_resolver
        )

    @CLICommand('nfs export create rgw', perm='rw')
    @object_format.Responder()
    def _cmd_nfs_export_create_rgw(
            self,
            cluster_id: str,
            pseudo_path: str,
            bucket: Optional[str] = None,
            user_id: Optional[str] = None,
            readonly: Optional[bool] = False,
            client_addr: Optional[List[str]] = None,
            squash: str = 'none',
            sectype: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Create an RGW export"""
        return self.export_mgr.create_export(
            fsal_type='rgw',
            bucket=bucket,
            user_id=user_id,
            cluster_id=cluster_id,
            pseudo_path=pseudo_path,
            read_only=readonly,
            squash=squash,
            addr=client_addr,
            sectype=sectype,
        )

    @CLICommand('nfs export rm', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_export_rm(self, cluster_id: str, pseudo_path: str) -> None:
        """Remove a cephfs export"""
        return self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export delete', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_export_delete(self, cluster_id: str, pseudo_path: str) -> None:
        """Delete a cephfs export (DEPRECATED)"""
        return self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export ls', perm='r')
    @object_format.Responder()
    def _cmd_nfs_export_ls(self, cluster_id: str, detailed: bool = False) -> List[Any]:
        """List exports of a NFS cluster"""
        return self.export_mgr.list_exports(cluster_id=cluster_id, detailed=detailed)

    @CLICommand('nfs export info', perm='r')
    @object_format.Responder()
    def _cmd_nfs_export_info(self, cluster_id: str, pseudo_path: str) -> Dict[str, Any]:
        """Fetch a export of a NFS cluster given the pseudo path/binding"""
        return self.export_mgr.get_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export get', perm='r')
    @object_format.Responder()
    def _cmd_nfs_export_get(self, cluster_id: str, pseudo_path: str) -> Dict[str, Any]:
        """Fetch a export of a NFS cluster given the pseudo path/binding (DEPRECATED)"""
        return self.export_mgr.get_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @CLICommand('nfs export apply', perm='rw')
    @CLICheckNonemptyFileInput(desc='Export JSON or Ganesha EXPORT specification')
    @object_format.Responder()
    def _cmd_nfs_export_apply(self, cluster_id: str, inbuf: str) -> AppliedExportResults:
        earmark_resolver = CephFSEarmarkResolver(self)
        """Create or update an export by `-i <json_or_ganesha_export_file>`"""
        return self.export_mgr.apply_export(cluster_id, export_config=inbuf,
                                            earmark_resolver=earmark_resolver)

    @CLICommand('nfs cluster create', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_create(self,
                                cluster_id: str,
                                placement: Optional[str] = None,
                                ingress: Optional[bool] = None,
                                virtual_ip: Optional[str] = None,
                                ingress_mode: Optional[IngressType] = None,
                                port: Optional[int] = None,
                                inbuf: Optional[str] = None) -> None:
        """Create an NFS Cluster"""
        ssl_cert = ssl_key = ssl_ca_cert = tls_min_version = tls_ciphers = None
        ssl = tls_ktls = tls_debug = False
        if inbuf:
            config = yaml.safe_load(inbuf)
            ssl = config.get('ssl')
            ssl_cert = config.get('ssl_cert')
            ssl_key = config.get('ssl_key')
            ssl_ca_cert = config.get('ssl_ca_cert')
            tls_min_version = config.get('tls_min_version')
            tls_ktls = config.get('tls_ktls')
            tls_debug = config.get('tls_debug')
            tls_ciphers = config.get('tls_ciphers')

        return self.nfs.create_nfs_cluster(cluster_id=cluster_id, placement=placement,
                                           virtual_ip=virtual_ip, ingress=ingress,
                                           ingress_mode=ingress_mode, port=port,
                                           ssl=ssl,
                                           ssl_cert=ssl_cert,
                                           ssl_key=ssl_key,
                                           ssl_ca_cert=ssl_ca_cert,
                                           tls_ktls=tls_ktls,
                                           tls_debug=tls_debug,
                                           tls_min_version=tls_min_version,
                                           tls_ciphers=tls_ciphers)

    @CLICommand('nfs cluster rm', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_rm(self, cluster_id: str) -> None:
        """Removes an NFS Cluster"""
        return self.nfs.delete_nfs_cluster(cluster_id=cluster_id)

    @CLICommand('nfs cluster delete', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_delete(self, cluster_id: str) -> None:
        """Removes an NFS Cluster (DEPRECATED)"""
        return self.nfs.delete_nfs_cluster(cluster_id=cluster_id)

    @CLICommand('nfs cluster ls', perm='r')
    @object_format.Responder()
    def _cmd_nfs_cluster_ls(self) -> List[str]:
        """List NFS Clusters"""
        return self.nfs.list_nfs_cluster()

    @CLICommand('nfs cluster info', perm='r')
    @object_format.Responder()
    def _cmd_nfs_cluster_info(self, cluster_id: Optional[str] = None) -> Dict[str, Any]:
        """Displays NFS Cluster info"""
        return self.nfs.show_nfs_cluster_info(cluster_id=cluster_id)

    @CLICommand('nfs cluster config get', perm='r')
    @object_format.ErrorResponseHandler()
    def _cmd_nfs_cluster_config_get(self, cluster_id: str) -> Tuple[int, str, str]:
        """Fetch NFS-Ganesha config"""
        conf = self.nfs.get_nfs_cluster_config(cluster_id=cluster_id)
        return 0, conf, ""

    @CLICommand('nfs cluster config set', perm='rw')
    @CLICheckNonemptyFileInput(desc='NFS-Ganesha Configuration')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_config_set(self, cluster_id: str, inbuf: str) -> None:
        """Set NFS-Ganesha config by `-i <config_file>`"""
        return self.nfs.set_nfs_cluster_config(cluster_id=cluster_id, nfs_config=inbuf)

    @CLICommand('nfs cluster config reset', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_config_reset(self, cluster_id: str) -> None:
        """Reset NFS-Ganesha Config to default"""
        return self.nfs.reset_nfs_cluster_config(cluster_id=cluster_id)

    def fetch_nfs_export_obj(self) -> ExportMgr:
        return self.export_mgr

    def export_ls(self, cluster_id: Optional[str] = None, detailed: bool = False) -> List[Dict[Any, Any]]:
        if not (cluster_id):
            return self.export_mgr.list_all_exports()
        return self.export_mgr.list_exports(cluster_id, detailed)

    def export_get(self, cluster_id: str, export_id: int) -> Optional[Dict[str, Any]]:
        return self.export_mgr.get_export_by_id(cluster_id, export_id)

    def export_rm(self, cluster_id: str, pseudo: str) -> None:
        self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo)

    def cluster_ls(self) -> List[str]:
        return available_clusters(self)

    def cluster_info(self, cluster_id: Optional[str] = None) -> Dict[str, Any]:
        return self.nfs.show_nfs_cluster_info(cluster_id=cluster_id)

    def fetch_nfs_cluster_obj(self) -> NFSCluster:
        return self.nfs

    @CLICommand('nfs export qos enable bandwidth_control', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_export_qos_bw_enable(self,
                                  cluster_id: str,
                                  pseudo_path: str,
                                  combined_rw_bw_ctrl: bool = False,
                                  max_export_write_bw: str = '0',
                                  max_export_read_bw: str = '0',
                                  max_client_write_bw: str = '0',
                                  max_client_read_bw: str = '0',
                                  max_export_combined_bw: str = '0',
                                  max_client_combined_bw: str = '0'
                                  ) -> None:
        """enable QOS config for NFS export and set different bandwidth"""
        try:
            bw_obj = QOSBandwidthControl(enable_bw_ctrl=True,
                                         combined_bw_ctrl=combined_rw_bw_ctrl,
                                         export_writebw=max_export_write_bw,
                                         export_readbw=max_export_read_bw,
                                         client_writebw=max_client_write_bw,
                                         client_readbw=max_client_read_bw,
                                         export_rw_bw=max_export_combined_bw,
                                         client_rw_bw=max_client_combined_bw)
        except Exception as e:
            raise object_format.ErrorResponse.wrap(e)
        return self.export_mgr.enable_export_qos_bw(cluster_id=cluster_id,
                                                    pseudo_path=pseudo_path,
                                                    bw_obj=bw_obj)

    @CLICommand('nfs export qos get', perm='r')
    @object_format.Responder()
    def _cmd_export_qos_get(self, cluster_id: str, pseudo_path: str) -> Dict[str, int]:
        """Get NFS export QOS config"""
        return self.export_mgr.get_export_qos(cluster_id, pseudo_path)

    @CLICommand('nfs export qos disable bandwidth_control', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_export_qos_bw_disable(self, cluster_id: str, pseudo_path: str) -> None:
        """Disable NFS export QOS config"""
        return self.export_mgr.disable_export_qos_bw(cluster_id, pseudo_path)

    @CLICommand('nfs cluster qos enable bandwidth_control', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_cluster_qos_bw_enable(self,
                                   cluster_id: str,
                                   qos_type: UserQoSType,
                                   combined_rw_bw_ctrl: bool = False,
                                   max_export_write_bw: str = '0',
                                   max_export_read_bw: str = '0',
                                   max_client_write_bw: str = '0',
                                   max_client_read_bw: str = '0',
                                   max_export_combined_bw: str = '0',
                                   max_client_combined_bw: str = '0') -> None:
        """Enable QOS ratelimiting for NFS cluster and set default export and client max bandwidth"""
        try:
            bw_obj = QOSBandwidthControl(enable_bw_ctrl=True,
                                         combined_bw_ctrl=combined_rw_bw_ctrl,
                                         export_writebw=max_export_write_bw,
                                         export_readbw=max_export_read_bw,
                                         client_writebw=max_client_write_bw,
                                         client_readbw=max_client_read_bw,
                                         export_rw_bw=max_export_combined_bw,
                                         client_rw_bw=max_client_combined_bw)
        except Exception as e:
            raise object_format.ErrorResponse.wrap(e)
        return self.nfs.enable_cluster_qos_bw(cluster_id=cluster_id,
                                              qos_type=QOSType[qos_type.value],
                                              bw_obj=bw_obj)

    @CLICommand('nfs cluster qos disable bandwidth_control', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_cluster_qos_bw_disable(self, cluster_id: str) -> None:
        """Disable QOS for NFS cluster"""
        return self.nfs.disable_cluster_qos_bw(cluster_id)

    @CLICommand('nfs cluster qos get', perm='r')
    @object_format.Responder()
    def _cmd_cluster_qos_get(self, cluster_id: str) -> Dict[str, Any]:
        """Get QOS configuration of NFS cluster"""
        return self.nfs.get_cluster_qos(cluster_id)
