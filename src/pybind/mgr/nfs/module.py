import logging
import threading
from typing import Tuple, Optional, List, Dict, Any
import yaml

from .cli import NFSCLICommand

from mgr_module import MgrModule, Option, CLICheckNonemptyFileInput
import object_format
import orchestrator
from orchestrator.module import IngressType
from mgr_util import CephFSEarmarkResolver

from .export import ExportMgr, AppliedExportResults
from .cluster import NFSCluster
from .utils import available_clusters, cephfs_client_for_mgr
from .qos_conf import QOSType, QOSBandwidthControl, UserQoSType, QOSOpsControl

log = logging.getLogger(__name__)


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    CLICommand = NFSCLICommand
    MODULE_OPTIONS: List[Option] = []

    def __init__(self, *args: str, **kwargs: Any) -> None:
        self.inited = False
        self.lock = threading.Lock()
        super(Module, self).__init__(*args, **kwargs)
        with self.lock:
            self.export_mgr = ExportMgr(self)
            self.nfs = NFSCluster(self)
            self.inited = True

    @NFSCLICommand('nfs export create cephfs', perm='rw')
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
            xprtsec: Optional[str] = None,
            cmount_path: Optional[str] = "/",
            transports: Optional[List[str]] = None,
            skip_notify_nfs_server: bool = False
    ) -> Dict[str, Any]:
        """Create a CephFS export"""
        self.export_mgr.skip_notify_nfs_server = skip_notify_nfs_server
        earmark_resolver = CephFSEarmarkResolver(
            self, client=cephfs_client_for_mgr(self))
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
            xprtsec=xprtsec,
            cmount_path=cmount_path,
            transports=transports,
            earmark_resolver=earmark_resolver
        )

    @NFSCLICommand('nfs export create rgw', perm='rw')
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
            xprtsec: Optional[str] = None,
            transports: Optional[List[str]] = None,
            skip_notify_nfs_server: bool = False
    ) -> Dict[str, Any]:
        """Create an RGW export"""
        self.export_mgr.skip_notify_nfs_server = skip_notify_nfs_server
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
            xprtsec=xprtsec,
            transports=transports,
        )

    @NFSCLICommand('nfs export rm', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_export_rm(self,
                           cluster_id: str,
                           pseudo_path: str,
                           skip_notify_nfs_server: bool = False) -> None:
        """Remove a cephfs export"""
        self.export_mgr.skip_notify_nfs_server = skip_notify_nfs_server
        return self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @NFSCLICommand('nfs export delete', perm='rw')
    @object_format.ErrorResponseHandler()
    def _cmd_nfs_export_delete(self,
                               cluster_id: str,
                               pseudo_path: str,
                               skip_notify_nfs_server: bool = False) -> Tuple[int, str, str]:
        """Delete a cephfs export (DEPRECATED)"""
        self.export_mgr.skip_notify_nfs_server = skip_notify_nfs_server
        self.export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=pseudo_path)
        return 0, "", ("`nfs export delete` is deprecated and will be removed "
                       "in a future release, please `nfs export rm` instead.")

    @NFSCLICommand('nfs export ls', perm='r')
    @object_format.Responder()
    def _cmd_nfs_export_ls(self, cluster_id: str, detailed: bool = False) -> List[Any]:
        """List exports of a NFS cluster"""
        return self.export_mgr.list_exports(cluster_id=cluster_id, detailed=detailed)

    @NFSCLICommand('nfs export info', perm='r')
    @object_format.Responder()
    def _cmd_nfs_export_info(self, cluster_id: str, pseudo_path: str) -> Dict[str, Any]:
        """Fetch a export of a NFS cluster given the pseudo path/binding"""
        return self.export_mgr.get_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @NFSCLICommand('nfs export get', perm='r')
    @object_format.Responder()
    def _cmd_nfs_export_get(self, cluster_id: str, pseudo_path: str) -> Dict[str, Any]:
        """Fetch a export of a NFS cluster given the pseudo path/binding (DEPRECATED)"""
        return self.export_mgr.get_export(cluster_id=cluster_id, pseudo_path=pseudo_path)

    @NFSCLICommand('nfs export apply', perm='rw')
    @CLICheckNonemptyFileInput(desc='Export JSON or Ganesha EXPORT specification')
    @object_format.Responder()
    def _cmd_nfs_export_apply(self,
                              cluster_id: str,
                              inbuf: str,
                              skip_notify_nfs_server: bool = False) -> AppliedExportResults:
        """Create or update an export by `-i <json_or_ganesha_export_file>`"""
        earmark_resolver = CephFSEarmarkResolver(
            self, client=cephfs_client_for_mgr(self))
        self.export_mgr.skip_notify_nfs_server = skip_notify_nfs_server
        return self.export_mgr.apply_export(cluster_id, export_config=inbuf,
                                            earmark_resolver=earmark_resolver)

    @NFSCLICommand('nfs cluster create', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_create(self,
                                cluster_id: str,
                                placement: Optional[str] = None,
                                ingress: Optional[bool] = None,
                                virtual_ip: Optional[str] = None,
                                ingress_mode: Optional[IngressType] = None,
                                port: Optional[int] = None,
                                bind_addrs: Optional[str] = None,
                                monitoring_addrs: Optional[str] = None,
                                monitoring_port: Optional[int] = None,
                                enable_rdma: bool = False,
                                rdma_port: Optional[int] = None,
                                enable_nfsv3: bool = False,
                                ingress_placement: Optional[str] = None,
                                inbuf: Optional[str] = None) -> None:
        """Create an NFS Cluster"""
        cluster_qos_config = None
        ssl_cert = ssl_key = ssl_ca_cert = tls_min_version = tls_ciphers = None
        ssl = tls_ktls = tls_debug = False
        if inbuf:
            config = yaml.safe_load(inbuf)
            cluster_qos_config = config.get('cluster_qos_config')
            ssl = config.get('ssl')
            ssl_cert = config.get('ssl_cert')
            ssl_key = config.get('ssl_key')
            ssl_ca_cert = config.get('ssl_ca_cert')
            tls_min_version = config.get('tls_min_version')
            tls_ktls = config.get('tls_ktls')
            tls_debug = config.get('tls_debug')
            tls_ciphers = config.get('tls_ciphers')

        # Parse bind_addrs and monitoring_addrs from CLI format
        ip_addrs = None
        if bind_addrs:
            ip_addrs = {}
            for pair in bind_addrs.split(','):
                if ':' in pair:
                    host, ip = pair.split(':', 1)
                    ip_addrs[host.strip()] = ip.strip()

        monitoring_ip_addrs = None
        if monitoring_addrs:
            monitoring_ip_addrs = {}
            for pair in monitoring_addrs.split(','):
                if ':' in pair:
                    host, ip = pair.split(':', 1)
                    monitoring_ip_addrs[host.strip()] = ip.strip()

        return self.nfs.create_nfs_cluster(cluster_id=cluster_id, placement=placement,
                                           virtual_ip=virtual_ip, ingress=ingress,
                                           ingress_mode=ingress_mode, port=port,
                                           cluster_qos_config=cluster_qos_config,
                                           enable_nfsv3=enable_nfsv3,
                                           ip_addrs=ip_addrs,
                                           monitoring_ip_addrs=monitoring_ip_addrs,
                                           monitoring_port=monitoring_port,
                                           ssl=ssl,
                                           ssl_cert=ssl_cert,
                                           ssl_key=ssl_key,
                                           ssl_ca_cert=ssl_ca_cert,
                                           tls_ktls=tls_ktls,
                                           tls_debug=tls_debug,
                                           tls_min_version=tls_min_version,
                                           tls_ciphers=tls_ciphers,
                                           enable_rdma=enable_rdma,
                                           rdma_port=rdma_port,
                                           ingress_placement=ingress_placement)

    @NFSCLICommand('nfs cluster rm', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_rm(self, cluster_id: str) -> None:
        """Removes an NFS Cluster"""
        return self.nfs.delete_nfs_cluster(cluster_id=cluster_id)

    @NFSCLICommand('nfs cluster delete', perm='rw')
    @object_format.ErrorResponseHandler()
    def _cmd_nfs_cluster_delete(self, cluster_id: str) -> Tuple[int, str, str]:
        """Removes an NFS Cluster (DEPRECATED)"""
        self.nfs.delete_nfs_cluster(cluster_id=cluster_id)
        return 0, "", ("`nfs cluster delete` is deprecated and will be removed "
                       "in a future release, please `nfs cluster rm` instead.")

    @NFSCLICommand('nfs cluster ls', perm='r')
    @object_format.Responder()
    def _cmd_nfs_cluster_ls(self) -> List[str]:
        """List NFS Clusters"""
        return self.nfs.list_nfs_cluster()

    @NFSCLICommand('nfs cluster info', perm='r')
    @object_format.Responder()
    def _cmd_nfs_cluster_info(self, cluster_id: Optional[str] = None) -> Dict[str, Any]:
        """Displays NFS Cluster info"""
        return self.nfs.show_nfs_cluster_info(cluster_id=cluster_id)

    @NFSCLICommand('nfs cluster config get', perm='r')
    @object_format.ErrorResponseHandler()
    def _cmd_nfs_cluster_config_get(self, cluster_id: str) -> Tuple[int, str, str]:
        """Fetch NFS-Ganesha config"""
        conf = self.nfs.get_nfs_cluster_config(cluster_id=cluster_id)
        return 0, conf, ""

    @NFSCLICommand('nfs cluster config set', perm='rw')
    @CLICheckNonemptyFileInput(desc='NFS-Ganesha Configuration')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_config_set(self, cluster_id: str, inbuf: str) -> None:
        """Set NFS-Ganesha config by `-i <config_file>`"""
        return self.nfs.set_nfs_cluster_config(cluster_id=cluster_id, nfs_config=inbuf)

    @NFSCLICommand('nfs cluster config reset', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_config_reset(self, cluster_id: str) -> None:
        """Reset NFS-Ganesha Config to default"""
        return self.nfs.reset_nfs_cluster_config(cluster_id=cluster_id)

    def export_apply(self, cluster_id: str, export_config: str) -> AppliedExportResults:
        """Create or update an export by `export_config` which can be json string or ganesha export specification"""
        earmark_resolver = CephFSEarmarkResolver(
            self, client=cephfs_client_for_mgr(self))
        return self.export_mgr.apply_export(cluster_id, export_config=export_config,
                                            earmark_resolver=earmark_resolver)

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
                                  max_client_combined_bw: str = '0',
                                  skip_notify_nfs_server: bool = False
                                  ) -> None:
        """enable QOS bandwidth control for NFS export and set different bandwidth"""
        try:
            self.export_mgr.skip_notify_nfs_server = skip_notify_nfs_server
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
    def _cmd_export_qos_bw_disable(self,
                                   cluster_id: str,
                                   pseudo_path: str,
                                   skip_notify_nfs_server: bool = False) -> None:
        """Disable NFS export QOS bandwidth control"""
        self.export_mgr.skip_notify_nfs_server = skip_notify_nfs_server
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
        """Enable QOS bandwidth control for NFS cluster and set default export and client max bandwidth"""
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
        """Disable QOS bandwidth control for NFS cluster"""
        return self.nfs.disable_cluster_qos_bw(cluster_id)

    @CLICommand('nfs cluster qos set', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_nfs_cluster_global_qos(self,
                                    cluster_id: str,
                                    msg_interval: int) -> None:
        """Set the message interval for cluster QoS synchronization among hosts."""
        return self.nfs.cluster_qos_set_config(cluster_id, msg_interval)

    @CLICommand('nfs cluster qos get', perm='r')
    @object_format.Responder()
    def _cmd_cluster_qos_get(self, cluster_id: str) -> Dict[str, Any]:
        """Get QOS configuration of NFS cluster"""
        return self.nfs.get_cluster_qos(cluster_id)

    @CLICommand('nfs export qos enable ops_control', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_export_qos_ops_enable(self,
                                   cluster_id: str,
                                   pseudo_path: str,
                                   max_export_iops: int = 0,
                                   max_client_iops: int = 0,
                                   skip_notify_nfs_server: bool = False
                                   ) -> None:
        """enable QOS IOPS control for NFS export"""
        try:
            self.export_mgr.skip_notify_nfs_server = skip_notify_nfs_server
            ops_obj = QOSOpsControl(enable_iops_ctrl=True,
                                    max_export_iops=max_export_iops,
                                    max_client_iops=max_client_iops)
        except Exception as e:
            raise object_format.ErrorResponse.wrap(e)
        return self.export_mgr.enable_export_qos_ops(cluster_id=cluster_id,
                                                     pseudo_path=pseudo_path,
                                                     ops_obj=ops_obj)

    @CLICommand('nfs export qos disable ops_control', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_export_qos_ops_disable(self,
                                    cluster_id: str,
                                    pseudo_path: str,
                                    skip_notify_nfs_server: bool = False) -> None:
        """Disable NFS export QOS IOPS control"""
        self.export_mgr.skip_notify_nfs_server = skip_notify_nfs_server
        return self.export_mgr.disable_export_qos_ops(cluster_id, pseudo_path)

    @CLICommand('nfs cluster qos enable ops_control', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_cluster_qos_ops_enable(self,
                                    cluster_id: str,
                                    qos_type: UserQoSType,
                                    max_export_iops: int = 0,
                                    max_client_iops: int = 0,
                                    ) -> None:
        """enable QOS IOPS control for NFS cluster"""
        try:
            ops_obj = QOSOpsControl(enable_iops_ctrl=True,
                                    max_export_iops=max_export_iops,
                                    max_client_iops=max_client_iops)
        except Exception as e:
            raise object_format.ErrorResponse.wrap(e)
        return self.nfs.enable_cluster_qos_ops(cluster_id=cluster_id,
                                               qos_type=QOSType[qos_type.value],
                                               ops_obj=ops_obj)

    @CLICommand('nfs cluster qos disable ops_control', perm='rw')
    @object_format.EmptyResponder()
    def _cmd_cluster_qos_ops_disable(self, cluster_id: str) -> None:
        """Disable NFS cluster QOS IOPS control"""
        return self.nfs.disable_cluster_qos_ops(cluster_id)

    def _load_nfs_grpc_certs(self, cluster_id: str):
        """Get NFS gRPC client certs from cephadm's cert_mgr via remote call.

        Uses self.remote('cephadm', ...) — the standard Ceph way for one
        MGR module to call a function in another MGR module. cert_mgr lives in
        cephadm so we call across the module boundary. No filesystem access needed.
        Returns (ca_cert_bytes, client_cert_bytes, client_key_bytes) or (None, None, None).
        """
        try:
            certs = self.remote('cephadm', 'get_nfs_grpc_certs', cluster_id)
            ca   = certs['ca_cert'].encode()     if isinstance(certs['ca_cert'], str)     else certs['ca_cert']
            cert = certs['client_cert'].encode() if isinstance(certs['client_cert'], str) else certs['client_cert']
            key  = certs['client_key'].encode()  if isinstance(certs['client_key'], str)  else certs['client_key']
            return ca, cert, key
        except Exception as e:
            log.warning(f'Could not load NFS gRPC certs via cephadm remote: {e}')
            return None, None, None

    @CLICommand('nfs grpc test', perm='r')
    @object_format.Responder()
    def _cmd_nfs_grpc_test(self,
                           cluster_id: str,
                           ca_cert: Optional[str] = None,
                           client_cert: Optional[str] = None,
                           client_key: Optional[str] = None) -> Dict[str, Any]:
        """Test gRPC connectivity to NFS-Ganesha by calling GetGracePeriod.

        Automatically reads certs from cert_mgr via cephadm remote call.
        Optionally pass cert contents as base64 strings to override.

        Usage: ceph nfs grpc test <cluster_id>
        """
        import base64
        if ca_cert and client_cert and client_key:
            ca_bytes   = base64.b64decode(ca_cert)
            cert_bytes = base64.b64decode(client_cert)
            key_bytes  = base64.b64decode(client_key)
        else:
            ca_bytes, cert_bytes, key_bytes = self._load_nfs_grpc_certs(cluster_id)
        return self.nfs.test_grpc_connection(
            cluster_id,
            ca_cert=ca_bytes,
            client_cert=cert_bytes,
            client_key=key_bytes,
        )

    @CLICommand('nfs ok-to-upgrade', perm='r')
    @object_format.Responder()
    def _cmd_nfs_ok_to_upgrade(self,
                                cluster_id: str,
                                daemon_name: str,
                                already_upgraded: str = '') -> Dict[str, Any]:
        """Ask NFS-Ganesha if a daemon is safe to upgrade via TSM gRPC.

        Automatically reads certs from cert_mgr. No manual cert passing needed.

        Usage: ceph nfs ok-to-upgrade <cluster_id> <daemon_name>
        Example: ceph nfs ok-to-upgrade mynfs nfs.mynfs.0.0.ceph-node-0.xxx
        """
        upgraded_list = [d.strip() for d in already_upgraded.split(',') if d.strip()]
        ca, cert, key = self._load_nfs_grpc_certs(cluster_id)
        return self.nfs.grpc_ok_to_upgrade(
            cluster_id, daemon_name, upgraded_list,
            ca_cert=ca, client_cert=cert, client_key=key
        )
