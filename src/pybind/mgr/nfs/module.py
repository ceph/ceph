import logging
import threading
from typing import Tuple, Optional, List, Dict, Any, Union
import yaml

from .cli import NFSCLICommand

from ceph.deployment.service_spec import PlacementSpec, SpecValidationError
from mgr_module import MgrModule, Option, CLICheckNonemptyFileInput
import object_format
import orchestrator
from orchestrator.module import IngressType
from mgr_util import CephFSEarmarkResolver

from .export import ExportMgr, AppliedExportResults
from .cluster import NFSCluster
from .exception import NFSInvalidOperation
from .utils import available_clusters, cephfs_client_for_mgr
from .qos_conf import QOSType, QOSBandwidthControl, UserQoSType, QOSOpsControl

log = logging.getLogger(__name__)


def _parse_host_ip_pairs(addr_pairs: Optional[str]) -> Optional[Dict[str, str]]:
    """Parse CLI 'host:ip,host:ip' strings into a host->ip mapping."""
    if not addr_pairs:
        return None
    ip_addrs: Dict[str, str] = {}
    for pair in addr_pairs.split(','):
        if ':' in pair:
            host, ip = pair.split(':', 1)
            ip_addrs[host.strip()] = ip.strip()
    return ip_addrs


def _normalize_placement(placement: Optional[Union[str, Dict]]) -> Optional[str]:
    """Normalise a placement that may arrive as a dict (JSON, from the dashboard)
    or as a placement string (from the CLI).  Dicts are parsed and validated by
    PlacementSpec.from_json() and returned in the string form accepted by
    PlacementSpec.from_string()."""
    if placement is None or isinstance(placement, str):
        return placement
    if not isinstance(placement, dict):
        raise NFSInvalidOperation(f"Invalid placement: {placement!r}")
    try:
        return PlacementSpec.from_json(placement).pretty_str()
    except SpecValidationError as e:
        raise NFSInvalidOperation(f"Invalid placement: {e}")


def _normalize_bind_addrs(
        bind_addrs: Optional[Union[str, List[Dict[str, str]]]]
) -> Optional[str]:
    """Normalise bind addresses to the CLI 'host:ip,host:ip' string that
    _parse_host_ip_pairs() and create_nfs_cluster() expect.

    Accepts either:
    * a pre-formatted string (CLI path) — returned as-is
    * a list of dicts with 'hostname'/'host' and 'ip'/'address' keys (dashboard path)
    """
    if not bind_addrs:
        return None
    if isinstance(bind_addrs, str):
        return bind_addrs
    pairs = []
    for entry in bind_addrs:
        hostname = (entry.get('hostname') or entry.get('host') or '').strip()
        ip = (entry.get('ip') or entry.get('address') or '').strip()
        if hostname and ip:
            pairs.append(f'{hostname}:{ip}')
    return ','.join(pairs) if pairs else None


def _normalize_networks(
        networks: Optional[Union[str, List[str]]]
) -> Optional[List[str]]:
    """Strip whitespace from CIDR entries.  Accepts a bare string or a list."""
    if networks is None:
        return None
    if isinstance(networks, str):
        networks = [networks]
    cleaned = [n.strip() for n in networks if n and str(n).strip()]
    return cleaned or None


def _normalize_ingress_mode(
        mode: Optional[Union[str, 'IngressType']]
) -> Optional['IngressType']:
    """Accept an IngressType or its string representation."""
    if mode is None:
        return None
    if isinstance(mode, IngressType):
        return mode
    try:
        return IngressType(mode)
    except ValueError:
        pass
    try:
        return IngressType[mode.replace('-', '_')]
    except KeyError:
        raise NFSInvalidOperation(f"Invalid ingress_mode: {mode}")


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
            skip_notify_nfs_server: bool = False,
            kmip_key_id: Optional[str] = None
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
            earmark_resolver=earmark_resolver,
            kmip_key_id=kmip_key_id
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
            skip_notify_nfs_server: bool = False,
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
            transports=transports
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

    @NFSCLICommand('nfs cluster rotate-key', perm='rw')
    @object_format.Responder()
    def _cmd_nfs_cluster_rotate_key(self,
                                    cluster_id: str,
                                    all_daemon_and_export_keys: bool = False,
                                    auth_entities: Optional[List[str]] = None,
                                    key_type: Optional[str] = None) -> Dict[str, Any]:
        """Rotate NFS cluster/daemon and export auth keys; redeploy service if daemon keys change"""
        return self.nfs.rotate_keys(
            cluster_id=cluster_id,
            all_daemon_and_export_keys=all_daemon_and_export_keys,
            auth_entities=auth_entities,
            key_type=key_type
        )

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
                                enable_nfs_metrics: bool = False,
                                ingress_placement: Optional[str] = None,
                                networks: Optional[List[str]] = None,
                                inbuf: Optional[str] = None,
                                ) -> None:
        """Create an NFS Cluster"""
        cluster_qos_config = None
        kmip_cert = kmip_key = kmip_ca_cert = kmip_host_list = None
        ssl_cert = ssl_key = ssl_ca_cert = tls_min_version = tls_ciphers = None
        ssl = tls_ktls = tls_debug = False
        if inbuf:
            config = yaml.safe_load(inbuf)
            cluster_qos_config = config.get('cluster_qos_config')
            kmip_cert = config.get('kmip_cert')
            kmip_key = config.get('kmip_key')
            kmip_ca_cert = config.get('kmip_ca_cert')
            kmip_host_list = config.get('kmip_host_list')
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
                                           cluster_qos_config=cluster_qos_config,
                                           enable_nfsv3=enable_nfsv3,
                                           enable_nfs_metrics=enable_nfs_metrics,
                                           ip_addrs=_parse_host_ip_pairs(bind_addrs),
                                           monitoring_ip_addrs=_parse_host_ip_pairs(monitoring_addrs),
                                           monitoring_port=monitoring_port,
                                           kmip_cert=kmip_cert,
                                           kmip_key=kmip_key,
                                           kmip_ca_cert=kmip_ca_cert,
                                           kmip_host_list=kmip_host_list,
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
                                           ingress_placement=ingress_placement,
                                           networks=networks)

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

    def cluster_create(self,
                       cluster_id: str,
                       placement: Optional[Union[str, Dict]] = None,
                       ingress: Optional[bool] = None,
                       virtual_ip: Optional[str] = None,
                       ingress_mode: Optional[Union[str, IngressType]] = None,
                       port: Optional[int] = None,
                       bind_addrs: Optional[Union[str, List[Dict[str, str]]]] = None,
                       monitoring_addrs: Optional[Union[str, List[Dict[str, str]]]] = None,
                       monitoring_port: Optional[int] = None,
                       enable_rdma: bool = False,
                       rdma_port: Optional[int] = None,
                       enable_nfsv3: bool = False,
                       ingress_placement: Optional[Union[str, Dict]] = None,
                       networks: Optional[Union[str, List[str]]] = None) -> None:
        """Python API for creating an NFS cluster (no CLI EmptyResponder wrapper).

        All placement/address/network parameters accept either their native JSON
        forms (dicts and lists, as sent by the dashboard) or the pre-formatted
        strings used by the CLI path.  Normalisation is done here so that every
        caller shares one code path.
        """
        return self.nfs.create_nfs_cluster(
            cluster_id=cluster_id,
            placement=_normalize_placement(placement),
            virtual_ip=virtual_ip,
            ingress=ingress,
            ingress_mode=_normalize_ingress_mode(ingress_mode),
            port=port,
            enable_nfsv3=enable_nfsv3,
            ip_addrs=_parse_host_ip_pairs(_normalize_bind_addrs(bind_addrs)),
            monitoring_ip_addrs=_parse_host_ip_pairs(_normalize_bind_addrs(monitoring_addrs)),
            monitoring_port=monitoring_port,
            enable_rdma=enable_rdma,
            rdma_port=rdma_port,
            ingress_placement=_normalize_placement(ingress_placement),
            networks=_normalize_networks(networks),
        )

    def cluster_rm(self, cluster_id: str) -> None:
        """Python API for removing an NFS cluster (no CLI EmptyResponder wrapper)."""
        return self.nfs.delete_nfs_cluster(cluster_id=cluster_id)

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
