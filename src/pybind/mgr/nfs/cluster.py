import ipaddress
import logging
import re
import socket
from typing import cast, Dict, List, Any, Union, Optional, TYPE_CHECKING
from enum import Enum

from mgr_module import NFS_POOL_NAME as POOL_NAME
from ceph.deployment.service_spec import NFSServiceSpec, PlacementSpec, IngressSpec
from object_format import ErrorResponse

import orchestrator
from orchestrator.module import IngressType

from .exception import NFSInvalidOperation, ClusterNotFound
from .utils import (
    ManualRestartRequired,
    NonFatalError,
    available_clusters,
    conf_obj_name,
    restart_nfs_service,
    user_conf_obj_name,
    USER_CONF_PREFIX,
    qos_conf_obj_name)
from .rados_utils import NFSRados
from .ganesha_conf import format_block, GaneshaConfParser
from .qos_conf import (
    QOS,
    QOSType,
    QOSBandwidthControl,
    QOSOpsControl,
    QOSParams,
    validate_clust_qos_msg_interval)

if TYPE_CHECKING:
    from nfs.module import Module
    from mgr_module import MgrModule


log = logging.getLogger(__name__)


class ClusterQosAction(Enum):
    enable = 'enable'
    disable = 'disable'


def resolve_ip(hostname: str) -> str:
    try:
        r = socket.getaddrinfo(hostname, None, flags=socket.AI_CANONNAME,
                               type=socket.SOCK_STREAM)
        # pick first v4 IP, if present
        for a in r:
            if a[0] == socket.AF_INET:
                return a[4][0]
        return r[0][4][0]
    except socket.gaierror as e:
        raise NFSInvalidOperation(f"Cannot resolve IP for host {hostname}: {e}")


def create_ganesha_pool(mgr: 'MgrModule') -> None:
    pool_list = [p['pool_name'] for p in mgr.get_osdmap().dump().get('pools', [])]
    if POOL_NAME not in pool_list:
        mgr.check_mon_command({'prefix': 'osd pool create',
                               'pool': POOL_NAME,
                               'yes_i_really_mean_it': True})
        mgr.check_mon_command({'prefix': 'osd pool application enable',
                               'pool': POOL_NAME,
                               'app': 'nfs'})
        log.debug("Successfully created nfs-ganesha pool %s", POOL_NAME)


def config_cluster_qos_from_dict(
    mgr: 'MgrModule',
    cluster_id: str,
    qos_dict: Dict[str, Union[str, bool, int]],
    update_existing_obj: bool = False,
) -> None:
    qos_type = qos_dict.get(QOSParams.qos_type.value)
    if not qos_type:
        raise NFSInvalidOperation('qos_type is not specified in qos dict')
    qos_type = QOSType[str(qos_type)]
    enable_cluster_qos = qos_dict.get(QOSParams.enable_cluster_qos.value, True)
    clust_qos_msg_interval = int(qos_dict.get(QOSParams.clust_qos_msg_interval.value, 0))
    assert isinstance(enable_cluster_qos, (bool, type(None)))
    enable_bw_ctrl = qos_dict.get(QOSParams.enable_bw_ctrl.value)
    combined_bw_ctrl = qos_dict.get(QOSParams.combined_bw_ctrl.value)
    enable_iops_ctrl = qos_dict.get(QOSParams.enable_iops_ctrl.value)
    bw_obj = ops_obj = None
    if enable_bw_ctrl:
        bw_obj = QOSBandwidthControl(
            bool(enable_bw_ctrl),
            bool(combined_bw_ctrl),
            export_writebw=str(qos_dict.get(QOSParams.export_writebw.value, "0")),
            export_readbw=str(qos_dict.get(QOSParams.export_readbw.value, "0")),
            client_writebw=str(qos_dict.get(QOSParams.client_writebw.value, "0")),
            client_readbw=str(qos_dict.get(QOSParams.client_readbw.value, "0")),
            export_rw_bw=str(qos_dict.get(QOSParams.export_rw_bw.value, "0")),
            client_rw_bw=str(qos_dict.get(QOSParams.client_rw_bw.value, "0")),
        )
        bw_obj.qos_bandwidth_checks(qos_type)
    if enable_iops_ctrl:
        ops_obj = QOSOpsControl(
            bool(enable_iops_ctrl),
            max_export_iops=int(qos_dict.get(QOSParams.max_export_iops.value, 0)),
            max_client_iops=int(qos_dict.get(QOSParams.max_client_iops.value, 0)),
        )
        ops_obj.qos_ops_checks(qos_type)

    write_cluster_qos_obj(
        mgr=mgr,
        cluster_id=cluster_id,
        qos_obj=None,
        enable_qos=True,
        enable_cluster_qos=enable_cluster_qos,
        clust_qos_msg_interval=clust_qos_msg_interval,
        qos_type=qos_type,
        bw_obj=bw_obj,
        ops_obj=ops_obj,
        update_existing_obj=update_existing_obj
    )


def write_cluster_qos_obj(
    mgr: 'MgrModule',
    cluster_id: str,
    qos_obj: Optional[QOS],
    enable_qos: bool,
    enable_cluster_qos: Optional[bool] = None,
    clust_qos_msg_interval: int = 0,
    qos_type: Optional[QOSType] = None,
    bw_obj: Optional[QOSBandwidthControl] = None,
    ops_obj: Optional[QOSOpsControl] = None,
    update_existing_obj: bool = False
) -> None:
    qos_obj_exists = False
    if not qos_obj:
        log.debug(f"Creating new QoS block for cluster {cluster_id}")
        qos_obj = QOS(True, enable_qos, enable_cluster_qos, clust_qos_msg_interval, qos_type, bw_obj, ops_obj)
    else:
        log.debug(f"Updating existing QoS block for cluster {cluster_id}")
        qos_obj_exists = True
        qos_obj.enable_qos = enable_qos
        qos_obj.enable_cluster_qos = enable_cluster_qos
        qos_obj.clust_qos_msg_interval = validate_clust_qos_msg_interval(clust_qos_msg_interval)
        qos_obj.qos_type = qos_type
        if bw_obj:
            qos_obj.bw_obj = bw_obj
        if ops_obj:
            qos_obj.ops_obj = ops_obj

    qos_config = format_block(qos_obj.to_qos_block())
    rados_obj = NFSRados(mgr.rados, cluster_id)
    if not qos_obj_exists and not update_existing_obj:
        rados_obj.write_obj(qos_config, qos_conf_obj_name(cluster_id),
                            conf_obj_name(cluster_id))
    else:
        rados_obj.update_obj(qos_config, qos_conf_obj_name(cluster_id),
                             conf_obj_name(cluster_id), should_notify=False)
    log.debug(f"Successfully saved {cluster_id}s QOS bandwidth control config: \n {qos_config}")


class NFSCluster:
    def __init__(self, mgr: 'Module') -> None:
        self.mgr = mgr

    def _call_orch_apply_nfs(
            self,
            cluster_id: str,
            placement: Optional[str] = None,
            virtual_ip: Optional[str] = None,
            ingress_mode: Optional[IngressType] = None,
            port: Optional[int] = None,
            cluster_qos_config: Optional[Dict[str, Union[str, bool, int]]] = None,
            ssl: bool = False,
            ssl_cert: Optional[str] = None,
            ssl_key: Optional[str] = None,
            ssl_ca_cert: Optional[str] = None,
            tls_ktls: bool = False,
            tls_debug: bool = False,
            tls_min_version: Optional[str] = None,
            tls_ciphers: Optional[str] = None,
    ) -> None:
        if not port:
            port = 2049   # default nfs port
        if virtual_ip:
            # nfs + ingress
            # run NFS on non-standard port
            if not ingress_mode:
                ingress_mode = IngressType.default
            ingress_mode = ingress_mode.canonicalize()
            pspec = PlacementSpec.from_string(placement)
            if ingress_mode == IngressType.keepalive_only:
                # enforce count=1 for nfs over keepalive only
                pspec.count = 1

            ganesha_port = 10000 + port  # semi-arbitrary, fix me someday
            frontend_port: Optional[int] = port
            virtual_ip_for_ganesha: Optional[str] = None
            keepalive_only: bool = False
            enable_haproxy_protocol: bool = False
            if ingress_mode != IngressType.haproxy_standard:
                virtual_ip_for_ganesha = virtual_ip.split('/')[0]
            if ingress_mode == IngressType.haproxy_protocol:
                enable_haproxy_protocol = True
            elif ingress_mode == IngressType.keepalive_only:
                keepalive_only = True
                ganesha_port = port
                frontend_port = None

            spec = NFSServiceSpec(service_type='nfs', service_id=cluster_id,
                                  placement=pspec,
                                  # use non-default port so we don't conflict with ingress
                                  port=ganesha_port,
                                  virtual_ip=virtual_ip_for_ganesha,
                                  enable_haproxy_protocol=enable_haproxy_protocol,
                                  cluster_qos_config=cluster_qos_config,
                                  ssl=ssl,
                                  ssl_cert=ssl_cert,
                                  ssl_key=ssl_key,
                                  ssl_ca_cert=ssl_ca_cert,
                                  tls_ktls=tls_ktls,
                                  tls_debug=tls_debug,
                                  tls_min_version=tls_min_version,
                                  tls_ciphers=tls_ciphers)
            completion = self.mgr.apply_nfs(spec)
            orchestrator.raise_if_exception(completion)
            ispec = IngressSpec(service_type='ingress',
                                service_id='nfs.' + cluster_id,
                                backend_service='nfs.' + cluster_id,
                                placement=pspec,
                                frontend_port=frontend_port,
                                monitor_port=7000 + port,   # semi-arbitrary, fix me someday
                                virtual_ip=virtual_ip,
                                keepalive_only=keepalive_only,
                                enable_haproxy_protocol=enable_haproxy_protocol)
            completion = self.mgr.apply_ingress(ispec)
            orchestrator.raise_if_exception(completion)
        else:
            # standalone nfs
            spec = NFSServiceSpec(service_type='nfs', service_id=cluster_id,
                                  placement=PlacementSpec.from_string(placement),
                                  port=port,
                                  cluster_qos_config=cluster_qos_config,
                                  ssl=ssl,
                                  ssl_cert=ssl_cert,
                                  ssl_key=ssl_key,
                                  ssl_ca_cert=ssl_ca_cert,
                                  tls_ktls=tls_ktls,
                                  tls_debug=tls_debug,
                                  tls_min_version=tls_min_version,
                                  tls_ciphers=tls_ciphers)
            completion = self.mgr.apply_nfs(spec)
            orchestrator.raise_if_exception(completion)
        log.debug("Successfully deployed nfs daemons with cluster id %s and placement %s",
                  cluster_id, placement)

    def create_empty_rados_obj(self, cluster_id: str) -> None:
        common_conf = conf_obj_name(cluster_id)
        self._rados(cluster_id).write_obj('', conf_obj_name(cluster_id))
        log.info("Created empty object:%s", common_conf)

    def delete_config_obj(self, cluster_id: str) -> None:
        self._rados(cluster_id).remove_all_obj()
        log.info("Deleted %s object and all objects in %s",
                 conf_obj_name(cluster_id), cluster_id)

    def create_nfs_cluster(
            self,
            cluster_id: str,
            placement: Optional[str],
            virtual_ip: Optional[str],
            ingress: Optional[bool] = None,
            ingress_mode: Optional[IngressType] = None,
            port: Optional[int] = None,
            cluster_qos_config: Optional[Dict[str, Union[str, bool, int]]] = None,
            ssl: bool = False,
            ssl_cert: Optional[str] = None,
            ssl_key: Optional[str] = None,
            ssl_ca_cert: Optional[str] = None,
            tls_ktls: bool = False,
            tls_debug: bool = False,
            tls_min_version: Optional[str] = None,
            tls_ciphers: Optional[str] = None,
    ) -> None:
        try:
            if virtual_ip:
                # validate virtual_ip value: ip_address throws a ValueError
                # exception in case it's not a valid ipv4 or ipv6 address
                ip = virtual_ip.split('/')[0]
                ipaddress.ip_address(ip)
            if virtual_ip and not ingress:
                raise NFSInvalidOperation('virtual_ip can only be provided with ingress enabled')
            if not virtual_ip and ingress:
                raise NFSInvalidOperation('ingress currently requires a virtual_ip')
            if ingress_mode and not ingress:
                raise NFSInvalidOperation('--ingress-mode must be passed along with --ingress')
            invalid_str = re.search('[^A-Za-z0-9-_.]', cluster_id)
            if invalid_str:
                raise NFSInvalidOperation(f"cluster id {cluster_id} is invalid. "
                                          f"{invalid_str.group()} is char not permitted")

            create_ganesha_pool(self.mgr)

            self.create_empty_rados_obj(cluster_id)

            if cluster_id not in available_clusters(self.mgr):
                self._call_orch_apply_nfs(
                    cluster_id,
                    placement,
                    virtual_ip,
                    ingress_mode,
                    port,
                    cluster_qos_config=cluster_qos_config,
                    ssl=ssl,
                    ssl_cert=ssl_cert,
                    ssl_key=ssl_key,
                    ssl_ca_cert=ssl_ca_cert,
                    tls_ktls=tls_ktls,
                    tls_debug=tls_debug,
                    tls_min_version=tls_min_version,
                    tls_ciphers=tls_ciphers
                )
                return
            raise NonFatalError(f"{cluster_id} cluster already exists")
        except Exception as e:
            log.exception(f"NFS Cluster {cluster_id} could not be created")
            raise ErrorResponse.wrap(e)

    def delete_nfs_cluster(self, cluster_id: str) -> None:
        try:
            cluster_list = available_clusters(self.mgr)
            if cluster_id in cluster_list:
                self.mgr.export_mgr.delete_all_exports(cluster_id)
                completion = self.mgr.remove_service('ingress.nfs.' + cluster_id)
                orchestrator.raise_if_exception(completion)
                completion = self.mgr.remove_service('nfs.' + cluster_id)
                orchestrator.raise_if_exception(completion)
                self.delete_config_obj(cluster_id)
                return
            raise NonFatalError("Cluster does not exist")
        except Exception as e:
            log.exception(f"Failed to delete NFS Cluster {cluster_id}")
            raise ErrorResponse.wrap(e)

    def list_nfs_cluster(self) -> List[str]:
        try:
            return available_clusters(self.mgr)
        except Exception as e:
            log.exception("Failed to list NFS Cluster")
            raise ErrorResponse.wrap(e)

    def _show_nfs_cluster_info(self, cluster_id: str) -> Dict[str, Any]:
        completion = self.mgr.list_daemons(daemon_type='nfs')
        # Here completion.result is a list DaemonDescription objects
        clusters = orchestrator.raise_if_exception(completion)
        backends: List[Dict[str, Union[Any]]] = []

        for cluster in clusters:
            if cluster_id == cluster.service_id():
                assert cluster.hostname
                try:
                    if cluster.ip:
                        ip = cluster.ip
                    else:
                        c = self.mgr.get_hosts()
                        orchestrator.raise_if_exception(c)
                        hosts = [h for h in c.result or []
                                 if h.hostname == cluster.hostname]
                        if hosts:
                            ip = resolve_ip(hosts[0].addr)
                        else:
                            # sigh
                            ip = resolve_ip(cluster.hostname)
                    backends.append({
                        "hostname": cluster.hostname,
                        "ip": ip,
                        "port": cluster.ports[0] if cluster.ports else None
                    })
                except orchestrator.OrchestratorError:
                    continue

        r: Dict[str, Any] = {
            'virtual_ip': None,
            'backend': backends,
        }
        sc = self.mgr.describe_service(service_type='ingress')
        services = orchestrator.raise_if_exception(sc)
        for i in services:
            spec = cast(IngressSpec, i.spec)
            if spec.backend_service == f'nfs.{cluster_id}':
                r['virtual_ip'] = i.virtual_ip.split('/')[0] if i.virtual_ip else None
                if i.ports:
                    r['port'] = i.ports[0]
                    if len(i.ports) > 1:
                        r['monitor_port'] = i.ports[1]
                if spec.keepalive_only:
                    ingress_mode = IngressType.keepalive_only
                elif spec.enable_haproxy_protocol:
                    ingress_mode = IngressType.haproxy_protocol
                else:
                    ingress_mode = IngressType.haproxy_standard
                r['ingress_mode'] = ingress_mode.value

        log.debug("Successfully fetched %s info: %s", cluster_id, r)
        return r

    def show_nfs_cluster_info(self, cluster_id: Optional[str] = None) -> Dict[str, Any]:
        try:
            if cluster_id and cluster_id not in available_clusters(self.mgr):
                raise ClusterNotFound()
            info_res = {}
            if cluster_id:
                cluster_ls = [cluster_id]
            else:
                cluster_ls = available_clusters(self.mgr)

            for cluster_id in cluster_ls:
                res = self._show_nfs_cluster_info(cluster_id)
                if res:
                    info_res[cluster_id] = res
            return info_res
        except Exception as e:
            log.exception("Failed to show info for cluster")
            raise ErrorResponse.wrap(e)

    def get_nfs_cluster_config(self, cluster_id: str) -> str:
        try:
            if cluster_id in available_clusters(self.mgr):
                rados_obj = self._rados(cluster_id)
                conf = rados_obj.read_obj(user_conf_obj_name(cluster_id))
                return conf or ""
            raise ClusterNotFound()
        except Exception as e:
            log.exception(f"Fetching NFS-Ganesha Config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def set_nfs_cluster_config(self, cluster_id: str, nfs_config: str) -> None:
        try:
            if cluster_id in available_clusters(self.mgr):
                rados_obj = self._rados(cluster_id)
                if rados_obj.check_config(USER_CONF_PREFIX):
                    raise NonFatalError("NFS-Ganesha User Config already exists")
                rados_obj.write_obj(nfs_config, user_conf_obj_name(cluster_id),
                                    conf_obj_name(cluster_id))
                log.debug("Successfully saved %s's user config: \n %s", cluster_id, nfs_config)
                restart_nfs_service(self.mgr, cluster_id)
                return
            raise ClusterNotFound()
        except NotImplementedError:
            raise ManualRestartRequired("NFS-Ganesha Config Added Successfully")
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha Config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def reset_nfs_cluster_config(self, cluster_id: str) -> None:
        try:
            if cluster_id in available_clusters(self.mgr):
                rados_obj = self._rados(cluster_id)
                if not rados_obj.check_config(USER_CONF_PREFIX):
                    raise NonFatalError("NFS-Ganesha User Config does not exist")
                rados_obj.remove_obj(user_conf_obj_name(cluster_id),
                                     conf_obj_name(cluster_id))
                restart_nfs_service(self.mgr, cluster_id)
                return
            raise ClusterNotFound()
        except NotImplementedError:
            raise ManualRestartRequired("NFS-Ganesha Config Removed Successfully")
        except Exception as e:
            log.exception(f"Resetting NFS-Ganesha Config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def _rados(self, cluster_id: str) -> NFSRados:
        """Return a new NFSRados object for the given cluster id."""
        return NFSRados(self.mgr.rados, cluster_id)

    def get_cluster_qos_config(self, cluster_id: str) -> Optional[QOS]:
        """Return QOS object for the given cluster id."""
        rados_obj = self._rados(cluster_id)
        conf = rados_obj.read_obj(qos_conf_obj_name(cluster_id))
        if conf:
            qos_block = GaneshaConfParser(conf).parse()
            qos_obj = QOS.from_qos_block(qos_block[0], True)
            return qos_obj
        return None

    def update_cluster_qos_obj(self,
                               cluster_id: str,
                               qos_obj: Optional[QOS],
                               enable_qos: bool,
                               enable_cluster_qos: Optional[bool] = None,
                               clust_qos_msg_interval: int = 0,
                               qos_type: Optional[QOSType] = None,
                               bw_obj: Optional[QOSBandwidthControl] = None,
                               ops_obj: Optional[QOSOpsControl] = None) -> None:
        """Update cluster QOS config"""
        write_cluster_qos_obj(
            mgr=self.mgr,
            cluster_id=cluster_id,
            qos_obj=qos_obj,
            enable_qos=enable_qos,
            enable_cluster_qos=enable_cluster_qos,
            clust_qos_msg_interval=clust_qos_msg_interval,
            qos_type=qos_type,
            bw_obj=bw_obj,
            ops_obj=ops_obj
        )

    def update_cluster_qos(self,
                           cluster_id: str,
                           qos_obj: Optional[QOS],
                           enable_qos: bool,
                           enable_cluster_qos: Optional[bool] = None,
                           clust_qos_msg_interval: int = 0,
                           qos_type: Optional[QOSType] = None,
                           bw_obj: Optional[QOSBandwidthControl] = None,
                           ops_obj: Optional[QOSOpsControl] = None) -> None:
        try:
            if cluster_id in available_clusters(self.mgr):
                self.update_cluster_qos_obj(cluster_id, qos_obj, enable_qos, enable_cluster_qos,
                                            clust_qos_msg_interval, qos_type, bw_obj, ops_obj)
                restart_nfs_service(self.mgr, cluster_id)
                return
            raise ClusterNotFound()
        except NotImplementedError:
            raise ManualRestartRequired(f"NFS-Ganesha QoS config added successfully for {cluster_id}")

    def validate_qos_type(self,
                          qos_obj: QOS,
                          qos_type: QOSType,
                          bw_obj: Optional[QOSBandwidthControl] = None,
                          ops_obj: Optional[QOSOpsControl] = None) -> None:
        if not qos_obj or not (bw_obj or ops_obj):
            return
        # if qos is not enabled then we can set new directly
        if not (qos_obj.enable_qos and qos_obj.qos_type):
            return

        other_qos_obj: Any = None
        if bw_obj:
            other_qos_obj = qos_obj.ops_obj
            is_other_enable = qos_obj.ops_obj.enable_iops_ctrl if qos_obj.ops_obj else False
            is_this_enable = qos_obj.bw_obj.enable_bw_ctrl if qos_obj.bw_obj else False
            ctrl_type = "IOPS"
        else:
            other_qos_obj = qos_obj.bw_obj
            is_other_enable = qos_obj.bw_obj.enable_bw_ctrl if qos_obj.bw_obj else False
            is_this_enable = qos_obj.ops_obj.enable_iops_ctrl if qos_obj.ops_obj else False
            ctrl_type = "Bandwidth"

        if other_qos_obj and is_other_enable:
            # if earlier only other qos control is enabled
            if not is_this_enable and qos_obj.qos_type != qos_type:
                raise Exception(f"{ctrl_type} control is using {qos_obj.qos_type.name} QoS type, please update that QoS type for {ctrl_type} first.")
            # if both qos control are enabled, the user will need to disable one first to change qos type
            elif is_this_enable and qos_obj.qos_type != qos_type:
                raise Exception(f"{ctrl_type} control is using {qos_obj.qos_type.name} QoS type, please disable {ctrl_type} control to update QoS type and then enable {ctrl_type} control again with new QoS type")

    def enable_cluster_qos_bw(self,
                              cluster_id: str,
                              qos_type: QOSType,
                              bw_obj: QOSBandwidthControl
                              ) -> None:
        """
        There are 2 cases to consider:
        1. If combined bandwith control is disabled
            a. If qos_type is pershare, then export_writebw and export_readbw parameters are compulsory
            b. If qos_type is perclient, then client_writebw and client_readbw parameters are compulsory
            c. If qos_type is pershare_perclient then export_writebw, export_readbw, client_writebw and
               client_readbw are compulsory parameters
        2. If combined bandwidth control is enabled
            a. If qos_type is pershare, then export_rw_bw parameter is compulsory
            b. If qos_type is perclient, then client_rw_bw parameter is compulsory
            c. If qos_type is pershare_perclient, then export_rw_bw and client_rw_bw parameters are compulsory
        """
        try:
            qos_obj = self.get_cluster_qos_config(cluster_id)
            if qos_obj:
                self.validate_qos_type(qos_obj, qos_type, bw_obj=bw_obj)
            bw_obj.qos_bandwidth_checks(qos_type)
            self.update_cluster_qos(
                cluster_id,
                qos_obj,
                True,
                enable_cluster_qos=True,
                qos_type=qos_type,
                bw_obj=bw_obj
            )
            log.info(f"QoS bandwidth control has been successfully enabled for cluster {cluster_id}. "
                     "If the qos_type is changed during this process, ensure that the bandwidth "
                     "values for all exports are updated accordingly.")
            return
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha QoS bandwidth control config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def get_cluster_qos(self, cluster_id: str, ret_bw_in_bytes: bool = False) -> Dict[str, Any]:
        try:
            if cluster_id in available_clusters(self.mgr):
                qos_obj = self.get_cluster_qos_config(cluster_id)
                return qos_obj.to_dict(ret_bw_in_bytes) if qos_obj else {}
            raise ClusterNotFound()
        except Exception as e:
            log.exception(f"Fetching NFS-Ganesha QoS bandwidth control config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def disable_cluster_qos_bw(self, cluster_id: str) -> None:
        try:
            qos_obj = self.get_cluster_qos_config(cluster_id)
            status = False
            qos_type = None
            enable_cluster_qos = None
            clust_qos_msg_interval = 0
            if qos_obj:
                status = qos_obj.get_enable_qos_val(disable_bw=True)
                if status:
                    qos_type = qos_obj.qos_type
                    enable_cluster_qos = qos_obj.enable_cluster_qos
                    if qos_obj.clust_qos_msg_interval:
                        clust_qos_msg_interval = qos_obj.clust_qos_msg_interval
            self.update_cluster_qos(cluster_id, qos_obj, status, enable_cluster_qos,
                                    clust_qos_msg_interval, qos_type=qos_type, bw_obj=QOSBandwidthControl())
            log.info("Cluster-level QoS bandwidth control has been successfully disabled for "
                     f"cluster {cluster_id}. As a result, export-level bandwidth control will "
                     "no longer have any effect, even if enabled.")
            return
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha QoS bandwidth control config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def enable_cluster_qos_ops(self, cluster_id: str, qos_type: QOSType, ops_obj: QOSOpsControl) -> None:
        try:
            qos_obj = self.get_cluster_qos_config(cluster_id)
            if qos_obj:
                self.validate_qos_type(qos_obj, qos_type, ops_obj=ops_obj)
            ops_obj.qos_ops_checks(qos_type)
            self.update_cluster_qos(
                cluster_id,
                qos_obj,
                True,
                enable_cluster_qos=True,
                qos_type=qos_type,
                ops_obj=ops_obj
            )
            log.info(f"QOS IOPS control has been successfully enabled for cluster {cluster_id}. "
                     "If the qos_type is changed during this process, ensure that ops count "
                     "values for all exports are updated accordingly.")
            return
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha QOS IOPS control config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def disable_cluster_qos_ops(self, cluster_id: str) -> None:
        try:
            qos_obj = self.get_cluster_qos_config(cluster_id)
            status = False
            qos_type = None
            enable_cluster_qos = None
            clust_qos_msg_interval = 0
            if qos_obj:
                status = qos_obj.get_enable_qos_val(disable_ops=True)
                if status:
                    qos_type = qos_obj.qos_type
                    enable_cluster_qos = qos_obj.enable_cluster_qos
                    if qos_obj.clust_qos_msg_interval:
                        clust_qos_msg_interval = qos_obj.clust_qos_msg_interval
            self.update_cluster_qos(cluster_id, qos_obj, status, enable_cluster_qos,
                                    clust_qos_msg_interval, qos_type=qos_type, ops_obj=QOSOpsControl())
            log.info("Cluster-level QoS IOPS control has been successfully disabled for "
                     f"cluster {cluster_id}. As a result, export-level ops control will "
                     "no longer have any effect, even if enabled.")
            return
        except Exception as e:
            log.exception(f"Setting NFS-Ganesha QoS IOPS control config failed for {cluster_id}")
            raise ErrorResponse.wrap(e)

    def global_cluster_qos_action(
        self,
        cluster_id: str,
        action: str,
        msg_interval: int = 0
    ) -> None:
        try:
            qos_obj = self.get_cluster_qos_config(cluster_id)
            if not qos_obj:
                err_msg = f'No existing QoS configuration found for cluster {cluster_id}. Can not {action} cluster-qos'
                log.error(err_msg)
                raise Exception(err_msg)

            clust_qos_msg_interval = 0
            if action == 'enable':
                if (qos_obj.enable_cluster_qos or qos_obj.enable_cluster_qos is None) and not msg_interval:
                    log.info('Cluster QoS is already enabled')
                    return

                enable_cluster_qos = True
                clust_qos_msg_interval = msg_interval
            else:  # disable
                enable_cluster_qos = False
            self.update_cluster_qos(
                cluster_id=cluster_id,
                qos_obj=qos_obj,
                enable_qos=qos_obj.enable_qos,
                enable_cluster_qos=enable_cluster_qos,
                clust_qos_msg_interval=clust_qos_msg_interval,
                qos_type=qos_obj.qos_type
            )
            action_past = "enabled" if action == "enable" else "disabled"
            log.info(f"Cluster-level QoS has been successfully {action_past} for cluster {cluster_id}")
        except Exception as e:
            log.exception(f"Failed to {action} cluster-level QoS for cluster {cluster_id}")
            raise ErrorResponse.wrap(e)
