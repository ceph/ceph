from typing import Any, Optional

from .cluster import NFSCluster
from .qos_conf import QOSType, QOSParams, QOSBandwidthControl, QOSOpsControl, QOS


def export_dict_bw_checks(cluster_id: str,
                          mgr_obj: Any,
                          qos_enable: bool,
                          qos_dict: dict) -> None:
    enable_bw_ctrl = qos_dict.get('enable_bw_control')
    combined_bw_ctrl = qos_dict.get('combined_rw_bw_control')
    bandwith_param_exists = any(key.endswith('bw') for key in qos_dict)
    if enable_bw_ctrl is None:
        if combined_bw_ctrl and bandwith_param_exists:
            raise Exception('Bandwidth control is not enabled but associated parameters exists')
        return
    if combined_bw_ctrl is None:
        combined_bw_ctrl = False
    if not qos_enable and enable_bw_ctrl:
        raise Exception('To enable bandwidth control, qos_enable and enable_bw_control should be true.')
    if not (isinstance(enable_bw_ctrl, bool) and isinstance(combined_bw_ctrl, bool)):
        raise Exception('Invalid values for the enable_bw_ctrl and combined_bw_ctrl parameters.')
    # if qos bandwidth control is disabled, then bandwidths should not be set and no need to bandwidth checks
    if not enable_bw_ctrl:
        if bandwith_param_exists:
            raise Exception('Bandwidths should not be passed when enable_bw_control is false.')
        return
    if enable_bw_ctrl and not bandwith_param_exists:
        raise Exception('Bandwidths should be set when enable_bw_control is true.')
    bw_obj = QOSBandwidthControl(enable_bw_ctrl,
                                 combined_bw_ctrl,
                                 export_writebw=qos_dict.get(QOSParams.export_writebw.value, '0'),
                                 export_readbw=qos_dict.get(QOSParams.export_readbw.value, '0'),
                                 client_writebw=qos_dict.get(QOSParams.client_writebw.value, '0'),
                                 client_readbw=qos_dict.get(QOSParams.client_readbw.value, '0'),
                                 export_rw_bw=qos_dict.get(QOSParams.export_rw_bw.value, '0'),
                                 client_rw_bw=qos_dict.get(QOSParams.client_rw_bw.value, '0'))
    export_qos_bw_checks(cluster_id, mgr_obj, bw_obj)


def export_dict_ops_checks(cluster_id: str,
                           mgr_obj: Any,
                           qos_enable: bool,
                           qos_dict: dict) -> None:
    enable_iops_ctrl = qos_dict.get(QOSParams.enable_iops_ctrl.value)
    if enable_iops_ctrl is None:
        return
    if not isinstance(enable_iops_ctrl, bool):
        raise Exception(f'Invalid values for the {QOSParams.enable_iops_ctrl.value} parameter')
    ops_param_exists = any(key.endswith('iops') for key in qos_dict)
    if not enable_iops_ctrl:
        if ops_param_exists:
            raise Exception(f'IOPS count parameters should not be passed when {QOSParams.enable_iops_ctrl.value} is false.')
        return
    if enable_iops_ctrl and not ops_param_exists:
        raise Exception(f'IOPS count parameters should be set when {QOSParams.enable_iops_ctrl.value} is true.')
    ops_obj = QOSOpsControl(enable_iops_ctrl,
                            max_export_iops=qos_dict.get(QOSParams.max_export_iops.value, 0),
                            max_client_iops=qos_dict.get(QOSParams.max_client_iops.value, 0))
    export_qos_ops_checks(cluster_id, mgr_obj, ops_obj)


def export_dict_qos_bw_ops_checks(cluster_id: str,
                                  mgr_obj: Any,
                                  qos_dict: dict,
                                  old_qos_block: dict = {}) -> None:
    """Validate the qos block of dict passed to apply_export method"""
    qos_enable = qos_dict.get('enable_qos')
    if qos_enable is None:
        raise Exception('The QoS block requires at least the enable_qos parameter')
    if not isinstance(qos_enable, bool):
        raise Exception('Invalid value for the enable_qos parameter')
    # if cluster level bandwidth or ops control is disabled or qos type changed to PerClient
    # but old qos block still has those values we should accept it in apply command
    validate_bw = True
    validate_ops = True
    clust_qos_obj = get_cluster_qos_config(cluster_id, mgr_obj)
    if clust_qos_obj:
        if not clust_qos_obj.enable_qos or clust_qos_obj.qos_type == QOSType.PerClient:
            if old_qos_block == qos_dict:
                return
        if clust_qos_obj.enable_qos and clust_qos_obj.bw_obj and not clust_qos_obj.bw_obj.enable_bw_ctrl:
            keys = [QOSParams.export_writebw.value, QOSParams.export_readbw.value,
                    QOSParams.client_writebw.value, QOSParams.client_readbw.value,
                    QOSParams.export_rw_bw.value, QOSParams.client_rw_bw.value,
                    QOSParams.enable_bw_ctrl.value, QOSParams.combined_bw_ctrl.value]
            if all(old_qos_block.get(key) == qos_dict.get(key) for key in keys):
                validate_bw = False
        if clust_qos_obj.enable_qos and clust_qos_obj.ops_obj and not clust_qos_obj.ops_obj.enable_iops_ctrl:
            keys = [QOSParams.max_export_iops.value, QOSParams.max_client_iops.value,
                    QOSParams.enable_iops_ctrl.value]
            if all(old_qos_block.get(key) == qos_dict.get(key) for key in keys):
                validate_ops = False
    if validate_bw:
        export_dict_bw_checks(cluster_id, mgr_obj, qos_enable, qos_dict)
    if validate_ops:
        export_dict_ops_checks(cluster_id, mgr_obj, qos_enable, qos_dict)


def export_qos_bw_checks(cluster_id: str,
                         mgr_obj: Any,
                         bw_obj: QOSBandwidthControl,
                         nfs_clust_obj: Any = None) -> None:
    """check cluster level qos bandwidth control is enabled to enable export level qos
    bandwidth control and validate bandwidths"""
    if not nfs_clust_obj:
        nfs_clust_obj = NFSCluster(mgr_obj)
    clust_qos_obj = nfs_clust_obj.get_cluster_qos_config(cluster_id)
    if not clust_qos_obj or (clust_qos_obj and not (clust_qos_obj.enable_qos
                                                    and clust_qos_obj.bw_obj
                                                    and clust_qos_obj.bw_obj.enable_bw_ctrl)):
        raise Exception(f'To configure bandwidth control for export, you must first enable bandwidth control at the cluster level for {cluster_id}.')
    if clust_qos_obj.qos_type:
        if clust_qos_obj.qos_type == QOSType.PerClient:
            raise Exception(f'Export-level QoS bandwidth control cannot be enabled if the QoS type at the cluster {cluster_id} level is set to PerClient.')
        bw_obj.qos_bandwidth_checks(clust_qos_obj.qos_type)


def export_qos_ops_checks(cluster_id: str,
                          mgr_obj: Any,
                          ops_obj: QOSOpsControl,
                          nfs_clust_obj: Any = None) -> None:
    """check cluster level qos IOPS is enabled to enable export level qos IOPS and validate IOPS count"""
    if not nfs_clust_obj:
        nfs_clust_obj = NFSCluster(mgr_obj)
    clust_qos_obj = nfs_clust_obj.get_cluster_qos_config(cluster_id)
    if not clust_qos_obj or (clust_qos_obj and not (clust_qos_obj.enable_qos
                                                    and clust_qos_obj.ops_obj
                                                    and clust_qos_obj.ops_obj.enable_iops_ctrl)):
        raise Exception(f'To configure IOPS control for export, you must first enable IOPS control at the cluster level {cluster_id}.')
    if clust_qos_obj.qos_type:
        if clust_qos_obj.qos_type == QOSType.PerClient:
            raise Exception(f'Export-level QoS IOPS control cannot be enabled if the QoS type at the cluster {cluster_id} level is set to PerClient.')
        ops_obj.qos_ops_checks(clust_qos_obj.qos_type)


def get_cluster_qos_config(cluster_id: str, mgr_obj: Any) -> Optional[QOS]:
    nfs_clust_obj = NFSCluster(mgr_obj)
    return nfs_clust_obj.get_cluster_qos_config(cluster_id)
