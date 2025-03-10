from typing import Any

from .cluster import NFSCluster
from .qos_conf import QOSType, QOSParams, QOSBandwidthControl


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
        raise Exception('To enable bandwidth control, qos_enable should be true.')
    if not (isinstance(enable_bw_ctrl, bool) and isinstance(combined_bw_ctrl, bool)):
        raise Exception('Invalid values for the enable_bw_ctrl and combined_bw_ctrl parameters.')
    # if qos is disabled, then bandwidths should not be set and no need to bandwidth checks
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


def export_dict_qos_checks(cluster_id: str,
                           mgr_obj: Any,
                           qos_dict: dict) -> None:
    """Validate the qos block of dict passed to apply_export method"""
    qos_enable = qos_dict.get('enable_qos')
    if qos_enable is None:
        raise Exception('The QOS block requires at least the enable_qos parameter')
    if not isinstance(qos_enable, bool):
        raise Exception('Invalid value for the enable_qos parameter')
    export_dict_bw_checks(cluster_id, mgr_obj, qos_enable, qos_dict)


def export_qos_bw_checks(cluster_id: str,
                         mgr_obj: Any,
                         bw_obj: QOSBandwidthControl,
                         nfs_clust_obj: Any = None) -> None:
    """check cluster level qos is enabled to enable export level qos and validate bandwidths"""
    if not nfs_clust_obj:
        nfs_clust_obj = NFSCluster(mgr_obj)
    clust_qos_obj = nfs_clust_obj.get_cluster_qos_config(cluster_id)
    if not clust_qos_obj or (clust_qos_obj and not (clust_qos_obj.enable_qos)):
        raise Exception('To configure bandwidth control for export, you must first enable bandwidth control at the cluster level.')
    if clust_qos_obj.qos_type:
        if clust_qos_obj.qos_type == QOSType.PerClient:
            raise Exception('Export-level QoS bandwidth control cannot be enabled if the QoS type at the cluster level is set to PerClient.')
        bw_obj.qos_bandwidth_checks(clust_qos_obj.qos_type)
