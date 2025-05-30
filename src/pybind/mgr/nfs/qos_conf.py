from typing import Dict, Any, Optional
from enum import Enum

from ceph.utils import bytes_to_human, with_units_to_int
from .ganesha_raw_conf import RawBlock


class QOSParams(Enum):
    clust_block = "QOS_DEFAULT_CONFIG"
    export_block = "QOS_BLOCK"
    enable_qos = "enable_qos"
    qos_type = "qos_type"
    # bandwidth control
    enable_bw_ctrl = "enable_bw_control"
    combined_bw_ctrl = "combined_rw_bw_control"
    export_writebw = "max_export_write_bw"
    export_readbw = "max_export_read_bw"
    client_writebw = "max_client_write_bw"
    client_readbw = "max_client_read_bw"
    export_rw_bw = "max_export_combined_bw"
    client_rw_bw = "max_client_combined_bw"
    # ops control
    enable_iops_ctrl = "enable_iops_control"
    max_export_iops = "max_export_iops"
    max_client_iops = "max_client_iops"


class UserQoSType(Enum):
    per_share = "PerShare"
    per_client = "PerClient"
    per_share_per_client = "PerShare_PerClient"


class QOSType(Enum):
    PerShare = 1
    PerClient = 2
    PerShare_PerClient = 3


def _validate_qos_bw(bandwidth: str) -> int:
    min_bw = 128 * 1024  # 128KiB
    max_bw = 100 * 1024 * 1024 * 1024  # 100GiB
    bw_bytes = with_units_to_int(bandwidth)
    if bw_bytes != 0 and (bw_bytes < min_bw or bw_bytes > max_bw):
        raise Exception(
            f"Provided bandwidth value is not in range, Please enter a value between {min_bw} (128KiB) and {max_bw} (100GiB) bytes per second."
        )
    return bw_bytes


def _validate_qos_ops(count: int) -> int:
    min_cnt = 10
    max_cnt = 1638400
    if count != 0 and (count < min_cnt or count > max_cnt):
        raise Exception(
            f"Provided IOS count value is not in range, Please enter a value between {min_cnt} and {max_cnt} count per second."
        )
    return count


QOS_REQ_BW_PARAMS = {
    "combined_bw_disabled": {
        "PerShare": ["max_export_write_bw", "max_export_read_bw"],
        "PerClient": ["max_client_write_bw", "max_client_read_bw"],
        "PerShare_PerClient": [
            "max_export_write_bw",
            "max_export_read_bw",
            "max_client_write_bw",
            "max_client_read_bw",
        ],
    },
    "combined_bw_enabled": {
        "PerShare": ["max_export_combined_bw"],
        "PerClient": ["max_client_combined_bw"],
        "PerShare_PerClient": ["max_export_combined_bw", "max_client_combined_bw"],
    },
}

QOS_REQ_OPS_PARAMS = {
    "PerShare": ["max_export_iops"],
    "PerClient": ["max_client_iops"],
    "PerShare_PerClient": ["max_export_iops", "max_client_iops"],
}


class QOSBandwidthControl(object):
    def __init__(
        self,
        enable_bw_ctrl: bool = False,
        combined_bw_ctrl: bool = False,
        export_writebw: str = "0",
        export_readbw: str = "0",
        client_writebw: str = "0",
        client_readbw: str = "0",
        export_rw_bw: str = "0",
        client_rw_bw: str = "0",
    ) -> None:
        self.enable_bw_ctrl = enable_bw_ctrl
        self.combined_bw_ctrl = combined_bw_ctrl
        try:
            self.export_writebw: int = _validate_qos_bw(export_writebw)
            self.export_readbw: int = _validate_qos_bw(export_readbw)
            self.client_writebw: int = _validate_qos_bw(client_writebw)
            self.client_readbw: int = _validate_qos_bw(client_readbw)
            self.export_rw_bw: int = _validate_qos_bw(export_rw_bw)
            self.client_rw_bw: int = _validate_qos_bw(client_rw_bw)
        except Exception as e:
            raise Exception(f"Invalid bandwidth value. {e}")

    @classmethod
    def from_dict(cls, qos_dict: Dict[str, Any]) -> "QOSBandwidthControl":
        # qos dict has bandwidths in human readable format(str)
        bw_kwargs = {
            "enable_bw_ctrl": qos_dict.get(QOSParams.enable_bw_ctrl.value, False),
            "combined_bw_ctrl": qos_dict.get(QOSParams.combined_bw_ctrl.value, False),
            "export_writebw": qos_dict.get(QOSParams.export_writebw.value, "0"),
            "export_readbw": qos_dict.get(QOSParams.export_readbw.value, "0"),
            "client_writebw": qos_dict.get(QOSParams.client_writebw.value, "0"),
            "client_readbw": qos_dict.get(QOSParams.client_readbw.value, "0"),
            "export_rw_bw": qos_dict.get(QOSParams.export_rw_bw.value, "0"),
            "client_rw_bw": qos_dict.get(QOSParams.client_rw_bw.value, "0"),
        }
        return cls(**bw_kwargs)

    @classmethod
    def from_qos_block(cls, qos_block: RawBlock) -> "QOSBandwidthControl":
        # qos block has bandwidths in bytes(int)
        bw_kwargs = {
            "enable_bw_ctrl": qos_block.values.get(
                QOSParams.enable_bw_ctrl.value, False
            ),
            "combined_bw_ctrl": qos_block.values.get(
                QOSParams.combined_bw_ctrl.value, False
            ),
            "export_writebw": str(
                qos_block.values.get(QOSParams.export_writebw.value, 0)
            ),
            "export_readbw": str(
                qos_block.values.get(QOSParams.export_readbw.value, 0)
            ),
            "client_writebw": str(
                qos_block.values.get(QOSParams.client_writebw.value, 0)
            ),
            "client_readbw": str(
                qos_block.values.get(QOSParams.client_readbw.value, 0)
            ),
            "export_rw_bw": str(qos_block.values.get(QOSParams.export_rw_bw.value, 0)),
            "client_rw_bw": str(qos_block.values.get(QOSParams.client_rw_bw.value, 0)),
        }
        return cls(**bw_kwargs)

    def to_qos_block(self) -> RawBlock:
        result = RawBlock("qos_bandwidths_control")
        result.values[QOSParams.enable_bw_ctrl.value] = self.enable_bw_ctrl
        result.values[QOSParams.combined_bw_ctrl.value] = self.combined_bw_ctrl
        if self.export_writebw:
            result.values[QOSParams.export_writebw.value] = self.export_writebw
        if self.export_readbw:
            result.values[QOSParams.export_readbw.value] = self.export_readbw
        if self.client_writebw:
            result.values[QOSParams.client_writebw.value] = self.client_writebw
        if self.client_readbw:
            result.values[QOSParams.client_readbw.value] = self.client_readbw
        if self.export_rw_bw:
            result.values[QOSParams.export_rw_bw.value] = self.export_rw_bw
        if self.client_rw_bw:
            result.values[QOSParams.client_rw_bw.value] = self.client_rw_bw
        return result

    @staticmethod
    def bw_for_to_dict(bandwidth: int, ret_bw_in_bytes: bool = False) -> str:
        return bytes_to_human(bandwidth, mode='binary') if not ret_bw_in_bytes else str(bandwidth)

    def to_dict(self, ret_bw_in_bytes: bool = False) -> Dict[str, Any]:
        r: dict[str, Any] = {}
        r[QOSParams.enable_bw_ctrl.value] = self.enable_bw_ctrl
        r[QOSParams.combined_bw_ctrl.value] = self.combined_bw_ctrl
        if self.export_writebw:
            r[QOSParams.export_writebw.value] = self.bw_for_to_dict(
                self.export_writebw, ret_bw_in_bytes
            )
        if self.export_readbw:
            r[QOSParams.export_readbw.value] = self.bw_for_to_dict(
                self.export_readbw, ret_bw_in_bytes
            )
        if self.client_writebw:
            r[QOSParams.client_writebw.value] = self.bw_for_to_dict(
                self.client_writebw, ret_bw_in_bytes
            )
        if self.client_readbw:
            r[QOSParams.client_readbw.value] = self.bw_for_to_dict(
                self.client_readbw, ret_bw_in_bytes
            )
        if self.export_rw_bw:
            r[QOSParams.export_rw_bw.value] = self.bw_for_to_dict(
                self.export_rw_bw, ret_bw_in_bytes
            )
        if self.client_rw_bw:
            r[QOSParams.client_rw_bw.value] = self.bw_for_to_dict(
                self.client_rw_bw, ret_bw_in_bytes
            )
        return r

    def qos_bandwidth_checks(self, qos_type: QOSType) -> None:
        """Checks for enabling qos bandwidth control"""
        params = {}
        d = vars(self)
        for key in d:
            if key.endswith("bw"):
                params[QOSParams[key].value] = d[key]
        if not self.combined_bw_ctrl:
            req_params = QOS_REQ_BW_PARAMS["combined_bw_disabled"][qos_type.name]
        else:
            req_params = QOS_REQ_BW_PARAMS["combined_bw_enabled"][qos_type.name]
        allowed_params = []
        not_allowed_params = []
        for key in params:
            if key in req_params and params[key] == 0:
                allowed_params.append(key)
            elif key not in req_params and params[key] != 0:
                not_allowed_params.append(key)
        if allowed_params or not_allowed_params:
            raise Exception(
                f"When combined_rw_bw is {'enabled' if self.combined_bw_ctrl else 'disabled'} "
                f"and qos_type is {qos_type.name}, "
                f"{'attribute ' + ', '.join(allowed_params) + ' required' if allowed_params else ''} "
                f"{'attribute ' + ', '.join(not_allowed_params) + ' are not allowed' if not_allowed_params else ''}."
            )


class QOSOpsControl(object):
    def __init__(
        self,
        enable_iops_ctrl: bool = False,
        max_export_iops: int = 0,
        max_client_iops: int = 0,
    ) -> None:
        self.enable_iops_ctrl = enable_iops_ctrl
        self.max_export_iops = _validate_qos_ops(max_export_iops)
        self.max_client_iops = _validate_qos_ops(max_client_iops)

    @classmethod
    def from_dict(cls, qos_dict: Dict[str, Any]) -> "QOSOpsControl":
        kwargs: dict[str, Any] = {}
        kwargs["enable_iops_ctrl"] = qos_dict.get(
            QOSParams.enable_iops_ctrl.value, False
        )
        kwargs["max_export_iops"] = qos_dict.get(QOSParams.max_export_iops.value, 0)
        kwargs["max_client_iops"] = qos_dict.get(QOSParams.max_client_iops.value, 0)
        return cls(**kwargs)

    @classmethod
    def from_qos_block(cls, qos_block: RawBlock) -> "QOSOpsControl":
        kwargs: dict[str, Any] = {}
        kwargs["enable_iops_ctrl"] = qos_block.values.get(
            QOSParams.enable_iops_ctrl.value, False
        )
        kwargs["max_export_iops"] = qos_block.values.get(
            QOSParams.max_export_iops.value, 0
        )
        kwargs["max_client_iops"] = qos_block.values.get(
            QOSParams.max_client_iops.value, 0
        )
        return cls(**kwargs)

    def to_qos_block(self) -> RawBlock:
        result = RawBlock("qos_ops_control")
        result.values[QOSParams.enable_iops_ctrl.value] = self.enable_iops_ctrl
        if self.max_export_iops:
            result.values[QOSParams.max_export_iops.value] = self.max_export_iops
        if self.max_client_iops:
            result.values[QOSParams.max_client_iops.value] = self.max_client_iops
        return result

    def to_dict(self) -> Dict[str, Any]:
        r: dict[str, Any] = {}
        r[QOSParams.enable_iops_ctrl.value] = self.enable_iops_ctrl
        if self.max_export_iops:
            r[QOSParams.max_export_iops.value] = self.max_export_iops
        if self.max_client_iops:
            r[QOSParams.max_client_iops.value] = self.max_client_iops
        return r

    def qos_ops_checks(self, qos_type: QOSType) -> None:
        """Checks for enabling qos IOPS control"""
        params = {}
        d = vars(self)
        for key in d:
            if key.endswith("iops"):
                params[QOSParams[key].value] = d[key]
        req_params = QOS_REQ_OPS_PARAMS[qos_type.name]
        allowed_params = []
        not_allowed_params = []
        for key in params:
            if key in req_params and params[key] == 0:
                allowed_params.append(key)
            elif key not in req_params and params[key] != 0:
                not_allowed_params.append(key)
        if allowed_params or not_allowed_params:
            raise Exception(
                f"When qos_type is {qos_type.name}, "
                f"{'attribute ' + ', '.join(allowed_params) + ' required' if allowed_params else ''} "
                f"{'attribute ' + ', '.join(not_allowed_params) + ' are not allowed' if not_allowed_params else ''}."
            )


class QOS(object):
    def __init__(
        self,
        cluster_op: bool = False,
        enable_qos: bool = False,
        qos_type: Optional[QOSType] = None,
        bw_obj: Optional[QOSBandwidthControl] = None,
        ops_obj: Optional[QOSOpsControl] = None,
    ) -> None:
        self.cluster_op = cluster_op
        self.enable_qos = enable_qos
        self.qos_type = qos_type
        self.bw_obj = bw_obj
        self.ops_obj = ops_obj

    @classmethod
    def from_dict(cls, qos_dict: Dict[str, Any], cluster_op: bool = False) -> "QOS":
        kwargs: dict[str, Any] = {}
        # qos dict will have qos type as enum name
        if cluster_op:
            qos_type = qos_dict.get(QOSParams.qos_type.value)
            if qos_type:
                kwargs["qos_type"] = QOSType[qos_type]
        kwargs["enable_qos"] = qos_dict.get(QOSParams.enable_qos.value)
        kwargs["bw_obj"] = QOSBandwidthControl.from_dict(qos_dict)
        kwargs["ops_obj"] = QOSOpsControl.from_dict(qos_dict)
        return cls(cluster_op, **kwargs)

    @classmethod
    def from_qos_block(cls, qos_block: RawBlock, cluster_op: bool = False) -> "QOS":
        kwargs: dict[str, Any] = {}
        # qos block will have qos type as enum value
        if cluster_op:
            qos_type = qos_block.values.get(QOSParams.qos_type.value)
            if qos_type:
                kwargs["qos_type"] = QOSType(qos_type)
        kwargs["enable_qos"] = qos_block.values.get(QOSParams.enable_qos.value)
        kwargs["bw_obj"] = QOSBandwidthControl.from_qos_block(qos_block)
        kwargs["ops_obj"] = QOSOpsControl.from_qos_block(qos_block)
        return cls(cluster_op, **kwargs)

    def to_qos_block(self) -> RawBlock:
        if self.cluster_op:
            result = RawBlock(QOSParams.clust_block.value)
        else:
            result = RawBlock(QOSParams.export_block.value)
        result.values[QOSParams.enable_qos.value] = self.enable_qos
        if self.cluster_op and self.qos_type:
            result.values[QOSParams.qos_type.value] = self.qos_type.value
        if self.bw_obj and (res := self.bw_obj.to_qos_block()):
            result.values.update(res.values)
        if self.ops_obj and (res := self.ops_obj.to_qos_block()):
            result.values.update(res.values)
        return result

    def to_dict(self, ret_bw_in_bytes: bool = False) -> Dict[str, Any]:
        r: Dict[str, Any] = {}
        r[QOSParams.enable_qos.value] = self.enable_qos
        if self.cluster_op and self.qos_type:
            r[QOSParams.qos_type.value] = self.qos_type.name
        if self.bw_obj and (res := self.bw_obj.to_dict(ret_bw_in_bytes)):
            r.update(res)
        if self.ops_obj and (res := self.ops_obj.to_dict()):
            r.update(res)
        return r

    def get_enable_qos_val(
        self, disable_bw: bool = False, disable_ops: bool = False
    ) -> bool:
        if not (self.enable_qos) or not (disable_bw or disable_ops):
            return False
        # check if ops control is enabled
        if disable_bw and self.ops_obj and self.ops_obj.enable_iops_ctrl:
            return True
        # check if bandwidth control is enabled
        if disable_ops and self.bw_obj and self.bw_obj.enable_bw_ctrl:
            return True
        return False
