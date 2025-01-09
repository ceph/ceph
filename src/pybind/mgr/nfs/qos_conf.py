from typing import List, Dict, Any, Optional
from enum import Enum

from ceph.utils import bytes_to_human, with_units_to_int


class RawBlock():
    def __init__(self, block_name: str, blocks: List['RawBlock'] = [], values: Dict[str, Any] = {}):
        if not values:  # workaround mutable default argument
            values = {}
        if not blocks:  # workaround mutable default argument
            blocks = []
        self.block_name = block_name
        self.blocks = blocks
        self.values = values

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, RawBlock):
            return False
        return self.block_name == other.block_name and \
            self.blocks == other.blocks and \
            self.values == other.values

    def __repr__(self) -> str:
        return f'RawBlock({self.block_name!r}, {self.blocks!r}, {self.values!r})'


class QOSParams(Enum):
    clust_block = "QOS_DEFAULT_CONFIG"
    export_block = "QOS_BLOCK"
    enable_qos = "enable_qos"
    enable_bw_ctrl = "enable_bw_control"
    combined_bw_ctrl = "combined_rw_bw_control"
    qos_type = "qos_type"
    export_writebw = "max_export_write_bw"
    export_readbw = "max_export_read_bw"
    client_writebw = "max_client_write_bw"
    client_readbw = "max_client_read_bw"
    export_rw_bw = "max_export_combined_bw"
    client_rw_bw = "max_client_combined_bw"


class UserQoSType(Enum):
    per_share = 'PerShare'
    per_client = 'PerClient'
    per_share_per_client = 'PerShare_PerClient'


class QOSType(Enum):
    PerShare = 1
    PerClient = 2
    PerShare_PerClient = 3


def _validate_qos_bw(bandwidth: str) -> int:
    min_bw = 1000000  # 1MB
    max_bw = 2000000000  # 2GB
    bw_bytes = with_units_to_int(bandwidth)
    if bw_bytes != 0 and (bw_bytes < min_bw or bw_bytes > max_bw):
        raise Exception(f"Provided bandwidth value is not in range, Please enter a value between {min_bw} (1MB) and {max_bw} (2GB) bytes")
    return bw_bytes


QOS_REQ_PARAMS = {
    'combined_bw_disabled': {
        'PerShare': ['max_export_write_bw', 'max_export_read_bw'],
        'PerClient': ['max_client_write_bw', 'max_client_read_bw'],
        'PerShare_PerClient': ['max_export_write_bw', 'max_export_read_bw', 'max_client_write_bw', 'max_client_read_bw']
    },
    'combined_bw_enabled': {
        'PerShare': ['max_export_combined_bw'],
        'PerClient': ['max_client_combined_bw'],
        'PerShare_PerClient': ['max_export_combined_bw', 'max_client_combined_bw'],
    }
}


class QOSBandwidthControl(object):
    def __init__(self,
                 enable_bw_ctrl: bool = False,
                 combined_bw_ctrl: bool = False,
                 export_writebw: str = '0',
                 export_readbw: str = '0',
                 client_writebw: str = '0',
                 client_readbw: str = '0',
                 export_rw_bw: str = '0',
                 client_rw_bw: str = '0'
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
    def from_dict(cls, qos_dict: Dict[str, Any]) -> 'QOSBandwidthControl':
        # json has bandwidths in human readable format(str)
        bw_kwargs = {
            'enable_bw_ctrl': qos_dict.get(QOSParams.enable_bw_ctrl.value, False),
            'combined_bw_ctrl': qos_dict.get(QOSParams.combined_bw_ctrl.value, False),
            'export_writebw': qos_dict.get(QOSParams.export_writebw.value, '0'),
            'export_readbw': qos_dict.get(QOSParams.export_readbw.value, '0'),
            'client_writebw': qos_dict.get(QOSParams.client_writebw.value, '0'),
            'client_readbw': qos_dict.get(QOSParams.client_readbw.value, '0'),
            'export_rw_bw': qos_dict.get(QOSParams.export_rw_bw.value, '0'),
            'client_rw_bw': qos_dict.get(QOSParams.client_rw_bw.value, '0')
        }
        return cls(**bw_kwargs)

    @classmethod
    def from_qos_block(cls, qos_block: RawBlock) -> 'QOSBandwidthControl':
        # block has bandwidths in bytes(int)
        bw_kwargs = {
            'enable_bw_ctrl': qos_block.values.get(QOSParams.enable_bw_ctrl.value, False),
            'combined_bw_ctrl': qos_block.values.get(QOSParams.combined_bw_ctrl.value, False),
            'export_writebw': str(qos_block.values.get(QOSParams.export_writebw.value, 0)),
            'export_readbw': str(qos_block.values.get(QOSParams.export_readbw.value, 0)),
            'client_writebw': str(qos_block.values.get(QOSParams.client_writebw.value, 0)),
            'client_readbw': str(qos_block.values.get(QOSParams.client_readbw.value, 0)),
            'export_rw_bw': str(qos_block.values.get(QOSParams.export_rw_bw.value, 0)),
            'client_rw_bw': str(qos_block.values.get(QOSParams.client_rw_bw.value, 0))
        }
        return cls(**bw_kwargs)

    def to_qos_block(self) -> RawBlock:
        result = RawBlock('qos_bandwidths_control')
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

    def to_dict(self) -> Dict[str, Any]:
        r: dict[str, Any] = {}
        r[QOSParams.enable_bw_ctrl.value] = self.enable_bw_ctrl
        r[QOSParams.combined_bw_ctrl.value] = self.combined_bw_ctrl
        if self.export_writebw:
            r[QOSParams.export_writebw.value] = bytes_to_human(self.export_writebw)
        if self.export_readbw:
            r[QOSParams.export_readbw.value] = bytes_to_human(self.export_readbw)
        if self.client_writebw:
            r[QOSParams.client_writebw.value] = bytes_to_human(self.client_writebw)
        if self.client_readbw:
            r[QOSParams.client_readbw.value] = bytes_to_human(self.client_readbw)
        if self.export_rw_bw:
            r[QOSParams.export_rw_bw.value] = bytes_to_human(self.export_rw_bw)
        if self.client_rw_bw:
            r[QOSParams.client_rw_bw.value] = bytes_to_human(self.client_rw_bw)
        return r

    def qos_bandwidth_checks(self, qos_type: QOSType) -> None:
        """Checks for enabling qos"""
        params = {}
        d = vars(self)
        for key in d:
            if key.endswith('bw'):
                params[QOSParams[key].value] = d[key]
        if not self.combined_bw_ctrl:
            req_params = QOS_REQ_PARAMS['combined_bw_disabled'][qos_type.name]
        else:
            req_params = QOS_REQ_PARAMS['combined_bw_enabled'][qos_type.name]
        allowed_params = []
        not_allowed_params = []
        for key in params:
            if key in req_params and params[key] == 0:
                allowed_params.append(key)
            elif key not in req_params and params[key] != 0:
                not_allowed_params.append(key)
        if allowed_params or not_allowed_params:
            raise Exception(f"When combined_rw_bw is {'enabled' if self.combined_bw_ctrl else 'disabled'} "
                            f"and qos_type is {qos_type.name}, "
                            f"{'attributes ' + ', '.join(allowed_params) + ' required' if allowed_params else ''} "
                            f"{'attributes ' + ', '.join(not_allowed_params) + ' are not allowed' if not_allowed_params else ''}.")


class QOS(object):
    def __init__(self,
                 cluster_op: bool = False,
                 enable_qos: bool = False,
                 qos_type: Optional[QOSType] = None,
                 bw_obj: Optional[QOSBandwidthControl] = None
                 ) -> None:
        self.cluster_op = cluster_op
        self.enable_qos = enable_qos
        self.qos_type = qos_type
        self.bw_obj = bw_obj

    @classmethod
    def from_dict(cls, qos_dict: Dict[str, Any], cluster_op: bool = False) -> 'QOS':
        kwargs: dict[str, Any] = {}
        # qos dict will have qos type as enum name
        if cluster_op:
            qos_type = qos_dict.get(QOSParams.qos_type.value)
            if qos_type:
                kwargs['qos_type'] = QOSType[qos_type]
        kwargs['enable_qos'] = qos_dict.get(QOSParams.enable_qos.value)
        kwargs['bw_obj'] = QOSBandwidthControl.from_dict(qos_dict)
        return cls(cluster_op, **kwargs)

    @classmethod
    def from_qos_block(cls, qos_block: RawBlock, cluster_op: bool = False) -> 'QOS':
        kwargs: dict[str, Any] = {}
        # qos block will have qos type as enum value
        if cluster_op:
            qos_type = qos_block.values.get(QOSParams.qos_type.value)
            if qos_type:
                kwargs['qos_type'] = QOSType(qos_type)
        kwargs['enable_qos'] = qos_block.values.get(QOSParams.enable_qos.value)
        kwargs['bw_obj'] = QOSBandwidthControl.from_qos_block(qos_block)
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
        return result

    def to_dict(self) -> Dict[str, Any]:
        r: Dict[str, Any] = {}
        r[QOSParams.enable_qos.value] = self.enable_qos
        if self.cluster_op and self.qos_type:
            r[QOSParams.qos_type.value] = self.qos_type.name
        if self.bw_obj and (res := self.bw_obj.to_dict()):
            r.update(res)
        return r
