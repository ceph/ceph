try:
    from typing import List, Optional, Dict, Any, Union
except ImportError:
    pass  # for type checking

from ceph.utils import datetime_now, datetime_to_str, str_to_datetime
import datetime
import json


class Devices(object):
    """
    A container for Device instances with reporting
    """

    def __init__(self, devices):
        # type: (List[Device]) -> None
        # sort devices by path name so ordering is consistent
        self.devices: List[Device] = sorted(devices, key=lambda d: d.path if d.path else '')

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Devices):
            return NotImplemented
        if len(self.devices) != len(other.devices):
            return False
        for d1, d2 in zip(other.devices, self.devices):
            if d1 != d2:
                return False
        return True

    def to_json(self):
        # type: () -> List[dict]
        return [d.to_json() for d in self.devices]

    @classmethod
    def from_json(cls, input):
        # type: (List[Dict[str, Any]]) -> Devices
        return cls([Device.from_json(i) for i in input])

    def copy(self):
        # type: () -> Devices
        return Devices(devices=list(self.devices))


class Device(object):
    report_fields = [
        'ceph_device',
        'rejected_reasons',
        'available',
        'path',
        'sys_api',
        'created',
        'lvs',
        'human_readable_type',
        'device_id',
        'lsm_data',
        'crush_device_class'
    ]

    def __init__(self,
                 path,  # type: str
                 sys_api=None,  # type: Optional[Dict[str, Any]]
                 available=None,  # type: Optional[bool]
                 rejected_reasons=None,  # type: Optional[List[str]]
                 lvs=None,  # type: Optional[List[Dict[str, str]]]
                 device_id=None,  # type: Optional[str]
                 lsm_data=None,  # type: Optional[Dict[str, Dict[str, str]]]
                 created=None,  # type: Optional[datetime.datetime]
                 ceph_device=None,  # type: Optional[bool]
                 crush_device_class=None  # type: Optional[str]
                 ):

        self.path = path
        self.sys_api = sys_api if sys_api is not None else {}  # type: Dict[str, Any]
        self.available = available
        self.rejected_reasons = rejected_reasons if rejected_reasons is not None else []
        self.lvs = lvs
        self.device_id = device_id
        self.lsm_data = lsm_data if lsm_data is not None else {}  # type: Dict[str, Dict[str, str]]
        self.created = created if created is not None else datetime_now()
        self.ceph_device = ceph_device
        self.crush_device_class = crush_device_class

    def __eq__(self, other):
        # type: (Any) -> bool
        if not isinstance(other, Device):
            return NotImplemented
        diff = [k for k in self.report_fields if k != 'created' and (getattr(self, k)
                                                                     != getattr(other, k))]
        return not diff

    def to_json(self):
        # type: () -> dict
        return {
            k: (getattr(self, k) if k != 'created'
                or not isinstance(getattr(self, k), datetime.datetime)
                else datetime_to_str(getattr(self, k)))
            for k in self.report_fields
        }

    @classmethod
    def from_json(cls, input):
        # type: (Dict[str, Any]) -> Device
        if not isinstance(input, dict):
            raise ValueError('Device: Expected dict. Got `{}...`'.format(json.dumps(input)[:10]))
        ret = cls(
            **{
                key: (input.get(key, None) if key != 'created'
                      or not input.get(key, None)
                      else str_to_datetime(input.get(key, None)))
                for key in Device.report_fields
                if key != 'human_readable_type'
            }
        )
        if ret.rejected_reasons:
            ret.rejected_reasons = sorted(ret.rejected_reasons)
        return ret

    @property
    def human_readable_type(self):
        # type: () -> str
        if self.sys_api is None or 'rotational' not in self.sys_api:
            return "unknown"
        return 'hdd' if self.sys_api["rotational"] == "1" else 'ssd'

    def __repr__(self) -> str:
        device_desc: Dict[str, Union[str, List[str], List[Dict[str, str]]]] = {
            'path': self.path if self.path is not None else 'unknown',
            'lvs': self.lvs if self.lvs else 'None',
            'available': str(self.available),
            'ceph_device': str(self.ceph_device),
            'crush_device_class': str(self.crush_device_class)
        }
        if not self.available and self.rejected_reasons:
            device_desc['rejection reasons'] = self.rejected_reasons
        return "Device({})".format(
            ', '.join('{}={}'.format(key, device_desc[key]) for key in device_desc.keys())
        )
