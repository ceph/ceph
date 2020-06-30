try:
    from typing import List, Optional, Dict, Any
except ImportError:
    pass  # for type checking

import json


class Devices(object):
    """
    A container for Device instances with reporting
    """

    def __init__(self, devices):
        # type: (List[Device]) -> None
        self.devices = devices  # type: List[Device]

    def __eq__(self, other):
        return self.to_json() == other.to_json()

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
        'rejected_reasons',
        'available',
        'path',
        'sys_api',
        'lvs',
        'human_readable_type',
        'device_id'
    ]

    def __init__(self,
                 path,  # type: str
                 sys_api=None,  # type: Optional[Dict[str, Any]]
                 available=None,  # type: Optional[bool]
                 rejected_reasons=None,  # type: Optional[List[str]]
                 lvs=None,  # type: Optional[List[str]]
                 device_id=None,  # type: Optional[str]
                 ):
        self.path = path
        self.sys_api = sys_api if sys_api is not None else {}  # type: Dict[str, Any]
        self.available = available
        self.rejected_reasons = rejected_reasons if rejected_reasons is not None else []
        self.lvs = lvs
        self.device_id = device_id

    def to_json(self):
        # type: () -> dict
        return {
            k: getattr(self, k) for k in self.report_fields
        }

    @classmethod
    def from_json(cls, input):
        # type: (Dict[str, Any]) -> Device
        if not isinstance(input, dict):
            raise ValueError('Device: Expected dict. Got `{}...`'.format(json.dumps(input)[:10]))
        ret = cls(
            **{
                key: input.get(key, None)
                for key in Device.report_fields
                if key != 'human_readable_type'
            }
        )
        return ret

    @property
    def human_readable_type(self):
        # type: () -> str
        if self.sys_api is None or 'rotational' not in self.sys_api:
            return "unknown"
        return 'hdd' if self.sys_api["rotational"] == "1" else 'ssd'
