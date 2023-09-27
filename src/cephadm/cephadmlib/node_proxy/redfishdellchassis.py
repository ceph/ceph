from .baseredfishsystem import BaseRedfishSystem
from .redfish_client import RedFishClient
from threading import Thread, Lock
from time import sleep
from .util import Logger, retry, normalize_dict, to_snake_case
from typing import Dict, Any, List, Union


class RedfishDellChassis(BaseRedfishSystem):
    def __init__(self, **kw: Any) -> None:
        self.chassis_endpoint = kw.get('chassis_endpoint', '/Chassis/System.Embedded.1')
        super().__init__(**kw)
        self.log = Logger(__name__)
        self.log.logger.info(f"{__name__} initialization.")

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['power']

    def get_fans(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['fans']

    def get_chassis(self) -> Dict[str, Dict[str, Dict]]:
        result = {
            'power': self.get_power(),
            'fans': self.get_fans()
        }
        return result

    def _update_power(self) -> None:
        fields = {
            "PowerSupplies": [
                "Name",
                "Model",
                "Manufacturer",
                "Status"
            ]
        }
        self.log.logger.info("Updating powersupplies")
        self._system['power'] = self.build_chassis_data(fields, 'Power')

    def _update_fans(self) -> None:
        fields = {
            "Fans": [
                "Name",
                "PhysicalContext",
                "Status"
            ],
        }
        self.log.logger.info("Updating fans")
        self._system['fans'] = self.build_chassis_data(fields, 'Thermal')

    def build_chassis_data(self,
                   fields: Dict[str, List[str]],
                   path: str) -> Dict[str, Dict[str, Dict]]:
        result: Dict[str, Dict[str, Dict]] = dict()
        data = self._get_path(f"{self.chassis_endpoint}/{path}")

        for elt, _fields in fields.items():
            for member_elt in data[elt]:
                _id = member_elt['MemberId']
                result[_id] = dict()
                for field in _fields:
                    try:
                        result[_id][to_snake_case(field)] = member_elt[field]
                    except KeyError:
                        self.log.logger.warning(f"Could not find field: {field} in data: {data[elt]}")
        return normalize_dict(result)
