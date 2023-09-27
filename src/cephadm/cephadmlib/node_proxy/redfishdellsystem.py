from .baseredfishsystem import BaseRedfishSystem
from .util import Logger, normalize_dict, to_snake_case
from typing import Dict, Any, List


class RedfishDellSystem(BaseRedfishSystem):
    def __init__(self, **kw: Any) -> None:
        self.system_endpoint = kw.get('systemd_endpoint', '/Systems/System.Embedded.1')
        super().__init__(**kw)
        self.log = Logger(__name__)

    def build_system_data(self,
                   fields: List,
                   path: str) -> Dict[str, Dict[str, Dict]]:
        result: Dict[str, Dict[str, Dict]] = dict()
        for member_info in self.get_members(path):
            member_id = member_info['Id']
            result[member_id] = dict()
            for field in fields:
                try:
                    result[member_id][to_snake_case(field)] = member_info[field]
                except KeyError:
                    self.log.logger.warning(f"Could not find field: {field} in member_info: {member_info}")

        return normalize_dict(result)

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['status']

    def get_metadata(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['metadata']

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['memory']

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['processors']

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['network']

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['storage']

    # def _update_system(self) -> None:
    #     redfish_system = self.client.get_path(self.system_endpoint)
    #     self._system = {**redfish_system, **self._system}

    def _update_network(self) -> None:
        fields = ['Description', 'Name', 'SpeedMbps', 'Status']
        self.log.logger.info("Updating network")
        self._system['network'] = self.build_system_data(fields, 'EthernetInterfaces')

    def _update_processors(self) -> None:
        fields = ['Description',
                  'TotalCores',
                  'TotalThreads',
                  'ProcessorType',
                  'Model',
                  'Status',
                  'Manufacturer']
        self.log.logger.info("Updating processors")
        self._system['processors'] = self.build_system_data(fields, 'Processors')

    def _update_storage(self) -> None:
        fields = ['Description',
                  'CapacityBytes',
                  'Model', 'Protocol',
                  'SerialNumber', 'Status',
                  'PhysicalLocation']
        entities = self.get_members('Storage')
        self.log.logger.info("Updating storage")
        result: Dict[str, Dict[str, Dict]] = dict()
        for entity in entities:
            for drive in entity['Drives']:
                drive_path = drive['@odata.id']
                drive_info = self._get_path(drive_path)
                drive_id = drive_info['Id']
                result[drive_id] = dict()
                for field in fields:
                    result[drive_id][to_snake_case(field)] = drive_info[field]
                    result[drive_id]['entity'] = entity['Id']
        self._system['storage'] = normalize_dict(result)

    def _update_metadata(self) -> None:
        self.log.logger.info("Updating metadata")
        pass

    def _update_memory(self) -> None:
        fields = ['Description',
                  'MemoryDeviceType',
                  'CapacityMiB',
                  'Status']
        self.log.logger.info("Updating memory")
        self._system['memory'] = self.build_system_data(fields, 'Memory')
