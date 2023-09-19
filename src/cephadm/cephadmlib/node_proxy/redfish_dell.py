from .redfish_system import RedfishSystem
from .util import Logger, normalize_dict, to_snake_case
from typing import Dict, Any


class RedfishDell(RedfishSystem):
    def __init__(self, **kw: Any) -> None:
        self.log = Logger(__name__)
        if kw.get('system_endpoint') is None:
            kw['system_endpoint'] = '/Systems/System.Embedded.1'
        super().__init__(**kw)

    def _update_network(self) -> None:
        fields = ['Description', 'Name', 'SpeedMbps', 'Status']
        self.log.logger.info("Updating network")
        self._system['network'] = self.build_data(fields, 'EthernetInterfaces')

    def _update_processors(self) -> None:
        fields = ['Description',
                  'TotalCores',
                  'TotalThreads',
                  'ProcessorType',
                  'Model',
                  'Status',
                  'Manufacturer']
        self.log.logger.info("Updating processors")
        self._system['processors'] = self.build_data(fields, 'Processors')

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
        self._system['memory'] = self.build_data(fields, 'Memory')

    def _update_power(self) -> None:
        self.log.logger.info("Updating power")
        pass
