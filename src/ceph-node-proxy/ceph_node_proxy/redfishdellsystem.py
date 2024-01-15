from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem
from ceph_node_proxy.util import Logger, normalize_dict, to_snake_case
from typing import Dict, Any, List


class RedfishDellSystem(BaseRedfishSystem):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.log = Logger(__name__)
        self.job_service_endpoint: str = '/redfish/v1/Managers/iDRAC.Embedded.1/Oem/Dell/DellJobService'
        self.create_reboot_job_endpoint: str = f'{self.job_service_endpoint}/Actions/DellJobService.CreateRebootJob'
        self.setup_job_queue_endpoint: str = f'{self.job_service_endpoint}/Actions/DellJobService.SetupJobQueue'

    def build_common_data(self,
                          data: Dict[str, Any],
                          fields: List,
                          path: str) -> Dict[str, Dict[str, Dict]]:
        result: Dict[str, Dict[str, Dict]] = dict()
        for member_info in self.get_members(data, path):
            member_id = member_info['Id']
            result[member_id] = dict()
            for field in fields:
                try:
                    result[member_id][to_snake_case(field)] = member_info[field]
                except KeyError:
                    self.log.logger.warning(f'Could not find field: {field} in member_info: {member_info}')

        return normalize_dict(result)

    def build_chassis_data(self,
                           fields: Dict[str, List[str]],
                           path: str) -> Dict[str, Dict[str, Dict]]:
        result: Dict[str, Dict[str, Dict]] = dict()
        data = self._get_path(f'{self.chassis_endpoint}/{path}')

        for elt, _fields in fields.items():
            for member_elt in data[elt]:
                _id = member_elt['MemberId']
                result[_id] = dict()
                for field in _fields:
                    try:
                        result[_id][to_snake_case(field)] = member_elt[field]
                    except KeyError:
                        self.log.logger.warning(f'Could not find field: {field} in data: {data[elt]}')
        return normalize_dict(result)

    def get_sn(self) -> str:
        return self._sys['SKU']

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys['status']

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys['memory']

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys['processors']

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys['network']

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys['storage']

    def get_firmwares(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys['firmwares']

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys['power']

    def get_fans(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys['fans']

    def _update_network(self) -> None:
        fields = ['Description', 'Name', 'SpeedMbps', 'Status']
        self.log.logger.debug('Updating network')
        self._sys['network'] = self.build_common_data(data=self._system['Systems'],
                                                      fields=fields,
                                                      path='EthernetInterfaces')

    def _update_processors(self) -> None:
        fields = ['Description',
                  'TotalCores',
                  'TotalThreads',
                  'ProcessorType',
                  'Model',
                  'Status',
                  'Manufacturer']
        self.log.logger.debug('Updating processors')
        self._sys['processors'] = self.build_common_data(data=self._system['Systems'],
                                                         fields=fields,
                                                         path='Processors')

    def _update_storage(self) -> None:
        fields = ['Description',
                  'CapacityBytes',
                  'Model', 'Protocol',
                  'SerialNumber', 'Status',
                  'PhysicalLocation']
        entities = self.get_members(data=self._system['Systems'],
                                    path='Storage')
        self.log.logger.debug('Updating storage')
        result: Dict[str, Dict[str, Dict]] = dict()
        for entity in entities:
            for drive in entity['Drives']:
                drive_path = drive['@odata.id']
                drive_info = self._get_path(drive_path)
                drive_id = drive_info['Id']
                result[drive_id] = dict()
                result[drive_id]['redfish_endpoint'] = drive['@odata.id']
                for field in fields:
                    result[drive_id][to_snake_case(field)] = drive_info[field]
                    result[drive_id]['entity'] = entity['Id']
        self._sys['storage'] = normalize_dict(result)

    def _update_sn(self) -> None:
        self.log.logger.debug('Updating serial number')
        self._sys['SKU'] = self._system['Systems']['SKU']

    def _update_memory(self) -> None:
        fields = ['Description',
                  'MemoryDeviceType',
                  'CapacityMiB',
                  'Status']
        self.log.logger.debug('Updating memory')
        self._sys['memory'] = self.build_common_data(data=self._system['Systems'],
                                                     fields=fields,
                                                     path='Memory')

    def _update_power(self) -> None:
        fields = {
            'PowerSupplies': [
                'Name',
                'Model',
                'Manufacturer',
                'Status'
            ]
        }
        self.log.logger.debug('Updating powersupplies')
        self._sys['power'] = self.build_chassis_data(fields, 'Power')

    def _update_fans(self) -> None:
        fields = {
            'Fans': [
                'Name',
                'PhysicalContext',
                'Status'
            ],
        }
        self.log.logger.debug('Updating fans')
        self._sys['fans'] = self.build_chassis_data(fields, 'Thermal')

    def _update_firmwares(self) -> None:
        fields = [
            'Name',
            'Description',
            'ReleaseDate',
            'Version',
            'Updateable',
            'Status',
        ]
        self.log.logger.debug('Updating firmwares')
        self._sys['firmwares'] = self.build_common_data(data=self._system['UpdateService'],
                                                        fields=fields,
                                                        path='FirmwareInventory')
