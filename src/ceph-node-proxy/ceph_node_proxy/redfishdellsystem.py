from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem, Endpoint
from ceph_node_proxy.util import get_logger, normalize_dict, to_snake_case
from typing import Dict, Any, List, Optional
from urllib.error import HTTPError


class RedfishDellSystem(BaseRedfishSystem):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.log = get_logger(__name__)
        self.job_service_endpoint: str = '/redfish/v1/Managers/iDRAC.Embedded.1/Oem/Dell/DellJobService'
        self.create_reboot_job_endpoint: str = f'{self.job_service_endpoint}/Actions/DellJobService.CreateRebootJob'
        self.setup_job_queue_endpoint: str = f'{self.job_service_endpoint}/Actions/DellJobService.SetupJobQueue'

    def build_data(self,
                   data: Dict[str, Any],
                   fields: List[str],
                   attribute: Optional[str] = None) -> Dict[str, Dict[str, Dict]]:
        result: Dict[str, Dict[str, Optional[Dict]]] = dict()
        member_id: str = ''

        def process_data(m_id: str, fields: List[str], data: Dict[str, Any]) -> Dict[str, Any]:
            result: Dict[str, Any] = {}
            for field in fields:
                try:
                    result[to_snake_case(field)] = data[field]
                except KeyError:
                    self.log.warning(f'Could not find field: {field} in data: {data}')
                    result[to_snake_case(field)] = None
            return result

        try:
            if attribute is not None:
                data_items = data[attribute]
            else:
                # The following is a hack to re-inject the key to the dict
                # as we have the following structure when `attribute` is passed:
                # "PowerSupplies": [ {"MemberId": "0", ...}, {"MemberId": "1", ...} ]
                # vs. this structure in the opposite case:
                # { "CPU.Socket.2": { "Id": "CPU.Socket.2", "Manufacturer": "Intel" }, "CPU.Socket.1": {} }
                # With the first case, we clearly use the field "MemberId".
                # With the second case, we use the key of the dict.
                # This is mostly for avoiding code duplication.
                data_items = [{'MemberId': k, **v} for k, v in data.items()]
            for d in data_items:
                member_id = d.get('MemberId')
                result[member_id] = {}
                result[member_id] = process_data(member_id, fields, d)

        except Exception as e:
            self.log.error(f"Can't build data: {e}")
        return normalize_dict(result)

    def get_sn(self) -> str:
        return self._sys.get('SKU', '')

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys.get('status', {})

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys.get('memory', {})

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys.get('processors', {})

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys.get('network', {})

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys.get('storage', {})

    def get_firmwares(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys.get('firmwares', {})

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys.get('power', {})

    def get_fans(self) -> Dict[str, Dict[str, Dict]]:
        return self._sys.get('fans', {})

    def _update_network(self) -> None:
        fields = ['Description', 'Name', 'SpeedMbps', 'Status']
        self.log.debug('Updating network')
        self.update('systems', 'network', 'EthernetInterfaces', fields)

    def update(self,
               collection: str,
               component: str,
               path: str,
               fields: List[str],
               attribute: Optional[str] = None) -> None:
        members: List[str] = self.endpoints[collection].get_members_names()
        result: Dict[str, Any] = {}
        data: Dict[str, Any] = {}
        data_built: Dict[str, Any] = {}
        if not members:
            data = self.endpoints[collection][path].get_members_data()
            data_built = self.build_data(data=data, fields=fields, attribute=attribute)
            result = data_built
        else:
            for member in members:
                data_built = {}
                try:
                    if attribute is None:
                        data = self.endpoints[collection][member][path].get_members_data()
                    else:
                        data = self.endpoints[collection][member][path].data
                except HTTPError as e:
                    self.log.debug(f'Error while updating {component}: {e}')
                else:
                    data_built = self.build_data(data=data, fields=fields, attribute=attribute)
                    result[member] = data_built
        self._sys[component] = result

    def _update_processors(self) -> None:
        fields = ['Description',
                  'TotalCores',
                  'TotalThreads',
                  'ProcessorType',
                  'Model',
                  'Status',
                  'Manufacturer']
        self.log.debug('Updating processors')
        self.update('systems', 'processors', 'Processors', fields)

    def _update_storage(self) -> None:
        fields = ['Description',
                  'CapacityBytes',
                  'Model', 'Protocol',
                  'LocationIndicatorActive',
                  'SerialNumber', 'Status',
                  'PhysicalLocation']
        result: Dict[str, Dict[str, Dict]] = dict()
        self.log.debug('Updating storage')
        for member in self.endpoints['systems'].get_members_names():
            result[member] = {}
            members_data = self.endpoints['systems'][member]['Storage'].get_members_data()
            for entity in members_data:
                for drive in members_data[entity]['Drives']:
                    data: Dict[str, Any] = Endpoint(drive['@odata.id'], self.endpoints.client).data
                    drive_id = data['Id']
                    result[member][drive_id] = dict()
                    result[member][drive_id]['redfish_endpoint'] = data['@odata.id']
                    for field in fields:
                        result[member][drive_id][to_snake_case(field)] = data[field]
                        result[member][drive_id]['entity'] = entity
            self._sys['storage'] = normalize_dict(result)

    def _update_sn(self) -> None:
        serials: List[str] = []
        self.log.debug('Updating serial number')
        data: Dict[str, Any] = self.endpoints['systems'].get_members_data()
        for sys in data.keys():
            serials.append(data[sys]['SKU'])
        self._sys['SKU'] = ','.join(serials)

    def _update_memory(self) -> None:
        fields = ['Description',
                  'MemoryDeviceType',
                  'CapacityMiB',
                  'Status']
        self.log.debug('Updating memory')
        self.update('systems', 'memory', 'Memory', fields)

    def _update_power(self) -> None:
        fields = [
            'Name',
            'Model',
            'Manufacturer',
            'Status'
        ]
        self.log.debug('Updating powersupplies')
        self.update('chassis', 'power', 'Power', fields, attribute='PowerSupplies')

    def _update_fans(self) -> None:
        fields = [
            'Name',
            'PhysicalContext',
            'Status'
        ]
        self.log.debug('Updating fans')
        self.update('chassis', 'fans', 'Thermal', fields, attribute='Fans')

    def _update_firmwares(self) -> None:
        fields = [
            'Name',
            'Description',
            'ReleaseDate',
            'Version',
            'Updateable',
            'Status',
        ]
        self.log.debug('Updating firmwares')
        self.update('update_service', 'firmwares', 'FirmwareInventory', fields)
