from redfish_system import RedfishSystem
from util import logger

log = logger(__name__)


class RedfishDell(RedfishSystem):
    def __init__(self, **kw):
        if kw.get('system_endpoint') is None:
            kw['system_endpoint'] = '/Systems/System.Embedded.1'
        super().__init__(**kw)

    def _update_network(self):
        net_path = self._system['EthernetInterfaces']['@odata.id']
        log.info("Updating network")
        network_info = self.client.get_path(net_path)
        self._system['network'] = {}
        result = dict()
        for interface in network_info['Members']:
            interface_path = interface['@odata.id']
            interface_info = self.client.get_path(interface_path)
            interface_id = interface_info['Id']
            result[interface_id] = dict()
            result[interface_id]['description'] = interface_info['Description']
            result[interface_id]['name'] = interface_info['Name']
            result[interface_id]['speed_mbps'] = interface_info['SpeedMbps']
            result[interface_id]['status'] = interface_info['Status']
        self._system['network'] = result

    def _update_processors(self):
        cpus_path = self._system['Processors']['@odata.id']
        log.info("Updating processors")
        cpus_info = self.client.get_path(cpus_path)
        self._system['processors'] = {}
        result = dict()
        for cpu in cpus_info['Members']:
            cpu_path = cpu['@odata.id']
            cpu_info = self.client.get_path(cpu_path)
            cpu_id = cpu_info['Id']
            result[cpu_id] = dict()
            result[cpu_id]['description'] = cpu_info['Description']
            result[cpu_id]['cores'] = cpu_info['TotalCores']
            result[cpu_id]['threads'] = cpu_info['TotalThreads']
            result[cpu_id]['type'] = cpu_info['ProcessorType']
            result[cpu_id]['model'] = cpu_info['Model']
            result[cpu_id]['status'] = cpu_info['Status']
            result[cpu_id]['manufacturer'] = cpu_info['Manufacturer']
        self._system['processors'] = result

    def _update_storage(self):
        storage_path = self._system['Storage']['@odata.id']
        log.info("Updating storage")
        storage_info = self.client.get_path(storage_path)
        result = dict()
        for storage in storage_info['Members']:
            entity_path = storage['@odata.id']
            entity_info = self.client.get_path(entity_path)
            for drive in entity_info['Drives']:
                drive_path = drive['@odata.id']
                drive_info = self.client.get_path(drive_path)
                drive_id = drive_info['Id']
                result[drive_id] = dict()
                result[drive_id]['description'] = drive_info['Description']
                result[drive_id]['capacity_bytes'] = drive_info['CapacityBytes']
                result[drive_id]['model'] = drive_info['Model']
                result[drive_id]['protocol'] = drive_info['Protocol']
                result[drive_id]['serial_number'] = drive_info['SerialNumber']
                result[drive_id]['status'] = drive_info['Status']
                result[drive_id]['location'] = drive_info['PhysicalLocation']
        self._system['storage'] = result

    def _update_metadata(self):
        log.info("Updating metadata")
        pass

    def _update_memory(self):
        log.info("Updating memory")
        pass

    def _update_power(self):
        log.info("Updating power")
        pass
