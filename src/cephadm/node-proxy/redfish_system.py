from system import System
from redfish_client import RedFishClient
from threading import Thread
from time import sleep
from flask import request
from util import logger

log = logger(__name__)


class RedfishSystem(System):
    def __init__(self,
                 host,
                 username,
                 password,
                 system_endpoint='/Systems/1'):
        log.info(f"redfish system initialization, host: {host}, user: {username}")
        self.client = RedFishClient(host, username, password)
        self.client.login()
        self._system = {}
        self.run = False
        self.thread = None
        self.system_endpoint = system_endpoint

    def get_system(self):
        return self._system

    def get_status(self):
        return self._system['Status']

    def get_metadata(self):
        return self._system['metadata']

    def get_memory(self):
        return self._system['memory']

    def get_power(self):
        return self._system['power']

    def get_processors(self):
        return self._system['processors']

    def get_network(self):
        return self._system['network']

    def get_storage(self):
        return self._system['storage']

    def _process_redfish_system(self, redfish_system):
        return redfish_system

    def _update_system(self):
        redfish_system = self.client.get_path(self.system_endpoint)
        self._system = self._process_redfish_system(redfish_system)

    def _update_metadata(self):
        log.info("Updating metadata")
        pass

    def _update_memory(self):
        log.info("Updating memory")
        pass

    def _update_power(self):
        log.info("Updating power")
        pass

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

    def start_update_loop(self):
        self.run = True
        self.thread = Thread(target=self.update)
        self.thread.start()

    def stop_update_loop(self):
        self.run = False
        self.thread.join()

    def update(self):
        #  this loop can have:
        #  - caching logic
        try:
            while self.run:
                self._update_system()
                # following calls in theory can be done in parallel
                self._update_metadata()
                self._update_memory()
                self._update_power()
                self._update_network()
                self._update_processors()
                self._update_storage()
                sleep(3)
        # Catching 'Exception' is probably not a good idea (devel only)
        except Exception:
            log.error(f"Error detected, logging out from redfish api")
            self.client.logout()
            raise
