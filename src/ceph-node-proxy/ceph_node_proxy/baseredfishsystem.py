import concurrent.futures
import dataclasses
import json
from dataclasses import dataclass
from ceph_node_proxy.basesystem import BaseSystem
from ceph_node_proxy.redfish_client import RedFishClient
from time import sleep
from ceph_node_proxy.util import get_logger, to_snake_case, normalize_dict
from typing import Dict, Any, List, Callable, Optional
from urllib.error import HTTPError, URLError


@dataclass
class ComponentUpdateSpec:
    collection: str
    path: str
    fields: List[str]
    attribute: Optional[str] = None


class EndpointMgr:
    NAME: str = 'EndpointMgr'

    def __init__(self,
                 client: RedFishClient,
                 prefix: str = RedFishClient.PREFIX) -> None:
        self.log = get_logger(f'{__name__}:{EndpointMgr.NAME}')
        self.prefix: str = prefix
        self.client: RedFishClient = client
        # Use explicit dictionary instead of dynamic attributes
        self._endpoints: Dict[str, Endpoint] = {}
        self._session_url: str = ''

    def __getitem__(self, index: str) -> 'Endpoint':
        if index not in self._endpoints:
            raise KeyError(f"'{index}' is not a valid endpoint. Available: {list(self._endpoints.keys())}")
        return self._endpoints[index]

    def get(self, name: str, default: Any = None) -> Any:
        return self._endpoints.get(name, default)

    def list_endpoints(self) -> List[str]:
        return list(self._endpoints.keys())

    @property
    def session(self) -> str:
        return self._session_url

    def init(self) -> None:
        error_msg: str = "Can't discover entrypoint(s)"
        try:
            _, _data, _ = self.client.query(endpoint=self.prefix)
            json_data: Dict[str, Any] = json.loads(_data)

            # Discover endpoints
            for k, v in json_data.items():
                if isinstance(v, dict) and '@odata.id' in v:
                    name: str = to_snake_case(k)
                    url: str = v['@odata.id']
                    self.log.info(f'entrypoint found: {name} = {url}')
                    self._endpoints[name] = Endpoint(url, self.client)

            # Extract session URL if available
            try:
                self._session_url = json_data['Links']['Sessions']['@odata.id']
            except (KeyError, TypeError):
                self.log.warning('Session URL not found in root response')
                self._session_url = ''

        except (URLError, KeyError, json.JSONDecodeError) as e:
            msg = f'{error_msg}: {e}'
            self.log.error(msg)
            raise RuntimeError(msg) from e


class Endpoint:
    NAME: str = 'Endpoint'

    def __init__(self, url: str, client: RedFishClient) -> None:
        self.log = get_logger(f'{__name__}:{Endpoint.NAME}')
        self.url: str = url
        self.client: RedFishClient = client
        self._children: Dict[str, 'Endpoint'] = {}
        self.data: Dict[str, Any] = self.get_data()
        self.id: str = ''
        self.members_names: List[str] = []

        if self.has_members:
            self.members_names = self.get_members_names()

        if self.data:
            try:
                self.id = self.data['Id']
            except KeyError:
                self.id = self.data['@odata.id'].split('/')[-1]
        else:
            self.log.warning(f'No data could be loaded for {self.url}')

    def __getitem__(self, key: str) -> 'Endpoint':
        if not isinstance(key, str) or not key or '/' in key:
            raise KeyError(key)

        if key not in self._children:
            child_url: str = f'{self.url.rstrip("/")}/{key}'
            self._children[key] = Endpoint(child_url, self.client)

        return self._children[key]

    def list_children(self) -> List[str]:
        return list(self._children.keys())

    def query(self, url: str) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        try:
            self.log.debug(f'Querying {url}')
            _, _data, _ = self.client.query(endpoint=url)
            if not _data:
                self.log.warning(f'Empty response from {url}')
            else:
                data = json.loads(_data)
        except KeyError as e:
            self.log.error(f'KeyError while querying {url}: {e}')
        except HTTPError as e:
            self.log.error(f'HTTP error while querying {url} - {e.code} - {e.reason}')
        except json.JSONDecodeError as e:
            self.log.error(f'JSON decode error while querying {url}: {e}')
        except Exception as e:
            self.log.error(f'Unexpected error while querying {url}: {type(e).__name__}: {e}')
        return data

    def get_data(self) -> Dict[str, Any]:
        return self.query(self.url)

    def get_members_names(self) -> List[str]:
        result: List[str] = []
        if self.has_members:
            for member in self.data['Members']:
                name: str = member['@odata.id'].split('/')[-1]
                result.append(name)
        return result

    def get_name(self, endpoint: str) -> str:
        return endpoint.split('/')[-1]

    def get_members_endpoints(self) -> Dict[str, str]:
        members: Dict[str, str] = {}

        self.log.error(f'get_members_endpoints called on {self.url}, has_members={self.has_members}')

        if self.has_members:
            url_parts = self.url.split('/redfish/v1/')
            if len(url_parts) > 1:
                base_path = '/redfish/v1/' + url_parts[1].split('/')[0]
            else:
                base_path = None

            for member in self.data['Members']:
                name = self.get_name(member['@odata.id'])
                endpoint_url = member['@odata.id']
                self.log.debug(f'Found member: {name} -> {endpoint_url}')
                
                if base_path and not endpoint_url.startswith(base_path):
                    self.log.warning(
                        f'Member endpoint {endpoint_url} does not match base path {base_path} '
                        f'from {self.url}. Skipping this member.'
                    )
                    continue

                members[name] = endpoint_url
        else:
            if self.data:
                name = self.get_name(self.url)
                members[name] = self.url
                self.log.warning(f'No Members array, using endpoint itself: {name} -> {self.url}')
            else:
                self.log.debug(f'Endpoint {self.url} has no data and no Members array')

        return members

    def get_members_data(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        self.log.debug(f'get_members_data called on {self.url}, has_members={self.has_members}')
        
        if self.has_members:
            self.log.debug(f'Endpoint {self.url} has Members array: {self.data.get("Members", [])}')
            members_endpoints = self.get_members_endpoints()
            
            # If no valid members after filtering, fall back to using the endpoint itself
            if not members_endpoints:
                self.log.warning(
                    f'Endpoint {self.url} has Members array but no valid members after filtering. '
                    f'Using endpoint itself as singleton resource.'
                )
                if self.data:
                    name = self.get_name(self.url)
                    result[name] = self.data
            else:
                for member, endpoint_url in members_endpoints.items():
                    self.log.info(f'Fetching data for member: {member} at {endpoint_url}')
                    result[member] = self.query(endpoint_url)
        else:
            self.log.debug(f'Endpoint {self.url} has no Members array, returning own data')
            if self.data:
                name = self.get_name(self.url)
                result[name] = self.data
            else:
                self.log.warning(f'Endpoint {self.url} has no members and empty data')

        return result

    @property
    def has_members(self) -> bool:
        return bool(self.data and 'Members' in self.data and isinstance(self.data['Members'], list))


class BaseRedfishSystem(BaseSystem):
    NETWORK_FIELDS: List[str] = ['Description', 'Name', 'SpeedMbps', 'Status']
    PROCESSORS_FIELDS: List[str] = [
        'Description', 'TotalCores', 'TotalThreads', 'ProcessorType', 'Model', 'Status', 'Manufacturer',
    ]
    MEMORY_FIELDS: List[str] = ['Description', 'MemoryDeviceType', 'CapacityMiB', 'Status']
    POWER_FIELDS: List[str] = ['Name', 'Model', 'Manufacturer', 'Status']
    FANS_FIELDS: List[str] = ['Name', 'PhysicalContext', 'Status']
    FIRMWARES_FIELDS: List[str] = [
        'Name', 'Description', 'ReleaseDate', 'Version', 'Updateable', 'Status',
    ]

    COMPONENT_SPECS: Dict[str, ComponentUpdateSpec] = {
        'network': ComponentUpdateSpec('systems', 'EthernetInterfaces', NETWORK_FIELDS, None),
        'processors': ComponentUpdateSpec('systems', 'Processors', PROCESSORS_FIELDS, None),
        'memory': ComponentUpdateSpec('systems', 'Memory', MEMORY_FIELDS, None),
        'power': ComponentUpdateSpec('chassis', 'Power', POWER_FIELDS, 'PowerSupplies'),
        'fans': ComponentUpdateSpec('chassis', 'Thermal', FANS_FIELDS, 'Fans'),
        'firmwares': ComponentUpdateSpec('update_service', 'FirmwareInventory', FIRMWARES_FIELDS, None),
    }

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.log = get_logger(__name__)
        self.host: str = kw['host']
        self.port: str = kw['port']
        self.username: str = kw['username']
        self.password: str = kw['password']
        # move the following line (class attribute?)
        self.client: RedFishClient = RedFishClient(host=self.host, port=self.port, username=self.username, password=self.password)
        self.endpoints: EndpointMgr = EndpointMgr(self.client)
        self.log.info(f'redfish system initialization, host: {self.host}, user: {self.username}')
        self.data_ready: bool = False
        self.previous_data: Dict = {}
        self.data: Dict[str, Dict[str, Any]] = {}
        self._system: Dict[str, Dict[str, Any]] = {}
        self._sys: Dict[str, Any] = {}
        self.job_service_endpoint: str = ''
        self.create_reboot_job_endpoint: str = ''
        self.setup_job_queue_endpoint: str = ''
        self.component_list: List[str] = kw.get('component_list', ['memory',
                                                                   'power',
                                                                   'fans',
                                                                   'network',
                                                                   'processors',
                                                                   'storage',
                                                                   'firmwares'])
        self.update_funcs: List[Callable] = []
        for component in self.component_list:
            self.log.debug(f'adding: {component} to hw component gathered list.')
            func = f'_update_{component}'
            if hasattr(self, func):
                f = getattr(self, func)
                self.update_funcs.append(f)

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
                    self.log.debug(f'Could not find field: {field} in data: {data}')
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
            self.log.error(f"GUITS_DEBUG: data_items= {data_items}")
            for d in data_items:
                member_id = d.get('MemberId')
                result[member_id] = {}
                result[member_id] = process_data(member_id, fields, d)
        except (KeyError, TypeError, AttributeError) as e:
            self.log.error(f"Can't build data: {e}")
            raise
        return normalize_dict(result)

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
                    self.log.error(f'Error while updating {component}: {e}')
                else:
                    data_built = self.build_data(data=data, fields=fields, attribute=attribute)
                    result[member] = data_built
        self._sys[component] = result

    def main(self) -> None:
        self.stop = False
        self.client.login()
        self.endpoints.init()

        while not self.stop:
            self.log.debug('waiting for a lock in the update loop.')
            with self.lock:
                if not self.pending_shutdown:
                    self.log.debug('lock acquired in the update loop.')
                    try:
                        self._update_system()
                        self._update_sn()

                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            executor.map(lambda f: f(), self.update_funcs)

                        self.data_ready = True
                    except RuntimeError as e:
                        self.stop = True
                        self.log.error(f'Error detected, trying to gracefully log out from redfish api.\n{e}')
                        self.client.logout()
                        raise
                    sleep(5)
            self.log.debug('lock released in the update loop.')
        self.log.debug('exiting update loop.')
        raise SystemExit(0)

    def flush(self) -> None:
        self.log.debug('Acquiring lock to flush data.')
        self.lock.acquire()
        self.log.debug('Lock acquired, flushing data.')
        self._system = {}
        self.previous_data = {}
        self.log.info('Data flushed.')
        self.data_ready = False
        self.log.debug('Data marked as not ready.')
        self.lock.release()
        self.log.debug('Released the lock after flushing data.')

    # @retry(retries=10, delay=2)
    def _get_path(self, path: str) -> Dict:
        result: Dict[str, Any] = {}
        try:
            if not self.pending_shutdown:
                self.log.debug(f'Getting path: {path}')
                result = self.client.get_path(path)
            else:
                self.log.debug(f'Pending shutdown, aborting query to {path}')
        except RuntimeError:
            raise

        return result

    def get_members(self, data: Dict[str, Any], path: str) -> List:
        return [self._get_path(member['@odata.id']) for member in data['Members']]

    def get_system(self) -> Dict[str, Any]:
        result = {
            'host': self.get_host(),
            'sn': self.get_sn(),
            'status': {
                'storage': self.get_storage(),
                'processors': self.get_processors(),
                'network': self.get_network(),
                'memory': self.get_memory(),
                'power': self.get_power(),
                'fans': self.get_fans()
            },
            'firmwares': self.get_firmwares(),
        }
        return result

    def _update_system(self) -> None:
        system_members: Dict[str, Any] = self.endpoints['systems'].get_members_data()
        update_service_members: Endpoint = self.endpoints['update_service']

        for member, data in system_members.items():
            self._system[member] = data
            self._sys[member] = dict()

        self._system[update_service_members.id] = update_service_members.data

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

    def get_component_spec_overrides(self) -> Dict[str, Dict[str, Any]]:
        return {}

    def get_update_spec(self, component: str) -> ComponentUpdateSpec:
        spec = self.COMPONENT_SPECS[component]
        overrides = self.get_component_spec_overrides().get(component)
        if not overrides:
            return spec
        return dataclasses.replace(spec, **overrides)

    def _run_update(self, component: str) -> None:
        self.log.debug(f'Updating {component}')
        spec = self.get_update_spec(component)
        self.update(spec.collection, component, spec.path, spec.fields, attribute=spec.attribute)

    def _update_network(self) -> None:
        self._run_update('network')

    def _update_processors(self) -> None:
        self._run_update('processors')

    def _update_storage(self) -> None:
        fields = ['Description',
                  'CapacityBytes',
                  'Model', 'Protocol',
                  'LocationIndicatorActive',
                  'SerialNumber', 'Status',
                  'PhysicalLocation']
        result: Dict[str, Dict[str, Dict]] = dict()
        self.log.debug('Updating storage')
        members_names = self.endpoints['systems'].get_members_names()
        for member in members_names:
            result[member] = {}
            members_data = self.endpoints['systems'][member]['Storage'].get_members_data()
            for entity in members_data:
                for drive in members_data[entity]['Drives']:
                    data: Dict[str, Any] = Endpoint(drive['@odata.id'], self.endpoints.client).data
                    drive_id = data['Id']
                    result[member][drive_id] = dict()
                    result[member][drive_id]['redfish_endpoint'] = data['@odata.id']
                    for field in fields:
                        result[member][drive_id][to_snake_case(field)] = data.get(field)
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
        self._run_update('memory')

    def _update_power(self) -> None:
        self._run_update('power')

    def _update_fans(self) -> None:
        self._run_update('fans')

    def _update_firmwares(self) -> None:
        self._run_update('firmwares')

    def device_led_on(self, device: str) -> int:
        raise NotImplementedError()

    def device_led_off(self, device: str) -> int:
        raise NotImplementedError()

    def chassis_led_on(self) -> int:
        raise NotImplementedError()

    def chassis_led_off(self) -> int:
        raise NotImplementedError()

    def get_device_led(self, device: str) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_device_led(self, device: str, data: Dict[str, bool]) -> int:
        raise NotImplementedError()

    def get_chassis_led(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_chassis_led(self, data: Dict[str, str]) -> int:
        raise NotImplementedError()

    def shutdown_host(self, force: bool = False) -> int:
        raise NotImplementedError()

    def powercycle(self) -> int:
        raise NotImplementedError()

    def create_reboot_job(self, reboot_type: str) -> str:
        raise NotImplementedError()

    def schedule_reboot_job(self, job_id: str) -> int:
        raise NotImplementedError()