import concurrent.futures
import json
from ceph_node_proxy.basesystem import BaseSystem
from ceph_node_proxy.redfish_client import RedFishClient
from time import sleep
from ceph_node_proxy.util import get_logger, to_snake_case
from typing import Dict, Any, List, Callable, Union
from urllib.error import HTTPError, URLError


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
        if result is None:
            self.log.error(f'The client reported an error when getting path: {path}')
            raise RuntimeError(f'Could not get path: {path}')
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

    def _update_sn(self) -> None:
        raise NotImplementedError()

    def _update_memory(self) -> None:
        raise NotImplementedError()

    def _update_power(self) -> None:
        raise NotImplementedError()

    def _update_fans(self) -> None:
        raise NotImplementedError()

    def _update_network(self) -> None:
        raise NotImplementedError()

    def _update_processors(self) -> None:
        raise NotImplementedError()

    def _update_storage(self) -> None:
        raise NotImplementedError()

    def _update_firmwares(self) -> None:
        raise NotImplementedError()

    def device_led_on(self, device: str) -> int:
        data: Dict[str, bool] = {'LocationIndicatorActive': True}
        try:
            result = self.set_device_led(device, data)
        except (HTTPError, KeyError):
            return 0
        return result

    def device_led_off(self, device: str) -> int:
        data: Dict[str, bool] = {'LocationIndicatorActive': False}
        try:
            result = self.set_device_led(device, data)
        except (HTTPError, KeyError):
            return 0
        return result

    def chassis_led_on(self) -> int:
        data: Dict[str, str] = {'IndicatorLED': 'Blinking'}
        result = self.set_chassis_led(data)
        return result

    def chassis_led_off(self) -> int:
        data: Dict[str, str] = {'IndicatorLED': 'Lit'}
        result = self.set_chassis_led(data)
        return result

    def get_device_led(self, device: str) -> Dict[str, Any]:
        endpoint = self._sys['storage'][device]['redfish_endpoint']
        try:
            result = self.client.query(method='GET',
                                       endpoint=endpoint,
                                       timeout=10)
        except HTTPError as e:
            self.log.error(f"Couldn't get the ident device LED status for device '{device}': {e}")
            raise
        response_json = json.loads(result[1])
        _result: Dict[str, Any] = {'http_code': result[2]}
        if result[2] == 200:
            _result['LocationIndicatorActive'] = response_json['LocationIndicatorActive']
        else:
            _result['LocationIndicatorActive'] = None
        return _result

    def set_device_led(self, device: str, data: Dict[str, bool]) -> int:
        try:
            _, _, status = self.client.query(
                data=json.dumps(data),
                method='PATCH',
                endpoint=self._sys['storage'][device]['redfish_endpoint']
            )
        except (HTTPError, KeyError) as e:
            self.log.error(f"Couldn't set the ident device LED for device '{device}': {e}")
            raise
        return status

    def get_chassis_led(self) -> Dict[str, Any]:
        endpoint = list(self.endpoints['chassis'].get_members_endpoints().values())[0]
        try:
            result = self.client.query(method='GET',
                                       endpoint=endpoint,
                                       timeout=10)
        except HTTPError as e:
            self.log.error(f"Couldn't get the ident chassis LED status: {e}")
            raise
        response_json = json.loads(result[1])
        _result: Dict[str, Any] = {'http_code': result[2]}
        if result[2] == 200:
            _result['LocationIndicatorActive'] = response_json['LocationIndicatorActive']
        else:
            _result['LocationIndicatorActive'] = None
        return _result

    def set_chassis_led(self, data: Dict[str, str]) -> int:
        # '{"IndicatorLED": "Lit"}'      -> LocationIndicatorActive = false
        # '{"IndicatorLED": "Blinking"}' -> LocationIndicatorActive = true
        try:
            _, _, status = self.client.query(
                data=json.dumps(data),
                method='PATCH',
                endpoint=list(self.endpoints['chassis'].get_members_endpoints().values())[0]
            )
        except HTTPError as e:
            self.log.error(f"Couldn't set the ident chassis LED: {e}")
            raise
        return status

    def shutdown_host(self, force: bool = False) -> int:
        reboot_type: str = 'GracefulRebootWithForcedShutdown' if force else 'GracefulRebootWithoutForcedShutdown'

        try:
            job_id: str = self.create_reboot_job(reboot_type)
            status = self.schedule_reboot_job(job_id)
        except (HTTPError, KeyError) as e:
            self.log.error(f"Couldn't create the reboot job: {e}")
            raise
        return status

    def powercycle(self) -> int:
        try:
            job_id: str = self.create_reboot_job('PowerCycle')
            status = self.schedule_reboot_job(job_id)
        except (HTTPError, URLError) as e:
            self.log.error(f"Couldn't perform power cycle: {e}")
            raise
        return status

    def create_reboot_job(self, reboot_type: str) -> str:
        data: Dict[str, str] = dict(RebootJobType=reboot_type)
        try:
            headers, _, _ = self.client.query(
                data=json.dumps(data),
                endpoint=self.create_reboot_job_endpoint
            )
            job_id: str = headers['Location'].split('/')[-1]
        except (HTTPError, URLError) as e:
            self.log.error(f"Couldn't create the reboot job: {e}")
            raise
        return job_id

    def schedule_reboot_job(self, job_id: str) -> int:
        data: Dict[str, Union[List[str], str]] = dict(JobArray=[job_id], StartTimeInterval='TIME_NOW')
        try:
            _, _, status = self.client.query(
                data=json.dumps(data),
                endpoint=self.setup_job_queue_endpoint
            )
        except (HTTPError, KeyError) as e:
            self.log.error(f"Couldn't schedule the reboot job: {e}")
            raise
        return status
