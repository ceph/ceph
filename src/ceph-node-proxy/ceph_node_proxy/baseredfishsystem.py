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

    def __getitem__(self, index: str) -> Any:
        if index in self.__dict__:
            return self.__dict__[index]
        else:
            raise RuntimeError(f'{index} is not a valid endpoint.')

    def init(self) -> None:
        _error_msg: str = "Can't discover entrypoint(s)"
        try:
            _, _data, _ = self.client.query(endpoint=self.prefix)
            json_data: Dict[str, Any] = json.loads(_data)
            for k, v in json_data.items():
                if '@odata.id' in v:
                    self.log.debug(f'entrypoint found: {to_snake_case(k)} = {v["@odata.id"]}')
                    _name: str = to_snake_case(k)
                    _url: str = v['@odata.id']
                    e = Endpoint(self, _url, self.client)
                    setattr(self, _name, e)
            setattr(self, 'session', json_data['Links']['Sessions']['@odata.id'])  # TODO(guits): needs to be fixed
        except (URLError, KeyError) as e:
            msg = f'{_error_msg}: {e}'
            self.log.error(msg)
            raise RuntimeError


class Endpoint:
    NAME: str = 'Endpoint'

    def __init__(self, url: str, client: RedFishClient) -> None:
        self.log = get_logger(f'{__name__}:{Endpoint.NAME}')
        self.url: str = url
        self.client: RedFishClient = client
        self.data: Dict[str, Any] = self.get_data()
        self.id: str = ''
        self.members_names: List[str] = []

        if self.has_members:
            self.members_names = self.get_members_names()

        if self.data:
            try:
                self.id = self.data['Id']
            except KeyError:
                self.id = self.data['@odata.id'].split('/')[-1:]
        else:
            self.log.warning(f'No data could be loaded for {self.url}')

    def __getitem__(self, index: str) -> Any:
        if not getattr(self, index, False):
            _url: str = f'{self.url}/{index}'
            setattr(self, index, Endpoint(_url, self.client))
        return self.__dict__[index]

    def query(self, url: str) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        try:
            self.log.debug(f'Querying {url}')
            _, _data, _ = self.client.query(endpoint=url)
            data = json.loads(_data)
        except KeyError as e:
            self.log.error(f'Error while querying {self.url}: {e}')
        return data

    def get_data(self) -> Dict[str, Any]:
        return self.query(self.url)

    def get_members_names(self) -> List[str]:
        result: List[str] = []
        if self.has_members:
            for member in self.data['Members']:
                name: str = member['@odata.id'].split('/')[-1:][0]
                result.append(name)
        return result

    def get_name(self, endpoint: str) -> str:
        return endpoint.split('/')[-1:][0]

    def get_members_endpoints(self) -> Dict[str, str]:
        members: Dict[str, str] = {}
        name: str = ''
        if self.has_members:
            for member in self.data['Members']:
                name = self.get_name(member['@odata.id'])
                members[name] = member['@odata.id']
        else:
            name = self.get_name(self.data['@odata.id'])
            members[name] = self.data['@odata.id']

        return members

    def get_members_data(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if self.has_members:
            for member, endpoint in self.get_members_endpoints().items():
                result[member] = self.query(endpoint)
        return result

    @property
    def has_members(self) -> bool:
        return 'Members' in self.data.keys()


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
