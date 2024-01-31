import concurrent.futures
import json
from ceph_node_proxy.basesystem import BaseSystem
from ceph_node_proxy.redfish_client import RedFishClient
from threading import Thread, Lock
from time import sleep
from ceph_node_proxy.util import Logger, retry
from typing import Dict, Any, List, Callable, Union
from urllib.error import HTTPError, URLError


class BaseRedfishSystem(BaseSystem):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.common_endpoints: List[str] = kw.get('common_endpoints', ['/Systems/System.Embedded.1',
                                                                       '/UpdateService'])
        self.chassis_endpoint: str = kw.get('chassis_endpoint', '/Chassis/System.Embedded.1')
        self.log = Logger(__name__)
        self.host: str = kw['host']
        self.port: str = kw['port']
        self.username: str = kw['username']
        self.password: str = kw['password']
        # move the following line (class attribute?)
        self.client: RedFishClient = RedFishClient(host=self.host, port=self.port, username=self.username, password=self.password)
        self.log.logger.info(f'redfish system initialization, host: {self.host}, user: {self.username}')

        self.run: bool = False
        self.thread: Thread
        self.data_ready: bool = False
        self.previous_data: Dict = {}
        self.lock: Lock = Lock()
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
            self.log.logger.debug(f'adding: {component} to hw component gathered list.')
            func = f'_update_{component}'
            if hasattr(self, func):
                f = getattr(self, func)
                self.update_funcs.append(f)

        self.start_client()

    def start_client(self) -> None:
        self.client.login()
        self.start_update_loop()

    def start_update_loop(self) -> None:
        self.run = True
        self.thread = Thread(target=self.update)
        self.thread.start()

    def stop_update_loop(self) -> None:
        self.run = False
        self.thread.join()

    def update(self) -> None:
        #  this loop can have:
        #  - caching logic
        while self.run:
            self.log.logger.debug('waiting for a lock in the update loop.')
            self.lock.acquire()
            self.log.logger.debug('lock acquired in the update loop.')
            try:
                self._update_system()
                self._update_sn()

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.map(lambda f: f(), self.update_funcs)

                self.data_ready = True
            except RuntimeError as e:
                self.run = False
                self.log.logger.error(f'Error detected, trying to gracefully log out from redfish api.\n{e}')
                self.client.logout()
            finally:
                self.lock.release()
                sleep(5)
                self.log.logger.debug('lock released in the update loop.')

    def flush(self) -> None:
        self.log.logger.debug('Acquiring lock to flush data.')
        self.lock.acquire()
        self.log.logger.debug('Lock acquired, flushing data.')
        self._system = {}
        self.previous_data = {}
        self.log.logger.info('Data flushed.')
        self.data_ready = False
        self.log.logger.debug('Data marked as not ready.')
        self.lock.release()
        self.log.logger.debug('Released the lock after flushing data.')

    @retry(retries=10, delay=2)
    def _get_path(self, path: str) -> Dict:
        try:
            result = self.client.get_path(path)
        except RuntimeError:
            raise
        if result is None:
            self.log.logger.error(f'The client reported an error when getting path: {path}')
            raise RuntimeError(f'Could not get path: {path}')
        return result

    def get_members(self, data: Dict[str, Any], path: str) -> List:
        _path = data[path]['@odata.id']
        _data = self._get_path(_path)
        return [self._get_path(member['@odata.id']) for member in _data['Members']]

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
            'chassis': {'redfish_endpoint': f'/redfish/v1{self.chassis_endpoint}'}  # TODO(guits): not ideal
        }
        return result

    def _update_system(self) -> None:
        for endpoint in self.common_endpoints:
            result = self.client.get_path(endpoint)
            _endpoint = endpoint.strip('/').split('/')[0]
            self._system[_endpoint] = result

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
            self.log.logger.error(f"Couldn't get the ident device LED status for device '{device}': {e}")
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
            _, response, status = self.client.query(
                data=json.dumps(data),
                method='PATCH',
                endpoint=self._sys['storage'][device]['redfish_endpoint']
            )
        except (HTTPError, KeyError) as e:
            self.log.logger.error(f"Couldn't set the ident device LED for device '{device}': {e}")
            raise
        return status

    def get_chassis_led(self) -> Dict[str, Any]:
        endpoint = f'/redfish/v1/{self.chassis_endpoint}'
        try:
            result = self.client.query(method='GET',
                                       endpoint=endpoint,
                                       timeout=10)
        except HTTPError as e:
            self.log.logger.error(f"Couldn't get the ident chassis LED status: {e}")
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
            _, response, status = self.client.query(
                data=json.dumps(data),
                method='PATCH',
                endpoint=f'/redfish/v1{self.chassis_endpoint}'
            )
        except HTTPError as e:
            self.log.logger.error(f"Couldn't set the ident chassis LED: {e}")
            raise
        return status

    def shutdown(self, force: bool = False) -> int:
        reboot_type: str = 'GracefulRebootWithForcedShutdown' if force else 'GracefulRebootWithoutForcedShutdown'

        try:
            job_id: str = self.create_reboot_job(reboot_type)
            status = self.schedule_reboot_job(job_id)
        except (HTTPError, KeyError) as e:
            self.log.logger.error(f"Couldn't create the reboot job: {e}")
            raise
        return status

    def powercycle(self) -> int:
        try:
            job_id: str = self.create_reboot_job('PowerCycle')
            status = self.schedule_reboot_job(job_id)
        except (HTTPError, URLError) as e:
            self.log.logger.error(f"Couldn't perform power cycle: {e}")
            raise
        return status

    def create_reboot_job(self, reboot_type: str) -> str:
        data: Dict[str, str] = dict(RebootJobType=reboot_type)
        try:
            headers, response, status = self.client.query(
                data=json.dumps(data),
                endpoint=self.create_reboot_job_endpoint
            )
            job_id: str = headers['Location'].split('/')[-1]
        except (HTTPError, URLError) as e:
            self.log.logger.error(f"Couldn't create the reboot job: {e}")
            raise
        return job_id

    def schedule_reboot_job(self, job_id: str) -> int:
        data: Dict[str, Union[List[str], str]] = dict(JobArray=[job_id], StartTimeInterval='TIME_NOW')
        try:
            headers, response, status = self.client.query(
                data=json.dumps(data),
                endpoint=self.setup_job_queue_endpoint
            )
        except (HTTPError, KeyError) as e:
            self.log.logger.error(f"Couldn't schedule the reboot job: {e}")
            raise
        return status
