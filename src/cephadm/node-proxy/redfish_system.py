from basesystem import BaseSystem
from redfish_client import RedFishClient
from threading import Thread, Lock
from time import sleep
from util import Logger, retry, normalize_dict, to_snake_case
from typing import Dict, Any, List

log = Logger(__name__)


class RedfishSystem(BaseSystem):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.host: str = kw['host']
        self.username: str = kw['username']
        self.password: str = kw['password']
        self.system_endpoint = kw.get('system_endpoint', '/Systems/1')
        log.logger.info(f"redfish system initialization, host: {self.host}, user: {self.username}")
        self.client = RedFishClient(self.host, self.username, self.password)

        self._system: Dict[str, Dict[str, Any]] = {}
        self.run: bool = False
        self.thread: Thread
        self.start_client()
        self.data_ready: bool = False
        self.previous_data: Dict = {}
        self.lock: Lock = Lock()

    @retry(retries=10, delay=2)
    def _get_path(self, path: str) -> Dict:
        result = self.client.get_path(path)
        if result is None:
            log.logger.error(f"The client reported an error when getting path: {path}")
            raise RuntimeError(f"Could not get path: {path}")
        return result

    def get_members(self, path: str) -> List:
        _path = self._system[path]['@odata.id']
        data = self._get_path(_path)
        return [self._get_path(member['@odata.id']) for member in data['Members']]

    def build_data(self,
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
                    log.logger.warning(f"Could not find field: {field} in member_info: {member_info}")

        return normalize_dict(result)

    def start_client(self) -> None:
        if not self.client:
            self.client = RedFishClient(self.host, self.username, self.password)
        self.client.login()

    def get_system(self) -> Dict[str, Dict[str, Dict]]:
        result = {
            'storage': self.get_storage(),
            'processors': self.get_processors(),
            'network': self.get_network(),
            'memory': self.get_memory(),
        }
        return result

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['status']

    def get_metadata(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['metadata']

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['memory']

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['power']

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['processors']

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['network']

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        return self._system['storage']

    def _update_system(self) -> None:
        redfish_system = self.client.get_path(self.system_endpoint)
        self._system = {**redfish_system, **self._system}

    def _update_metadata(self) -> None:
        raise NotImplementedError()

    def _update_memory(self) -> None:
        raise NotImplementedError()

    def _update_power(self) -> None:
        raise NotImplementedError()

    def _update_network(self) -> None:
        raise NotImplementedError()

    def _update_processors(self) -> None:
        raise NotImplementedError()

    def _update_storage(self) -> None:
        raise NotImplementedError()

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
        try:
            while self.run:
                log.logger.debug("waiting for a lock.")
                self.lock.acquire()
                log.logger.debug("lock acquired.")
                try:
                    self._update_system()
                    # following calls in theory can be done in parallel
                    self._update_metadata()
                    self._update_memory()
                    self._update_power()
                    self._update_network()
                    self._update_processors()
                    self._update_storage()
                    self.data_ready = True
                    sleep(5)
                finally:
                    self.lock.release()
                    log.logger.debug("lock released.")
        # Catching 'Exception' is probably not a good idea (devel only)
        except Exception as e:
            log.logger.error(f"Error detected, logging out from redfish api.\n{e}")
            self.client.logout()
            raise

    def flush(self) -> None:
        log.logger.info("Acquiring lock to flush data.")
        self.lock.acquire()
        log.logger.info("Lock acquired, flushing data.")
        self._system = {}
        self.previous_data = {}
        log.logger.info("Data flushed.")
        self.data_ready = False
        log.logger.info("Data marked as not ready.")
        self.lock.release()
        log.logger.info("Lock released.")
