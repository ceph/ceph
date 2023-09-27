from .basesystem import BaseSystem
from .redfish_client import RedFishClient
from threading import Thread, Lock
from time import sleep
from .util import Logger, retry
from typing import Dict, Any, List


class BaseRedfishSystem(BaseSystem):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.log = Logger(__name__)
        self.host: str = kw['host']
        self.username: str = kw['username']
        self.password: str = kw['password']
        # move the following line (class attribute?)
        self.client = RedFishClient(host=self.host, username=self.username, password=self.password)
        self.log.logger.info(f"redfish system initialization, host: {self.host}, user: {self.username}")

        self.run: bool = False
        self.thread: Thread
        self.data_ready: bool = False
        self.previous_data: Dict = {}
        self.lock: Lock = Lock()
        self.data: Dict[str, Dict[str, Any]] = {}
        self._system: Dict[str, Dict[str, Any]] = {}
        self.start_client()

    def start_client(self) -> None:
        if not self.client:
            self.client = RedFishClient(host=self.host, username=self.username, password=self.password)
        self.client.login()

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
                self.log.logger.debug("waiting for a lock.")
                self.lock.acquire()
                self.log.logger.debug("lock acquired.")
                try:
                    self._update_system()
                    # following calls in theory can be done in parallel
                    self._update_metadata()
                    self._update_memory()
                    self._update_power()
                    self._update_fans()
                    self._update_network()
                    self._update_processors()
                    self._update_storage()
                    self.data_ready = True
                    sleep(5)
                finally:
                    self.lock.release()
                    self.log.logger.debug("lock released.")
        # Catching 'Exception' is probably not a good idea (devel only)
        except Exception as e:
            self.log.logger.error(f"Error detected, logging out from redfish api.\n{e}")
            self.client.logout()
            raise

    def flush(self) -> None:
        self.log.logger.info("Acquiring lock to flush data.")
        self.lock.acquire()
        self.log.logger.info("Lock acquired, flushing data.")
        self._system = {}
        self.previous_data = {}
        self.log.logger.info("Data flushed.")
        self.data_ready = False
        self.log.logger.info("Data marked as not ready.")
        self.lock.release()
        self.log.logger.info("Lock released.")

    @retry(retries=10, delay=2)
    def _get_path(self, path: str) -> Dict:
        result = self.client.get_path(path)
        if result is None:
            self.log.logger.error(f"The client reported an error when getting path: {path}")
            raise RuntimeError(f"Could not get path: {path}")
        return result

    def get_members(self, path: str) -> List:
        _path = self._system[path]['@odata.id']
        data = self._get_path(_path)
        return [self._get_path(member['@odata.id']) for member in data['Members']]

    def build_data(self,
                   fields: List,
                   path: str) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    # def _update_system(self) -> None:
    #     raise NotImplementedError()

    def get_system(self) -> Dict[str, Dict[str, Dict]]:
        result = {
            'storage': self.get_storage(),
            'processors': self.get_processors(),
            'network': self.get_network(),
            'memory': self.get_memory(),
            'power': self.get_power(),
            'fans': self.get_fans()
        }
        return result

    def _update_system(self) -> None:
        redfish_system = self.client.get_path(self.system_endpoint)
        self._system = {**redfish_system, **self._system}

    def _update_metadata(self) -> None:
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
