from system import System
from redfish_client import RedFishClient
from threading import Thread, Lock
from time import sleep
from util import logger

log = logger(__name__)


class RedfishSystem(System):
    def __init__(self, **kw):
        self.host = kw.get('host')
        self.username = kw.get('username')
        self.password = kw.get('password')
        self.system_endpoint = kw.get('system_endpoint', '/Systems/1')
        log.info(f"redfish system initialization, host: {self.host}, user: {self.username}")
        self.client = RedFishClient(self.host, self.username, self.password)
        self._system = {}
        self.run = False
        self.thread = None
        self.start_client()
        self.data_ready = False
        self.previous_data = {}
        self.lock = Lock()

    def start_client(self):
        log.info(f"redfish system initialization, host: {self.host}, user: {self.username}")
        self.client = RedFishClient(self.host, self.username, self.password)
        self.client.login()

    def get_system(self):
        result = {
            'storage': self.get_storage(),
            'processors': self.get_processors(),
            'network': self.get_network(),
        }
        return result

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

    def _update_system(self):
        redfish_system = self.client.get_path(self.system_endpoint)
        self._system = {**redfish_system, **self._system}

    def _update_metadata(self):
        raise NotImplementedError()

    def _update_memory(self):
        raise NotImplementedError()

    def _update_power(self):
        raise NotImplementedError()

    def _update_network(self):
        raise NotImplementedError()

    def _update_processors(self):
        raise NotImplementedError()

    def _update_storage(self):
        raise NotImplementedError()

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
                log.debug("waiting for a lock.")
                self.lock.acquire()
                log.debug("lock acquired.")
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
                    log.debug("lock released.")
        # Catching 'Exception' is probably not a good idea (devel only)
        except Exception as e:
            log.error(f"Error detected, logging out from redfish api.\n{e}")
            self.client.logout()
            raise
