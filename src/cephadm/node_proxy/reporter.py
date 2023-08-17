from threading import Thread
import requests
import time
from util import Logger
from typing import Any

log = Logger(__name__)


class Reporter:
    def __init__(self, system: Any, observer_url: str) -> None:
        self.system = system
        self.observer_url = observer_url
        self.finish = False

    def stop(self) -> None:
        self.finish = True
        self.thread.join()

    def run(self) -> None:
        self.thread = Thread(target=self.loop)
        self.thread.start()

    def loop(self) -> None:
        while not self.finish:
            # Any logic to avoid sending the all the system
            # information every loop can go here. In a real
            # scenario probably we should just send the sub-parts
            # that have changed to minimize the traffic in
            # dense clusters
            log.logger.debug("waiting for a lock.")
            self.system.lock.acquire()
            log.logger.debug("lock acquired.")
            if self.system.data_ready:
                log.logger.info('data ready to be sent to the mgr.')
                if not self.system.get_system() == self.system.previous_data:
                    log.logger.info('data has changed since last iteration.')
                    d = self.system.get_system()
                    try:
                        # TODO: add a timeout parameter to the reporter in the config file
                        requests.post(f"{self.observer_url}/", json=d, timeout=5)
                    except requests.exceptions.RequestException as e:
                        log.logger.error(f"The reporter couldn't send data to the mgr: {e}")
                        # Need to add a new parameter 'max_retries' to the reporter if it can't
                        # send the data for more than x times, maybe the daemon should stop altogether
                    else:
                        self.system.previous_data = self.system.get_system()
                else:
                    log.logger.info('no diff, not sending data to the mgr.')
            self.system.lock.release()
            log.logger.debug("lock released.")
            time.sleep(5)
