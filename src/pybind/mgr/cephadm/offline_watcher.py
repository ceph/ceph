import logging
from typing import List, Optional, TYPE_CHECKING

import multiprocessing as mp
import threading

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class OfflineHostWatcher(threading.Thread):
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.hosts: Optional[List[str]] = None
        self.new_hosts: Optional[List[str]] = None
        self.stop = False
        self.event = threading.Event()
        super(OfflineHostWatcher, self).__init__(target=self.run)

    def run(self) -> None:
        self.thread_pool = mp.pool.ThreadPool(10)
        while not self.stop:
            # only need to take action if we have hosts to check
            if self.hosts or self.new_hosts:
                if self.new_hosts:
                    self.hosts = self.new_hosts
                    self.new_hosts = None
                logger.debug(f'OfflineHostDetector: Checking if hosts: {self.hosts} are offline.')
                assert self.hosts is not None
                self.thread_pool.map(self.check_host, self.hosts)
            self.event.wait(20)
            self.event.clear()
        self.thread_pool.close()
        self.thread_pool.join()

    def check_host(self, host: str) -> None:
        if host not in self.mgr.offline_hosts:
            try:
                self.mgr.ssh.check_execute_command(host, ['true'], log_command=self.mgr.log_refresh_metadata)
            except Exception:
                logger.debug(f'OfflineHostDetector: detected {host} to be offline')
                # kick serve loop in case corrective action must be taken for offline host
                self.mgr._kick_serve_loop()

    def set_hosts(self, hosts: List[str]) -> None:
        hosts.sort()
        if (not self.hosts or self.hosts != hosts) and hosts:
            self.new_hosts = hosts
            logger.debug(
                f'OfflineHostDetector: Hosts to check if offline swapped to: {self.new_hosts}.')
            self.wakeup()

    def wakeup(self) -> None:
        self.event.set()

    def shutdown(self) -> None:
        self.stop = True
        self.wakeup()
