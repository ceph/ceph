from threading import Thread
import time
import json
from ceph_node_proxy.util import Logger, http_req
from urllib.error import HTTPError, URLError
from typing import Dict, Any


class Reporter:
    def __init__(self,
                 system: Any,
                 cephx: Dict[str, Any],
                 reporter_scheme: str = 'https',
                 reporter_hostname: str = '',
                 reporter_port: str = '443',
                 reporter_endpoint: str = '/node-proxy/data') -> None:
        self.system = system
        self.data: Dict[str, Any] = {}
        self.finish = False
        self.cephx = cephx
        self.data['cephx'] = self.cephx
        self.reporter_scheme: str = reporter_scheme
        self.reporter_hostname: str = reporter_hostname
        self.reporter_port: str = reporter_port
        self.reporter_endpoint: str = reporter_endpoint
        self.log = Logger(__name__)
        self.reporter_url: str = (f'{reporter_scheme}://{reporter_hostname}:'
                                  f'{reporter_port}{reporter_endpoint}')
        self.log.logger.info(f'Reporter url set to {self.reporter_url}')

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
            self.log.logger.debug('waiting for a lock in reporter loop.')
            self.system.lock.acquire()
            self.log.logger.debug('lock acquired in reporter loop.')
            if self.system.data_ready:
                self.log.logger.info('data ready to be sent to the mgr.')
                if not self.system.get_system() == self.system.previous_data:
                    self.log.logger.info('data has changed since last iteration.')
                    self.data['patch'] = self.system.get_system()
                    try:
                        # TODO: add a timeout parameter to the reporter in the config file
                        self.log.logger.info(f'sending data to {self.reporter_url}')
                        http_req(hostname=self.reporter_hostname,
                                 port=self.reporter_port,
                                 method='POST',
                                 headers={'Content-Type': 'application/json'},
                                 endpoint=self.reporter_endpoint,
                                 scheme=self.reporter_scheme,
                                 data=json.dumps(self.data))
                    except (HTTPError, URLError) as e:
                        self.log.logger.error(f"The reporter couldn't send data to the mgr: {e}")
                        # Need to add a new parameter 'max_retries' to the reporter if it can't
                        # send the data for more than x times, maybe the daemon should stop altogether
                    else:
                        self.system.previous_data = self.system.get_system()
                else:
                    self.log.logger.info('no diff, not sending data to the mgr.')
            self.system.lock.release()
            self.log.logger.debug('lock released in reporter loop.')
            time.sleep(5)
