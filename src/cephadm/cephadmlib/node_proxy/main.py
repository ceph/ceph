from threading import Thread
from .redfishdellsystem import RedfishDellSystem
from .reporter import Reporter
from .util import Config, Logger
from typing import Dict, Any, Optional
import traceback

DEFAULT_CONFIG = {
    'reporter': {
        'check_interval': 5,
        'push_data_max_retries': 30,
        'endpoint': 'https://127.0.0.1:7150/node-proxy/data',
    },
    'system': {
        'refresh_interval': 5
    },
    'server': {
        'port': 8080,
    },
    'logging': {
        'level': 20,
    }
}


class NodeProxy(Thread):
    def __init__(self, **kw: Dict[str, Any]) -> None:
        super().__init__()
        for k, v in kw.items():
            setattr(self, k, v)
        self.exc: Optional[Exception] = None
        self.log = Logger(__name__)

    def run(self) -> None:
        try:
            self.main()
        except Exception as e:
            self.exc = e
            return

    def shutdown(self) -> None:
        self.log.logger.info('Shutting down node-proxy...')
        self.system.client.logout()
        self.system.stop_update_loop()
        self.reporter_agent.stop()

    def check_auth(self, realm: str, username: str, password: str) -> bool:
        return self.__dict__['username'] == username and \
            self.__dict__['password'] == password

    def check_status(self) -> bool:
        if self.__dict__.get('system') and not self.system.run:
            raise RuntimeError('node-proxy encountered an error.')
        if self.exc:
            traceback.print_tb(self.exc.__traceback__)
            self.log.logger.error(f'{self.exc.__class__.__name__}: {self.exc}')
            raise self.exc
        return True

    def main(self) -> None:
        # TODO: add a check and fail if host/username/password/data aren't passed
        self.config = Config('/etc/ceph/node-proxy.yml', default_config=DEFAULT_CONFIG)
        self.log = Logger(__name__, level=self.config.__dict__['logging']['level'])

        # create the redfish system and the obsever
        self.log.logger.info('Server initialization...')
        try:
            self.system = RedfishDellSystem(host=self.__dict__['host'],
                                            port=self.__dict__.get('port', 443),
                                            username=self.__dict__['username'],
                                            password=self.__dict__['password'],
                                            config=self.config)
        except RuntimeError:
            self.log.logger.error("Can't initialize the redfish system.")
            raise

        try:
            self.reporter_agent = Reporter(self.system,
                                           self.__dict__['cephx'],
                                           reporter_scheme=self.__dict__.get('reporter_scheme', 'https'),
                                           reporter_hostname=self.__dict__['mgr_target_ip'],
                                           reporter_port=self.__dict__['mgr_target_port'],
                                           reporter_endpoint=self.__dict__.get('reporter_endpoint', '/node-proxy/data'))
            self.reporter_agent.run()
        except RuntimeError:
            self.log.logger.error("Can't initialize the reporter.")
            raise
