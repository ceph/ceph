import json
from urllib.error import HTTPError, URLError
from ceph_node_proxy.baseclient import BaseClient
from ceph_node_proxy.util import Logger, http_req
from typing import Dict, Any, Tuple, Optional
from http.client import HTTPMessage


class RedFishClient(BaseClient):
    PREFIX = '/redfish/v1/'

    def __init__(self,
                 host: str = '',
                 port: str = '443',
                 username: str = '',
                 password: str = ''):
        super().__init__(host, username, password)
        self.log: Logger = Logger(__name__)
        self.log.logger.info(f'Initializing redfish client {__name__}')
        self.host: str = host
        self.port: str = port
        self.url: str = f'https://{self.host}:{self.port}'
        self.token: str = ''
        self.location: str = ''

    def login(self) -> None:
        if not self.is_logged_in():
            self.log.logger.info('Logging in to '
                                 f"{self.url} as '{self.username}'")
            oob_credentials = json.dumps({'UserName': self.username,
                                          'Password': self.password})
            headers = {'Content-Type': 'application/json'}

            try:
                _headers, _data, _status_code = self.query(data=oob_credentials,
                                                           headers=headers,
                                                           endpoint='/redfish/v1/SessionService/Sessions/')
                if _status_code != 201:
                    self.log.logger.error(f"Can't log in to {self.url} as '{self.username}': {_status_code}")
                    raise RuntimeError
            except URLError as e:
                msg = f"Can't log in to {self.url} as '{self.username}': {e}"
                self.log.logger.error(msg)
                raise RuntimeError
            self.token = _headers['X-Auth-Token']
            self.location = _headers['Location']

    def is_logged_in(self) -> bool:
        self.log.logger.debug(f'Checking token validity for {self.url}')
        if not self.location or not self.token:
            self.log.logger.debug(f'No token found for {self.url}.')
            return False
        headers = {'X-Auth-Token': self.token}
        try:
            _headers, _data, _status_code = self.query(headers=headers,
                                                       endpoint=self.location)
        except URLError as e:
            self.log.logger.error("Can't check token "
                                  f'validity for {self.url}: {e}')
            raise
        return _status_code == 200

    def logout(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        try:
            if self.is_logged_in():
                _, _data, _status_code = self.query(method='DELETE',
                                                    headers={'X-Auth-Token': self.token},
                                                    endpoint=self.location)
                result = json.loads(_data)
        except URLError:
            self.log.logger.error(f"Can't log out from {self.url}")

        self.location = ''
        self.token = ''

        return result

    def get_path(self, path: str) -> Dict[str, Any]:
        if self.PREFIX not in path:
            path = f'{self.PREFIX}{path}'
        try:
            _, result, _status_code = self.query(endpoint=path)
            result_json = json.loads(result)
            return result_json
        except URLError as e:
            self.log.logger.error(f"Can't get path {path}:\n{e}")
            raise RuntimeError

    def query(self,
              data: Optional[str] = None,
              headers: Dict[str, str] = {},
              method: Optional[str] = None,
              endpoint: str = '',
              timeout: int = 10) -> Tuple[HTTPMessage, str, int]:
        _headers = headers.copy() if headers else {}
        if self.token:
            _headers['X-Auth-Token'] = self.token
        if not _headers.get('Content-Type') and method in ['POST', 'PUT', 'PATCH']:
            _headers['Content-Type'] = 'application/json'
        try:
            (response_headers,
             response_str,
             response_status) = http_req(hostname=self.host,
                                         port=self.port,
                                         endpoint=endpoint,
                                         headers=_headers,
                                         method=method,
                                         data=data,
                                         timeout=timeout)

            return response_headers, response_str, response_status
        except (HTTPError, URLError) as e:
            self.log.logger.debug(f'{e}')
            raise
