import time
import datetime
import ssl
import json
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from .baseclient import BaseClient
from .util import Logger
from typing import Dict, Any, Tuple, Optional


class RedFishClient(BaseClient):
    PREFIX = '/redfish/v1/'
    def __init__(self,
                 host: str = "",
                 port: str = "443",
                 username: str = "",
                 password: str = ""):
        super().__init__(host, username, password)
        self.log: Logger = Logger(__name__)
        self.log.logger.info(f"Initializing redfish client {__name__}")
        self.host: str = f"https://{host}:{port}"
        self.token: Dict[str, str] = {}
        self.location: str = ''

    def login(self) -> Dict[str, Any]:
        self.log.logger.info(f"Logging in to {self.host} as '{self.username}'")
        idrac_credentials = json.dumps({"UserName": self.username, "Password": self.password})
        headers = {"Content-Type": "application/json"}

        try:
            _headers, _data = self.query(data=idrac_credentials,
                                         headers=headers,
                                         endpoint='/redfish/v1/SessionService/Sessions/')
        except URLError as e:
            self.log.logger.error(f"Can't log in to {self.host} as '{self.username}'.\n{e}")
            return {}
        self.token = {"X-Auth-Token": _headers['X-Auth-Token']}
        self.location = _headers['Location']

        return json.loads(_data)

    def logout(self) -> Dict[str, Any]:
        try:
            _, _data = self.query(method='DELETE', headers=self.token, endpoint=self.location)
        except URLError as e:
            self.log.logger.error(f"Can't log out from {self.host}")
            return {}

        response_str = _data

        return json.loads(response_str)

    def get_path(self, path: str) -> Dict[str, Any]:
        if self.PREFIX not in path:
            path = f"{self.PREFIX}{path}"
        try:
            _, result = self.query(headers=self.token, endpoint=path)
            result_json = json.loads(result)
            return result_json
        except URLError as e:
            self.log.logger.error(f"Can't get path {path}:\n{e}")
            return {}

    def query(self,
              data: Optional[str] = None,
              headers: Dict[str, str] = {},
              method: Optional[str] = None,
              endpoint: str = '') -> Tuple[Dict[str, str], str]:
        url = f'{self.host}{endpoint}'

        # ssl_ctx = ssl.create_default_context()
        # ssl_ctx.check_hostname = True
        # ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        ssl_ctx = ssl._create_unverified_context()
        _data = bytes(data, 'ascii') if data else None
        try:
            req = Request(url, _data, headers=headers, method=method)
            send_time = time.monotonic()
            with urlopen(req, context=ssl_ctx) as response:
                response_str = response.read()
                response_headers = response.headers
                response_json = json.loads(response_str)
                total_request_time = datetime.timedelta(seconds=(time.monotonic() - send_time)).total_seconds()
        except Exception as e:
            self.log.logger.error(f"{e}")
            raise

        return response_headers, response_str
