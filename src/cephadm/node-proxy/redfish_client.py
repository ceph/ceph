from redfish.rest.v1 import ServerDownOrUnreachableError, \
    SessionCreationError, \
    InvalidCredentialsError
import redfish
import sys
from util import Logger
from typing import Dict

log = Logger(__name__)


class RedFishClient:

    PREFIX = '/redfish/v1'

    def __init__(self,
                 host: str,
                 username: str,
                 password: str) -> None:
        log.logger.info("redfish client initialization...")
        self.host = host
        self.username = username
        self.password = password
        self.redfish_obj: 'redfish.redfish_client' = None

    def login(self) -> 'redfish.redfish_client':
        self.redfish_obj = redfish.redfish_client(base_url=self.host,
                                                  username=self.username,
                                                  password=self.password,
                                                  default_prefix=self.PREFIX)
        try:
            # TODO: add a retry? check for a timeout setting
            self.redfish_obj.login(auth="session")
            log.logger.info(f"Logging to redfish api at {self.host} with user: {self.username}")
            return self.redfish_obj
        except InvalidCredentialsError as e:
            log.logger.error(f"Invalid credentials for {self.username} at {self.host}:\n{e}")
        except (SessionCreationError, ServerDownOrUnreachableError) as e:
            log.logger.error(f"Server not reachable or does not support RedFish:\n{e}")
        sys.exit(1)

    def get_path(self, path: str) -> Dict:
        try:
            if self.PREFIX not in path:
                path = f"{self.PREFIX}{path}"
            log.logger.debug(f"getting: {path}")
            response = self.redfish_obj.get(path)
            return response.dict
        except Exception as e:
            # TODO
            log.logger.error(f"Error detected.\n{e}")
            pass

    def logout(self) -> None:
        log.logger.info('logging out...')
        self.redfish_obj.logout()
