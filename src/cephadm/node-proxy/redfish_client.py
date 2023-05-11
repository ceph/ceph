from redfish.rest.v1 import ServerDownOrUnreachableError
import redfish
from util import logger

log = logger(__name__, level=10)

class RedFishClient:

    PREFIX = '/redfish/v1'

    def __init__(self, host, username, password):
        log.info(f"redfish client initialization...")
        self.host = host
        self.username = username
        self.password = password
        self.redfish_obj = None

    def login(self):
        self.redfish_obj = redfish.redfish_client(base_url=self.host,
                                                  username=self.username,
                                                  password=self.password,
                                                  default_prefix=self.PREFIX)
        try:
            self.redfish_obj.login(auth="session")
            log.info(f"Logging to redfish api at {self.host} with user: {self.username}")
        except ServerDownOrUnreachableError as e:
            log.error(f"Server not reachable or does not support RedFish {e}", e)

    def get_path(self, path):
        try:
            if self.PREFIX not in path:
                path = f"{self.PREFIX}{path}"
            log.debug(f"getting: {path}")
            response = self.redfish_obj.get(path)
            return response.dict
        except Exception as e:
            #TODO
            log.error(f"Error detected.\n{e}")
            pass

    def logout(self):
        log.info('logging out...')
        self.redfish_obj.logout()
