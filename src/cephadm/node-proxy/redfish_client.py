from redfish.rest.v1 import ServerDownOrUnreachableError
import redfish
import logging

class RedFishClient:

    PREFIX = '/redfish/v1'

    def __init__(self, host, username, password):
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
        except ServerDownOrUnreachableError as e:
            logging.error(f"Server not reachable or does not support RedFish {e}", e)

    def get_path(self, path):
        try:
            if self.PREFIX not in path:
                path = f"{self.PREFIX}{path}"
            print(f"getting: {path}")
            response = self.redfish_obj.get(path)
            return response.dict
        except Exception as e:
            #TODO
            pass

    def logout(self):
        self.redfish_obj.logout()
