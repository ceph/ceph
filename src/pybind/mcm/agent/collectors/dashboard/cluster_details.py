from ..interface import Collector
from ...clients.http import TokenHttpClient, UnguardedHttpClient
from ...models.config import MCMAgentConfig
from ...models.clusterinfo import ClusterInfo
import os
from .constants import *
from ..framework import register_collector
from ...models.base import MCMAgentBase
from ...models.capacity import Capacity

@register_collector(data_type=ClusterInfo)
class ClusterDetailsCollector(Collector):
    def __init__(self, config: MCMAgentConfig, entity: MCMAgentBase):
        super().__init__(config, entity)
        self._config = config.dashboard_config
        self._http_client = TokenHttpClient(self._config.api_base_path, self.login, self._config)
        self._unguarded_http_client = UnguardedHttpClient(self._config.api_base_path, self.login, self._config)
        self._object = entity
        
    def collect(self):
        try:
            health_response = self._http_client.get(DASHBOARD_CLUSTER_HEALTH_API)
            if health_response.status_code == 200:
                self._object.health = health_response.json().get("health").get("status")
                self._object.capacity = Capacity(float(health_response.json().get("df").get("stats").get("total_used_raw_bytes")), float(health_response.json().get("df").get("stats").get("total_bytes")))
            fsid_response = self._http_client.get(DASHBOARD_CEPH_CLUSTER_FSID_API)
            if fsid_response.status_code == 200:
                self._object.fsid = str(fsid_response.json())
            version_response = self._http_client.get(DASHBOARD_CEPH_VERSION_API)
            if version_response.status_code == 200:
               self._object.version = version_response.json().get("version")
            return self._object
        except Exception as e:
            print("error: ", e)
        return super().collect()

    def hash(self, data: dict):
        pass

    def shutdown(self):
        pass

    def login(self) -> str:
        try:
            response = self._unguarded_http_client.post(
                DASHBOARD_LOGIN_PATH,
                {
                    DASHBOARD_USERNAME_KEY: self._config.auth_config.username,
                    DASHBOARD_PASSWORD_KEY: self._config.auth_config.password,
                },
            )
            print("login response: ", response, "\n")
            if response.status_code == 201:
                data = response.json()
                token = data.get("token")
                return token
            else:
                print("Login failed:", response.status_code, response.text)
        except Exception as e:
            print("login failed with exception: ", e)
