from ...models.clusterinfo import ClusterInfo
from ...models.base import MCMAgentBase
from ...models.config import RESTConfig
from ...clients.http import UnguardedHttpClient
from ..interface import Despatcher
from ..framework import register

@register(data_type=ClusterInfo)
class ClusterDetails(Despatcher):
    def __init__(self, config: RESTConfig):
        self._config = config

    def despatch(self, cluster: MCMAgentBase):
        #cluster.remove_unserializable_attributes()
        payload = cluster.to_json()

        client = UnguardedHttpClient(
            base_url=self._config.api_base_path,
            login_func=self.login,
            login_config=self._config
        )

        try:
            response = client.post("/cluster", data=payload)
            print("response from MCM API: ", response, "\n")
            if hasattr(response, 'status_code') and response.status_code not in (200, 201):
                print(f"[ERROR] Dispatch failed: {response.status_code} {response.text}")
            else:
                print("[INFO] Successfully dispatched cluster info.")
        except Exception as e:
            print(f"[ERROR] Dispatch exception: {e}")

    def login(self) -> str:
        return ""

    def shutdown(self):
        pass

    def hash(self, data):
        pass
