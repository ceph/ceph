from ...models.clusterinfo import ClusterInfo
from ...models.base import MCMAgentBase
from ...models.config import RESTConfig
from ...clients.http import TokenHttpClient
from ..interface import Despatcher
from ..framework import register

@register(data_type=ClusterInfo)
class ClusterDetails(Despatcher):
    def __init__(self, config: RESTConfig):
        self._config = config

    def despatch(self, cluster: MCMAgentBase):
        print("depatching ", cluster.__dict__, " from ClusterDetails Despatcher")
        #print(
        #    "reult is : ",
        #    TokenHttpClient("/api", self.login, self._config).post("/clusterinfo", cluster.__dict__),
        #)
    
    def login(self) -> str:
        return ""
    
    def shutdown(self):
        pass

    def hash(self, data):
        pass