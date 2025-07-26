from abc import ABC, abstractmethod
from ..models.config import RESTConfig
from ..models.base import MCMAgentBase

class Collector(ABC):
    @abstractmethod
    def __init__(self, config: RESTConfig, entity: MCMAgentBase):
        """
        Initializes the collector
        """
        self.config = config

    @abstractmethod
    def collect(self) -> dict:
        """
        Collects and returns data from the source.
        """
        pass

    @abstractmethod
    def hash(self, data):
        """
        Collects and returns data from the source.
        """
        pass

    @abstractmethod
    def shutdown(self):
        """
        Gracefully exits the collector thread.
        """
        pass

    @abstractmethod
    def login(self) -> str:
        """
        Login and procure a token
        """
        pass