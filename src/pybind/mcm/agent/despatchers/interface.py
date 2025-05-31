from abc import ABC, abstractmethod
from ..models.config import RESTConfig
from ..models.base import MCMAgentBase

class Despatcher(ABC):
    @abstractmethod
    def __init__(self, config: RESTConfig):
        """
        Initializes the despatcher
        """
        self.config = config

    @abstractmethod
    def despatch(self, data: MCMAgentBase) -> dict:
        """
        Dispatches data from collectors.
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
        Gracefully exits the despatcher thread.
        """
        pass

    @abstractmethod
    def login(self) -> str:
        """
        Login and procure a token
        """
        pass