from util import Config
from typing import Dict, Any


class BaseSystem:
    def __init__(self, **kw: Any) -> None:
        self._system: Dict = {}
        self.config: Config = kw['config']

    def get_system(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_metadata(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()
