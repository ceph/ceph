import socket
from .util import Config
from typing import Dict, Any
from .baseclient import BaseClient


class BaseSystem:
    def __init__(self, **kw: Any) -> None:
        self._system: Dict = {}
        self.config: Config = kw['config']
        self.client: BaseClient

    def get_system(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_metadata(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_fans(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_firmwares(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_sn(self) -> str:
        raise NotImplementedError()

    def get_led(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_led(self, data: Dict[str, str]) -> int:
        raise NotImplementedError()

    def get_host(self) -> str:
        return socket.gethostname()

    def start_update_loop(self) -> None:
        raise NotImplementedError()

    def stop_update_loop(self) -> None:
        raise NotImplementedError()

    def start_client(self) -> None:
        raise NotImplementedError()

    def flush(self) -> None:
        raise NotImplementedError()
