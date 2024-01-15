import socket
from ceph_node_proxy.util import Config
from typing import Dict, Any
from ceph_node_proxy.baseclient import BaseClient


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

    def get_chassis_led(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_chassis_led(self, data: Dict[str, str]) -> int:
        raise NotImplementedError()

    def device_led_on(self, device: str) -> int:
        raise NotImplementedError()

    def device_led_off(self, device: str) -> int:
        raise NotImplementedError()

    def get_device_led(self, device: str) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_device_led(self, device: str, data: Dict[str, bool]) -> int:
        raise NotImplementedError()

    def chassis_led_on(self) -> int:
        raise NotImplementedError()

    def chassis_led_off(self) -> int:
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

    def shutdown(self, force: bool = False) -> int:
        raise NotImplementedError()

    def powercycle(self) -> int:
        raise NotImplementedError()
