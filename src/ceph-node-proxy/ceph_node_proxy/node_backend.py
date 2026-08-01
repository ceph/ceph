from time import sleep
from typing import Any, Dict

from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem
from ceph_node_proxy.basesystem import BaseSystem
from ceph_node_proxy.local_collectors import LocalCollectorRunner
from ceph_node_proxy.util import get_logger


class NodeBackend(BaseSystem):
    def __init__(
        self,
        redfish: BaseRedfishSystem,
        local_collectors: LocalCollectorRunner,
    ) -> None:
        super().__init__(config=redfish.config)
        self.redfish = redfish
        self._local = local_collectors
        self.data_ready = False
        self.previous_data: Dict[str, Any] = {}
        self.log = get_logger(__name__)

    @property
    def client(self) -> Any:
        return self.redfish.client

    def main(self) -> None:
        self.stop = False
        self.redfish.stop = False
        self.redfish.initialize_redfish_session()

        while not self.stop:
            self.log.debug("waiting for a lock in the update loop.")
            with self.lock:
                if not self.pending_shutdown:
                    self.log.debug("lock acquired in the update loop.")
                    try:
                        self.redfish.run_update_cycle()
                        self._local.update()
                        self.data_ready = True
                    except RuntimeError as exc:
                        self.stop = True
                        self.redfish.stop = True
                        self.log.error(
                            "Error detected, trying to gracefully log out from "
                            "redfish api.\n%s",
                            exc,
                        )
                        self.redfish.client.logout()
                        raise
                    sleep(self.redfish.refresh_interval)
            self.log.debug("lock released in the update loop.")
        self.log.debug("exiting update loop.")
        raise SystemExit(0)

    def flush(self) -> None:
        self.log.debug("Acquiring lock to flush data.")
        self.lock.acquire()
        self.log.debug("Lock acquired, flushing data.")
        self.redfish.flush()
        self._local.flush()
        self.previous_data = {}
        self.data_ready = False
        self.log.info("Data flushed.")
        self.lock.release()

    def get_system(self) -> Dict[str, Any]:
        result = self.redfish.get_system()
        for category in self._local.categories():
            result["status"][category] = self._local.get_category(category)
        return result

    def get_local_category(self, name: str) -> Dict[str, Any]:
        return self._local.get_category(name)

    def get_fcm(self) -> Dict[str, Any]:
        return self.get_local_category("fcm")

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self.redfish.get_status())

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        return self.redfish.get_memory()

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        return self.redfish.get_processors()

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        return self.redfish.get_network()

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        return self.redfish.get_storage()

    def get_firmware(self) -> Dict[str, Dict[str, Dict]]:
        return self.redfish.get_firmware()

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        return self.redfish.get_power()

    def get_fans(self) -> Dict[str, Dict[str, Dict]]:
        return self.redfish.get_fans()

    def get_temperatures(self) -> Dict[str, Dict[str, Dict]]:
        return self.redfish.get_temperatures()

    def get_sn(self) -> str:
        return self.redfish.get_sn()

    def get_led(self) -> Dict[str, Any]:
        return self.redfish.get_led()

    def set_led(self, data: Dict[str, str]) -> int:
        return self.redfish.set_led(data)

    def get_chassis_led(self) -> Dict[str, Any]:
        return self.redfish.get_chassis_led()

    def set_chassis_led(self, data: Dict[str, str]) -> int:
        return self.redfish.set_chassis_led(data)

    def device_led_on(self, device: str) -> int:
        return self.redfish.device_led_on(device)

    def device_led_off(self, device: str) -> int:
        return self.redfish.device_led_off(device)

    def get_device_led(self, device: str) -> Dict[str, Any]:
        return self.redfish.get_device_led(device)

    def set_device_led(self, device: str, data: Dict[str, bool]) -> int:
        return self.redfish.set_device_led(device, data)

    def chassis_led_on(self) -> int:
        return self.redfish.chassis_led_on()

    def chassis_led_off(self) -> int:
        return self.redfish.chassis_led_off()

    def shutdown_host(self, force: bool = False) -> int:
        return self.redfish.shutdown_host(force=force)

    def powercycle(self) -> int:
        return self.redfish.powercycle()
