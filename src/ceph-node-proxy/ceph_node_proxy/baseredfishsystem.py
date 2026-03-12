import concurrent.futures
import dataclasses
from time import sleep
from typing import Any, Callable, Dict, List, Optional

from ceph_node_proxy.basesystem import BaseSystem
from ceph_node_proxy.redfish import (
    ComponentUpdateSpec,
    Endpoint,
    EndpointMgr,
    get_component_data,
    update_component,
)
from ceph_node_proxy.redfish_client import RedFishClient
from ceph_node_proxy.util import DEFAULTS, get_logger, normalize_dict, to_snake_case


class BaseRedfishSystem(BaseSystem):
    NETWORK_FIELDS: List[str] = ["Description", "Name", "SpeedMbps", "Status"]
    PROCESSORS_FIELDS: List[str] = [
        "Description",
        "TotalCores",
        "TotalThreads",
        "ProcessorType",
        "Model",
        "Status",
        "Manufacturer",
    ]
    MEMORY_FIELDS: List[str] = [
        "Description",
        "MemoryDeviceType",
        "CapacityMiB",
        "Status",
    ]
    POWER_FIELDS: List[str] = ["Name", "Model", "Manufacturer", "Status"]
    FANS_FIELDS: List[str] = ["Name", "PhysicalContext", "Status"]
    FIRMWARES_FIELDS: List[str] = [
        "Name",
        "Description",
        "ReleaseDate",
        "Version",
        "Updateable",
        "Status",
    ]

    COMPONENT_SPECS: Dict[str, List[ComponentUpdateSpec]] = {
        "network": [
            ComponentUpdateSpec("systems", "EthernetInterfaces", NETWORK_FIELDS, None),
            ComponentUpdateSpec("systems", "NetworkInterfaces", NETWORK_FIELDS, None),
        ],
        "processors": [
            ComponentUpdateSpec("systems", "Processors", PROCESSORS_FIELDS, None),
        ],
        "memory": [
            ComponentUpdateSpec("systems", "Memory", MEMORY_FIELDS, None),
        ],
        # Power supplies: Chassis/.../PowerSubsystem/PowerSupplies (not like other components: like Systems/.../Memory)
        "power": [
            ComponentUpdateSpec(
                "chassis", "PowerSubsystem/PowerSupplies", POWER_FIELDS, None
            ),
        ],
        "fans": [
            ComponentUpdateSpec("chassis", "Thermal", FANS_FIELDS, "Fans"),
        ],
        "firmwares": [
            ComponentUpdateSpec(
                "update_service", "FirmwareInventory", FIRMWARES_FIELDS, None
            ),
        ],
    }

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.log = get_logger(__name__)
        self.host: str = kw["host"]
        self.port: str = kw["port"]
        self.username: str = kw["username"]
        self.password: str = kw["password"]
        # move the following line (class attribute?)
        self.client: RedFishClient = RedFishClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )
        self.endpoints: EndpointMgr = EndpointMgr(self.client)
        self.log.info(
            f"redfish system initialization, host: {self.host}, user: {self.username}"
        )
        self.data_ready: bool = False
        self.previous_data: Dict = {}
        self.data: Dict[str, Dict[str, Any]] = {}
        self._system: Dict[str, Dict[str, Any]] = {}
        self._sys: Dict[str, Any] = {}
        self.job_service_endpoint: str = ""
        self.create_reboot_job_endpoint: str = ""
        self.setup_job_queue_endpoint: str = ""
        self.component_list: List[str] = kw.get(
            "component_list",
            [
                "memory",
                "power",
                "fans",
                "network",
                "processors",
                "storage",
                "firmwares",
            ],
        )
        self.update_funcs: List[Callable] = []
        for component in self.component_list:
            self.log.debug(f"adding: {component} to hw component gathered list.")
            func = f"_update_{component}"
            if hasattr(self, func):
                f = getattr(self, func)
                self.update_funcs.append(f)
        config = kw.get("config") or {}
        self.refresh_interval: int = config.get("system", {}).get(
            "refresh_interval", DEFAULTS["system"]["refresh_interval"]
        )

    def update(
        self,
        collection: str,
        component: str,
        path: str,
        fields: List[str],
        attribute: Optional[str] = None,
    ) -> None:
        update_component(
            self.endpoints,
            collection,
            component,
            path,
            fields,
            self._sys,
            self.log,
            attribute=attribute,
        )

    def main(self) -> None:
        self.stop = False
        self.client.login()
        self.endpoints.init()

        while not self.stop:
            self.log.debug("waiting for a lock in the update loop.")
            with self.lock:
                if not self.pending_shutdown:
                    self.log.debug("lock acquired in the update loop.")
                    try:
                        self._update_system()
                        self._update_sn()

                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            executor.map(lambda f: f(), self.update_funcs)

                        self.data_ready = True
                    except RuntimeError as e:
                        self.stop = True
                        self.log.error(
                            f"Error detected, trying to gracefully log out from redfish api.\n{e}"
                        )
                        self.client.logout()
                        raise
                    sleep(self.refresh_interval)
            self.log.debug("lock released in the update loop.")
        self.log.debug("exiting update loop.")
        raise SystemExit(0)

    def flush(self) -> None:
        self.log.debug("Acquiring lock to flush data.")
        self.lock.acquire()
        self.log.debug("Lock acquired, flushing data.")
        self._system = {}
        self.previous_data = {}
        self.log.info("Data flushed.")
        self.data_ready = False
        self.log.debug("Data marked as not ready.")
        self.lock.release()
        self.log.debug("Released the lock after flushing data.")

    # @retry(retries=10, delay=2)
    def _get_path(self, path: str) -> Dict:
        result: Dict[str, Any] = {}
        try:
            if not self.pending_shutdown:
                self.log.debug(f"Getting path: {path}")
                result = self.client.get_path(path)
            else:
                self.log.debug(f"Pending shutdown, aborting query to {path}")
        except RuntimeError:
            raise

        return result

    def get_members(self, data: Dict[str, Any], path: str) -> List:
        return [self._get_path(member["@odata.id"]) for member in data["Members"]]

    def get_system(self) -> Dict[str, Any]:
        result = {
            "host": self.get_host(),
            "sn": self.get_sn(),
            "status": {
                "storage": self.get_storage(),
                "processors": self.get_processors(),
                "network": self.get_network(),
                "memory": self.get_memory(),
                "power": self.get_power(),
                "fans": self.get_fans(),
            },
            "firmwares": self.get_firmwares(),
        }
        return result

    def _update_system(self) -> None:
        system_members: Dict[str, Any] = self.endpoints["systems"].get_members_data()
        update_service_members: Endpoint = self.endpoints["update_service"]

        for member, data in system_members.items():
            self._system[member] = data
            self._sys[member] = dict()

        self._system[update_service_members.id] = update_service_members.data

    def get_sn(self) -> str:
        return str(self._sys.get("SN", ""))

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("status", {}))

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("memory", {}))

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("processors", {}))

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("network", {}))

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("storage", {}))

    def get_firmwares(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("firmwares", {}))

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("power", {}))

    def get_fans(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("fans", {}))

    def get_component_spec_overrides(self) -> Dict[str, Dict[str, Any]]:
        return {}

    def get_specs(self, component: str) -> List[ComponentUpdateSpec]:
        return [
            self._apply_spec_overrides(component, spec)
            for spec in self.COMPONENT_SPECS[component]
        ]

    def _apply_spec_overrides(
        self, component: str, spec: ComponentUpdateSpec
    ) -> ComponentUpdateSpec:
        overrides = self.get_component_spec_overrides().get(component)
        if not overrides:
            return spec
        return dataclasses.replace(spec, **overrides)

    def _run_update(self, component: str) -> None:
        self.log.debug(f"Updating {component}")
        specs = self.get_specs(component)
        self._sys[component] = {}
        use_single_key = len(specs) == 1
        for spec in specs:
            try:
                result = get_component_data(
                    self.endpoints,
                    spec.collection,
                    spec.path,
                    spec.fields,
                    self.log,
                    attribute=spec.attribute,
                )
                path_prefix = spec.path.split("/")[-1].lower() or component
                for sys_id, members in result.items():
                    if sys_id not in self._sys[component]:
                        self._sys[component][sys_id] = {}
                    for member_id, data in members.items():
                        key = (
                            member_id
                            if use_single_key
                            else f"{path_prefix}_{member_id}"
                        )
                        self._sys[component][sys_id][key] = data
            except Exception as e:
                self.log.debug(
                    "Skipping %s path %s (not available on this hardware): %s",
                    component,
                    spec.path,
                    e,
                )

    def _update_network(self) -> None:
        self._run_update("network")

    def _update_processors(self) -> None:
        self._run_update("processors")

    def _update_storage(self) -> None:
        fields = [
            "Description",
            "CapacityBytes",
            "Model",
            "Protocol",
            "LocationIndicatorActive",
            "SerialNumber",
            "Status",
            "PhysicalLocation",
        ]
        result: Dict[str, Dict[str, Dict]] = dict()
        self.log.debug("Updating storage")
        members_names = self.endpoints["systems"].get_members_names()
        for member in members_names:
            result[member] = {}
            members_data = self.endpoints["systems"][member][
                "Storage"
            ].get_members_data()
            for entity in members_data:
                for drive in members_data[entity]["Drives"]:
                    data: Dict[str, Any] = Endpoint(
                        drive["@odata.id"], self.endpoints.client
                    ).data
                    drive_id = data["Id"]
                    result[member][drive_id] = dict()
                    result[member][drive_id]["redfish_endpoint"] = data["@odata.id"]
                    for field in fields:
                        result[member][drive_id][to_snake_case(field)] = data.get(field)
                        result[member][drive_id]["entity"] = entity
            # do not normalize the first level of the dictionary
            result[member] = normalize_dict(result[member])
            self._sys["storage"] = result

    def _update_sn(self) -> None:
        serials: List[str] = []
        self.log.debug("Updating serial number")
        data: Dict[str, Any] = self.endpoints["systems"].get_members_data()
        for chassis_id in data:
            serial = data[chassis_id].get("SerialNumber")
            if serial:
                serials.append(serial)
        self._sys["SN"] = ",".join(serials)

    def _update_memory(self) -> None:
        self._run_update("memory")

    def _update_power(self) -> None:
        self._run_update("power")

    def _update_fans(self) -> None:
        self._run_update("fans")

    def _update_firmwares(self) -> None:
        self._run_update("firmwares")

    def device_led_on(self, device: str) -> int:
        raise NotImplementedError()

    def device_led_off(self, device: str) -> int:
        raise NotImplementedError()

    def chassis_led_on(self) -> int:
        raise NotImplementedError()

    def chassis_led_off(self) -> int:
        raise NotImplementedError()

    def get_device_led(self, device: str) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_device_led(self, device: str, data: Dict[str, bool]) -> int:
        raise NotImplementedError()

    def get_chassis_led(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_chassis_led(self, data: Dict[str, str]) -> int:
        raise NotImplementedError()

    def shutdown_host(self, force: bool = False) -> int:
        raise NotImplementedError()

    def powercycle(self) -> int:
        raise NotImplementedError()

    def create_reboot_job(self, reboot_type: str) -> str:
        raise NotImplementedError()

    def schedule_reboot_job(self, job_id: str) -> int:
        raise NotImplementedError()
