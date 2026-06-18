import re
from typing import Any, Dict, Optional

from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem
from ceph_node_proxy.util import get_logger

INVALID_SERIALS = ("unknown", "not available", "n/a", "")


class AtollonRedfishProvider(BaseRedfishSystem):
    """Redfish provider for Atollon BMC quirks."""

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.log = get_logger(__name__)

    def get_component_spec_overrides(self) -> Dict[str, Dict[str, Any]]:
        return {
            "memory": {
                "fields": [
                    "Id" if field == "Description" else field
                    for field in BaseRedfishSystem.MEMORY_FIELDS
                ],
            },
        }

    def _update_memory(self) -> None:
        super()._update_memory()
        for members in self._sys.get("memory", {}).values():
            for member_data in members.values():
                member_id = member_data.get("id")
                if member_id is not None and member_data.get("description") is None:
                    member_data["description"] = member_id

    def _update_storage(self) -> None:
        super()._update_storage()
        self.fix_storage_descriptions()
        self.enrich_storage_from_controllers()

    def fix_storage_descriptions(self) -> None:
        for members in self._sys.get("storage", {}).values():
            for drive_id, drive_data in members.items():
                description = drive_data.get("description")
                if description is not None and description != "unknown":
                    continue
                endpoint = drive_data.get("redfish_endpoint", "")
                if isinstance(endpoint, str) and endpoint:
                    drive_data["description"] = endpoint.rstrip("/").split("/")[-1]
                else:
                    drive_data["description"] = drive_id

    def enrich_storage_from_controllers(self) -> None:
        for member in self.endpoints["systems"].get_members_names():
            drives = self._sys.get("storage", {}).get(member)
            if not drives:
                continue
            storage_units = self.endpoints["systems"][member]["Storage"].get_members_data()
            for entity, storage_unit in storage_units.items():
                controllers = storage_unit.get("StorageControllers") or []
                by_serial: Dict[str, Dict[str, Any]] = {}
                by_member_id: Dict[str, Dict[str, Any]] = {}
                for controller in controllers:
                    member_id = controller.get("MemberId")
                    if member_id is not None:
                        by_member_id[str(member_id)] = controller
                    serial = controller.get("SerialNumber")
                    if isinstance(serial, str) and serial.lower() not in INVALID_SERIALS:
                        by_serial[serial] = controller
                for drive_key, drive_data in drives.items():
                    if drive_data.get("entity") != entity:
                        continue
                    controller = self.match_storage_controller(
                        drive_key, drive_data, by_serial, by_member_id
                    )
                    if controller is not None:
                        self.apply_storage_controller(drive_data, controller)

    def match_storage_controller(
        self,
        drive_key: str,
        drive_data: Dict[str, Any],
        by_serial: Dict[str, Dict[str, Any]],
        by_member_id: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        serial = drive_data.get("serial_number")
        if isinstance(serial, str) and serial.lower() not in INVALID_SERIALS:
            controller = by_serial.get(serial)
            if controller is not None:
                return controller
        description = drive_data.get("description")
        device_index = self.parse_drive_device_index(drive_key, description)
        if device_index is not None:
            return by_member_id.get(device_index)
        return None

    def parse_drive_device_index(
        self, drive_key: str, description: Any
    ) -> Optional[str]:
        for text in (description, drive_key):
            if not isinstance(text, str):
                continue
            match = re.search(r"Device(\d+)", text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def apply_storage_controller(
        self, drive_data: Dict[str, Any], controller: Dict[str, Any]
    ) -> None:
        firmware_version = controller.get("FirmwareVersion")
        if firmware_version is not None:
            drive_data["firmware_version"] = firmware_version
        member_id = controller.get("MemberId")
        if member_id is not None:
            drive_data["slot"] = member_id
            if drive_data.get("physical_location") in (None, "unknown"):
                slot_value: Any = (
                    int(member_id) if str(member_id).isdigit() else member_id
                )
                drive_data["physical_location"] = {
                    "partlocation": {
                        "locationordinalvalue": slot_value,
                        "locationtype": "Slot",
                    }
                }
        speed_gbps = controller.get("SpeedGbps")
        if speed_gbps is not None:
            drive_data["speed_gbps"] = speed_gbps


# Backward-compatible alias for entry points and external imports.
AtollonSystem = AtollonRedfishProvider
