from unittest.mock import MagicMock, patch

import pytest

from ceph_node_proxy.atollon import AtollonRedfishProvider
from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem


@pytest.fixture
def atollon_redfish():
    with (
        patch("ceph_node_proxy.baseredfishsystem.RedFishClient", return_value=MagicMock()),
        patch("ceph_node_proxy.baseredfishsystem.EndpointMgr", return_value=MagicMock()),
    ):
        return AtollonRedfishProvider(
            host="testhost",
            port="443",
            username="user",
            password="secret",
            config={},
        )


class TestAtollonRedfishProviderMemoryOverrides:
    def test_get_specs_memory_uses_id_instead_of_description(self, atollon_redfish):
        specs = atollon_redfish.get_specs("memory")
        assert len(specs) == 1
        assert "Id" in specs[0].fields
        assert "Description" not in specs[0].fields
        assert "MemoryDeviceType" in specs[0].fields

    def test_update_memory_maps_id_to_description(self, atollon_redfish):
        atollon_redfish._sys["memory"] = {
            "1": {
                "dimm0": {
                    "id": "dimm0",
                    "memory_device_type": "DDR4",
                    "capacity_mib": 16384,
                    "status": {"health": "OK", "state": "Enabled"},
                },
            },
        }

        with patch.object(BaseRedfishSystem, "_update_memory") as mock_super:
            mock_super.side_effect = lambda: None
            atollon_redfish._update_memory()

        assert atollon_redfish._sys["memory"]["1"]["dimm0"]["description"] == "dimm0"


class TestAtollonRedfishProviderStorageOverrides:
    def test_update_storage_replaces_unknown_description(self, atollon_redfish):
        atollon_redfish._sys["storage"] = {
            "Self": {
                "nvme_device0_nsid1": {
                    "description": "unknown",
                    "model": "Micron_2550_MTFDKBK512TGE",
                    "capacity_bytes": 512110190592,
                    "protocol": "NVMe",
                    "serial_number": "24424BAA3C40",
                    "status": {"health": "OK", "state": "Enabled"},
                    "redfish_endpoint": (
                        "/redfish/v1/Systems/Self/Storage/StorageUnit_0/"
                        "Drives/NVMe_Device0_NSID1"
                    ),
                    "entity": "StorageUnit_0",
                },
            },
        }

        with patch.object(BaseRedfishSystem, "_update_storage") as mock_super:
            mock_super.side_effect = lambda: None
            atollon_redfish.fix_storage_descriptions()

        drive = atollon_redfish._sys["storage"]["Self"]["nvme_device0_nsid1"]
        assert drive["description"] == "NVMe_Device0_NSID1"

    def test_enrich_storage_from_controllers_by_serial(self, atollon_redfish):
        atollon_redfish._sys["storage"] = {
            "Self": {
                "nvme_device0_nsid1": {
                    "description": "NVMe_Device0_NSID1",
                    "model": "Micron_2550_MTFDKBK512TGE",
                    "serial_number": "24424BAA3C40",
                    "entity": "StorageUnit_0",
                    "physical_location": "unknown",
                },
            },
        }
        mock_storage = MagicMock()
        mock_storage.get_members_data.return_value = {
            "StorageUnit_0": {
                "StorageControllers": [
                    {
                        "MemberId": "0",
                        "SerialNumber": "24424BAA3C40",
                        "FirmwareVersion": "V6MA001",
                        "SpeedGbps": 63.02,
                    },
                ],
            },
        }
        member_endpoint = MagicMock()
        member_endpoint.__getitem__ = MagicMock(
            side_effect=lambda key: mock_storage if key == "Storage" else MagicMock()
        )
        systems_endpoint = MagicMock()
        systems_endpoint.get_members_names.return_value = ["Self"]
        systems_endpoint.__getitem__ = MagicMock(
            side_effect=lambda key: member_endpoint if key == "Self" else MagicMock()
        )
        atollon_redfish.endpoints = MagicMock()
        atollon_redfish.endpoints.__getitem__ = MagicMock(
            side_effect=lambda key: systems_endpoint if key == "systems" else MagicMock()
        )

        atollon_redfish.enrich_storage_from_controllers()

        drive = atollon_redfish._sys["storage"]["Self"]["nvme_device0_nsid1"]
        assert drive["firmware_version"] == "V6MA001"
        assert drive["slot"] == "0"
        assert drive["speed_gbps"] == 63.02
        assert drive["physical_location"]["partlocation"]["locationordinalvalue"] == 0

    def test_enrich_storage_from_controllers_by_device_index(self, atollon_redfish):
        atollon_redfish._sys["storage"] = {
            "Self": {
                "nvme_device2_nsid1": {
                    "description": "NVMe_Device2_NSID1",
                    "serial_number": "unknown",
                    "entity": "StorageUnit_0",
                },
            },
        }
        mock_storage = MagicMock()
        mock_storage.get_members_data.return_value = {
            "StorageUnit_0": {
                "StorageControllers": [
                    {
                        "MemberId": "2",
                        "SerialNumber": "03NK797YS344D57S056",
                        "FirmwareVersion": "000A5305",
                    },
                ],
            },
        }
        member_endpoint = MagicMock()
        member_endpoint.__getitem__ = MagicMock(
            side_effect=lambda key: mock_storage if key == "Storage" else MagicMock()
        )
        systems_endpoint = MagicMock()
        systems_endpoint.get_members_names.return_value = ["Self"]
        systems_endpoint.__getitem__ = MagicMock(
            side_effect=lambda key: member_endpoint if key == "Self" else MagicMock()
        )
        atollon_redfish.endpoints = MagicMock()
        atollon_redfish.endpoints.__getitem__ = MagicMock(
            side_effect=lambda key: systems_endpoint if key == "systems" else MagicMock()
        )

        atollon_redfish.enrich_storage_from_controllers()

        drive = atollon_redfish._sys["storage"]["Self"]["nvme_device2_nsid1"]
        assert drive["firmware_version"] == "000A5305"
        assert drive["slot"] == "2"
