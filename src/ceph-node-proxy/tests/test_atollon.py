from unittest.mock import MagicMock, patch

import pytest

from ceph_node_proxy.atollon import AtollonRedfishProvider


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


class TestAtollonRedfishProviderStorageOverrides:
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
