from ceph_node_proxy.util import fill_missing_identity, is_unknown


def test_is_unknown_treats_none_empty_and_unknown() -> None:
    assert is_unknown(None)
    assert is_unknown("")
    assert is_unknown("unknown")
    assert is_unknown("Unknown")
    assert not is_unknown("DIMM A1")
    assert not is_unknown("DevType2_DIMM0")


def test_fill_missing_identity_keeps_existing_description() -> None:
    member = {"id": "DevType2_DIMM0", "description": "DIMM A1"}
    fill_missing_identity(member, "dimm0")
    assert member["description"] == "DIMM A1"


def test_fill_missing_identity_uses_id() -> None:
    member = {"id": "DevType2_DIMM0", "description": "unknown"}
    fill_missing_identity(member, "dimm0")
    assert member["description"] == "DevType2_DIMM0"


def test_fill_missing_identity_uses_member_key() -> None:
    member = {"id": "unknown", "description": "unknown"}
    fill_missing_identity(member, "DevType2_DIMM3")
    assert member["description"] == "DevType2_DIMM3"


def test_fill_missing_identity_prefers_redfish_endpoint() -> None:
    member = {
        "id": "nvme_device0_nsid1",
        "description": "unknown",
        "redfish_endpoint": (
            "/redfish/v1/Systems/Self/Storage/StorageUnit_0/Drives/NVMe_Device0_NSID1"
        ),
    }
    fill_missing_identity(member, "nvme_device0_nsid1")
    assert member["description"] == "NVMe_Device0_NSID1"


def test_fill_missing_identity_fills_name_when_present() -> None:
    member = {"id": "NIC.Slot.1", "name": "unknown", "description": "unknown"}
    fill_missing_identity(member, "nic1")
    assert member["name"] == "NIC.Slot.1"
    assert member["description"] == "NIC.Slot.1"


def test_fill_missing_identity_does_not_add_name() -> None:
    member = {"id": "DevType2_DIMM0", "description": "unknown"}
    fill_missing_identity(member, "dimm0")
    assert "name" not in member
