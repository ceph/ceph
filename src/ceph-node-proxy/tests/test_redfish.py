from unittest.mock import MagicMock

from ceph_node_proxy.redfish import build_data, get_component_data


def test_build_data_thermal_fans_and_temperatures() -> None:
    thermal = {
        "Fans": [
            {
                "MemberId": "0",
                "Name": "FAN1_TACH_IN",
                "PhysicalContext": "Fan",
                "Reading": 23835,
                "ReadingUnits": "RPM",
                "Status": {"Health": "OK", "State": "Enabled"},
            }
        ],
        "Temperatures": [
            {
                "MemberId": "0",
                "Name": "C1_DCSCM_TEMP",
                "PhysicalContext": "Intake",
                "ReadingCelsius": 31,
                "Status": {"Health": "OK", "State": "Enabled"},
            }
        ],
    }
    log = MagicMock()
    fields = ["Name", "PhysicalContext", "Reading", "ReadingUnits", "Status"]

    fans = build_data(thermal, fields, log, attribute="Fans")
    assert fans["0"]["reading"] == 23835
    assert fans["0"]["reading_units"] == "RPM"

    temps = build_data(thermal, fields, log, attribute="Temperatures")
    assert temps["0"]["reading"] == 31
    assert temps["0"]["reading_units"] == "Cel"


def test_build_data_skips_absent_members() -> None:
    thermal = {
        "Temperatures": [
            {
                "MemberId": "0",
                "Name": "C1_DCSCM_TEMP",
                "PhysicalContext": "Intake",
                "ReadingCelsius": 31,
                "Status": {"Health": "OK", "State": "Enabled"},
            },
            {
                "MemberId": "1",
                "Name": "C1_DIMMB_TEMP",
                "PhysicalContext": "Intake",
                "Status": {"State": "Absent"},
            },
        ],
    }
    log = MagicMock()
    fields = ["Name", "PhysicalContext", "Reading", "ReadingUnits", "Status"]

    temps = build_data(thermal, fields, log, attribute="Temperatures")
    assert list(temps.keys()) == ["0"]
    assert temps["0"]["name"] == "C1_DCSCM_TEMP"


def test_get_component_data_refetches_attribute_endpoint() -> None:
    """Fans/temps use attribute=Fans on Thermal; must not reuse cached Endpoint.data."""
    log = MagicMock()
    fields = ["Name", "PhysicalContext", "Reading", "ReadingUnits", "Status"]
    thermal_ep = MagicMock()
    thermal_ep.data = {
        "Fans": [
            {
                "MemberId": "0",
                "Name": "FAN1_TACH_IN",
                "PhysicalContext": "Fan",
                "Reading": 18480,
                "ReadingUnits": "RPM",
                "Status": {"Health": "OK", "State": "Enabled"},
            }
        ]
    }
    thermal_ep.get_data.return_value = {
        "Fans": [
            {
                "MemberId": "0",
                "Name": "FAN1_TACH_IN",
                "PhysicalContext": "Fan",
                "Status": {"State": "Absent"},
            }
        ]
    }

    member_ep = MagicMock()
    member_ep.__getitem__.return_value = thermal_ep
    collection_ep = MagicMock()
    collection_ep.get_members_names.return_value = ["Self"]
    collection_ep.__getitem__.return_value = member_ep
    endpoints = MagicMock()
    endpoints.__getitem__.return_value = collection_ep

    result = get_component_data(
        endpoints, "chassis", "Thermal", fields, log, attribute="Fans"
    )

    thermal_ep.get_data.assert_called_once()
    assert result["Self"] == {}
