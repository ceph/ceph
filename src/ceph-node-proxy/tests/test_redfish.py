from unittest.mock import MagicMock

from ceph_node_proxy.redfish import build_data


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
