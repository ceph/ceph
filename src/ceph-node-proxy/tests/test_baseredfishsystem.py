from unittest.mock import MagicMock, patch

import pytest

from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem
from ceph_node_proxy.redfish import ComponentUpdateSpec


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def mock_endpoints():
    return MagicMock()


@pytest.fixture
def system(mock_client, mock_endpoints):
    with (
        patch(
            "ceph_node_proxy.baseredfishsystem.RedFishClient", return_value=mock_client
        ),
        patch(
            "ceph_node_proxy.baseredfishsystem.EndpointMgr", return_value=mock_endpoints
        ),
    ):
        return BaseRedfishSystem(
            host="testhost",
            port="443",
            username="user",
            password="secret",
            config={},
        )


class TestBaseRedfishSystemInit:
    def test_init_sets_client_and_endpoints(self, system, mock_client, mock_endpoints):
        assert system.client is mock_client
        assert system.endpoints is mock_endpoints

    def test_init_default_component_list(self, system):
        assert "memory" in system.component_list
        assert "power" in system.component_list
        assert "network" in system.component_list
        assert "processors" in system.component_list
        assert "storage" in system.component_list
        assert "firmwares" in system.component_list
        assert "fans" in system.component_list

    def test_init_update_funcs_populated(self, system):
        # Should have one callable per component that has _update_<name>
        assert len(system.update_funcs) >= 6

    def test_init_custom_component_list(self, mock_client, mock_endpoints):
        with (
            patch(
                "ceph_node_proxy.baseredfishsystem.RedFishClient",
                return_value=mock_client,
            ),
            patch(
                "ceph_node_proxy.baseredfishsystem.EndpointMgr",
                return_value=mock_endpoints,
            ),
        ):
            sys = BaseRedfishSystem(
                host="h",
                port="443",
                username="u",
                password="p",
                config={},
                component_list=["memory", "network"],
            )
        assert sys.component_list == ["memory", "network"]
        assert len(sys.update_funcs) == 2


class TestBaseRedfishSystemGetters:
    def test_get_sn_empty(self, system):
        assert system.get_sn() == ""

    def test_get_sn_from_sys(self, system):
        system._sys["SN"] = "ABC123"
        assert system.get_sn() == "ABC123"

    def test_get_memory_empty(self, system):
        assert system.get_memory() == {}

    def test_get_memory_from_sys(self, system):
        system._sys["memory"] = {"slot1": {"capacity_mib": 16384}}
        assert system.get_memory() == {"slot1": {"capacity_mib": 16384}}

    def test_get_processors_empty(self, system):
        assert system.get_processors() == {}

    def test_get_network_empty(self, system):
        assert system.get_network() == {}

    def test_get_storage_empty(self, system):
        assert system.get_storage() == {}

    def test_get_power_empty(self, system):
        assert system.get_power() == {}

    def test_get_fans_empty(self, system):
        assert system.get_fans() == {}

    def test_get_firmwares_empty(self, system):
        assert system.get_firmwares() == {}

    def test_get_status_empty(self, system):
        assert system.get_status() == {}


class TestBaseRedfishSystemGetSystem:
    def test_get_system_structure(self, system):
        system._sys["SN"] = "SN1"
        system._sys["memory"] = {}
        system._sys["processors"] = {}
        system._sys["network"] = {}
        system._sys["storage"] = {}
        system._sys["power"] = {}
        system._sys["fans"] = {}
        system._sys["firmwares"] = {}
        result = system.get_system()
        assert "host" in result
        assert "sn" in result
        assert result["sn"] == "SN1"
        assert "status" in result
        assert result["status"]["memory"] == {}
        assert result["status"]["processors"] == {}
        assert result["status"]["network"] == {}
        assert result["status"]["storage"] == {}
        assert result["status"]["power"] == {}
        assert result["status"]["fans"] == {}
        assert "firmwares" in result


class TestBaseRedfishSystemGetSpecs:

    def test_get_specs_network(self, system):
        specs = system.get_specs("network")
        assert len(specs) == 2
        assert all(isinstance(s, ComponentUpdateSpec) for s in specs)
        paths = [s.path for s in specs]
        assert "EthernetInterfaces" in paths
        assert "NetworkInterfaces" in paths
        assert specs[0].collection == "systems"
        assert "Name" in specs[0].fields
        assert specs[0].attribute is None

    def test_get_specs_memory(self, system):
        specs = system.get_specs("memory")
        assert len(specs) == 1
        assert specs[0].collection == "systems"
        assert specs[0].path == "Memory"
        assert "CapacityMiB" in specs[0].fields

    def test_get_specs_power(self, system):
        specs = system.get_specs("power")
        assert len(specs) == 1
        assert specs[0].collection == "chassis"
        assert "PowerSubsystem" in specs[0].path
        assert specs[0].attribute is None

    def test_get_specs_fans(self, system):
        specs = system.get_specs("fans")
        assert len(specs) == 1
        assert specs[0].collection == "chassis"
        assert specs[0].path == "Thermal"
        assert specs[0].attribute == "Fans"

    def test_get_component_spec_overrides_empty(self, system):
        assert system.get_component_spec_overrides() == {}


class TestBaseRedfishSystemFlush:
    def test_flush_clears_state(self, system):
        system._system = {"x": 1}
        system.previous_data = {"y": 2}
        system.data_ready = True
        system.flush()
        assert system._system == {}
        assert system.previous_data == {}
        assert system.data_ready is False


class TestBaseRedfishSystemUpdate:
    def test_update_calls_update_component(self, system):
        with patch("ceph_node_proxy.baseredfishsystem.update_component") as mock_update:
            system.update(
                collection="systems",
                component="memory",
                path="Memory",
                fields=BaseRedfishSystem.MEMORY_FIELDS,
                attribute=None,
            )
            mock_update.assert_called_once()
            call_kw = mock_update.call_args[1]
            assert call_kw.get("attribute") is None
            assert mock_update.call_args[0][1] == "systems"
            assert mock_update.call_args[0][2] == "memory"
            assert mock_update.call_args[0][3] == "Memory"


class TestBaseRedfishSystemNotImplemented:
    @pytest.mark.parametrize(
        "method,args",
        [
            ("device_led_on", ("disk1",)),
            ("device_led_off", ("disk1",)),
            ("chassis_led_on", ()),
            ("chassis_led_off", ()),
            ("get_device_led", ("disk1",)),
            ("set_device_led", ("disk1", {"on": True})),
            ("get_chassis_led", ()),
            ("set_chassis_led", ({"state": "on"},)),
            ("shutdown_host", (False,)),
            ("powercycle", ()),
        ],
    )
    def test_not_implemented(self, system, method, args):
        with pytest.raises(NotImplementedError):
            getattr(system, method)(*args)


class TestBaseRedfishSystemComponentSpecs:
    def test_component_specs_has_expected_keys(self):
        assert "network" in BaseRedfishSystem.COMPONENT_SPECS
        assert "processors" in BaseRedfishSystem.COMPONENT_SPECS
        assert "memory" in BaseRedfishSystem.COMPONENT_SPECS
        assert "power" in BaseRedfishSystem.COMPONENT_SPECS
        assert "fans" in BaseRedfishSystem.COMPONENT_SPECS
        assert "firmwares" in BaseRedfishSystem.COMPONENT_SPECS

    def test_field_lists_non_empty(self):
        assert len(BaseRedfishSystem.NETWORK_FIELDS) > 0
        assert len(BaseRedfishSystem.MEMORY_FIELDS) > 0
        assert len(BaseRedfishSystem.POWER_FIELDS) > 0
