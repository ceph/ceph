from unittest.mock import MagicMock, patch

import pytest

from ceph_node_proxy.local_collectors import FCMCollector, LocalCollectorRunner
from ceph_node_proxy.node_backend import NodeBackend
from ceph_node_proxy.registry import create_local_collector_runner, create_node_backend


@pytest.fixture
def mock_redfish():
    redfish = MagicMock()
    redfish.config = {}
    redfish.refresh_interval = 1
    redfish.get_system.return_value = {
        "host": "node01",
        "sn": "SN123",
        "status": {
            "storage": {},
            "processors": {},
            "network": {},
            "memory": {},
            "power": {},
            "fans": {},
            "temperatures": {},
        },
        "firmware": {},
    }
    return redfish


class TestLocalCollectorRunnerRegistry:
    def test_atollon_vendor_includes_fcm_collector(self):
        runner = create_local_collector_runner("atollon")
        assert runner.categories() == ["fcm"]

    def test_generic_vendor_has_no_collectors(self):
        runner = create_local_collector_runner("generic")
        assert runner.categories() == []


class TestNodeBackend:
    def test_get_fcm_delegates_to_local_runner(self, mock_redfish):
        runner = LocalCollectorRunner([FCMCollector()])
        backend = NodeBackend(mock_redfish, runner)
        sample_stats = {
            "nvme2n1": {
                "device": "nvme2n1",
                "valid": True,
                "compression_ratio": 2.5,
                "compression_ratio_str": "2.5:1",
            },
        }
        with patch(
            "ceph_node_proxy.local_collectors.collect_fcm_stats",
            return_value=sample_stats,
        ):
            runner.update()

        assert backend.get_fcm() == {"local": sample_stats}

    def test_get_system_merges_redfish_and_local_categories(self, mock_redfish):
        runner = LocalCollectorRunner([FCMCollector()])
        backend = NodeBackend(mock_redfish, runner)
        sample_stats = {"nvme2n1": {"valid": True, "compression_ratio": 2.0}}
        with patch(
            "ceph_node_proxy.local_collectors.collect_fcm_stats",
            return_value=sample_stats,
        ):
            runner.update()

        result = backend.get_system()
        assert result["status"]["fcm"] == {"local": sample_stats}
        assert "storage" in result["status"]
        mock_redfish.get_system.assert_called_once()

    def test_flush_clears_local_collectors(self, mock_redfish):
        runner = LocalCollectorRunner([FCMCollector()])
        backend = NodeBackend(mock_redfish, runner)
        runner.update()
        backend.flush()
        assert backend.get_fcm() == {}
        mock_redfish.flush.assert_called_once()

    def test_delegates_storage_to_redfish(self, mock_redfish):
        mock_redfish.get_storage.return_value = {"Self": {"drive1": {}}}
        backend = NodeBackend(mock_redfish, LocalCollectorRunner([]))
        assert backend.get_storage() == {"Self": {"drive1": {}}}


class TestCreateNodeBackend:
    def test_create_node_backend_atollon(self):
        with patch("ceph_node_proxy.registry.get_redfish_provider_class") as mock_provider:
            mock_redfish = MagicMock()
            mock_provider.return_value.return_value = mock_redfish
            backend = create_node_backend(
                "atollon",
                host="bmc",
                port="443",
                username="user",
                password="secret",
                config=MagicMock(),
            )

        assert isinstance(backend, NodeBackend)
        assert backend.redfish is mock_redfish
        assert backend._local.categories() == ["fcm"]
