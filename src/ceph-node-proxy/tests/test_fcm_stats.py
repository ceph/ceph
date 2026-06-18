import struct
from unittest.mock import patch

from ceph_node_proxy.fcm_stats import (
    apply_fcm_display_fields,
    collect_fcm_stats,
    fcm_usage_display,
    is_fcm_device,
    read_fcm_stats,
)


class TestFCMStatsHelpers:
    def test_is_fcm_device_true(self):
        with patch(
            "ceph_node_proxy.fcm_stats.read_sysfs_block",
            return_value="IBM FCM5 3.2TB",
        ):
            assert is_fcm_device("nvme2n1") is True

    def test_is_fcm_device_false(self):
        with patch(
            "ceph_node_proxy.fcm_stats.read_sysfs_block",
            return_value="Micron_2550_MTFDKBK512TGE",
        ):
            assert is_fcm_device("nvme0n1") is False


class TestReadFCMStats:
    def _sample_log_page(self) -> bytes:
        return struct.pack(
            "<QQQQ",
            3_200_000_000_000,
            1_600_000_000_000,
            6_400_000_000_000,
            3_200_000_000_000,
        )

    def test_read_fcm_stats_success(self):
        with (
            patch(
                "ceph_node_proxy.fcm_stats.read_sysfs_block",
                side_effect=lambda device, attr: {
                    "device/model": "IBM FCM5 3.2TB",
                    "device/serial": "03NK797YS344D57S056",
                }[attr],
            ),
            patch(
                "ceph_node_proxy.fcm_stats.query_nvme_log_page",
                return_value=self._sample_log_page(),
            ),
        ):
            stats = read_fcm_stats("nvme2n1")

        assert stats["valid"] is True
        assert stats["device"] == "nvme2n1"
        assert stats["compression_ratio"] == 2.0
        assert stats["compression_ratio_str"] == "2:1"
        assert stats["savings_bytes"] == 1_600_000_000_000
        assert stats["compression_ratio_display"] == "2:1"
        assert stats["savings_display"] == "1.6 TB"
        assert stats["phy_usage_display"].endswith("(50%)")
        assert stats["log_usage_display"].endswith("(50%)")
        assert stats["status"]["health"] == "OK"

    def test_read_fcm_stats_ioctl_failure(self):
        with (
            patch(
                "ceph_node_proxy.fcm_stats.read_sysfs_block",
                side_effect=lambda device, attr: {
                    "device/model": "IBM FCM5 3.2TB",
                    "device/serial": "03NK797YS344D57S056",
                }[attr],
            ),
            patch(
                "ceph_node_proxy.fcm_stats.query_nvme_log_page",
                return_value=None,
            ),
        ):
            stats = read_fcm_stats("nvme2n1")

        assert stats["valid"] is False
        assert stats["compression_ratio_str"] == ""
        assert stats["compression_ratio_display"] == ""
        assert stats["phy_usage_display"] == ""
        assert stats["status"]["health"] == "Unknown"


class TestFCMDisplayFields:
    def test_fcm_usage_display_shows_human_bytes_and_int_percent(self):
        usage = fcm_usage_display(15833446, 0.00027025391332477435)
        assert usage.endswith("(0%)")
        assert "MB" in usage

    def test_apply_fcm_display_fields_hides_ratio_when_logical_usage_is_low(self):
        stats = apply_fcm_display_fields({
            "device": "nvme2n1",
            "model": "FCM5",
            "serial_number": "SN1",
            "valid": True,
            "phy_size_bytes": 0,
            "phy_util_bytes": 0,
            "log_size_bytes": 0,
            "log_util_bytes": 0,
            "phy_util_percent": 0.0,
            "log_util_percent": 0.0,
            "compression_ratio": 0.0,
            "compression_ratio_str": "0:1",
            "savings_bytes": 0,
            "compression_ratio_display": "",
            "savings_display": "",
            "phy_usage_display": "",
            "log_usage_display": "",
            "status": {"health": "OK", "state": "Enabled"},
        })
        assert stats["compression_ratio_display"] == ""
        assert stats["log_usage_display"].endswith("(0%)")

    def test_apply_fcm_display_fields_shows_ratio_and_formatted_values(self):
        stats = apply_fcm_display_fields({
            "device": "nvme5n1",
            "model": "FCM5",
            "serial_number": "SN2",
            "valid": True,
            "phy_size_bytes": 1,
            "phy_util_bytes": 15833446,
            "log_size_bytes": 1,
            "log_util_bytes": 16032506,
            "phy_util_percent": 0.00027025391332477435,
            "log_util_percent": 1.0,
            "compression_ratio": 1.0,
            "compression_ratio_str": "1:1",
            "savings_bytes": 199060,
            "compression_ratio_display": "",
            "savings_display": "",
            "phy_usage_display": "",
            "log_usage_display": "",
            "status": {"health": "OK", "state": "Enabled"},
        })
        assert stats["compression_ratio_display"] == "1:1"
        assert stats["savings_display"] == "199.1 KB"
        assert stats["phy_usage_display"].endswith("(0%)")
        assert stats["log_usage_display"].endswith("(1%)")

    def test_apply_fcm_display_fields_clears_metrics_when_invalid(self):
        stats = apply_fcm_display_fields({
            "device": "nvme0n1",
            "model": "FCM5",
            "serial_number": "SN3",
            "valid": False,
            "phy_size_bytes": 0,
            "phy_util_bytes": 0,
            "log_size_bytes": 0,
            "log_util_bytes": 0,
            "phy_util_percent": 0.0,
            "log_util_percent": 0.0,
            "compression_ratio": 0.0,
            "compression_ratio_str": "",
            "savings_bytes": 0,
            "compression_ratio_display": "",
            "savings_display": "",
            "phy_usage_display": "",
            "log_usage_display": "",
            "status": {"health": "Unknown", "state": "Unavailable"},
        })
        assert stats["compression_ratio_display"] == ""
        assert stats["savings_display"] == ""
        assert stats["phy_usage_display"] == ""
        assert stats["log_usage_display"] == ""


class TestCollectFCMStats:
    def test_collect_fcm_stats_only_fcm_devices(self):
        with (
            patch(
                "ceph_node_proxy.fcm_stats.list_nvme_namespace_names",
                return_value=["nvme0n1", "nvme2n1"],
            ),
            patch(
                "ceph_node_proxy.fcm_stats.is_fcm_device",
                side_effect=lambda device: device == "nvme2n1",
            ),
            patch(
                "ceph_node_proxy.fcm_stats.read_fcm_stats",
                side_effect=lambda device: {
                    "device": device,
                    "valid": True,
                    "compression_ratio": 2.0,
                    "compression_ratio_str": "2:1",
                },
            ),
        ):
            stats = collect_fcm_stats()

        assert set(stats.keys()) == {"nvme2n1"}
        assert stats["nvme2n1"]["compression_ratio_str"] == "2:1"
