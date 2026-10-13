import struct
from unittest.mock import patch

from ceph_node_proxy.fcm_stats import (
    apply_fcm_display_fields,
    collect_fcm_stats,
    fcm_usage_display,
    is_fcm_device,
    is_nvme_namespace_block_device,
    list_nvme_namespace_names,
    query_nvme_log_page,
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


class TestQueryNVMeLogPage:
    def test_open_failure_returns_none(self):
        with patch(
            "ceph_node_proxy.fcm_stats.os.open",
            side_effect=FileNotFoundError("No such file or directory"),
        ):
            assert query_nvme_log_page("nvme3n1") is None


class TestIsNVMeNamespaceBlockDevice:
    def test_true_for_whole_namespace_with_dev_node(self):
        present = {
            "/sys/block/nvme0n1/dev",
            "/dev/nvme0n1",
        }
        with patch(
            "ceph_node_proxy.fcm_stats.os.path.exists",
            side_effect=lambda path: path in present,
        ):
            assert is_nvme_namespace_block_device("nvme0n1") is True

    def test_false_when_dev_node_missing(self):
        present = {"/sys/block/nvme3n1/dev"}
        with patch(
            "ceph_node_proxy.fcm_stats.os.path.exists",
            side_effect=lambda path: path in present,
        ):
            assert is_nvme_namespace_block_device("nvme3n1") is False

    def test_false_for_sysfs_alias_without_dev_node(self):
        present = {"/sys/block/nvme2c2n1/dev"}
        with patch(
            "ceph_node_proxy.fcm_stats.os.path.exists",
            side_effect=lambda path: path in present,
        ):
            assert is_nvme_namespace_block_device("nvme2c2n1") is False

    def test_false_for_partition(self):
        present = {
            "/sys/block/nvme0n1p1/partition",
            "/sys/block/nvme0n1p1/dev",
            "/dev/nvme0n1p1",
        }
        with patch(
            "ceph_node_proxy.fcm_stats.os.path.exists",
            side_effect=lambda path: path in present,
        ):
            assert is_nvme_namespace_block_device("nvme0n1p1") is False


class TestListNVMeNamespaceNames:
    def test_skips_alias_and_missing_dev_node(self):
        present = {
            "/sys/block/nvme0n1/dev",
            "/dev/nvme0n1",
            "/sys/block/nvme2c2n1/dev",
            "/sys/block/nvme3n1/dev",
        }
        with (
            patch(
                "ceph_node_proxy.fcm_stats.glob.glob",
                return_value=[
                    "/sys/block/nvme0n1",
                    "/sys/block/nvme2c2n1",
                    "/sys/block/nvme3n1",
                ],
            ),
            patch(
                "ceph_node_proxy.fcm_stats.os.path.exists",
                side_effect=lambda path: path in present,
            ),
        ):
            assert list_nvme_namespace_names() == ["nvme0n1"]


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

    def test_collect_fcm_stats_continues_when_one_device_raises(self):
        def read_stats(device: str):
            if device == "nvme1n1":
                raise OSError("device gone")
            return {"device": device, "valid": True}

        with (
            patch(
                "ceph_node_proxy.fcm_stats.list_nvme_namespace_names",
                return_value=["nvme0n1", "nvme1n1", "nvme2n1"],
            ),
            patch(
                "ceph_node_proxy.fcm_stats.is_fcm_device",
                return_value=True,
            ),
            patch(
                "ceph_node_proxy.fcm_stats.read_fcm_stats",
                side_effect=read_stats,
            ),
        ):
            stats = collect_fcm_stats()

        assert set(stats.keys()) == {"nvme0n1", "nvme2n1"}
