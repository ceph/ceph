from unittest.mock import patch

from ceph_node_proxy.local_collectors import FCMCollector, LocalCollectorRunner


class TestFCMCollector:
    def test_name(self):
        assert FCMCollector().name == "fcm"

    def test_update_and_get_data(self):
        sample_stats = {
            "nvme2n1": {
                "device": "nvme2n1",
                "valid": True,
                "compression_ratio": 2.5,
                "compression_ratio_str": "2.5:1",
            },
        }
        collector = FCMCollector()
        with patch(
            "ceph_node_proxy.local_collectors.collect_fcm_stats",
            return_value=sample_stats,
        ):
            collector.update()

        assert collector.get_data() == {"local": sample_stats}

    def test_flush_clears_data(self):
        collector = FCMCollector()
        collector._data = {"local": {"nvme2n1": {"valid": True}}}
        collector.flush()
        assert collector.get_data() == {}


class TestLocalCollectorRunner:
    def test_runner_update_and_get_category(self):
        sample_stats = {"nvme2n1": {"valid": True}}
        runner = LocalCollectorRunner([FCMCollector()])
        with patch(
            "ceph_node_proxy.local_collectors.collect_fcm_stats",
            return_value=sample_stats,
        ):
            runner.update()

        assert runner.get_category("fcm") == {"local": sample_stats}
        assert runner.get_category("missing") == {}

    def test_runner_flush(self):
        runner = LocalCollectorRunner([FCMCollector()])
        runner._collectors[0]._data = {"local": {"nvme2n1": {"valid": True}}}
        runner.flush()
        assert runner.get_category("fcm") == {}
