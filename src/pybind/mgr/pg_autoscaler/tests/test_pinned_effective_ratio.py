# python unit test
from collections import defaultdict

from pytest import approx
from tests import mock

from pg_autoscaler import module


class FakeRoot:

    def __init__(self,
                 total_target_ratio=0.0,
                 total_target_bytes=0,
                 total_pinned_ratio=0.0):
        self.total_target_ratio = total_target_ratio
        self.total_target_bytes = total_target_bytes
        self.total_pinned_ratio = total_pinned_ratio


class TestPinnedEffectiveRatio:

    def setup_method(self):
        self.autoscaler = module.PgAutoscaler('module_name', 0, 0)

    def calc_metrics(self, options, root, raw_used_rate=3.0, bytes_used=0,
                     capacity=1000):
        osdmap = mock.Mock()
        osdmap.pool_raw_used_rate.return_value = raw_used_rate
        pool_metrics = defaultdict(dict)
        pool_stats = {0: {'bytes_used': bytes_used}}
        p = {'pool': 0, 'options': options}
        self.autoscaler._calculate_pool_metrics(
            osdmap, {0: root}, 0, 0, pool_stats, capacity, False, p,
            pool_metrics)
        return pool_metrics[0]

    def test_pinned_is_absolute(self):
        # a pinned effective_ratio is neither normalized against other
        # pools' ratios nor shaved by target_size_bytes reservations
        root = FakeRoot(total_target_ratio=2.0,
                        total_target_bytes=500,
                        total_pinned_ratio=0.9)
        metrics = self.calc_metrics({'effective_ratio': 0.5}, root)
        assert metrics['pinned_ratio'] == approx(0.5)
        assert metrics['target_ratio'] == approx(0.5)

    def test_pinned_ignores_target_size_bytes(self):
        root = FakeRoot(total_pinned_ratio=0.5)
        metrics = self.calc_metrics(
            {'effective_ratio': 0.5, 'target_size_bytes': 100}, root)
        assert metrics['target_bytes'] == 0
        assert metrics['target_ratio'] == approx(0.5)

    def test_target_size_ratio_normalized_into_remainder(self):
        # a legacy target_size_ratio pool only gets a share of what the
        # pinned pools leave behind
        root = FakeRoot(total_target_ratio=1.0, total_pinned_ratio=0.5)
        metrics = self.calc_metrics({'target_size_ratio': 0.5}, root)
        assert metrics['pinned_ratio'] == approx(0.0)
        assert metrics['target_ratio'] == approx(0.25)

    def test_no_pins_unchanged(self):
        root = FakeRoot(total_target_ratio=1.0)
        metrics = self.calc_metrics({'target_size_ratio': 0.5}, root)
        assert metrics['pinned_ratio'] == approx(0.0)
        assert metrics['target_ratio'] == approx(0.5)
