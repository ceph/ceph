# python unit test
from collections import defaultdict

from pytest import approx
from tests import mock

from pg_autoscaler import module


class FakeRoot:

    def __init__(self,
                 pg_target=1000,
                 total_target_ratio=0.0,
                 total_target_bytes=0,
                 total_pinned_ratio=0.0):
        self.pg_target = pg_target
        self.total_target_ratio = total_target_ratio
        self.total_target_bytes = total_target_bytes
        self.total_pinned_ratio = total_pinned_ratio


class TestEffectiveRatioPlanner:

    def setup_method(self):
        self.autoscaler = module.PgAutoscaler('module_name', 0, 0)
        # the simpleautoscale flag is the master switch for the planner;
        # individual tests flip it off to assert inertness
        self.autoscaler.has_simpleautoscale_flag = lambda: True

    def calc_metrics(self, options, root, mode='warn', raw_used_rate=3.0,
                     bytes_used=0, capacity=1000):
        osdmap = mock.Mock()
        osdmap.pool_raw_used_rate.return_value = raw_used_rate
        pool_metrics = defaultdict(dict)
        pool_stats = {0: {'bytes_used': bytes_used}}
        p = {'pool': 0, 'options': options, 'pg_autoscale_mode': mode}
        self.autoscaler._calculate_pool_metrics(
            osdmap, {0: root}, 0, 0, pool_stats, capacity, False, p,
            pool_metrics, self.autoscaler.has_simpleautoscale_flag())
        return pool_metrics[0]

    def test_plan_is_absolute_share_of_budget(self):
        # a planner pool's plan is ratio x budget: never normalized against
        # other pools' ratios, never shaved by target_size_bytes reservations
        root = FakeRoot(pg_target=1000,
                        total_target_ratio=2.0,
                        total_target_bytes=500,
                        total_pinned_ratio=0.9)
        metrics = self.calc_metrics({'effective_ratio': 0.5}, root)
        assert metrics['planner']
        assert metrics['pinned_pgs'] == approx(500.0)
        assert metrics['target_ratio'] == approx(0.5)

    def test_ratio_inert_without_flag(self):
        # the simpleautoscale flag is the master switch: a declared (or
        # pre-staged) effective_ratio does nothing until it is set
        self.autoscaler.has_simpleautoscale_flag = lambda: False
        root = FakeRoot()
        for mode in ('on', 'warn', 'off'):
            metrics = self.calc_metrics({'effective_ratio': 0.5}, root,
                                        mode=mode)
            assert not metrics['planner']
            assert metrics['pinned_pgs'] == approx(0.0)

    def test_mode_on_is_planner(self):
        root = FakeRoot(pg_target=1000)
        metrics = self.calc_metrics({'effective_ratio': 0.25}, root, mode='on')
        assert metrics['planner']
        assert metrics['pinned_pgs'] == approx(250.0)

    def test_mode_off_opts_out(self):
        # mode 'off' keeps its classic absolute meaning: the planner
        # ignores the pool even under the flag
        root = FakeRoot(pg_target=2000)
        metrics = self.calc_metrics({'effective_ratio': 0.25}, root,
                                    mode='off')
        assert not metrics['planner']
        assert metrics['pinned_pgs'] == approx(0.0)

    def test_planner_ignores_target_size_bytes(self):
        root = FakeRoot()
        metrics = self.calc_metrics(
            {'effective_ratio': 0.5, 'target_size_bytes': 100}, root)
        assert metrics['target_bytes'] == 0
        assert metrics['pinned_pgs'] == approx(500.0)

    def test_target_size_ratio_normalized_into_remainder(self):
        # a legacy target_size_ratio pool only gets a share of what the
        # planner pools leave behind
        root = FakeRoot(total_target_ratio=1.0, total_pinned_ratio=0.5)
        metrics = self.calc_metrics({'target_size_ratio': 0.5}, root,
                                    mode='on')
        assert metrics['pinned_pgs'] == approx(0.0)
        assert metrics['target_ratio'] == approx(0.25)

    def test_no_ratios_unchanged(self):
        root = FakeRoot(total_target_ratio=1.0)
        metrics = self.calc_metrics({'target_size_ratio': 0.5}, root,
                                    mode='on')
        assert metrics['pinned_pgs'] == approx(0.0)
        assert metrics['target_ratio'] == approx(0.5)
