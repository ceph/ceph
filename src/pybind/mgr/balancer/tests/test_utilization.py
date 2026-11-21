# Tests for size-aware utilization planner helpers (no native mgr required).

import importlib.util
from pathlib import Path

_util_path = Path(__file__).resolve().parents[1] / 'utilization.py'
_spec = importlib.util.spec_from_file_location('balancer_utilization', _util_path)
assert _spec and _spec.loader
_utilization = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_utilization)

utilization = _utilization.utilization
fill_variance = _utilization.fill_variance
shard_bytes_for_pg = _utilization.shard_bytes_for_pg
remap_pairs = _utilization.remap_pairs
apply_move = _utilization.apply_move
plan_utilization_upmaps = _utilization.plan_utilization_upmaps
build_pg_shards = _utilization.build_pg_shards
pool_pg_num_unsettled = _utilization.pool_pg_num_unsettled
within_utilization_deviation = _utilization.within_utilization_deviation
DEVIATION_MEAN = _utilization.DEVIATION_MEAN
DEVIATION_MINMAX = _utilization.DEVIATION_MINMAX


def test_pool_pg_num_unsettled():
    # Settled: remaps allowed.
    assert pool_pg_num_unsettled({'pg_num': 32, 'pg_num_target': 32}) is False
    # Pending split (autoscaler raised target): defer remaps.
    assert pool_pg_num_unsettled({'pg_num': 32, 'pg_num_target': 64}) is True
    # Pending merge: defer remaps (same gate as stock upmap merge skip).
    assert pool_pg_num_unsettled({'pg_num': 64, 'pg_num_target': 32}) is True


def test_utilization_and_variance():
    assert utilization(50, 100) == 0.5
    assert utilization(10, 0) == 0.0
    used = {0: 90.0, 1: 10.0}
    cap = {0: 100.0, 1: 100.0}
    assert fill_variance(used, cap) > 0.0
    balanced = {0: 50.0, 1: 50.0}
    assert fill_variance(balanced, cap) == 0.0


def test_shard_bytes_replicated_and_ec():
    # replicated size=3, raw_used_rate=3 → full logical bytes per OSD
    assert shard_bytes_for_pg(1000, 3, 3.0) == 1000.0
    # EC k=4 m=2 → size=6, raw_used_rate=1.5 → num_bytes/k
    assert shard_bytes_for_pg(1000, 6, 1.5) == 250.0


def test_remap_pairs_and_apply_move():
    assert remap_pairs([0, 1, 2], [0, 3, 2]) == [(1, 3)]
    used = {0: 100.0, 1: 20.0}
    nxt = apply_move(used, 0, 1, 30.0)
    assert nxt[0] == 70.0 and nxt[1] == 50.0
    assert used[0] == 100.0  # original unchanged


def test_plan_accepts_variance_improving_move():
    # Four OSDs so a mid-tier device (osd.2) is neither under_pref nor in the PG.
    used = {0: 90.0, 1: 10.0, 2: 55.0, 3: 50.0}
    capacity = {0: 100.0, 1: 100.0, 2: 100.0, 3: 100.0}
    pg_shards = {
        '1.0': {'osds': [0, 3], 'bytes': 40.0, 'pool_id': 1, 'ps': 0},
    }
    seen_more = []

    def try_fn(pool_id, ps, overfull, underfull, more_underfull):
        assert pool_id == 1 and ps == 0
        assert 0 in overfull
        assert 1 in underfull
        # osd.2 is at/above mean and not in the PG → secondary destination list.
        seen_more.append(more_underfull)
        return {'orig': [0, 3], 'out': [1, 3]}

    pending = plan_utilization_upmaps(
        used=used,
        capacity=capacity,
        pg_shards=pg_shards,
        try_pg_upmap=try_fn,
        max_optimizations=5,
        max_deviation=0.01,
    )
    assert '1.0' in pending
    assert pending['1.0'][-1] == [0, 1]
    assert seen_more and seen_more[0] is not None
    assert 2 in seen_more[0]


def test_plan_rejects_when_already_balanced():
    used = {0: 60.0, 1: 50.0, 2: 50.0}
    capacity = {0: 100.0, 1: 100.0, 2: 100.0}
    pg_shards = {
        '1.0': {'osds': [0, 2], 'bytes': 40.0, 'pool_id': 1, 'ps': 0},
    }

    def try_fn(pool_id, ps, overfull, underfull, more_underfull):
        return {'orig': [0, 2], 'out': [1, 2]}

    # Huge max_deviation → fullest already within slack of mean → no work.
    pending = plan_utilization_upmaps(
        used=used,
        capacity=capacity,
        pg_shards=pg_shards,
        try_pg_upmap=try_fn,
        max_optimizations=5,
        max_deviation=0.5,
    )
    assert pending == {}


def test_within_utilization_deviation_mean_vs_minmax():
    # Fills ~0.2695 / 0.28 / 0.2911 → range ~0.0216, max-mean ~0.011.
    used = {0: 26.95, 1: 28.0, 2: 29.11}
    capacity = {0: 100.0, 1: 100.0, 2: 100.0}
    assert within_utilization_deviation(
        used, capacity, 0.02, DEVIATION_MEAN) is True
    assert within_utilization_deviation(
        used, capacity, 0.02, DEVIATION_MINMAX) is False
    assert within_utilization_deviation(
        used, capacity, 0.03, DEVIATION_MINMAX) is True


def test_plan_minmax_keeps_working_when_mean_satisfied():
    # Mean gate would stop (fullest within 2pp of mean); minmax should move.
    used = {0: 26.95, 1: 28.0, 2: 29.11}
    capacity = {0: 100.0, 1: 100.0, 2: 100.0}
    pg_shards = {
        '1.0': {'osds': [2, 1], 'bytes': 2.0, 'pool_id': 1, 'ps': 0},
    }

    def try_fn(pool_id, ps, overfull, underfull, more_underfull):
        assert 2 in overfull
        assert 0 in underfull
        return {'orig': [2, 1], 'out': [0, 1]}

    mean_pending = plan_utilization_upmaps(
        used=used,
        capacity=capacity,
        pg_shards=pg_shards,
        try_pg_upmap=try_fn,
        max_optimizations=5,
        max_deviation=0.02,
        deviation_mode=DEVIATION_MEAN,
    )
    assert mean_pending == {}

    minmax_pending = plan_utilization_upmaps(
        used=used,
        capacity=capacity,
        pg_shards=pg_shards,
        try_pg_upmap=try_fn,
        max_optimizations=5,
        max_deviation=0.02,
        deviation_mode=DEVIATION_MINMAX,
    )
    assert '1.0' in minmax_pending
    assert minmax_pending['1.0'][-1] == [2, 0]


def test_build_pg_shards_active_clean_filter():
    pg_up = {'1.0': [0, 1], '1.1': [1, 2]}
    pg_stat = {
        '1.0': {'num_bytes': 1000},
        '1.1': {'num_bytes': 2000},
    }
    pools = {1: {'pool': 1, 'size': 2}}

    def rate(_):
        return 2.0

    shards = build_pg_shards(pg_up, pg_stat, pools, rate, active_clean_pgids=['1.0'])
    assert list(shards.keys()) == ['1.0']
    assert shards['1.0']['bytes'] == 1000.0
