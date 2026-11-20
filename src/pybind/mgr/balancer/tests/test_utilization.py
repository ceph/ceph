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
    used = {0: 90.0, 1: 10.0, 2: 50.0}
    capacity = {0: 100.0, 1: 100.0, 2: 100.0}
    pg_shards = {
        '1.0': {'osds': [0, 2], 'bytes': 40.0, 'pool_id': 1, 'ps': 0},
    }

    def try_fn(pool_id, ps, overfull, underfull, more_underfull):
        assert pool_id == 1 and ps == 0
        assert 0 in overfull
        assert 1 in underfull
        return {'orig': [0, 2], 'out': [1, 2]}

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
