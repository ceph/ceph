"""
Size-aware (byte utilization) upmap planner helpers.

Clean-room implementation of a utilization-based balancer inspired by jj-balancer: evacuate shards from overfull OSDs toward underfull OSDs when the move decreases fill-rate variance. CRUSH legality is left to OSDMap.try_pg_upmap.
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple


TryPgUpmapFn = Callable[
    [int, int, List[int], List[int], Optional[List[int]]],
    Optional[Dict[str, List[int]]],
]


def utilization(used: float, capacity: float) -> float:
    """Return fill ratio; empty capacity yields 0.0."""
    if capacity <= 0:
        return 0.0
    return float(used) / float(capacity)


def pool_pg_num_unsettled(pool: Dict[str, Any]) -> bool:
    """True while the mon is still advancing pg_num toward pg_num_target (split or merge)."""
    return int(pool['pg_num']) != int(pool['pg_num_target'])


# Stop-gate modes for upmap-utilization (see plan_utilization_upmaps).
DEVIATION_MEAN = 'mean'
DEVIATION_MINMAX = 'minmax'


def osd_fill_ratios(used: Dict[int, float], capacity: Dict[int, float]) -> Dict[int, float]:
    """Per-OSD fill ratios for OSDs with capacity > 0."""
    return {
        osd: utilization(used.get(osd, 0.0), cap)
        for osd, cap in capacity.items()
        if cap > 0
    }


def within_utilization_deviation(
    used: Dict[int, float],
    capacity: Dict[int, float],
    max_deviation: float,
    deviation_mode: str = DEVIATION_MEAN,
) -> bool:
    """
    True when fill ratios already satisfy the configured stop gate.

    mean: fullest <= mean + max_deviation
    minmax: (max - min) <= max_deviation
    """
    fills = list(osd_fill_ratios(used, capacity).values())
    if not fills:
        return True
    mode = (deviation_mode or DEVIATION_MEAN).lower()
    if mode == DEVIATION_MINMAX:
        return (max(fills) - min(fills)) <= float(max_deviation)
    mean = float(sum(fills) / len(fills))
    return max(fills) <= mean + float(max_deviation)


def fill_variance(used: Dict[int, float], capacity: Dict[int, float]) -> float:
    """Population variance of per-OSD utilization for OSDs with capacity > 0."""
    fills = [
        utilization(used.get(osd, 0.0), cap)
        for osd, cap in capacity.items()
        if cap > 0
    ]
    if len(fills) < 2:
        return 0.0
    return float(statistics.pvariance(fills))


def shard_bytes_for_pg(num_bytes: float, pool_size: int, raw_used_rate: float) -> float:
    """
    Approximate physical bytes stored on each OSD in the PG's up set.

    For replicated pools raw_used_rate == size, so this equals num_bytes.
    For EC pools this is roughly num_bytes / k.
    """
    if pool_size <= 0:
        return float(num_bytes)
    return float(num_bytes) * float(raw_used_rate) / float(pool_size)


def pool_compression_shard_scale(
    sum_num_bytes: float,
    data_bytes_used: float,
    compress_under_bytes: float,
    blowup: float,
) -> float:
    """
    JJ-style per-pool scale so logical PG num_bytes track on-disk usage after BlueStore compression.

    When compress_under_bytes and data_bytes_used are present and positive, return
    data_bytes_used / (sum_num_bytes * blowup). Otherwise return 1.0 (no scaling).
    """
    if (float(compress_under_bytes) <= 0.0
            or float(data_bytes_used) <= 0.0
            or float(sum_num_bytes) <= 0.0
            or float(blowup) <= 0.0):
        return 1.0
    scale = float(data_bytes_used) / (float(sum_num_bytes) * float(blowup))
    # Keep estimates finite and positive; refuse pathological ratios.
    if not math.isfinite(scale) or scale <= 0.0:
        return 1.0
    return scale


def sum_num_bytes_by_pool(
    pg_stat: Dict[str, Dict[str, Any]],
    active_clean_pgids: Optional[Iterable[str]] = None,
) -> Dict[int, float]:
    """Sum logical PG num_bytes per pool_id (optionally restricted to active+clean PGs)."""
    allow = set(active_clean_pgids) if active_clean_pgids is not None else None
    totals: Dict[int, float] = {}
    for pgid, stat in pg_stat.items():
        if allow is not None and pgid not in allow:
            continue
        try:
            pool_id, _ps = parse_pgid(pgid)
        except ValueError:
            continue
        totals[pool_id] = totals.get(pool_id, 0.0) + float(
            (stat or {}).get('num_bytes', 0) or 0)
    return totals


def parse_pgid(pgid: str) -> Tuple[int, int]:
    """Parse 'pool.ps' hex seed into (pool_id, ps)."""
    pool_s, seed_s = pgid.split('.', 1)
    return int(pool_s), int(seed_s, 16)


def remap_pairs(orig: Sequence[int], out: Sequence[int]) -> List[Tuple[int, int]]:
    """Build from->to pairs for positions that differ between orig and out."""
    return [(a, b) for a, b in zip(orig, out) if a != b]


def apply_move(used: Dict[int, float], from_osd: int, to_osd: int, bytes_: float) -> Dict[int, float]:
    """Return a shallow-copied used map after moving bytes_ from from_osd to to_osd."""
    next_used = dict(used)
    next_used[from_osd] = next_used.get(from_osd, 0.0) - bytes_
    next_used[to_osd] = next_used.get(to_osd, 0.0) + bytes_
    return next_used


def rank_osds_by_utilization(
    used: Dict[int, float],
    capacity: Dict[int, float],
) -> Tuple[List[int], List[int]]:
    """
    Return (overfull_desc, underfull_asc) OSD id lists by fill ratio.

    Only OSDs present in capacity with capacity > 0 are considered.
    """
    ranked = sorted(
        (osd for osd, cap in capacity.items() if cap > 0),
        key=lambda o: utilization(used.get(o, 0.0), capacity[o]),
    )
    # emptiest first / fullest last
    underfull_asc = list(ranked)
    overfull_desc = list(reversed(ranked))
    return overfull_desc, underfull_asc


def existing_upmap_items_from_dump(osdmap_dump: Dict[str, Any]) -> Dict[str, List[List[int]]]:
    """Extract pgid -> [[from, to], ...] from an osdmap dump."""
    out: Dict[str, List[List[int]]] = {}
    for item in osdmap_dump.get('pg_upmap_items', []) or []:
        pgid = item.get('pgid')
        if not pgid:
            continue
        pairs = []
        for m in item.get('mappings', []) or []:
            pairs.append([int(m['from']), int(m['to'])])
        out[str(pgid)] = pairs
    return out


def plan_utilization_upmaps(
    *,
    used: Dict[int, float],
    capacity: Dict[int, float],
    # pgid -> {osds, bytes, pool_id, ps}
    pg_shards: Dict[str, Dict[str, Any]],
    try_pg_upmap: TryPgUpmapFn,
    existing_items: Optional[Dict[str, List[List[int]]]] = None,
    max_optimizations: int = 10,
    max_deviation: float = 0.02,
    deviation_mode: str = DEVIATION_MEAN,
) -> Dict[str, List[List[int]]]:
    """
    Greedy global planner: move large shards off fullest OSDs when variance drops.

    max_deviation is absolute fill-ratio slack (e.g. 0.02 = 2%).
    deviation_mode 'mean': stop when fullest <= mean + max_deviation.
    deviation_mode 'minmax': stop when (max - min) <= max_deviation.

    Returns pgid -> full remapping-pair list to write into OSDMapIncremental.
    """
    if max_optimizations <= 0 or not capacity:
        return {}

    mode = (deviation_mode or DEVIATION_MEAN).lower()
    if mode not in (DEVIATION_MEAN, DEVIATION_MINMAX):
        mode = DEVIATION_MEAN

    working_used = dict(used)
    pending: Dict[str, List[List[int]]] = {}
    existing = dict(existing_items or {})
    skipped: set = set()

    def mean_fill() -> float:
        fills = list(osd_fill_ratios(working_used, capacity).values())
        return float(sum(fills) / len(fills)) if fills else 0.0

    for _ in range(int(max_optimizations)):
        overfull_desc, underfull_asc = rank_osds_by_utilization(working_used, capacity)
        if not overfull_desc or not underfull_asc:
            break

        if within_utilization_deviation(
                working_used, capacity, max_deviation, mode):
            break

        mean = mean_fill()
        baseline = fill_variance(working_used, capacity)
        made_progress = False

        for over_osd in overfull_desc:
            # mean mode: only touch OSDs still above mean + slack.
            if mode == DEVIATION_MEAN:
                if utilization(working_used.get(over_osd, 0.0),
                               capacity[over_osd]) <= mean + max_deviation:
                    break
            elif within_utilization_deviation(
                    working_used, capacity, max_deviation, mode):
                break

            # Candidate PGs on this OSD, largest shard first.
            candidates = []
            for pgid, info in pg_shards.items():
                if pgid in skipped or pgid in pending:
                    continue
                osds = [int(o) for o in info['osds']]
                if over_osd not in osds:
                    continue
                candidates.append((float(info['bytes']), pgid, info))
            candidates.sort(key=lambda t: t[0], reverse=True)

            # Prefer emptier destinations; more_underfull is a secondary list (same idea as calc_pg_upmaps).
            if mode == DEVIATION_MINMAX:
                under_pref = [
                    o for o in underfull_asc
                    if o != over_osd
                ]
            else:
                under_pref = [
                    o for o in underfull_asc
                    if o != over_osd and utilization(
                        working_used.get(o, 0.0), capacity[o]) < mean
                ]
            under_pref_set = set(under_pref)
            more_pref = [
                o for o in underfull_asc
                if o != over_osd and o not in under_pref_set
            ]

            for bytes_, pgid, info in candidates:
                if bytes_ <= 0:
                    continue
                pool_id = int(info['pool_id'])
                ps = int(info['ps'])
                pg_osds = {int(o) for o in info['osds']}
                underfull = [o for o in under_pref if o not in pg_osds]
                more_underfull = [o for o in more_pref if o not in pg_osds]
                if not underfull and not more_underfull:
                    continue

                result = try_pg_upmap(
                    pool_id, ps, [over_osd], underfull,
                    more_underfull if more_underfull else None)
                if not result:
                    continue
                orig = list(result['orig'])
                out = list(result['out'])
                pairs = remap_pairs(orig, out)
                if not pairs:
                    continue

                # Prefer the pair that evacuates the overfull OSD.
                chosen = next((p for p in pairs if p[0] == over_osd), pairs[0])
                from_osd, to_osd = chosen
                if to_osd in pg_osds and to_osd != from_osd:
                    # Destination already holds this PG (should be filtered).
                    continue

                next_used = apply_move(working_used, from_osd, to_osd, bytes_)
                if fill_variance(next_used, capacity) >= baseline:
                    continue

                # Merge with any existing / already-planned items for this PG.
                base_items = [list(p) for p in existing.get(pgid, [])]
                if pgid in pending:
                    base_items = [list(p) for p in pending[pgid]]
                # Drop prior pairs involving either endpoint, then append.
                merged = [
                    p for p in base_items
                    if p[0] not in (from_osd, to_osd) and p[1] not in (from_osd, to_osd)
                ]
                merged.append([from_osd, to_osd])
                pending[pgid] = merged
                working_used = next_used
                skipped.add(pgid)
                made_progress = True
                break

            if made_progress:
                break

        if not made_progress:
            break

    return pending


def build_pg_shards(
    pg_up: Dict[str, Iterable[int]],
    pg_stat: Dict[str, Dict[str, Any]],
    pool_info_by_id: Dict[int, Dict[str, Any]],
    pool_raw_used_rate: Callable[[int], float],
    active_clean_pgids: Optional[Iterable[str]] = None,
    shard_scale_by_pool: Optional[Dict[int, float]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Build planner pg_shards map from MappingState-like inputs.

    pool_info_by_id values need at least {'pool': id, 'size': N}.
    shard_scale_by_pool optionally multiplies per-PG shard bytes (BlueStore compression).
    """
    allow = set(active_clean_pgids) if active_clean_pgids is not None else None
    scales = shard_scale_by_pool or {}
    out: Dict[str, Dict[str, Any]] = {}
    for pgid, up in pg_up.items():
        if allow is not None and pgid not in allow:
            continue
        try:
            pool_id, ps = parse_pgid(pgid)
        except ValueError:
            continue
        pi = pool_info_by_id.get(pool_id)
        if not pi:
            continue
        stat = pg_stat.get(pgid) or {}
        num_bytes = float(stat.get('num_bytes', 0) or 0)
        size = int(pi.get('size', 0) or 0)
        rate = float(pool_raw_used_rate(pool_id))
        scale = float(scales.get(pool_id, 1.0))
        out[pgid] = {
            'osds': [int(o) for o in up],
            'bytes': shard_bytes_for_pg(num_bytes, size, rate) * scale,
            'pool_id': pool_id,
            'ps': ps,
        }
    return out
