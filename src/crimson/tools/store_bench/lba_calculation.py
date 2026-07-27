#!/usr/bin/env python3
"""
Aggregates LBA-tree conflicts (cache_trans_invalidated_by_extent for
ext=LADDR_INTERNAL/LADDR_LEAF) from a crimson-store-bench --track-metrics
JSON result and reports them as a percentage of ios_completed, per shard
and overall.

Also aggregates cache_lba_conflicts_mergeable/cache_lba_conflicts_overlapping
(step 2 of the LBA no_conflict_publish investigation -- of the LBA conflicts
above, how many were mutate-vs-mutate conflicts on disjoint (mergeable) vs
overlapping keys; read-only conflicts aren't counted in either and make up
the rest of the total LBA conflict count).

Usage: ./lba_calculation.py <store-bench-output.json>
"""
import json
import sys

LBA_EXT_LABELS = ("ext=LADDR_INTERNAL", "ext=LADDR_LEAF")


def lba_conflicts(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_trans_invalidated_by_extent")
        and any(label in k for label in LBA_EXT_LABELS)
    )


def lba_conflicts_mergeable(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_lba_conflicts_mergeable")
    )


def lba_conflicts_overlapping(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_lba_conflicts_overlapping")
    )


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <store-bench-output.json>")

    with open(sys.argv[1]) as f:
        data = json.load(f)

    total_ios = 0
    total_conflicts = 0
    total_mergeable = 0
    total_overlapping = 0

    print(f'{"shard":<6}{"ios_completed":<15}{"LBA conflicts":<15}{"pct":<8}'
          f'{"mergeable":<11}{"overlapping":<13}')
    for result in data["results"]:
        ios = result["ios_completed"]
        metrics = result["track_metrics"]
        conflicts = lba_conflicts(metrics)
        mergeable = lba_conflicts_mergeable(metrics)
        overlapping = lba_conflicts_overlapping(metrics)
        total_ios += ios
        total_conflicts += conflicts
        total_mergeable += mergeable
        total_overlapping += overlapping
        pct = 100 * conflicts / ios if ios else 0
        print(f'{result["shard"]:<6}{ios:<15}{conflicts:<15}{pct:<8.3f}'
              f'{mergeable:<11}{overlapping:<13}')

    print("---")
    print(f"total ios_completed: {total_ios}")
    print(f"total LBA-caused conflicts (LADDR_INTERNAL + LADDR_LEAF): {total_conflicts}")
    if total_ios:
        print(f"overall conflict rate: {100 * total_conflicts / total_ios:.3f}%")

    classified = total_mergeable + total_overlapping
    read_only = total_conflicts - classified
    print(f"mutate-vs-mutate conflicts (classified): {classified} "
          f"(mergeable={total_mergeable}, overlapping={total_overlapping})")
    print(f"read-only conflicts (not classified, out of scope): {read_only}")
    if classified:
        print(f"mergeable % of classified conflicts: "
              f"{100 * total_mergeable / classified:.3f}%")


if __name__ == "__main__":
    main()
