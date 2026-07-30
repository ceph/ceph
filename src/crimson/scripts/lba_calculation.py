#!/usr/bin/env python3
"""
Aggregates LBA-tree conflicts (cache_trans_invalidated_by_extent for
ext=LADDR_INTERNAL/LADDR_LEAF) from a crimson-store-bench --track-metrics
JSON result and reports them as a percentage of ios_completed, per shard
and overall.

Breaks the LADDR_LEAF conflicts down by mechanism:
  - "leaf insert" (non-split direct edit, cache_lba_conflicts_mergeable/
    overlapping): the committer's own edited leaf copy is still alive when
    the conflict is detected, so the disjoint-key check runs inline.
  - "leaf split" (cache_lba_retire_conflicts_mergeable/overlapping): the
    leaf got retired because the committer's own edit caused a split, so
    both sides' touched keys have to be snapshotted ahead of time (see
    capture_retired_leaf_keys in cache.cc) before being compared the same
    way.

Also breaks out LADDR_INTERNAL conflicts (cache_lba_conflicts_mergeable/
overlapping on the internal node itself -- e.g. two transactions both
writing the routing entry for the same child boundary, not a data
collision, but the same disjoint-key check applies one level up).

Everything left over after that (true read-only conflicts, where the
other transaction never edited the node at all) falls into "read
conflicts" below -- a residual, not a directly-measured bucket.

How to generate the input JSON:

  1. Rebuild after any cache.cc/cache.h changes:
       ninja bin/crimson-store-bench

  2. Reset the store-bench scratch directory before each run:
       ./reset_store_bench_dir.sh

  3. Run the benchmark with the relevant metrics tracked. random_write is
     the workload that actually exercises LBA-tree conflicts:
       ./build/bin/crimson-store-bench --store-path store_bench_dir --smp 4 \
         --duration 10 --work-load-type random_write \
         --seastore_device_size 10G --track-metrics \
         "cache_trans_invalidated_by_extent,cache_lba_conflicts_mergeable,\
cache_lba_conflicts_overlapping,cache_lba_retire_conflicts_mergeable,\
cache_lba_retire_conflicts_overlapping" \
         > run.json

  4. Feed the JSON into this script:
       python3 src/crimson/scripts/lba_calculation.py run.json

Usage: ./lba_calculation.py <store-bench-output.json>
"""
import json
import sys

LBA_EXT_LABELS = ("ext=LADDR_INTERNAL", "ext=LADDR_LEAF")
LEAF_EXT_LABEL = "ext=LADDR_LEAF"
INTERNAL_EXT_LABEL = "ext=LADDR_INTERNAL"


def lba_conflicts(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_trans_invalidated_by_extent")
        and any(label in k for label in LBA_EXT_LABELS)
    )


def leaf_insert_mergeable(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_lba_conflicts_mergeable") and LEAF_EXT_LABEL in k
    )


def leaf_insert_overlapping(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_lba_conflicts_overlapping") and LEAF_EXT_LABEL in k
    )


def leaf_split_mergeable(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_lba_retire_conflicts_mergeable") and LEAF_EXT_LABEL in k
    )


def leaf_split_overlapping(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_lba_retire_conflicts_overlapping") and LEAF_EXT_LABEL in k
    )


def internal_mergeable(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_lba_conflicts_mergeable") and INTERNAL_EXT_LABEL in k
    )


def internal_overlapping(track_metrics):
    return sum(
        v for k, v in track_metrics.items()
        if k.startswith("cache_lba_conflicts_overlapping") and INTERNAL_EXT_LABEL in k
    )


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <store-bench-output.json>")

    with open(sys.argv[1]) as f:
        data = json.load(f)

    total_ios = 0
    total_conflicts = 0
    total_insert_mergeable = 0
    total_insert_overlapping = 0
    total_split_mergeable = 0
    total_split_overlapping = 0
    total_internal_mergeable = 0
    total_internal_overlapping = 0

    print(f'{"shard":<6}{"ios_completed":<15}{"LBA conflicts":<15}{"pct":<8}'
          f'{"ins_merge":<11}{"ins_overlap":<13}'
          f'{"split_merge":<13}{"split_overlap":<15}'
          f'{"int_merge":<11}{"int_overlap":<13}')
    for result in data["results"]:
        ios = result["ios_completed"]
        metrics = result["track_metrics"]
        conflicts = lba_conflicts(metrics)
        ins_mergeable = leaf_insert_mergeable(metrics)
        ins_overlapping = leaf_insert_overlapping(metrics)
        split_mergeable = leaf_split_mergeable(metrics)
        split_overlapping = leaf_split_overlapping(metrics)
        int_mergeable = internal_mergeable(metrics)
        int_overlapping = internal_overlapping(metrics)
        total_ios += ios
        total_conflicts += conflicts
        total_insert_mergeable += ins_mergeable
        total_insert_overlapping += ins_overlapping
        total_split_mergeable += split_mergeable
        total_split_overlapping += split_overlapping
        total_internal_mergeable += int_mergeable
        total_internal_overlapping += int_overlapping
        pct = 100 * conflicts / ios if ios else 0
        print(f'{result["shard"]:<6}{ios:<15}{conflicts:<15}{pct:<8.3f}'
              f'{ins_mergeable:<11}{ins_overlapping:<13}'
              f'{split_mergeable:<13}{split_overlapping:<15}'
              f'{int_mergeable:<11}{int_overlapping:<13}')

    print("---")
    print(f"total ios_completed: {total_ios}")
    print(f"total LBA-caused conflicts (LADDR_INTERNAL + LADDR_LEAF): {total_conflicts}")
    if total_ios:
        print(f"overall conflict rate: {100 * total_conflicts / total_ios:.3f}%")

    print(f"leaf insert (non-split) conflicts: "
          f"mergeable={total_insert_mergeable}, non-mergeable={total_insert_overlapping}")
    print(f"leaf split (retire) conflicts: "
          f"mergeable={total_split_mergeable}, non-mergeable={total_split_overlapping}")
    print(f"internal node conflicts: "
          f"mergeable={total_internal_mergeable}, non-mergeable={total_internal_overlapping}")

    classified = (total_insert_mergeable + total_insert_overlapping
                  + total_split_mergeable + total_split_overlapping
                  + total_internal_mergeable + total_internal_overlapping)
    read_conflicts = total_conflicts - classified
    print(f"read conflicts (true read-only -- other transaction never edited "
          f"the node at all): {read_conflicts}")

    moderate_mergeable = total_insert_mergeable + total_split_mergeable
    if total_conflicts:
        print(f"proportion of all LBA conflicts mergeable via a moderate fix "
              f"(leaf insert + leaf split, no full overhaul needed): "
              f"{100 * moderate_mergeable / total_conflicts:.3f}%")


if __name__ == "__main__":
    main()
