#!/usr/bin/env python3

"""Report LBA conflict % and mergeability breakdown from a store-bench --track-metrics run."""

import json
import sys


LEAF = "ext=LADDR_LEAF"
INTERNAL = "ext=LADDR_INTERNAL"
LBA_TREE = "tree=LBA"


def metric(metrics, prefix, ext):
    """Return the value of a metric matching prefix and extent label."""
    return sum(
        value
        for key, value in metrics.items()
        if key.startswith(prefix) and ext in key
    )


def get_conflicts(metrics):
    """Get total LBA conflicts split by extent."""
    leaf = metric(
        metrics,
        "cache_trans_invalidated_by_extent",
        LEAF,
    )
    internal = metric(
        metrics,
        "cache_trans_invalidated_by_extent",
        INTERNAL,
    )

    return leaf, internal


def get_category(metrics, prefix):
    """Get mergeable/overlapping counts for leaf and internal nodes."""
    leaf_mergeable = metric(metrics, f"{prefix}_mergeable", LEAF)
    leaf_overlapping = metric(metrics, f"{prefix}_overlapping", LEAF)

    internal_mergeable = metric(metrics, f"{prefix}_mergeable", INTERNAL)
    internal_overlapping = metric(metrics, f"{prefix}_overlapping", INTERNAL)

    return {
        "leaf_mergeable": leaf_mergeable,
        "leaf_overlapping": leaf_overlapping,
        "internal_mergeable": internal_mergeable,
        "internal_overlapping": internal_overlapping,
        "mergeable": leaf_mergeable + internal_mergeable,
        "overlapping": leaf_overlapping + internal_overlapping,
    }


def get_tree_shape(metrics):
    """Get LBA tree depth and node count for one shard."""
    depth = metric(metrics, "cache_tree_depth", LBA_TREE)
    nodes = metric(metrics, "cache_tree_extents_num", LBA_TREE)
    return depth, nodes


def cell(counts, side):
    """Format a category's mergeable/overlapping counts for one side as 'merge/over'."""
    return f"{counts[f'{side}_mergeable']}/{counts[f'{side}_overlapping']}"


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <store-bench-output.json>")

    with open(sys.argv[1]) as f:
        data = json.load(f)

    total_ios = 0
    total_lba_conflicts = 0
    total_split = {"mergeable": 0, "overlapping": 0}
    max_tree_depth = 0
    total_tree_nodes = 0

    print(f'{"shard":<7}{"ios":<10}{"LBA %":<9}'
          f'{"ii_leaf":<12}{"ii_internal":<14}{"it_leaf":<12}{"it_internal":<12}')

    for result in data["results"]:
        shard = result["shard"]
        ios = result["ios_completed"]
        metrics = result["track_metrics"]

        leaf_conflicts, internal_conflicts = get_conflicts(metrics)
        lba_conflicts = leaf_conflicts + internal_conflicts
        pct = 100 * lba_conflicts / ios if ios else 0

        insert = get_category(metrics, "cache_lba_conflicts")
        traversal = get_category(metrics, "cache_lba_traversal_conflicts")
        split = get_category(metrics, "cache_lba_retire_conflicts")
        tree_depth, tree_nodes = get_tree_shape(metrics)

        total_ios += ios
        total_lba_conflicts += lba_conflicts
        total_split["mergeable"] += split["mergeable"]
        total_split["overlapping"] += split["overlapping"]
        max_tree_depth = max(max_tree_depth, tree_depth)
        total_tree_nodes += tree_nodes

        print(f'{shard:<7}{ios:<10}{pct:<9.3f}'
              f'{cell(insert, "leaf"):<12}{cell(insert, "internal"):<14}'
              f'{cell(traversal, "leaf"):<12}{cell(traversal, "internal"):<12}')

    pct = 100 * total_lba_conflicts / total_ios if total_ios else 0
    print()
    print(f"LBA conflicts: {total_lba_conflicts} / {total_ios} ios ({pct:.3f}%)")
    print(f"split case (leaf+internal, not shown in table above):"
          f" mergeable={total_split['mergeable']}, non-mergeable={total_split['overlapping']}"
          f" -- already counted in the total LBA conflicts above, not on top of it")
    print(f"LBA tree depth (max across shards): {max_tree_depth},"
          f" LBA tree nodes (summed across shards): {total_tree_nodes}")


if __name__ == "__main__":
    main()