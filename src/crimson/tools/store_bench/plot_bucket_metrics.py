#!/usr/bin/env python3
"""
Turn the bucketed CSV output produced by store-bench.cc's --csv-output flag
into interactive HTML plots (hover to see raw values), using Plotly.

CSV columns: shard,bucket_index,time_s,ios_completed,avg_latency_s,<tracked metric>...
(time_s is bucket_index * --bucket-sample-period seconds; older CSVs without it fall
back to plotting bucket_index directly.)

A tracked metric name can expand into many columns, one per label-instance
(e.g. --track-metrics cache_trans_invalidated_by_extent produces columns like
"cache_trans_invalidated_by_extent{ext=LADDR_LEAF,shard=0,src=MUTATE}", one
per ext/shard/src combination). Before plotting, all columns sharing the same
prefix before "{" are summed together into a single column per bucket/shard,
so the rest of this script only ever sees one series per base metric name.

For each tracked metric, compares it (min-max normalized, one axis) against
a base metric (avg_latency_s or ios_completed), per shard, over time.
Produces two self-contained HTML files: one comparing avg_latency_s, one
comparing ios_completed.

Grid layout: rows = tracked metric names, columns = shards.

Also produces one aggregate-over-shards HTML file: one subplot per metric
(ios_completed summed across shards, avg_latency_s averaged across shards
weighted by ios_completed, tracked metrics averaged across shards), over time.

Optionally (--raw-latency-prefix), also reads the raw per-op (elapsed_s,
latency_s) samples written by store-bench.cc's --raw-conflict-csv flag
(one file per shard: <prefix>.shard0, <prefix>.shard1, ...), pools every
shard's samples together, and plots an interactive latency percentile
distribution (empirical CDF) for the whole run.
"""

import argparse
import csv
import glob
import os

import plotly.graph_objects as go
from plotly.subplots import make_subplots

BASE_METRICS = ("avg_latency_s", "ios_completed")
NON_METRIC_COLUMNS = ("shard", "bucket_index", "time_s")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def read_csv(path):
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def get_rows_for_shard(rows, shard):
    return [row for row in rows if row["shard"] == shard]


def get_unique_shards(rows):
    return sorted({row["shard"] for row in rows}, key=int)


def get_unique_metric_names(rows):
    if not rows:
        return []
    return [name for name in rows[0].keys() if name not in NON_METRIC_COLUMNS]


def get_tracked_metric_names(rows):
    return [name for name in get_unique_metric_names(rows) if name not in BASE_METRICS]


def get_base_metric_name(column_name):
    return column_name.split("{", 1)[0]


def collapse_metric_groups(rows):
    """
    Sum every tracked-metric column sharing the same base name (the part
    before "{") into one column per base name, per row. A metric requested
    via --track-metrics can expand into one CSV column per label-instance
    (e.g. one per ext/shard/src combination), and plotting hundreds of those
    individually isn't useful -- summing them gives one series per base
    metric name instead.
    """
    if not rows:
        return rows

    groups = {}
    for name in get_tracked_metric_names(rows):
        groups.setdefault(get_base_metric_name(name), []).append(name)

    collapsed = []
    for row in rows:
        new_row = {col: row[col] for col in NON_METRIC_COLUMNS + BASE_METRICS if col in row}
        for base_name, columns in groups.items():
            present = [row[col] for col in columns if row[col] != ""]
            new_row[base_name] = str(sum(float(v) for v in present)) if present else ""
        collapsed.append(new_row)
    return collapsed


def has_time_column(rows):
    return bool(rows) and "time_s" in rows[0]


def get_x_value(row):
    if "time_s" in row and row["time_s"] != "":
        return float(row["time_s"])
    return float(row["bucket_index"])


def get_x_axis_title(rows):
    return "time (s)" if has_time_column(rows) else "bucket_index"


def get_data_for_metric(shard_rows, metric_name):
    shard_rows = sorted(shard_rows, key=lambda row: int(row["bucket_index"]))
    x = [get_x_value(row) for row in shard_rows]
    y = [float(row[metric_name]) if row[metric_name] != "" else float("nan")
         for row in shard_rows]
    return x, y


def normalize(values):
    finite = [v for v in values if v == v]  # drop NaNs
    if not finite:
        return values
    lo, hi = min(finite), max(finite)
    if hi == lo:
        return [0.5 if v == v else v for v in values]
    return [(v - lo) / (hi - lo) if v == v else v for v in values]


def plot_comparison_grid(rows, base_metric_name, output_path):
    shards = get_unique_shards(rows)
    metric_names = get_tracked_metric_names(rows)
    x_axis_title = get_x_axis_title(rows)

    fig = make_subplots(
        rows=len(metric_names), cols=len(shards),
        subplot_titles=[f"shard {shard}" for shard in shards] * len(metric_names),
        shared_xaxes=True,
    )

    for metric_row, metric_name in enumerate(metric_names, start=1):
        for shard_col, shard in enumerate(shards, start=1):
            shard_rows = get_rows_for_shard(rows, shard)
            x, base_y = get_data_for_metric(shard_rows, base_metric_name)
            _, metric_y = get_data_for_metric(shard_rows, metric_name)
            show_legend = metric_row == 1 and shard_col == 1

            fig.add_trace(
                go.Scatter(
                    x=x, y=normalize(base_y), mode="lines+markers",
                    name=base_metric_name, legendgroup=base_metric_name,
                    showlegend=show_legend, marker_color="#2a78d6",
                    customdata=base_y,
                    hovertemplate=f"bucket %{{x}}<br>{base_metric_name}: %{{customdata:.6g}}<extra></extra>",
                ),
                row=metric_row, col=shard_col,
            )
            fig.add_trace(
                go.Scatter(
                    x=x, y=normalize(metric_y), mode="lines+markers",
                    name=metric_name, legendgroup=metric_name,
                    showlegend=show_legend, marker_color="#1baf7a",
                    customdata=metric_y,
                    hovertemplate=f"bucket %{{x}}<br>{metric_name}: %{{customdata:.6g}}<extra></extra>",
                ),
                row=metric_row, col=shard_col,
            )
            fig.update_yaxes(title_text=metric_name if shard_col == 1 else None,
                              row=metric_row, col=shard_col)
            fig.update_xaxes(title_text=x_axis_title, row=metric_row, col=shard_col)

    fig.update_layout(
        height=300 * len(metric_names), width=350 * len(shards),
        title=f"{base_metric_name} vs tracked metrics (normalized)",
        hovermode="x unified",
    )
    fig.write_html(output_path, include_plotlyjs="inline")
    print(f"wrote {output_path}")


def aggregate_across_shards(rows):
    """
    Combine all shards' rows into one series per bucket: ios_completed summed
    (it's a per-shard count), avg_latency_s weighted by each shard's
    ios_completed (so idle shards don't skew the average), and tracked
    metrics averaged (they're gauges, not counts).
    """
    metric_names = get_unique_metric_names(rows)
    buckets = sorted({int(row["bucket_index"]) for row in rows})
    x = []
    agg = {name: [] for name in metric_names}
    for b in buckets:
        bucket_rows = [row for row in rows if int(row["bucket_index"]) == b]
        x.append(get_x_value(bucket_rows[0]))
        total_ios = sum(int(row["ios_completed"]) for row in bucket_rows)
        agg["ios_completed"].append(total_ios)
        if total_ios > 0:
            weighted_latency = sum(
                float(row["avg_latency_s"]) * int(row["ios_completed"])
                for row in bucket_rows
            ) / total_ios
        else:
            weighted_latency = float("nan")
        agg["avg_latency_s"].append(weighted_latency)
        for name in metric_names:
            if name in BASE_METRICS:
                continue
            vals = [float(row[name]) for row in bucket_rows if row[name] != ""]
            agg[name].append(sum(vals) / len(vals) if vals else float("nan"))
    return x, agg


def plot_aggregate(rows, output_path):
    x_axis_title = get_x_axis_title(rows)
    x, agg = aggregate_across_shards(rows)
    ordered_names = ["ios_completed", "avg_latency_s"] + get_tracked_metric_names(rows)

    fig = make_subplots(
        rows=len(ordered_names), cols=1, shared_xaxes=True,
        subplot_titles=[f"{name} (all shards)" for name in ordered_names],
    )
    for i, name in enumerate(ordered_names, start=1):
        fig.add_trace(
            go.Scatter(
                x=x, y=agg[name], mode="lines+markers", name=name,
                showlegend=False,
                hovertemplate=f"{x_axis_title} %{{x}}<br>{name}: %{{y:.6g}}<extra></extra>",
            ),
            row=i, col=1,
        )
        fig.update_yaxes(title_text=name, row=i, col=1)
    fig.update_xaxes(title_text=x_axis_title, row=len(ordered_names), col=1)

    fig.update_layout(
        height=250 * len(ordered_names), width=900,
        title="Aggregate metrics across all shards",
        hovermode="x unified",
    )
    fig.write_html(output_path, include_plotlyjs="inline")
    print(f"wrote {output_path}")


PERCENTILE_MARKERS = (50, 90, 95, 99, 99.9)


def discover_shard_files(prefix):
    """Find <prefix>.shard<N> files written by --raw-conflict-csv."""
    paths = []
    for path in glob.glob(f"{prefix}.shard*"):
        suffix = path[len(prefix) + len(".shard"):]
        if suffix.isdigit():
            paths.append(path)
    return paths


def read_raw_samples(paths):
    """
    Pool every shard file's (latency_s, ever_lba_conflicted) columns into one
    flat list of (latency_s, conflicted) pairs. Older raw files written before
    the ever_lba_conflicted column existed treat every sample as unconflicted.
    """
    samples = []
    for path in paths:
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                conflicted = row.get("ever_lba_conflicted", "0") == "1"
                samples.append((float(row["latency_s"]), conflicted))
    return samples


def percentile(sorted_values, p):
    if not sorted_values:
        return float("nan")
    idx = max(0, min(len(sorted_values) - 1, round(p / 100.0 * len(sorted_values)) - 1))
    return sorted_values[idx]


def cdf_trace(latencies_s, name, color=None):
    latencies_s = sorted(latencies_s)
    n = len(latencies_s)
    if n == 0:
        return None
    latencies_ms = [v * 1000.0 for v in latencies_s]
    ranks = [100.0 * (i + 1) / n for i in range(n)]
    return go.Scatter(
        x=latencies_ms, y=ranks, mode="lines", name=f"{name} ({n})",
        line=dict(color=color) if color else None,
        hovertemplate=f"{name}<br>latency %{{x:.6g}} ms<br>percentile %{{y:.2f}}<extra></extra>",
    )


def plot_latency_distribution(raw_latency_prefix, output_path):
    paths = discover_shard_files(raw_latency_prefix)
    if not paths:
        raise SystemExit(f"no shard files found matching {raw_latency_prefix}.shard*")

    samples = read_raw_samples(paths)
    n = len(samples)
    latencies_s = sorted(latency for latency, _ in samples)
    conflicted_s = [latency for latency, conflicted in samples if conflicted]
    not_conflicted_s = [latency for latency, conflicted in samples if not conflicted]

    fig = go.Figure()
    fig.add_trace(cdf_trace(latencies_s, "all shards (pooled)", "#2a78d6"))
    conflicted_trace = cdf_trace(conflicted_s, "LBA-conflicted (retried)", "#d64545")
    if conflicted_trace:
        fig.add_trace(conflicted_trace)
    not_conflicted_trace = cdf_trace(not_conflicted_s, "not conflicted", "#1baf7a")
    if not_conflicted_trace:
        fig.add_trace(not_conflicted_trace)

    for p in PERCENTILE_MARKERS:
        fig.add_hline(y=p, line_dash="dot", line_color="gray", opacity=0.5)

    # p90/p95/p99/p99.9 all sit within 10 points of each other on the
    # percentile axis -- too close together for inline line labels to avoid
    # overlapping, so list them as a stacked callout instead.
    for i, p in enumerate(PERCENTILE_MARKERS):
        v_ms = percentile(latencies_s, p) * 1000.0
        fig.add_annotation(
            xref="paper", yref="paper", x=1.02, y=0.98 - i * 0.06,
            xanchor="left", yanchor="top", align="left", showarrow=False,
            text=f"p{p}: {v_ms:.3f} ms", font=dict(size=11),
        )

    # Are LBA-conflicted (retried) IOs overrepresented in the tail? Compare
    # the overall p99 cutoff's conflicted share against the whole run's.
    p99_cutoff_s = percentile(latencies_s, 99)
    tail = [conflicted for latency, conflicted in samples if latency >= p99_cutoff_s]
    overall_conflicted_frac = len(conflicted_s) / n if n else float("nan")
    tail_conflicted_frac = sum(tail) / len(tail) if tail else float("nan")
    print(f"conflicted share overall: {overall_conflicted_frac:.1%}")
    print(f"conflicted share in p99+ tail: {tail_conflicted_frac:.1%} "
          f"({len(tail)} samples at/above p99)")

    fig.update_xaxes(title_text="latency (ms)", type="log")
    fig.update_yaxes(title_text="percentile", range=[0, 100])
    fig.update_layout(
        title=f"Latency distribution, all shards pooled ({n} samples) -- "
              f"conflicted overall {overall_conflicted_frac:.1%}, "
              f"conflicted in p99+ tail {tail_conflicted_frac:.1%}",
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
        height=650, width=1000, margin=dict(r=160, b=100),
    )
    fig.write_html(output_path, include_plotlyjs="inline")
    print(f"wrote {output_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", nargs="?", help="path to the bucketed CSV file")
    parser.add_argument(
        "-o", "--output-prefix", default=os.path.join(SCRIPT_DIR, "bucket_metrics"),
        help="prefix for the output HTML files (default: bucket_metrics, written "
             "next to this script)",
    )
    parser.add_argument(
        "--raw-latency-prefix",
        help="path passed to --raw-conflict-csv; reads <prefix>.shard<N> files "
             "and plots a pooled latency percentile distribution",
    )
    args = parser.parse_args()

    if not args.csv_path and not args.raw_latency_prefix:
        parser.error("must provide csv_path and/or --raw-latency-prefix")

    if args.csv_path:
        rows = collapse_metric_groups(read_csv(args.csv_path))
        plot_comparison_grid(rows, "avg_latency_s", f"{args.output_prefix}_latency.html")
        plot_comparison_grid(rows, "ios_completed", f"{args.output_prefix}_ios.html")
        plot_aggregate(rows, f"{args.output_prefix}_aggregate.html")

    if args.raw_latency_prefix:
        plot_latency_distribution(
            args.raw_latency_prefix, f"{args.output_prefix}_latency_distribution.html")


if __name__ == "__main__":
    main()
