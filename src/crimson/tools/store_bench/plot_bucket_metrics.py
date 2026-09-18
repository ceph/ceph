#!/usr/bin/env python3
"""
Turn the bucketed CSV output produced by store-bench.cc's --csv-output flag
into interactive HTML plots (hover to see raw values), using Plotly.

CSV columns: shard,bucket_index,ios_completed,avg_latency_s,<tracked metric>...

For each tracked metric, compares it (min-max normalized, one axis) against
a base metric (avg_latency_s or ios_completed), per shard, over buckets.
Produces two self-contained HTML files: one comparing avg_latency_s, one
comparing ios_completed.

Grid layout: rows = tracked metric names, columns = shards.
"""

import argparse
import csv
import os

import plotly.graph_objects as go
from plotly.subplots import make_subplots

BASE_METRICS = ("avg_latency_s", "ios_completed")
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
    return [name for name in rows[0].keys() if name not in ("shard", "bucket_index")]


def get_tracked_metric_names(rows):
    return [name for name in get_unique_metric_names(rows) if name not in BASE_METRICS]


def get_data_for_metric(shard_rows, metric_name):
    shard_rows = sorted(shard_rows, key=lambda row: int(row["bucket_index"]))
    x = [int(row["bucket_index"]) for row in shard_rows]
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
            fig.update_xaxes(title_text="bucket_index", row=metric_row, col=shard_col)

    fig.update_layout(
        height=300 * len(metric_names), width=350 * len(shards),
        title=f"{base_metric_name} vs tracked metrics (normalized)",
        hovermode="x unified",
    )
    fig.write_html(output_path, include_plotlyjs="inline")
    print(f"wrote {output_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", help="path to the bucketed CSV file")
    parser.add_argument(
        "-o", "--output-prefix", default=os.path.join(SCRIPT_DIR, "bucket_metrics"),
        help="prefix for the output HTML files (default: bucket_metrics, written "
             "next to this script)",
    )
    args = parser.parse_args()

    rows = read_csv(args.csv_path)
    plot_comparison_grid(rows, "avg_latency_s", f"{args.output_prefix}_latency.html")
    plot_comparison_grid(rows, "ios_completed", f"{args.output_prefix}_ios.html")


if __name__ == "__main__":
    main()
