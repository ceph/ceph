#!/usr/bin/env python3
"""
Generate the Grafana dashboard for the crimson OSD Prometheus endpoint
(crimson_prometheus_port_base, see doc/crimson/crimson.rst).

Usage: gen_dashboard.py [output.json]   (default: crimson-osd.json next to this file)

The Prometheus scrape job must add a ceph_daemon="osd.<id>" label to each
target. vstart-monitoring.sh does this.
"""

import json
import os
import sys

DS = {"type": "prometheus", "uid": "${DS}"}
F = '{ceph_daemon=~"$osd"}'
FT = '{ceph_daemon=~"$osd", tail="all"}'
R = "$__rate_interval"

panels = []
y = 0
pid = 0


def row(title):
    global y, pid
    pid += 1
    panels.append({"type": "row", "title": title, "id": pid, "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    y += 1


def place(panel, w, h=8):
    """Put the panel right of the previous one, or on a new line if it does not fit."""
    global y, pid
    pid += 1
    prev = panels[-1] if panels else None
    if prev is None or prev["type"] == "row":
        x = 0
    else:
        g = prev["gridPos"]
        x = g["x"] + g["w"]
        if x + w > 24:
            x = 0
            y += g["h"]
    panel.update({"id": pid, "gridPos": {"h": h, "w": w, "x": x, "y": y}})
    panels.append(panel)


def pad_shard(expr):
    # Pad shard to 2 digits so that the columns sort 00, 01, ... 10, not 0, 1, 10, 2.
    return f'label_replace({expr}, "shard", "0$1", "shard", "^([0-9])$")'


def matrix(title, expr, row_field, col_field, unit, w=24, h=8, desc="",
           lo=0, hi=None, decimals=0):
    """Table with one row per row_field value and one column per col_field value.

    With hi=None, the color scale goes up to the largest cell in the table
    (Grafana uses one min/max for all columns), so shards stay comparable.
    """
    corner = f"{row_field}\\{col_field}"
    limits = {"min": lo} if hi is None else {"min": lo, "max": hi}
    place({
        "type": "table", "title": title, "datasource": DS, "description": desc,
        "targets": [{"datasource": DS, "expr": expr, "refId": "A",
                     "instant": True, "range": False, "format": "table"}],
        "transformations": [{"id": "groupingToMatrix", "options": {
            "rowField": row_field, "columnField": col_field,
            "valueField": "Value", "emptyValue": "null"}}],
        "fieldConfig": {
            "defaults": {
                "unit": unit, "decimals": decimals, **limits,
                "color": {"mode": "continuous-GrYlRd"},
                "custom": {"align": "center",
                           "cellOptions": {"type": "color-background", "mode": "basic"}},
            },
            "overrides": [{
                "matcher": {"id": "byName", "options": corner},
                "properties": [
                    {"id": "displayName", "value": "OSD \\ shard"},
                    {"id": "custom.cellOptions", "value": {"type": "auto"}},
                    {"id": "custom.align", "value": "left"},
                    {"id": "custom.width", "value": 110},
                ]}],
        },
        "options": {"showHeader": True, "cellHeight": "sm",
                    "sortBy": [{"displayName": "OSD \\ shard", "desc": False}]},
    }, w, h)


def ts(title, targets, unit="short", w=12, desc="", stack=False, h=8,
       legend="bottom", repeat=None, max_per_row=2):
    """Time series panel. With repeat=<variable>, Grafana makes one copy of the
    panel for each selected value, max_per_row copies side by side in width w."""
    extra = {"repeat": repeat, "repeatDirection": "h",
             "maxPerRow": max_per_row} if repeat else {}
    place({
        **extra,
        "type": "timeseries", "title": title, "datasource": DS,
        "description": desc,
        "fieldConfig": {"defaults": {"unit": unit, "custom": {
            "lineWidth": 1, "fillOpacity": 10 if stack else 0,
            "stacking": {"mode": "normal" if stack else "none"}}},
            "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": legend,
                               "calcs": ["mean", "max", "lastNotNull"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": [{"datasource": DS, "expr": e, "legendFormat": l,
                     "refId": chr(65 + i)} for i, (e, l) in enumerate(targets)],
    }, w, h)


def end_row():
    global y
    y += panels[-1]["gridPos"]["h"]


# ---------------- Reactor / CPU ----------------
row("Reactor / CPU")
matrix("Reactor utilization per shard (avg over time range)",
       pad_shard(f"avg_over_time(osd_reactor_utilization{F}[$__range])"),
       "ceph_daemon", "shard", "percent", hi=100, decimals=6,
       desc="Rows: OSDs. Columns: reactor shards. Cell: average utilization "
            "over the dashboard time range. Select the test window in the "
            "time picker to see that run only.")
ts("Reactor utilization per shard: $osd",
   [(pad_shard(f"osd_reactor_utilization{F}"), "shard {{shard}}")],
   "percent", w=24, h=9, legend="right", repeat="osd",
   desc="One copy of this panel for each OSD selected in the OSD list. "
        "One line for each reactor shard of that OSD.")
ts("Reactor utilization (avg of shards)",
   [(f"avg by (ceph_daemon) (osd_reactor_utilization{F})", "{{ceph_daemon}}")],
   "percent", desc="Seastar reactor utilization, averaged over the shards of each OSD.")
ts("Shard imbalance (max - min utilization)",
   [(f"max by (ceph_daemon) (osd_reactor_utilization{F}) - "
     f"min by (ceph_daemon) (osd_reactor_utilization{F})", "{{ceph_daemon}}")],
   "percent", desc="High value: the load is not balanced between the shards of this OSD.")
ts("CPU busy (cores)",
   [(f"sum by (ceph_daemon) (rate(osd_reactor_cpu_busy_ms{F}[{R}])) / 1000", "{{ceph_daemon}}")],
   "none", desc="Busy reactor time per second = number of cores kept busy.")
ts("Reactor stalls / s",
   [(f"sum by (ceph_daemon) (rate(osd_reactor_stalls_count{F}[{R}]))", "{{ceph_daemon}} stalls"),
    (f"sum by (ceph_daemon) (rate(osd_stall_detector_reported{F}[{R}]))", "{{ceph_daemon}} stall reports")],
   "ops")
ts("Tasks processed / s",
   [(f"sum by (ceph_daemon) (rate(osd_reactor_tasks_processed{F}[{R}]))", "{{ceph_daemon}}")],
   "ops")
ts("Tasks pending",
   [(f"sum by (ceph_daemon) (osd_reactor_tasks_pending{F})", "{{ceph_daemon}}")])
end_row()

# ---------------- Network and disk IO ----------------
row("Network and disk IO")
RANGE_DESC = ("Rows: OSDs. Columns: reactor shards. Cell: average rate over the "
              "dashboard time range. The color scale is shared by all cells.")
for title, metric in [("Network rx per shard", "osd_network_bytes_received"),
                      ("Network tx per shard", "osd_network_bytes_sent"),
                      ("Disk read bandwidth per shard", "osd_io_queue_total_read_bytes"),
                      ("Disk write bandwidth per shard", "osd_io_queue_total_write_bytes")]:
    matrix(f"{title} (avg over time range)",
           pad_shard(f"sum by (ceph_daemon, shard) (rate({metric}{F}[$__range]))"),
           "ceph_daemon", "shard", "Bps", w=12, desc=RANGE_DESC)
ts("Network throughput",
   [(f"sum by (ceph_daemon) (rate(osd_network_bytes_received{F}[{R}]))", "{{ceph_daemon}} rx"),
    (f"sum by (ceph_daemon) (rate(osd_network_bytes_sent{F}[{R}]))", "{{ceph_daemon}} tx")],
   "Bps")
ts("Disk throughput (io_queue)",
   [(f"sum by (ceph_daemon) (rate(osd_io_queue_total_read_bytes{F}[{R}]))", "{{ceph_daemon}} read"),
    (f"sum by (ceph_daemon) (rate(osd_io_queue_total_write_bytes{F}[{R}]))", "{{ceph_daemon}} write")],
   "Bps")
ts("Disk IOPS (io_queue)",
   [(f"sum by (ceph_daemon) (rate(osd_io_queue_total_read_ops{F}[{R}]))", "{{ceph_daemon}} read"),
    (f"sum by (ceph_daemon) (rate(osd_io_queue_total_write_ops{F}[{R}]))", "{{ceph_daemon}} write")],
   "iops")
ts("io_queue delay and queue length",
   [(f"max by (ceph_daemon) (osd_io_queue_delay{F})", "{{ceph_daemon}} delay (s)"),
    (f"sum by (ceph_daemon) (osd_io_queue_queue_length{F})", "{{ceph_daemon}} queue length")],
   "short")
end_row()

# ---------------- mClock scheduler ----------------
# src/crimson/osd/osd_operation.cc: OperationThrottler::register_metrics().
# Only ops that waited >= 1 ms are recorded (record_throttle_wait skips 0 ms).
row("mClock scheduler")
M = "osd_osd_mclock"
BY = "sum by (ceph_daemon, op_class)"
ts("Throttled ops / s",
   [(f"{BY} (rate({M}_throttled_ops{F}[{R}]))", "{{ceph_daemon}} {{op_class}}")],
   "ops", desc="Ops that waited 1 ms or more in the mClock scheduler, by op class.")
ts("Avg wait per throttled op",
   [(f"{BY} (rate({M}_total_wait_ms{F}[{R}])) / {BY} (rate({M}_throttled_ops{F}[{R}]))",
     "{{ceph_daemon}} {{op_class}}")],
   "ms", desc="total_wait_ms / throttled_ops. Ops that did not wait are not included.")
ts("p99 wait (throttled ops)",
   [(f"histogram_quantile(0.99, sum by (le, ceph_daemon, op_class) "
     f"(rate({M}_throttle_wait_latency_bucket{F}[{R}])))", "{{ceph_daemon}} {{op_class}}")],
   "ms", desc="From the throttle_wait_latency histogram. Buckets: 1, 5, 10, 50, "
              "100, 500, 1000 ms, so the value is an estimate between two buckets.")
ts("Total wait time / s",
   [(f"{BY} (rate({M}_total_wait_ms{F}[{R}]))", "{{ceph_daemon}} {{op_class}}")],
   "ms", desc="Milliseconds of mClock wait added up over all ops, per second. "
              "Shows the total queueing load of each op class.")
ts("Max wait since OSD start",
   [(f"max by (ceph_daemon, op_class) ({M}_max_wait_ms{F})", "{{ceph_daemon}} {{op_class}}")],
   "ms", desc="Highest wait seen since the OSD started. It never goes down; "
              "it resets only when the OSD restarts.")
matrix("Client throttled ops per shard (avg over time range)",
       pad_shard(f'sum by (ceph_daemon, shard) '
                 f'(rate({M}_throttled_ops{{ceph_daemon=~"$osd", op_class="client"}}[$__range]))'),
       "ceph_daemon", "shard", "ops", w=12,
       desc="Rows: OSDs. Columns: reactor shards. Cell: average rate of client "
            "ops that waited in mClock, over the dashboard time range.")
end_row()

# ---------------- SeaStore ----------------
row("SeaStore")
ts("Avg op latency by op type",
   [(f"sum by (ceph_daemon, latency) (rate(osd_seastore_op_lat_sum{F}[{R}])) / "
     f"sum by (ceph_daemon, latency) (rate(osd_seastore_op_lat_count{F}[{R}]))",
     "{{ceph_daemon}} {{latency}}")],
   "ms", desc="sum/count of osd_seastore_op_lat (milliseconds).")
ts("Avg do_transaction latency by stage",
   [(f"sum by (ceph_daemon, stage) (rate(osd_seastore_do_transaction_stage_lat_sum{FT}[{R}])) / "
     f"sum by (ceph_daemon, stage) (rate(osd_seastore_do_transaction_stage_lat_count{FT}[{R}]))",
     "{{ceph_daemon}} {{stage}}")],
   "ms", desc="tail=\"all\" samples only (milliseconds).")
ts("Transactions in flight",
   [(f"sum by (ceph_daemon) (osd_seastore_concurrent_transactions{F})", "{{ceph_daemon}} concurrent"),
    (f"sum by (ceph_daemon) (osd_seastore_pending_transactions{F})", "{{ceph_daemon}} pending")])
ts("Transaction conflicts (invalidated / committed)",
   [(f"sum by (ceph_daemon) (rate(osd_cache_trans_invalidated{F}[{R}])) / "
     f"sum by (ceph_daemon) (rate(osd_cache_trans_committed{F}[{R}]))", "{{ceph_daemon}}")],
   "percentunit")
end_row()

# ---------------- SeaStore cache ----------------
# Cache::register_metrics() (cache.cc) and ExtentPinboard (extent_pinboard.cc).
# Sizes (cached_*, dirty_*, lru_size_bytes, lru_num_extents) are exported as
# counters but are current values, so no rate() on them.
row("SeaStore cache")
C = "osd_cache"
SUM = "sum by (ceph_daemon)"
matrix("Cache hit ratio per shard (over time range)",
       pad_shard(f"sum by (ceph_daemon, shard) (increase({C}_cache_hit{F}[$__range])) / "
                 f"sum by (ceph_daemon, shard) (increase({C}_cache_access{F}[$__range]))"),
       "ceph_daemon", "shard", "percentunit", hi=1,
       desc="Rows: OSDs. Columns: reactor shards. Cell: cache hits / cache "
            "accesses over the dashboard time range.")
ts("Cache hit ratio",
   [(f"{SUM} (rate({C}_cache_hit{F}[{R}])) / {SUM} (rate({C}_cache_access{F}[{R}]))",
     "{{ceph_daemon}}")],
   "percentunit", desc="cache_hit / cache_access: extent lookups found in the cache.")
ts("Cache accesses / s",
   [(f"{SUM} (rate({C}_cache_access{F}[{R}]))", "{{ceph_daemon}} access"),
    (f"{SUM} (rate({C}_cache_hit{F}[{R}]))", "{{ceph_daemon}} hit")],
   "ops")
ts("LRU hit ratio",
   [(f"{SUM} (rate({C}_lru_hit{F}[{R}])) / "
     f"({SUM} (rate({C}_lru_hit{F}[{R}])) + {SUM} (rate({C}_lru_miss{F}[{R}])))",
     "{{ceph_daemon}}")],
   "percentunit", desc="lru_hit / (lru_hit + lru_miss): touched extents that "
                       "were already linked in the LRU.")
ts("Data read / write hit ratio (LBC)",
   [(f"{SUM} (rate({C}_read_hit_hot{F}[{R}])) / "
     f"({SUM} (rate({C}_read_hit_hot{F}[{R}])) + {SUM} (rate({C}_read_hit_cold{F}[{R}])))",
     "{{ceph_daemon}} read"),
    (f"{SUM} (rate({C}_write_hit_hot{F}[{R}])) / "
     f"({SUM} (rate({C}_write_hit_hot{F}[{R}])) + {SUM} (rate({C}_write_hit_cold{F}[{R}])))",
     "{{ceph_daemon}} write")],
   "percentunit", desc="hot / (hot + cold). hot = LBC hit, cold = LBC miss, "
                       "for data reads and data writes.")
ts("Cache size (bytes)",
   [(f"{SUM} ({C}_cached_extent_bytes{F})", "{{ceph_daemon}} cached"),
    (f"{SUM} ({C}_lru_size_bytes{F})", "{{ceph_daemon}} LRU"),
    (f"{SUM} ({C}_dirty_extent_bytes{F})", "{{ceph_daemon}} dirty")],
   "bytes")
ts("Cache size (extents)",
   [(f"{SUM} ({C}_cached_extents{F})", "{{ceph_daemon}} cached"),
    (f"{SUM} ({C}_lru_num_extents{F})", "{{ceph_daemon}} LRU"),
    (f"{SUM} ({C}_dirty_extents{F})", "{{ceph_daemon}} dirty")])
ts("Committed extent bytes / s by effort",
   [(f"sum by (ceph_daemon, effort) (rate({C}_committed_extent_bytes{F}[{R}]))",
     "{{ceph_daemon}} {{effort}}")],
   "Bps", desc="Extent bytes in committed transactions, by effort "
               "(READ, MUTATE, RETIRE, FRESH_INVALID, FRESH_INLINE, FRESH_OOL).")
ts("Successful read transactions",
   [(f"{SUM} (rate({C}_successful_read_extent_bytes{F}[{R}]))", "{{ceph_daemon}} bytes / s")],
   "Bps")
ts("Transactions created / s by source",
   [(f"sum by (ceph_daemon, src) (rate({C}_trans_created{F}[{R}]))", "{{ceph_daemon}} {{src}}")],
   "ops")
ts("Cursor refreshes / s",
   [(f"{SUM} (rate({C}_refresh_parent_total{F}[{R}]))", "{{ceph_daemon}} total"),
    (f"{SUM} (rate({C}_refresh_invalid_parent{F}[{R}]))", "{{ceph_daemon}} invalid parent"),
    (f"{SUM} (rate({C}_refresh_unviewable_parent{F}[{R}]))", "{{ceph_daemon}} unviewable parent"),
    (f"{SUM} (rate({C}_refresh_modified_viewable_parent{F}[{R}]))", "{{ceph_daemon}} modified viewable parent")],
   "ops")
end_row()

# ---------------- Journal and background work (both backends) ----------------
row("Journal and background work (segmented and RBM)")
ts("Journal records / s",
   [(f"sum by (ceph_daemon) (rate(osd_journal_record_num{F}[{R}]))", "{{ceph_daemon}}")],
   "ops")
ts("Journal record group bytes / s",
   [(f"sum by (ceph_daemon) (rate(osd_journal_record_group_data_bytes{F}[{R}]))", "{{ceph_daemon}} data"),
    (f"sum by (ceph_daemon) (rate(osd_journal_record_group_metadata_bytes{F}[{R}]))", "{{ceph_daemon}} metadata"),
    (f"sum by (ceph_daemon) (rate(osd_journal_record_group_padding_bytes{F}[{R}]))", "{{ceph_daemon}} padding")],
   "Bps")
ts("Journal trimmer: dirty and alloc journal size",
   [(f"sum by (ceph_daemon) (osd_journal_trimmer_dirty_journal_bytes{F})", "{{ceph_daemon}} dirty"),
    (f"sum by (ceph_daemon) (osd_journal_trimmer_alloc_journal_bytes{F})", "{{ceph_daemon}} alloc")],
   "bytes", desc="Journal bytes not yet trimmed. Exported as counter, but it is a current size.")
ts("Background IO blocked / s",
   [(f"sum by (ceph_daemon) (rate(osd_background_process_io_blocked_count{F}[{R}]))", "{{ceph_daemon}}")],
   "ops", desc="User IO blocked by the background cleaner/trimmer.")
end_row()

# ---------------- Segmented backend ----------------
row("Segmented backend (segment cleaner and segment manager)")
ts("Available space ratio",
   [(f"min by (ceph_daemon) (osd_segment_cleaner_available_ratio{F})", "{{ceph_daemon}}")],
   "percentunit")
ts("Reclaim ratio",
   [(f"max by (ceph_daemon) (osd_segment_cleaner_reclaim_ratio{F})", "{{ceph_daemon}}")],
   "percentunit")
ts("Segment writes",
   [(f"sum by (ceph_daemon) (rate(osd_segment_manager_data_write_bytes{F}[{R}]))", "{{ceph_daemon}} data"),
    (f"sum by (ceph_daemon) (rate(osd_segment_manager_metadata_write_bytes{F}[{R}]))", "{{ceph_daemon}} metadata")],
   "Bps")
ts("Reclaimed bytes / s",
   [(f"sum by (ceph_daemon) (rate(osd_segment_cleaner_reclaimed_bytes{F}[{R}]))", "{{ceph_daemon}}")],
   "Bps")
end_row()

# ---------------- RBM backend ----------------
# RBMCleaner::register_metrics() (async_cleaner.cc): total/available/used bytes,
# exported as counters but they are current sizes.
# CircularBoundedJournal::register_metrics() (circular_bounded_journal.cc): all
# gauges, but *_count, *_size and *_latency_total only go up (latency in
# seconds), so rate() works. The *_average gauges are lifetime averages, so
# the panels calculate the average over the rate window instead.
row("RBM backend (RBM cleaner and circular bounded journal)")
ts("RBM space",
   [(f"sum by (ceph_daemon) (osd_rbm_cleaner_used_bytes{F})", "{{ceph_daemon}} used"),
    (f"sum by (ceph_daemon) (osd_rbm_cleaner_available_bytes{F})", "{{ceph_daemon}} available"),
    (f"sum by (ceph_daemon) (osd_rbm_cleaner_total_bytes{F})", "{{ceph_daemon}} total")],
   "bytes", desc="available = total - journal - used.")
ts("RBM used ratio",
   [(f"sum by (ceph_daemon) (osd_rbm_cleaner_used_bytes{F}) / "
     f"sum by (ceph_daemon) (osd_rbm_cleaner_total_bytes{F})", "{{ceph_daemon}}")],
   "percentunit", desc="Space used by live extents / total space.")
CBJ = "osd_seastore_cbj_submit_record"
SUM = "sum by (ceph_daemon)"
ts("CBJ records submitted / s",
   [(f"{SUM} (rate({CBJ}_count{F}[{R}]))", "{{ceph_daemon}} submitted"),
    (f"{SUM} (rate({CBJ}_wait_count{F}[{R}]))", "{{ceph_daemon}} had to wait"),
    (f"{SUM} (rate({CBJ}_roll_count{F}[{R}]))", "{{ceph_daemon}} caused a roll")],
   "ops", desc="'had to wait': the record submitter was not available. "
               "'caused a roll': the journal rolled before the submit.")
ts("CBJ avg submit latency",
   [(f"{SUM} (rate({CBJ}_latency_total{F}[{R}])) / {SUM} (rate({CBJ}_count{F}[{R}]))",
     "{{ceph_daemon}} submit (all records)"),
    (f"{SUM} (rate({CBJ}_wait_latency_total{F}[{R}])) / {SUM} (rate({CBJ}_wait_count{F}[{R}]))",
     "{{ceph_daemon}} wait (records that waited)"),
    (f"{SUM} (rate({CBJ}_roll_latency_total{F}[{R}])) / {SUM} (rate({CBJ}_roll_count{F}[{R}]))",
     "{{ceph_daemon}} roll (records that rolled)")],
   "s", desc="Average over the rate window: rate(latency_total) / rate(count).")
ts("CBJ avg record metadata size",
   [(f"{SUM} (rate({CBJ}_size{F}[{R}])) / {SUM} (rate({CBJ}_count{F}[{R}]))", "{{ceph_daemon}}")],
   "bytes", desc="submit_record_size counts only the raw metadata length of each record.")
ts("CBJ wait time / s",
   [(f"{SUM} (rate({CBJ}_wait_latency_total{F}[{R}]))", "{{ceph_daemon}} waiting for submitter"),
    (f"{SUM} (rate({CBJ}_roll_latency_total{F}[{R}]))", "{{ceph_daemon}} rolling")],
   "s", desc="Seconds spent waiting per second, added up over all records.")
end_row()

# ---------------- Memory ----------------
row("Memory")
ts("Seastar memory",
   [(f"sum by (ceph_daemon) (osd_memory_allocated_memory{F})", "{{ceph_daemon}} allocated"),
    (f"sum by (ceph_daemon) (osd_memory_free_memory{F})", "{{ceph_daemon}} free")],
   "bytes")
ts("Malloc failures / s",
   [(f"sum by (ceph_daemon) (rate(osd_memory_malloc_failed{F}[{R}]))", "{{ceph_daemon}}")],
   "ops")

GRAFANA_DS = {"type": "grafana", "uid": "-- Grafana --"}

dash = {
    # Annotations tagged "perf-test" (for example from a test wrapper that
    # POSTs to /api/annotations) are shown on all time series panels.
    "annotations": {"list": [
        {"builtIn": 1, "name": "Annotations & Alerts", "type": "dashboard",
         "datasource": GRAFANA_DS, "enable": True, "hide": True,
         "iconColor": "rgba(0, 211, 255, 1)"},
        {"name": "Perf tests", "datasource": GRAFANA_DS, "enable": True,
         "iconColor": "orange",
         "target": {"type": "tags", "tags": ["perf-test"], "matchAny": True,
                    "limit": 500}},
    ]},
    "title": "Crimson OSD",
    "uid": "crimson-osd",
    "tags": ["ceph", "crimson"],
    "timezone": "browser",
    "schemaVersion": 39,
    "refresh": "30s",
    "time": {"from": "now-1h", "to": "now"},
    "templating": {"list": [
        {"name": "DS", "label": "Data source", "type": "datasource",
         "query": "prometheus", "current": {}, "hide": 0},
        {"name": "osd", "label": "OSD", "type": "query", "datasource": DS,
         "query": {"query": "label_values(osd_reactor_utilization, ceph_daemon)",
                   "refId": "osd"},
         "definition": "label_values(osd_reactor_utilization, ceph_daemon)",
         "refresh": 2, "multi": True, "includeAll": True, "allValue": ".*",
         "current": {"text": "All", "value": "$__all"}, "sort": 3},
    ]},
    "panels": panels,
}

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "crimson-osd.json")
    with open(out, "w") as f:
        json.dump(dash, f, indent=2)
        f.write("\n")
    print(f"{out}: {len([p for p in panels if p['type'] != 'row'])} panels")
