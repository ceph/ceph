#!/usr/bin/env python3

import json
import subprocess
import sys
import argparse
from typing import Dict, List, Any


# Convert raw nanosecond values to more readable values
# with suffixes
def human_readable_ns(ns: int) -> str:
    if ns == 0:
        return "0ns"
    suffixes = ["ns", "us", "ms", "s"]
    index = 0
    value = float(ns)
    while value >= 1000 and index < len(suffixes) - 1:
        index += 1
        value = value / 1000.0

    if value == int(value):
        return f"{int(value)}{suffixes[index]}"
    return f"{value:.1f}{suffixes[index]}"


def call_ceph_daemon(daemon: str, command: List[str]) -> str:
    try:
        # For debugging, print the command being called
        # print(f"Calling ceph daemon: {daemon} command={command}")
        cmd = ["ceph", "daemon", daemon, "lockstat"] + command
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error calling ceph daemon: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


def get_lockstat_json(daemon: str) -> Dict[str, Any]:
    try:
        stdout = call_ceph_daemon(daemon, ["dump"])
        return json.loads(stdout)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON output: {e}", file=sys.stderr)
        sys.exit(1)


def format_lock_type(type_val: int) -> str:
    # From lockstat.h:
    # UNKNOWN = 'U', MUTEX = 'M', RW_LOCK = 'R', COND_VAR = 'C', NO_LOCK = 'N'
    return chr(type_val) if 32 <= type_val <= 126 else str(type_val)


def main():
    parser = argparse.ArgumentParser(
        description="Produce human readable lockstat output"
    )
    parser.add_argument("daemon", help="Name of the daemon")

    subparsers = parser.add_subparsers(dest="command", help="Subcommand to execute")

    # dump subcommand
    dump_parser = subparsers.add_parser("dump", help="Dump lockstat data (default)")
    dump_parser.add_argument(
        "--detail", action="store_true", help="Output histogram data"
    )

    # start subcommand
    start_parser = subparsers.add_parser("start", help="Start lockstat profiling")
    start_parser.add_argument("--threshold", help="Optional threshold in microseconds")

    # stop subcommand
    subparsers.add_parser("stop", help="Stop lockstat profiling")

    # reset subcommand
    subparsers.add_parser("reset", help="Reset lockstat profiling")

    args = parser.parse_args()

    # Default to dump if no subcommand is provided
    if args.command is None or args.command == "dump":
        data = get_lockstat_json(args.daemon)
        render_dump(data, getattr(args, "detail", False))
    elif args.command == "start":
        cmd = ["start"]
        if args.threshold:
            cmd.append(args.threshold)
        print(call_ceph_daemon(args.daemon, cmd))
    elif args.command == "stop":
        print(call_ceph_daemon(args.daemon, ["stop"]))
    elif args.command == "reset":
        print(call_ceph_daemon(args.daemon, ["reset"]))


def render_dump(data: Dict[str, Any], detail: bool):
    if "lockstat" not in data:
        print("No lockstat data found in output")
        return

    lockstat = data["lockstat"]
    if "status" in lockstat and lockstat["status"] == "lockstat is not enabled":
        print("Lockstat is not enabled on this daemon.")
        return

    total_usec = lockstat.get("total_usec", 1)
    if total_usec == 0:
        total_usec = 1

    entries_data = lockstat.get("entries", [])
    entries = []
    for e in entries_data:
        if isinstance(e, dict) and "entry" in e:
            entries.append(e["entry"])
        else:
            entries.append(e)

    # Headers
    headers = [
        "idx[lock type]",
        "wait_usec[W]",
        "wait_count[W]",
        "usec/wait[W]",
        "wait_usec[R]",
        "wait_count[R]",
        "usec/wait[R]",
        "max_wait",
        "busy ratio",
        "(r + w)/w",
        "num_instances",
    ]

    bin_headers = []
    if detail:
        bin_ranges = lockstat.get("bin_ranges", [])
        for br in bin_ranges:
            min_val = br.get("min_val", 0)
            max_val = br.get("max_val", 0)
            if max_val == 0xFFFFFFFFFFFFFFFF:
                header = f">{human_readable_ns(min_val)}"
            else:
                header = f"<{human_readable_ns(max_val)}"
            bin_headers.append(header)
        headers.extend(bin_headers)

    headers.append("name")

    # Column widths
    col_widths = [len(h) for h in headers]
    if detail:
        # Set histogram columns to 12 chars width as requested
        # Histogram columns start after the first 11 columns
        for i in range(11, 11 + len(bin_headers)):
            col_widths[i] = 12

    rows = []
    for entry in entries:
        eid = entry.get("id", 0)
        etype = format_lock_type(entry.get("type", 0))
        idx_type = f"{eid}[{etype}]"

        stats = {s["name"]: s for s in entry.get("stats", [])}

        # WRITE
        w_stats = stats.get("WRITE", {})
        w_wait_usec = w_stats.get("wait_duration_ns", 0) / 1000.0
        w_wait_count = w_stats.get("wait_count", 0)
        w_avg_wait = w_wait_usec / w_wait_count if w_wait_count > 0 else 0.0

        # READ
        r_stats = stats.get("READ", {})
        r_wait_usec = r_stats.get("wait_duration_ns", 0) / 1000.0
        r_wait_count = r_stats.get("wait_count", 0)
        r_avg_wait = r_wait_usec / r_wait_count if r_wait_count > 0 else 0.0

        # Total wait for busy ratio
        total_wait_ns = sum(
            s.get("wait_duration_ns", 0) for s in entry.get("stats", [])
        )
        busy_ratio = (total_wait_ns / 1000.0) / total_usec

        # (r + w)/w
        r_plus_w_over_w = (
            (r_wait_count + w_wait_count) / w_wait_count if w_wait_count > 0 else 0.0
        )

        max_wait_ns = entry.get("max_wait_ns", 0)
        num_instances = entry.get("num_instances", 0)
        name = entry.get("name", "unknown")

        row = [
            idx_type,
            f"{w_wait_usec:.3f}",
            str(w_wait_count),
            f"{w_avg_wait:.3f}",
            f"{r_wait_usec:.3f}",
            str(r_wait_count),
            f"{r_avg_wait:.3f}",
            f"{max_wait_ns/1000.0:.3f}",
            f"{busy_ratio:.6f}",
            f"{r_plus_w_over_w:.3f}",
            str(num_instances),
        ]

        if detail:
            # To handle non-WRITE modes being displayed correctly, we first
            # ensure the main row has the WRITE histogram or empty histogram cells
            w_stats = stats.get("WRITE", {})
            hist = w_stats.get("wait_time_histogram", [])
            while len(hist) < len(bin_headers):
                hist.append(0)
            for val in hist[: len(bin_headers)]:
                row.append(str(val))

            row.append(name)
            rows.append(row)

            # Now add additional rows for other modes with non-zero wait count
            for mode_name in ["READ", "TRY_WRITE", "TRY_READ"]:
                m_stats = stats.get(mode_name, {})
                if m_stats.get("wait_count", 0) > 0:
                    hist = m_stats.get("wait_time_histogram", [])
                    while len(hist) < len(bin_headers):
                        hist.append(0)

                    # Empty cells for the first 10 columns, label in 11th column (num_instances)
                    # We use a space before the mode_name for slight indentation/distinction
                    hist_row = [""] * 10
                    hist_row.append(f"[{mode_name}]")
                    for val in hist[: len(bin_headers)]:
                        hist_row.append(str(val))
                    hist_row.append("")  # Empty name column
                    rows.append(hist_row)
        else:
            row.append(name)
            rows.append(row)

        # Update column widths for all rows we just added
        # Since rows can have different lengths (if no detail), but format_str will handle it
        for r in rows[-4:]:  # Check at most the last 4 rows added for this entry
            if len(r) == len(col_widths):
                for i, val in enumerate(r):
                    col_widths[i] = max(col_widths[i], len(val))
            elif len(r) == len(headers):
                for i, val in enumerate(r):
                    col_widths[i] = max(col_widths[i], len(val))

    # Print headers
    format_str = "  ".join(f"{{:>{w}}}" for w in col_widths[:-1]) + "  {}"
    print(format_str.format(*headers))

    # Print rows
    for row in rows:
        print(format_str.format(*row))

    # Print footers
    format_str = "  ".join(f"{{:>{w}}}" for w in col_widths[:-1]) + "  {}"
    print(format_str.format(*headers))


if __name__ == "__main__":
    main()
