#!/usr/bin/env python3
"""Plot WAF-over-time from crimson-osd `[WAF]` log lines.

Optional companion to waf_report.py. Reads $BASE_DIR/out/osd.<i>.log
files, extracts every periodic line of the form

    [WAF] osd.<i> user_written=<U> device_written=<D> waf=<W>

emitted by SeaStore::Shard::log_waf_stats every 10 s, and plots one
WAF curve per OSD into $WAF_PLOT_OUT.

Designed to be skipped cleanly when matplotlib is absent: invocation
returns 0 and prints a single notice line so the calling shell can
still complete a CI run on a machine without plotting libraries.

Inputs (env):
  WAF_NUM_OSDS    number of OSDs the run targeted
  WAF_BASE_DIR    cluster base dir (where out/osd.<i>.log lives)
  WAF_PLOT_OUT    output PNG path; parent dir created if missing
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime
from pathlib import Path


# Example matched line:
#  INFO  2026-05-10 18:54:45,614 [shard 0:main] seastore - SeaStore::Shard::log_waf_stats: \
#  [WAF] osd.0 user_written=614408 device_written=1110016 waf=1.807
WAF_LINE_RE = re.compile(
    r"^\S+\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}),\d+\s+\[shard\s+\d+:\S+\]\s+"
    r"seastore\s+-\s+SeaStore::Shard::log_waf_stats:\s+\[WAF\]\s+osd\.(\d+)\s+"
    r"user_written=(\d+)\s+device_written=(\d+)\s+waf=([0-9.]+)"
)


def _parse_log(path: Path) -> list[tuple[datetime, float]]:
    samples: list[tuple[datetime, float]] = []
    if not path.exists():
        return samples
    with path.open() as fh:
        for line in fh:
            m = WAF_LINE_RE.search(line)
            if not m:
                continue
            ts = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H:%M:%S")
            samples.append((ts, float(m.group(6))))
    return samples


def main() -> int:
    num_osds = int(os.environ["WAF_NUM_OSDS"])
    base_dir = Path(os.environ["WAF_BASE_DIR"])
    plot_out = Path(os.environ["WAF_PLOT_OUT"])

    try:
        import matplotlib  # noqa: F401
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ModuleNotFoundError:
        # Per spec: skip cleanly if matplotlib is unavailable, do not error.
        print(
            "[waf_plot] matplotlib not available; skipping WAF-over-time plot.",
            file=sys.stderr,
        )
        return 0

    log_dir = base_dir / "out"
    series: list[tuple[int, list[tuple[datetime, float]]]] = []
    for i in range(num_osds):
        samples = _parse_log(log_dir / f"osd.{i}.log")
        series.append((i, samples))

    if not any(samples for _, samples in series):
        print(
            f"[waf_plot] No [WAF] log lines found under {log_dir} — "
            "is the cluster running with the seastore_waf instrumentation? "
            "Skipping plot.",
            file=sys.stderr,
        )
        return 0

    plot_out.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    for osd_id, samples in series:
        if not samples:
            continue
        xs = [t for t, _ in samples]
        ys = [v for _, v in samples]
        ax.plot(xs, ys, marker="o", label=f"osd.{osd_id}")
    ax.set_xlabel("time")
    ax.set_ylabel("WAF (device_written / user_written)")
    ax.set_title("seastore Write Amplification over time")
    ax.grid(True, linestyle=":")
    ax.legend(loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(plot_out)
    print(f"[waf_plot] wrote {plot_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
