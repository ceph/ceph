#!/usr/bin/env python3
"""Format a WAF benchmark report from fio JSON + per-OSD asok dumps.

Driven by run_waf_bench.sh via env vars; lives as a separate script so
the same formatting logic can be exercised from a unit test or a
re-run after the cluster has been torn down.

Inputs (env):
  WAF_NUM_OSDS   number of OSDs the run targeted
  WAF_OUT_DIR    directory containing fio.json, asok/osd.<i>.seastore_waf.json
  WAF_FIO_JSON   path to fio's --output-format=json file
  WAF_REPORT     destination file for the human-readable report

The script is intentionally permissive: if fio crashed mid-run and the
JSON is truncated, the report still emits a useful per-OSD WAF table
with a clear "fio: PARTIAL FAILURE" header. Aggregate WAF is computed
independently from raw counter sums so the value cannot drift if a
formatter bug is introduced (we recompute, we don't read it back).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _env(name: str, *, required: bool = True, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if required and v is None:
        print(f"waf_report: missing required env var {name}", file=sys.stderr)
        sys.exit(2)
    return v  # type: ignore[return-value]


def _read_json(path: Path) -> dict | None:
    """Best-effort JSON read; returns None if the file is missing or unparseable.

    fio writes its JSON document atomically only on clean exit, so
    crashes can leave a truncated file. The report explicitly flags
    that case rather than aborting — operators still want the asok
    side of the picture in that situation.
    """
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"waf_report: warning: failed to parse {path}: {exc}", file=sys.stderr)
        return None


def _osd_counters(out_dir: Path, num_osds: int) -> list[tuple[int, int | None, int | None]]:
    """Return [(osd_id, user_written, device_written), ...] from asok dumps.

    user_written / device_written are None when the asok dump for that
    OSD is missing or malformed. The aggregate-WAF computation skips
    None entries; per-OSD lines display as "N/A".
    """
    out: list[tuple[int, int | None, int | None]] = []
    for i in range(num_osds):
        dump = out_dir / "asok" / f"osd.{i}.seastore_waf.json"
        data = _read_json(dump)
        if data is None:
            out.append((i, None, None))
            continue
        # Crimson dumps as {"seastore_waf": {"bytes_user_written": ..., "bytes_device_written": ...}}
        block = data.get("seastore_waf", {}) if isinstance(data, dict) else {}
        u = block.get("bytes_user_written")
        d = block.get("bytes_device_written")
        out.append((
            i,
            int(u) if isinstance(u, int) else None,
            int(d) if isinstance(d, int) else None,
        ))
    return out


def _format_int(n: int | None, width: int = 16) -> str:
    return f"{n:>{width},}" if isinstance(n, int) else f"{'N/A':>{width}}"


def _format_waf(u: int | None, d: int | None) -> str:
    if not isinstance(u, int) or not isinstance(d, int) or u <= 0:
        return "   N/A"
    return f"{d / u:6.3f}"


def _fio_summary(fio_doc: dict | None) -> tuple[str, list[str]]:
    """Returns (status_line, [body_lines]) for the FIO part of the report.

    status_line is OK / PARTIAL / MISSING — operators read this first.
    """
    if fio_doc is None:
        return "MISSING (fio JSON not produced or unreadable)", []
    jobs = fio_doc.get("jobs") or []
    if not jobs:
        return "PARTIAL (fio JSON has no jobs)", []
    body: list[str] = []
    total_bw = 0.0
    total_iops = 0.0
    total_lat_ns = 0
    total_io_bytes = 0
    for j in jobs:
        write = j.get("write", {}) or {}
        bw_kib = write.get("bw", 0)
        iops = write.get("iops", 0.0)
        lat_ns = (write.get("clat_ns") or {}).get("mean", 0)
        io_bytes = write.get("io_bytes", 0)
        body.append(
            f"  {j.get('jobname', '?'):>14} "
            f"bw={bw_kib / 1024:7.1f} MiB/s  "
            f"iops={iops:8.0f}  "
            f"clat_mean={lat_ns / 1000:7.1f} us  "
            f"io={io_bytes / (1 << 20):8.1f} MiB"
        )
        total_bw += bw_kib
        total_iops += iops
        total_lat_ns += int(lat_ns)
        total_io_bytes += int(io_bytes)
    if jobs:
        body.append(
            f"  {'TOTAL':>14} "
            f"bw={total_bw / 1024:7.1f} MiB/s  "
            f"iops={total_iops:8.0f}  "
            f"clat_mean={(total_lat_ns / len(jobs)) / 1000:7.1f} us  "
            f"io={total_io_bytes / (1 << 20):8.1f} MiB"
        )
    return "OK", body


def main() -> int:
    num_osds = int(_env("WAF_NUM_OSDS"))
    out_dir = Path(_env("WAF_OUT_DIR"))
    fio_json = Path(_env("WAF_FIO_JSON"))
    report_path = Path(_env("WAF_REPORT"))

    counters = _osd_counters(out_dir, num_osds)
    fio_doc = _read_json(fio_json)
    fio_status, fio_body = _fio_summary(fio_doc)

    # Compute aggregate WAF independently from raw sums — never read back
    # from a precomputed field, so a formatter bug elsewhere can't make
    # the headline number lie.
    agg_user = sum(u for _, u, _ in counters if isinstance(u, int))
    agg_dev = sum(d for _, _, d in counters if isinstance(d, int))
    agg_waf = (agg_dev / agg_user) if agg_user > 0 else None

    lines: list[str] = []
    lines.append("seastore WAF benchmark report")
    lines.append("=" * 60)
    lines.append(f"timestamp: {fio_doc.get('time') if isinstance(fio_doc, dict) else 'unknown'}")
    lines.append(f"OSDs:      {num_osds}")
    lines.append("")
    lines.append("Per-OSD seastore_waf counters (from asok perfcounters_dump):")
    lines.append(f"  {'osd':>4}  {'user_written (B)':>16}  {'device_written (B)':>18}  {'WAF':>6}")
    for osd, u, d in counters:
        lines.append(f"  {osd:>4}  {_format_int(u, 16)}  {_format_int(d, 18)}  {_format_waf(u, d)}")
    lines.append("")
    lines.append("Aggregate (independent recompute of sum(d) / sum(u)):")
    lines.append(f"  user_written  total: {agg_user:>16,} B")
    lines.append(f"  device_written total: {agg_dev:>16,} B")
    lines.append(f"  WAF                 : "
                 + (f"{agg_waf:.3f}" if agg_waf is not None else "N/A (no user writes)"))
    lines.append("")
    lines.append(f"FIO summary: {fio_status}")
    lines.extend(fio_body)
    lines.append("")
    lines.append(f"Raw inputs:")
    lines.append(f"  fio JSON:  {fio_json}")
    lines.append(f"  asok dir:  {out_dir / 'asok'}")
    lines.append("")

    report_path.write_text("\n".join(lines) + "\n")

    # Console echo so a CI run shows the headline without an extra cat.
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
