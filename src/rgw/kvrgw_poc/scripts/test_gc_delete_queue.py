#!/usr/bin/env python3
"""Upload 1000 objects (100B–8MiB), suspend GC, random batch deletes, strict G:O verification."""

from __future__ import annotations

import argparse
import math
import os
import random
import subprocess
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple


MIN_SIZE = 100
MAX_SIZE = 8 * 1024 * 1024
NUM_OBJECTS = 1000
MAX_BATCH = 256
MAX_GAP = 1


def size_tier_from_size(n: int) -> int:
    if n <= 0:
        return 0
    tier = int(math.floor(math.log2(n))) - 10
    return max(0, min(34, tier))


def tier_min_bytes(tier: int) -> int:
    if tier <= 0:
        return 0
    return 1 << (tier + 10)


def tier_max_bytes(tier: int) -> int:
    if tier >= 34:
        return (1 << 64) - 1
    return (1 << (tier + 11)) - 1


def tier_histogram(sizes: Sequence[int]) -> Dict[int, Tuple[int, int]]:
    """tier -> (count, total_bytes)"""
    hist: Dict[int, List[int]] = {}
    for s in sizes:
        t = size_tier_from_size(s)
        hist.setdefault(t, []).append(s)
    return {t: (len(v), sum(v)) for t, v in hist.items()}


@dataclass
class GcCtl:
    path: str
    env: dict

    def run(self, *args: str) -> str:
        cmd = [self.path, *args]
        proc = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            env=self.env,
        )
        return proc.stdout

    def count(self) -> dict[str, int]:
        out = {}
        for line in self.run("count").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k] = int(v)
        return out

    def list_entries(self, limit: int) -> list[dict]:
        lines = self.run("list", "--limit", str(limit)).splitlines()
        rows = []
        for line in lines:
            if line.startswith("ref_tag\t") or line.startswith("listed="):
                continue
            parts = line.split("\t")
            if len(parts) < 6:
                continue
            rows.append(
                {
                    "ref_tag": parts[0],
                    "size_tier": int(parts[1]),
                    "blob_bytes": None if parts[2] == "-" else int(parts[2]),
                    "bucket_id": parts[3],
                }
            )
        return rows

    def count_by_tier(self) -> dict[int, dict]:
        lines = self.run("count-by-tier").splitlines()
        tiers = {}
        for line in lines[1:]:
            cols = line.split("\t")
            if len(cols) < 6:
                continue
            tier = int(cols[0])
            tiers[tier] = {
                "count": int(cols[1]),
                "total_bytes": int(cols[2]),
                "missing_blobs": int(cols[3]),
                "tier_min_bytes": int(cols[4]),
                "tier_max_bytes": int(cols[5]),
            }
        return tiers

    def list_by_size(self, lo: int, hi: int, limit: int) -> list[dict]:
        lines = self.run("list-by-size", str(lo), str(hi), "--limit", str(limit)).splitlines()
        rows = []
        for line in lines:
            if line.startswith("ref_tag\t") or line.startswith("listed="):
                continue
            parts = line.split("\t")
            if len(parts) < 6:
                continue
            rows.append(
                {
                    "ref_tag": parts[0],
                    "size_tier": int(parts[1]),
                    "blob_bytes": None if parts[2] == "-" else int(parts[2]),
                }
            )
        return rows

    def set_gc_config(self, **fields: int) -> int:
        args = ["set-gc-config"]
        for k, v in fields.items():
            args.append(f"{k}={v}")
        line = self.run(*args).strip()
        if not line.startswith("HANDLE="):
            raise RuntimeError(f"set-gc-config: {line}")
        return int(line.split("=", 1)[1])

    def wait_applied(self, handle: int) -> None:
        proc = subprocess.run(
            [self.path, "wait-applied", "--handle", str(handle), "--max-gap", str(MAX_GAP)],
            capture_output=True,
            text=True,
            env=self.env,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr or proc.stdout or "wait-applied failed")

    def get_gc_config(self) -> dict[str, int]:
        cfg = {}
        for line in self.run("get-gc-config").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                cfg[k] = int(v)
        return cfg


def fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def aws_run(endpoint: str, *args: str) -> None:
    subprocess.run(
        ["aws", "--endpoint-url", endpoint, *args],
        check=True,
        capture_output=True,
    )


def aws_get_fails(endpoint: str, bucket: str, key: str) -> bool:
    proc = subprocess.run(
        ["aws", "--endpoint-url", endpoint, "s3", "cp", f"s3://{bucket}/{key}", os.devnull],
        capture_output=True,
    )
    return proc.returncode != 0


def drain_gc_queue(gc: GcCtl) -> None:
    handle = gc.set_gc_config(
        suspended=0,
        interval_sec=1,
        max_objects_per_sec=1000,
        max_mb_per_sec=0,
    )
    gc.wait_applied(handle)
    for _ in range(120):
        if gc.count().get("pending_gc_entries", -1) == 0:
            return
        time.sleep(1)
    fail("GC queue not empty after drain")


def suspend_gc(gc: GcCtl) -> None:
    handle = gc.set_gc_config(
        suspended=1,
        interval_sec=3600,
        max_objects_per_sec=0,
        max_mb_per_sec=0,
    )
    gc.wait_applied(handle)
    cfg = gc.get_gc_config()
    if cfg.get("suspended") != 1:
        fail(f"GC not suspended: {cfg}")


def resume_gc(gc: GcCtl) -> None:
    handle = gc.set_gc_config(
        suspended=0,
        interval_sec=1,
        max_objects_per_sec=500,
        max_mb_per_sec=0,
    )
    gc.wait_applied(handle)


def generate_sizes(rng: random.Random, n: int) -> list[int]:
    log_min = math.log(MIN_SIZE)
    log_max = math.log(MAX_SIZE)
    sizes = []
    for _ in range(n):
        log_s = rng.uniform(log_min, log_max)
        sizes.append(int(round(math.exp(log_s))))
    sizes = [max(MIN_SIZE, min(MAX_SIZE, s)) for s in sizes]
    return sizes


def _storage_threshold() -> int:
    return int(os.environ.get("KVRGW_MAX_KV_STORE", "4096"))


def verify_gc_domain(gc: GcCtl, deleted_sizes: Sequence[int], batch_label: str) -> None:
    threshold = _storage_threshold()
    gc_sizes = [s for s in deleted_sizes if s > threshold]
    expected_count = len(gc_sizes)
    expected_bytes = sum(gc_sizes)
    expected_hist = tier_histogram(gc_sizes)

    stats = gc.count()
    if stats.get("pending_gc_entries") != expected_count:
        fail(f"{batch_label}: pending_gc_entries {stats.get('pending_gc_entries')} != {expected_count}")
    if stats.get("total_bytes") != expected_bytes:
        fail(f"{batch_label}: total_bytes {stats.get('total_bytes')} != {expected_bytes}")
    if stats.get("missing_blobs", 0) != 0:
        fail(f"{batch_label}: missing_blobs={stats.get('missing_blobs')}")

    entries = gc.list_entries(limit=max(expected_count + 10, 2000))
    if len(entries) != expected_count:
        fail(f"{batch_label}: list count {len(entries)} != {expected_count}")

    ref_tags = [e["ref_tag"] for e in entries]
    if len(set(ref_tags)) != expected_count:
        fail(f"{batch_label}: duplicate ref_tag in G:O list")

    listed_bytes = 0
    list_hist: Dict[int, List[int]] = {}
    for e in entries:
        if e["blob_bytes"] is None:
            fail(f"{batch_label}: missing blob for ref_tag {e['ref_tag']}")
        blob = e["blob_bytes"]
        listed_bytes += blob
        tier = e["size_tier"]
        want_tier = size_tier_from_size(blob)
        if tier != want_tier:
            fail(
                f"{batch_label}: ref_tag {e['ref_tag']} size_tier {tier} != "
                f"tier_from_size({blob})={want_tier}"
            )
        if blob < tier_min_bytes(tier) or blob > tier_max_bytes(tier):
            fail(
                f"{batch_label}: blob_bytes {blob} outside tier {tier} range "
                f"[{tier_min_bytes(tier)}, {tier_max_bytes(tier)}]"
            )
        list_hist.setdefault(tier, []).append(blob)

    if listed_bytes != expected_bytes:
        fail(f"{batch_label}: sum(list blob_bytes) {listed_bytes} != {expected_bytes}")

    tier_stats = gc.count_by_tier()
    if sum(t["count"] for t in tier_stats.values()) != expected_count:
        fail(f"{batch_label}: count-by-tier total != {expected_count}")

    for tier, (exp_count, exp_bytes) in expected_hist.items():
        if tier not in tier_stats:
            fail(f"{batch_label}: missing tier {tier} in count-by-tier")
        got = tier_stats[tier]
        if got["count"] != exp_count:
            fail(f"{batch_label}: tier {tier} count {got['count']} != {exp_count}")
        if got["total_bytes"] != exp_bytes:
            fail(f"{batch_label}: tier {tier} bytes {got['total_bytes']} != {exp_bytes}")
        if got["missing_blobs"] != 0:
            fail(f"{batch_label}: tier {tier} missing_blobs={got['missing_blobs']}")

    for tier in tier_stats:
        if tier not in expected_hist:
            fail(f"{batch_label}: unexpected tier {tier} in count-by-tier")

    for tier, (exp_count, _exp_bytes) in expected_hist.items():
        lo, hi = tier_min_bytes(tier), tier_max_bytes(tier)
        ranged = gc.list_by_size(lo, hi, limit=exp_count + 10)
        if len(ranged) != exp_count:
            fail(
                f"{batch_label}: list-by-size tier {tier} [{lo},{hi}] "
                f"count {len(ranged)} != {exp_count}"
            )
        for e in ranged:
            if e["blob_bytes"] is None:
                fail(f"{batch_label}: list-by-size missing blob tier {tier}")
            b = e["blob_bytes"]
            if b < lo or b > hi:
                fail(f"{batch_label}: list-by-size blob {b} outside [{lo},{hi}]")
            if size_tier_from_size(b) != e["size_tier"]:
                fail(
                    f"{batch_label}: list-by-size ref_tag {e['ref_tag']} "
                    f"tier {e['size_tier']} != tier_from_size({b})"
                )

    print(
        f"ok: {batch_label} count={expected_count} bytes={expected_bytes} "
        f"tiers={len(expected_hist)} unique_ref_tags={expected_count}"
    )


def delete_batch(endpoint: str, bucket: str, keys: list[str]) -> None:
    if len(keys) == 1:
        aws_run(endpoint, "s3", "rm", f"s3://{bucket}/{keys[0]}")
        return
    objs = ",".join(f"{{Key={k}}}" for k in keys)
    aws_run(
        endpoint,
        "s3api",
        "delete-objects",
        "--bucket",
        bucket,
        "--delete",
        f"Objects=[{objs}]",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--gc-ctl", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-objects", type=int, default=NUM_OBJECTS)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    gc_env = os.environ.copy()
    gc = GcCtl(args.gc_ctl, gc_env)

    # Admin reachability is validated by gc_ctl.sh (multi-instance fan-out).

    sizes = generate_sizes(rng, args.num_objects)
    keys = [f"obj-{i:05d}" for i in range(args.num_objects)]
    key_size = dict(zip(keys, sizes))

    print(f"=== Drain GC queue ===")
    drain_gc_queue(gc)

    print(f"=== Suspend GC ===")
    suspend_gc(gc)

    tmpdir = tempfile.mkdtemp(prefix="kv-gc-del-")
    max_blob = tempfile.NamedTemporaryFile(delete=False, dir=tmpdir, prefix="max-", suffix=".bin")
    max_blob.write(b"\0" * MAX_SIZE)
    max_blob.close()

    try:
        print(f"=== Upload {args.num_objects} objects ({MIN_SIZE}B–{MAX_SIZE}B) ===")
        aws_run(args.endpoint, "s3", "mb", f"s3://{args.bucket}")
        assert gc.count()["pending_gc_entries"] == 0

        blob_cache: dict[int, str] = {}
        for i, (key, size) in enumerate(zip(keys, sizes)):
            if size not in blob_cache:
                blob = os.path.join(tmpdir, f"blob-{size}")
                subprocess.run(["bash", "-c", f"head -c {size} /dev/zero > '{blob}'"], check=True)
                blob_cache[size] = blob
            aws_run(args.endpoint, "s3", "cp", blob_cache[size], f"s3://{args.bucket}/{key}")
            if (i + 1) % 100 == 0:
                print(f"  uploaded {i + 1}/{args.num_objects}")

        remaining = keys.copy()
        rng.shuffle(remaining)
        deleted_sizes: list[int] = []
        batch_num = 0

        print(f"=== Random batch deletes (1–{MAX_BATCH} keys) ===")
        while remaining:
            batch_num += 1
            batch_size = min(len(remaining), rng.randint(1, MAX_BATCH))
            batch = [remaining.pop() for _ in range(batch_size)]

            delete_batch(args.endpoint, args.bucket, batch)

            for k in batch:
                if not aws_get_fails(args.endpoint, args.bucket, k):
                    fail(f"GET succeeded after delete: {k}")
                deleted_sizes.append(key_size[k])

            verify_gc_domain(gc, deleted_sizes, f"batch-{batch_num}(n={batch_size})")

        if len(deleted_sizes) != args.num_objects:
            fail(f"deleted {len(deleted_sizes)} != {args.num_objects}")

        tier_cov = Counter(size_tier_from_size(s) for s in deleted_sizes)
        print(
            f"ok: all {args.num_objects} objects deleted; "
            f"tier coverage {len(tier_cov)} tiers (min={min(tier_cov)}, max={max(tier_cov)})"
        )

        print("=== Resume GC ===")
        resume_gc(gc)
        print("PASS: gc delete queue (strict G:O verification)")

    finally:
        subprocess.run(
            ["aws", "--endpoint-url", args.endpoint, "s3", "rb", f"s3://{args.bucket}", "--force"],
            capture_output=True,
        )
        subprocess.run(["rm", "-rf", tmpdir], check=False)


if __name__ == "__main__":
    main()
