#!/usr/bin/env python3
"""Verify bucket: list (aws + s5cmd), bulk download (s5cmd sync), diff vs kvrgw tree."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(__file__))
from list_verify import (  # noqa: E402
    aws_list_all,
    s5cmd_list_all,
    s5cmd_run,
    verify_count_after_sync,
)


def _ts() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def step(msg: str) -> None:
    print(f"[{_ts()}] STEP: {msg}")


def ok(msg: str) -> None:
    print(f"[{_ts()}] OK: {msg}")


def s5cmd_sync_down(endpoint: str, bucket: str, dest: str) -> None:
    os.makedirs(dest, exist_ok=True)
    cmd = [
        "s5cmd",
        "--endpoint-url",
        endpoint,
        "sync",
        f"s3://{bucket}/*",
        f"{dest}/",
    ]
    s5cmd_run(f"s5cmd-sync-down-{bucket}", cmd)


def diff_trees(left: str, right: str) -> None:
    proc = subprocess.run(
        ["diff", "-rq", left, right],
        capture_output=True,
        text=True,
    )
    if proc.returncode > 1:
        raise RuntimeError(f"diff failed: {proc.stderr.strip()}")
    if proc.returncode == 1:
        detail = (proc.stdout + proc.stderr).strip()
        raise RuntimeError(f"tree mismatch:\n{detail}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--kvrgw", required=True, help="Local directory with source files")
    p.add_argument("--expected", type=int, default=65536)
    args = p.parse_args()

    step(f"list aws bucket={args.bucket}")
    aws_keys = verify_count_after_sync(
        aws_list_all,
        args.endpoint,
        args.bucket,
        expected=args.expected,
        sync_label=f"verify-{args.bucket}",
    )
    ok(f"aws list count={len(aws_keys)}")

    step(f"list s5cmd bucket={args.bucket}")
    s5_keys = verify_count_after_sync(
        s5cmd_list_all,
        args.endpoint,
        args.bucket,
        expected=args.expected,
        sync_label=f"verify-{args.bucket}",
    )
    ok(f"s5cmd ls count={len(s5_keys)}")

    tmp = tempfile.mkdtemp(prefix="kv-verify-")
    try:
        step(f"s5cmd sync download bucket={args.bucket}")
        s5cmd_sync_down(args.endpoint, args.bucket, tmp)
        dl_files = [f for f in os.listdir(tmp) if os.path.isfile(os.path.join(tmp, f))]
        if len(dl_files) != args.expected:
            raise RuntimeError(
                f"download file count {len(dl_files)} != expected {args.expected}"
            )
        ok(f"downloaded {len(dl_files)} files")

        step(f"diff kvrgw={args.kvrgw} download={tmp}")
        diff_trees(args.kvrgw, tmp)
        ok("kvrgw matches download")
    finally:
        subprocess.call(["rm", "-rf", tmp])

    ok(f"verify_bucket_all bucket={args.bucket} count={args.expected}")


if __name__ == "__main__":
    main()
