#!/usr/bin/env python3
"""Verify S3 list results: strict key order (KV(n) < KV(n+1)), no duplicates, count rules."""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional


def verify_keys(
    keys: list[str],
    *,
    expected: Optional[int] = None,
    max_count: Optional[int] = None,
) -> int:
    """Return key count; exit 1 on violation."""
    count = len(keys)
    if expected is not None and count != expected:
        print(f"FAIL: count {count} != expected {expected}", file=sys.stderr)
        sys.exit(1)
    if max_count is not None and count > max_count:
        print(f"FAIL: count {count} > max {max_count}", file=sys.stderr)
        sys.exit(1)

    seen: set[str] = set()
    prev: Optional[str] = None
    for key in keys:
        if key in seen:
            print(f"FAIL: duplicate key {key!r}", file=sys.stderr)
            sys.exit(1)
        seen.add(key)
        if prev is not None and key <= prev:
            print(f"FAIL: keys not strictly increasing: {prev!r} then {key!r}", file=sys.stderr)
            sys.exit(1)
        prev = key

    return count


def keys_from_s3cmd(text: str) -> list[str]:
    keys: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.search(r"s3://[^/]+/(.+)$", line)
        if m:
            keys.append(m.group(1))
    return keys


def keys_from_aws_pages(out_dir: str) -> list[str]:
    keys: list[str] = []
    for path in sorted(glob.glob(os.path.join(out_dir, "page-*.json"))):
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        keys.extend(o["Key"] for o in (data.get("Contents") or []))
    return keys


def aws_list_all(endpoint: str, bucket: str, prefix: str = "") -> list[str]:
    token = ""
    keys: list[str] = []
    while True:
        cmd = [
            "aws",
            "--endpoint-url",
            endpoint,
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--output",
            "json",
        ]
        if prefix:
            cmd += ["--prefix", prefix]
        if token:
            cmd += ["--continuation-token", token]
        data = json.loads(subprocess.check_output(cmd, text=True))
        keys.extend(o["Key"] for o in (data.get("Contents") or []))
        if not data.get("IsTruncated"):
            break
        token = data.get("NextContinuationToken") or ""
        if not token:
            print("FAIL: IsTruncated but no NextContinuationToken", file=sys.stderr)
            sys.exit(1)
    return keys


def s3cmd_list_all(config: str, bucket: str, prefix: str = "") -> list[str]:
    uri = f"s3://{bucket}/"
    if prefix:
        uri = f"s3://{bucket}/{prefix}"
    out = subprocess.check_output(
        ["s3cmd", "-c", config, "ls", uri],
        text=True,
        stderr=subprocess.DEVNULL,
    )
    return keys_from_s3cmd(out)


def cmd_verify(args: argparse.Namespace) -> None:
    if args.stdin:
        keys = [line.strip() for line in sys.stdin if line.strip()]
    elif args.keys:
        keys = list(args.keys)
    else:
        print("FAIL: no keys provided", file=sys.stderr)
        sys.exit(1)

    count = verify_keys(keys, expected=args.expected, max_count=args.max_count)
    print(f"ok: {count} keys, strictly increasing")


def cmd_aws(args: argparse.Namespace) -> None:
    keys = aws_list_all(args.endpoint, args.bucket, args.prefix or "")
    count = verify_keys(keys, expected=args.expected, max_count=args.max_count)
    print(f"ok: aws {args.bucket} count={count} strictly increasing")


def _s5cmd_log_path() -> str:
    env = os.environ.get("KVRGW_S5CMD_LOG")
    if env:
        return env
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root, ".logs", "s5cmd.log")


def _append_s5cmd_log(label: str, cmd: list[str], rc: int, stderr: str) -> None:
    path = _s5cmd_log_path()
    log_dir = os.path.dirname(path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(f"[{ts}] {label}: {' '.join(cmd)} (exit={rc})\n")
        if stderr:
            fh.write(stderr)
            if not stderr.endswith("\n"):
                fh.write("\n")
        fh.write("\n")


def s5cmd_run(label: str, cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        _append_s5cmd_log(label, cmd, proc.returncode, proc.stderr or "")
        if check:
            print(
                f"FAIL: s5cmd {label} exit={proc.returncode} see {_s5cmd_log_path()}",
                file=sys.stderr,
            )
            sys.exit(1)
    return proc


def verify_count_after_sync(
    list_fn: Callable[..., list[str]],
    *args: object,
    expected: int,
    sync_label: str,
    **kwargs: object,
) -> list[str]:
    """Re-list once on count mismatch when sync already succeeded (exit 0)."""
    keys = list_fn(*args, **kwargs)
    count = len(keys)
    if count == expected:
        verify_keys(keys, expected=expected)
        return keys
    print(
        f"WARN: list count {count} != {expected} after {sync_label}; re-list in 2s",
        file=sys.stderr,
    )
    import time

    time.sleep(2)
    keys2 = list_fn(*args, **kwargs)
    count2 = len(keys2)
    if count2 == expected:
        print(
            f"FAIL: test timing: list before visible ({count} then {count2}); "
            "should not happen on POC",
            file=sys.stderr,
        )
        sys.exit(1)
    log_path = _s5cmd_log_path()
    print(
        f"FAIL: lost objects suspected: {sync_label} exit 0 but list "
        f"{count} then {count2} != {expected}; see {log_path}",
        file=sys.stderr,
    )
    sys.exit(1)


def s5cmd_list_all(endpoint: str, bucket: str, prefix: str = "") -> list[str]:
    if prefix:
        uri = f"s3://{bucket}/{prefix}*"
    else:
        uri = f"s3://{bucket}/*"
    cmd = ["s5cmd", "--endpoint-url", endpoint, "ls", uri]
    proc = s5cmd_run(f"s5cmd-ls-{bucket}", cmd)
    out = proc.stdout
    base = f"s3://{bucket}/"
    keys: list[str] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        path = line.split()[-1]
        if path.startswith(base):
            keys.append(path[len(base) :])
        elif path.startswith("s3://"):
            keys.append(path.rsplit("/", 1)[-1])
        else:
            keys.append(path)
    return keys


def cmd_s3cmd(args: argparse.Namespace) -> None:
    keys = s3cmd_list_all(args.config, args.bucket, args.prefix or "")
    count = verify_keys(keys, expected=args.expected, max_count=args.max_count)
    print(f"ok: s3cmd {args.bucket} count={count} strictly increasing")


def cmd_s5cmd(args: argparse.Namespace) -> None:
    keys = s5cmd_list_all(args.endpoint, args.bucket, args.prefix or "")
    count = verify_keys(keys, expected=args.expected, max_count=args.max_count)
    print(f"ok: s5cmd {args.bucket} count={count} strictly increasing")


def cmd_pages(args: argparse.Namespace) -> None:
    keys = keys_from_aws_pages(args.dir)
    count = verify_keys(keys, expected=args.expected, max_count=args.max_count)
    print(f"ok: pages count={count} strictly increasing")


def expected_bucket_names(name_prefix: str, count: int) -> set[str]:
    return {f"{name_prefix}{i:04d}" for i in range(1, count + 1)}


def normalize_bucket_prefix(prefix: str) -> str:
    return prefix if prefix.endswith("-") else f"{prefix}-"


def verify_bucket_names(
    names: list[str],
    *,
    prefix: str,
    expected: Optional[int] = None,
    expected_names: Optional[set[str]] = None,
    check_creation_date: bool = False,
    buckets: Optional[list[dict]] = None,
) -> int:
    """Return bucket count for names matching prefix; exit 1 on violation."""
    name_prefix = normalize_bucket_prefix(prefix)
    filtered = [n for n in names if n.startswith(name_prefix)]
    count = len(filtered)
    if expected is not None and count != expected:
        print(f"FAIL: bucket count {count} != expected {expected}", file=sys.stderr)
        sys.exit(1)

    verify_keys(filtered, expected=expected)

    if expected_names is not None:
        found = set(filtered)
        missing = expected_names - found
        extra = found - expected_names
        if missing:
            print(f"FAIL: missing buckets: {sorted(missing)[:5]}...", file=sys.stderr)
            sys.exit(1)
        if extra:
            print(f"FAIL: unexpected buckets with prefix {prefix!r}: {sorted(extra)[:5]}...", file=sys.stderr)
            sys.exit(1)

    if check_creation_date:
        if buckets is None:
            print("FAIL: check_creation_date requires buckets metadata", file=sys.stderr)
            sys.exit(1)
        by_name = {b["Name"]: b for b in buckets if b["Name"].startswith(name_prefix)}
        for name in filtered:
            raw = by_name[name].get("CreationDate")
            if not raw:
                print(f"FAIL: missing CreationDate for {name!r}", file=sys.stderr)
                sys.exit(1)
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if dt.year < 2020 or dt.year > 2100:
                print(f"FAIL: invalid CreationDate {raw!r} for {name!r}", file=sys.stderr)
                sys.exit(1)

    return count


def aws_list_buckets(endpoint: str) -> list[dict]:
    data = json.loads(
        subprocess.check_output(
            [
                "aws",
                "--endpoint-url",
                endpoint,
                "s3api",
                "list-buckets",
                "--output",
                "json",
            ],
            text=True,
        )
    )
    return list(data.get("Buckets") or [])


def cmd_aws_buckets(args: argparse.Namespace) -> None:
    buckets = aws_list_buckets(args.endpoint)
    names = [b["Name"] for b in buckets]
    name_prefix = normalize_bucket_prefix(args.prefix)
    expected_names = expected_bucket_names(name_prefix, args.expected)
    count = verify_bucket_names(
        names,
        prefix=args.prefix,
        expected=args.expected,
        expected_names=expected_names,
        check_creation_date=args.check_creation_date,
        buckets=buckets,
    )
    print(f"ok: list-buckets prefix={name_prefix!r} count={count} strictly increasing")


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Verify S3 list ordering and counts")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("verify", help="Verify keys from args or stdin")
    p.add_argument("--stdin", action="store_true", help="Read one key per line from stdin")
    p.add_argument("--expected", type=int, default=None, help="Exact count required")
    p.add_argument("--max-count", type=int, default=None, help="Count must be <= this")
    p.add_argument("keys", nargs="*", help="Key names")
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("aws", help="List via aws s3api and verify")
    p.add_argument("--endpoint", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", default="")
    p.add_argument("--expected", type=int, default=None)
    p.add_argument("--max-count", type=int, default=None)
    p.set_defaults(func=cmd_aws)

    p = sub.add_parser("s3cmd", help="List via s3cmd and verify")
    p.add_argument("--config", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", default="")
    p.add_argument("--expected", type=int, default=None)
    p.add_argument("--max-count", type=int, default=None)
    p.set_defaults(func=cmd_s3cmd)

    p = sub.add_parser("s5cmd", help="List via s5cmd ls and verify")
    p.add_argument("--endpoint", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", default="")
    p.add_argument("--expected", type=int, default=None)
    p.add_argument("--max-count", type=int, default=None)
    p.set_defaults(func=cmd_s5cmd)

    p = sub.add_parser("pages", help="Verify keys collected in page-*.json files")
    p.add_argument("--dir", required=True)
    p.add_argument("--expected", type=int, default=None)
    p.add_argument("--max-count", type=int, default=None)
    p.set_defaults(func=cmd_pages)

    p = sub.add_parser("aws-buckets", help="List buckets via aws s3api and verify")
    p.add_argument("--endpoint", required=True)
    p.add_argument("--prefix", required=True, help="Only buckets with this name prefix")
    p.add_argument("--expected", type=int, required=True, help="Exact matching bucket count")
    p.add_argument(
        "--check-creation-date",
        action="store_true",
        help="Validate CreationDate on each matching bucket",
    )
    p.set_defaults(func=cmd_aws_buckets)

    args = parser.parse_args(list(argv) if argv is not None else None)
    args.func(args)


if __name__ == "__main__":
    main()
