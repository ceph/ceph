#!/usr/bin/env python3
"""
Reproducer for RGW crash/freeze on realm reload during period update.

Precondition: a 2-zone multisite cluster is already running.

For single-cluster multisite (recommended, matches teuthology topology):

    cd build && ../src/test/rgw/test-rgw-multisite-single-cluster.sh \\
        --rgw-data-sync-poll-interval=5 \\
        --rgw-meta-sync-poll-interval=5 \\
        --rgw-sync-log-trim-interval=0 \\
        --rgw-data-notify-interval-msec=0 \\
        --debug-rgw=1

    python3 ../src/test/rgw/test-rgw-period-reload-crash.py \\
        --port=8001 --cluster=c1 --zone=z2

For multi-cluster multisite (test-rgw-multisite.sh):

    python3 ../src/test/rgw/test-rgw-period-reload-crash.py \\
        --port=8101 --cluster=c2 --zone=zg1-2

This script:
  1. Slams the primary zone RGW with concurrent PUT traffic via boto3
  2. Fires period update --commit from the secondary zone (with a zone config
     change each time so the period is genuinely new)
  3. Verifies that realm reloads actually fired (checks the RGW log)
  4. Checks whether the primary zone RGW crashed or froze
"""

import argparse
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import (
    ConnectionClosedError,
    EndpointConnectionError,
    ClientError,
)

SCRIPT_DIR = Path(__file__).resolve().parent
MRUN = SCRIPT_DIR / "../../mrun"

# Toggle between these compression types to force a genuine zone config change
# before each period commit, ensuring the period is actually new and the
# realm reload fires.
COMPRESSION_TOGGLE = ["zlib", "snappy"]


def mrun(cluster, *cmd):
    """Run a command via mrun against the given cluster."""
    result = subprocess.run(
        [str(MRUN), cluster] + list(cmd),
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout, result.stderr


def make_s3_client(port, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{port}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            retries={"max_attempts": 0},
            max_pool_connections=50,
        ),
    )


def process_running(port):
    """Return True if a radosgw process for this port is alive."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"radosgw.*{port}"],
            capture_output=True,
        )
        return result.returncode == 0
    except Exception:
        return False


def check_alive(port, access_key, secret_key, retries=6, delay=10):
    """Return 'ok', 'frozen', or 'crashed'.

    Retries the HTTP check several times to ride out reload pauses.
    Falls back to checking whether the process is still running.
    """
    for attempt in range(retries):
        try:
            s3 = make_s3_client(port, access_key, secret_key)
            s3.list_buckets()
            return "ok"
        except Exception:
            if not process_running(port):
                return "crashed"
            if attempt < retries - 1:
                time.sleep(delay)
    return "frozen"


def put_worker(client, bucket, worker_id, stop_event):
    """Continuously PUT small objects until told to stop."""
    body = b"x" * 4096
    i = 0
    while not stop_event.is_set():
        try:
            client.put_object(
                Bucket=bucket,
                Key=f"w{worker_id}-{i:06d}",
                Body=body,
            )
        except (ConnectionClosedError, EndpointConnectionError, ClientError):
            time.sleep(0.1)
        except Exception:
            time.sleep(0.1)
        i += 1


def poke_zone_config(cluster, zone, iteration):
    """Make a trivial zone config change so the next period commit is genuine."""
    compression = COMPRESSION_TOGGLE[iteration % len(COMPRESSION_TOGGLE)]
    rc, _, stderr = mrun(
        cluster, "radosgw-admin", "zone", "placement", "modify",
        "--rgw-zone", zone,
        "--placement-id", "default-placement",
        "--compression", compression,
    )
    if rc != 0:
        print(f"  warning: zone config tweak failed (rc={rc}): {stderr.strip()}")
    else:
        print(f"  toggled compression to {compression} on {zone}")


def period_update_commit(cluster):
    """Run radosgw-admin period update --commit."""
    return mrun(cluster, "radosgw-admin", "period", "update", "--commit")


def check_reload_fired(port, since_line):
    """Check if a realm reload was triggered since a given line in the RGW log.

    Returns (fired, current_line_count).
    """
    rgw_log = Path(f"run/c1/out/radosgw.{port}.log")
    if not rgw_log.exists():
        return False, since_line

    lines = rgw_log.read_text().splitlines()
    current_count = len(lines)

    for line in lines[since_line:]:
        if "reconfiguration scheduled" in line or "Pausing frontends" in line:
            return True, current_count

    return False, current_count


def report_crash(port):
    """Print diagnostic info after a crash or freeze."""
    rgw_log = Path(f"run/c1/out/radosgw.{port}.log")
    if rgw_log.exists():
        print(f"\nRGW log: {rgw_log}")
        print("Last 30 lines:")
        lines = rgw_log.read_text().splitlines()
        for line in lines[-30:]:
            print(f"  {line}")

    cores = sorted(Path(".").glob("core*"), key=lambda p: p.stat().st_mtime, reverse=True)
    if cores:
        print(f"\nCore dump found: {cores[0]}")
        print("Get a backtrace with:")
        print(f"  gdb bin/radosgw {cores[0]} -ex 'thread apply all bt' -ex quit")
    else:
        print("\nNo core dump found. If the process is still running (frozen),")
        print("get a live backtrace with:")
        print(f"  gdb -batch -ex 'set pagination off' -ex 'thread apply all bt' "
              f"-p $(pgrep -f 'radosgw.*{port}') 2>&1 | tee /tmp/rgw-frozen-bt.txt")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=8001,
                        help="Primary zone RGW port (default: 8001 for single-cluster)")
    parser.add_argument("--cluster", default="c1",
                        help="Cluster to issue period updates from (default: c1)")
    parser.add_argument("--zone", default="z2",
                        help="Secondary zone name to poke config on (default: z2)")
    parser.add_argument("--access-key", default="0987654321",
                        help="S3 access key")
    parser.add_argument("--secret-key", default="crayon",
                        help="S3 secret key")
    parser.add_argument("--writers", type=int, default=25,
                        help="Number of concurrent PUT writer threads")
    parser.add_argument("--updates", type=int, default=10,
                        help="Number of period update commits to fire")
    parser.add_argument("--sleep", type=int, default=15,
                        help="Seconds to wait between period updates")
    args = parser.parse_args()

    port = args.port
    endpoint = f"http://localhost:{port}"

    print("=== RGW period reload crash reproducer ===")
    print(f"Primary zone RGW: {endpoint}")
    print(f"Period updates from cluster: {args.cluster}, zone config poke: {args.zone}")
    print(f"Writers: {args.writers}")
    print(f"Period updates: {args.updates}")
    print(f"Sleep between updates: {args.sleep}s")
    print()

    if check_alive(port, args.access_key, args.secret_key, retries=1, delay=0) != "ok":
        print(f"FATAL: primary zone RGW is not responding on {endpoint}")
        sys.exit(1)
    print("Primary zone RGW is alive.")

    rgw_log = Path(f"run/c1/out/radosgw.{port}.log")
    log_line = len(rgw_log.read_text().splitlines()) if rgw_log.exists() else 0

    client = make_s3_client(port, args.access_key, args.secret_key)
    bucket = f"reload-crash-test-{os.getpid()}"

    client.create_bucket(Bucket=bucket)
    print(f"Created bucket: {bucket}")

    stop_event = threading.Event()
    threads = []
    print(f"Starting {args.writers} background PUT writers...")
    for i in range(args.writers):
        t = threading.Thread(target=put_worker,
                             args=(client, bucket, i, stop_event),
                             daemon=True)
        t.start()
        threads.append(t)

    time.sleep(3)

    print()
    print("=== Firing period update commits ===")
    crashed = False
    reloads_confirmed = 0
    try:
        for i in range(1, args.updates + 1):
            print(f"\n--- period update #{i} of {args.updates} ---")

            poke_zone_config(args.cluster, args.zone, i)

            rc, _, stderr = period_update_commit(args.cluster)
            if rc != 0:
                print(f"  period update returned {rc} (may be expected if RGW died)")
                if stderr:
                    print(f"  stderr: {stderr.strip()}")
            else:
                print("  period committed")

            time.sleep(args.sleep)

            fired, log_line = check_reload_fired(port, log_line)
            if fired:
                print("  realm reload confirmed in RGW log")
                reloads_confirmed += 1
            else:
                print("  WARNING: no realm reload detected in RGW log!")

            status = check_alive(port, args.access_key, args.secret_key)
            if status == "crashed":
                print()
                print("************************************************************")
                print(f"* RGW CRASHED after period update #{i}!")
                print("************************************************************")
                report_crash(port)
                crashed = True
                return
            elif status == "frozen":
                print()
                print("************************************************************")
                print(f"* RGW FROZEN after period update #{i}!")
                print(f"* (process alive but not responding after 60s)")
                print("************************************************************")
                report_crash(port)
                crashed = True
                return

            print(f"RGW still alive after period update #{i}")

        print()
        print(f"=== All {args.updates} period updates completed without crash ===")
        print(f"    (reloads confirmed: {reloads_confirmed}/{args.updates})")
        if reloads_confirmed == 0:
            print("    WARNING: no reloads were detected! The bug was NOT exercised.")
        else:
            print("    (If testing the fix, this is the expected result.)")

    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=5)


if __name__ == "__main__":
    main()
