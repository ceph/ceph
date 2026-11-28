#!/usr/bin/env python3
"""
S3 Vectors session benchmark against RGW, driven by the s3vtest container image.

Compares the performance of QueryVectors, and the use of the session caches,
between the session modes of a vector bucket:
  default   - a session with the default LanceDB cache sizes
              (rgw_s3vector_session_index_cache_size = 0 and
              rgw_s3vector_session_metadata_cache_size = 0).
  small     - a session whose caches are small compared to the dataset: both
              cache sizes are set to --small-cache-size bytes, by default
              --small-cache-percent of the raw size of the loaded vectors.
  disabled  - no session (rgw_s3vector_session_inactive_timeout = -1): every
              query uses a connection of its own, without the caches of a session.

The RGW and the credentials come from the configuration file that the
S3VTESTS_CONF environment variable points to, as in s3vtest-basic-benchmark.py.
The code that is shared with it is in s3vtest_benchmark_common.py. The s3vtest
container is killed when the benchmark is stopped with Ctrl-C; the session
options are still removed then, but the vector bucket is left in place.

Phases, in order:
  1. PutVectors   - s3vtest creates one vector bucket and one index and loads an
                    ann-benchmarks dataset into it.
  2. wait         - until the background index rebuild of that index is finished.
  3. for each of the --modes, in the given order:
     a. the session options of the mode are set on the RGW with "ceph config set",
        and the existing session of the vector bucket is deleted through the
        admin API (DELETE /admin/vectorbucket/session), so that the queries
        start with a new session that was created with the options of the mode.
     b. warmup: --warmup-queries queries. the first vector operation creates the
        session, asynchronously; the benchmark waits for it (or, when sessions
        are disabled, verifies that there is none).
     c. QueryVectors: --queries queries.
     d. the cache stats of the session are read from the admin API
        (GET /admin/vectorbucket/session).
  4. cleanup      - the session options are removed ("ceph config rm"), and
                    s3vtest deletes the vector bucket (skipped with --keep), also
                    when a phase failed. a vector bucket that a previous run left
                    behind is reused with --no-load, or deleted with --cleanup-first.

The admin API needs the main user to have the buckets=* cap, which is granted
with radosgw-admin first, as the pytest suite does.

Example:
  S3VTESTS_CONF=s3vtests.conf.SAMPLE ./s3vtest-session-benchmark.py --dataset sift-128-euclidean
"""

import argparse
import json
import os
import sys
import time
import urllib.error

# the code that the benchmarks of this directory share
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import s3vtest_benchmark_common as common     # noqa: E402
from s3vtest_benchmark_common import log      # noqa: E402

MODES = ("default", "small", "disabled")
INDEX_CACHE_OPT = "rgw_s3vector_session_index_cache_size"
METADATA_CACHE_OPT = "rgw_s3vector_session_metadata_cache_size"
TIMEOUT_OPT = "rgw_s3vector_session_inactive_timeout"
CACHE_NAMES = ("index_cache", "metadata_cache")
MB = 1024 * 1024


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    w = common.add_workload_args(p, "s3vt-session-")
    w.add_argument("--queries", type=int, default=1000,
                   help="measured QueryVectors calls of each mode (default %(default)s)")
    w.add_argument("--warmup-queries", type=int, default=50,
                   help="QueryVectors calls of each mode before the measured ones (default %(default)s)")

    s = p.add_argument_group("sessions")
    s.add_argument("--modes", nargs="+", default=list(MODES), choices=MODES, metavar="MODE",
                   help="session modes to benchmark, in this order (default: %(default)s)")
    s.add_argument("--small-cache-size", type=int,
                   help="bytes of each of the two caches in the 'small' mode "
                        "(default: --small-cache-percent of the raw size of the vectors)")
    s.add_argument("--small-cache-percent", type=float, default=1,
                   help="size of each cache in the 'small' mode, in percent of the raw "
                        "size of the loaded vectors (default %(default)s)")
    s.add_argument("--inactive-timeout", type=int, default=0,
                   help="rgw_s3vector_session_inactive_timeout of the modes that use a session; "
                        "0 never evicts it (default %(default)s)")
    s.add_argument("--session-timeout", type=float, default=30,
                   help="seconds to wait for a session to be created or removed (default %(default)s)")

    common.add_rebuild_args(p)
    e = common.add_execution_args(p)
    e.add_argument("--no-load", action="store_true",
                   help="use the vector bucket that a previous run left with --keep")
    e.add_argument("--cleanup-first", action="store_true",
                   help="delete the vector bucket that a previous run left behind, before loading")
    return p.parse_args()


# ---------------------------------------------------------------- tested zone

class Zone(common.Zone):
    """The RGW under test, with the session options and the session admin API."""

    def set_option(self, option, value):
        out, rc = self.ceph(["config", "set", self.rgw_client, option, str(value)])
        if rc != 0:
            sys.exit(f"could not set {option}={value} on {self.rgw_client} (rc={rc}): {out.strip()[-300:]}")

    def remove_option(self, option):
        out, rc = self.ceph(["config", "rm", self.rgw_client, option])
        if rc != 0:
            log(f"warning: could not remove {option} of {self.rgw_client} (rc={rc}): {out.strip()[-300:]}")

    def _session_request(self, method, bucket):
        params = {"vectorbucket": bucket}
        if self.tenant:
            params["tenant"] = self.tenant
        try:
            return self.admin_request(method, "/admin/vectorbucket/session", params)
        except urllib.error.HTTPError as e:
            sys.exit(f"admin API {method} /admin/vectorbucket/session: HTTP {e.code} {e.read()[:200]!r} "
                     f"(user {self.user_id} needs the buckets=* cap)")

    def get_session(self, bucket):
        """The "session" section of GET /admin/vectorbucket/session: whether the
        session is active and, when it is, the stats of its caches."""
        return json.loads(self._session_request("GET", bucket))["session"]

    def delete_session(self, bucket):
        self._session_request("DELETE", bucket)

    def wait_for_session(self, bucket, active, timeout):
        deadline = time.monotonic() + timeout
        while True:
            session = self.get_session(bucket)
            if session["active"] == active:
                return session
            if time.monotonic() > deadline:
                sys.exit(f"session of {bucket} did not become active={active} in {timeout}s")
            time.sleep(0.5)


# ---------------------------------------------------------------- modes

def mode_options(args, mode, small_cache_size):
    """The session options of a mode."""
    if mode == "disabled":
        return {TIMEOUT_OPT: -1, INDEX_CACHE_OPT: 0, METADATA_CACHE_OPT: 0}
    size = small_cache_size if mode == "small" else 0
    return {TIMEOUT_OPT: args.inactive_timeout, INDEX_CACHE_OPT: size, METADATA_CACHE_OPT: size}


def run_mode(args, zone, mode, options, bucket):
    """Benchmark the queries of one session mode. returns its results."""
    log(f"mode {mode}: {options}")
    for option, value in options.items():
        zone.set_option(option, value)
    # the cache sizes are read when a session is created, so the session of the
    # previous mode must go. when sessions are disabled the manager removes it
    zone.delete_session(bucket)
    zone.wait_for_session(bucket, False, args.session_timeout)

    result = {"mode": mode, "options": options}
    if args.warmup_queries > 0:
        # the first vector operation asks the manager for the session
        warmup = common.run_s3vtest(args, zone, f"{mode}: warmup",
                                    common.query_flow(args, args.warmup_queries))
        result["warmup"] = warmup
        if any(s["errors"] for s in warmup):
            log(f"mode {mode}: warmup queries failed: {[s.get('error_codes') for s in warmup]}")
    if mode == "disabled":
        # give the manager the time to wrongly create one
        time.sleep(2)
        if zone.get_session(bucket)["active"]:
            sys.exit(f"mode {mode}: {bucket} has a session although sessions are disabled")
    elif args.warmup_queries > 0:
        zone.wait_for_session(bucket, True, args.session_timeout)
    stats_before = zone.get_session(bucket)

    steps = common.run_s3vtest(args, zone, f"{mode}: query_vectors",
                               common.query_flow(args, args.queries))
    for s in steps:
        s["step"] = f"query ({mode})"
    result["steps"] = steps
    result["session_before"] = stats_before
    result["session"] = zone.get_session(bucket)
    return result


def cleanup_options(zone):
    for option in (TIMEOUT_OPT, INDEX_CACHE_OPT, METADATA_CACHE_OPT):
        zone.remove_option(option)


# ---------------------------------------------------------------- report

def _mb(value):
    return f"{value / MB:.1f}" if value is not None else "-"


def _cache_delta(result, cache_name, counter):
    """The change of a cache counter during the measured queries."""
    after = result["session"].get(cache_name)
    if after is None:
        return None
    before = result["session_before"].get(cache_name) or {}
    return after[counter] - before.get(counter, 0)


def print_table(results, title):
    """One row per mode: the performance of its measured queries, and the stats
    of the caches of its session. the hits are of the measured queries only."""
    cols = [("mode", "<", 9), ("session", "<", 7), ("cache cfg MB", ">", 12),
            ("ops", ">", 7), ("errors", ">", 6), ("ops/s", ">", 8),
            ("p50 ms", ">", 9), ("p90 ms", ">", 9), ("p99 ms", ">", 9), ("max ms", ">", 9), ("wall s", ">", 7),
            ("idx cache MB", ">", 12), ("idx entries", ">", 11), ("idx hit %", ">", 9),
            ("meta cache MB", ">", 13), ("meta entries", ">", 12), ("meta hit %", ">", 10)]
    print(title)
    print(" ".join(f"{c[0]:{c[1]}{c[2]}}" for c in cols))
    for r in results:
        session = r["session"]
        size = r["options"][INDEX_CACHE_OPT]
        cache_vals = []
        for cache_name in CACHE_NAMES:
            stats = session.get(cache_name, {})
            hits = _cache_delta(r, cache_name, "hits")
            misses = _cache_delta(r, cache_name, "misses")
            cache_vals += [_mb(stats.get("size_bytes")), stats.get("num_entries", "-"),
                           f"{100 * hits / (hits + misses):.1f}" if hits is not None and hits + misses > 0 else "-"]
        for s in r["steps"]:
            vals = [r["mode"], "yes" if session["active"] else "no",
                    "-" if r["mode"] == "disabled" else ("default" if size == 0 else _mb(size)),
                    s["ops"], s["errors"], f"{s['ops_per_sec']:.1f}",
                    f"{s['p50_ms']:.1f}", f"{s['p90_ms']:.1f}", f"{s['p99_ms']:.1f}", f"{s['max_ms']:.1f}",
                    f"{s['wall_secs']:.1f}"] + cache_vals
            print(" ".join(f"{str(v):{c[1]}{c[2]}}" for v, c in zip(vals, cols)))
            if s.get("error_codes"):
                print(f"    errors: {s['error_codes']}")


def main():
    args = parse_args()
    common.check_conf()
    zone = Zone()
    dim, metric = common.resolve_dataset(args)
    raw_bytes = args.vectors * dim * 4      # float32 vectors
    small_cache_size = args.small_cache_size
    if small_cache_size is None:
        small_cache_size = max(1, int(raw_bytes * args.small_cache_percent / 100))
    bucket, index = common.bucket_and_index(args)

    log(f"{args.dataset}: {args.vectors} vectors, dim {dim}, {metric}, {raw_bytes / MB:.1f} MB raw; "
        f"bucket {bucket} index {index}; {args.threads} threads; endpoint {zone.endpoint} "
        f"user {zone.user_id}; RGW {zone.rgw_client}; modes {args.modes}; "
        f"small cache {small_cache_size} bytes")
    # buckets=* lets the main user read and delete sessions with the admin API
    zone.grant_admin_caps("buckets=*")

    since = int(time.time()) - 5          # admin API filter, epoch seconds
    results = {"dataset": args.dataset, "vectors": args.vectors, "dim": dim, "metric": metric,
               "raw_bytes": raw_bytes, "small_cache_size": small_cache_size,
               "threads": args.threads, "queries": args.queries, "endpoint": zone.endpoint,
               "modes": []}

    if args.cleanup_first and not args.no_load:
        common.s3vtest_cleanup(args, zone)

    # the vector bucket is deleted at the end, also when a phase failed, unless it
    # was not created by this run
    created = not args.no_load
    interrupted = False
    try:
        if not args.no_load:
            put = common.s3vtest_load(args, zone)
            results["load"] = put
            if any(s["errors"] for s in put):
                common.print_table(put, "put_vectors phase failed; not continuing")
                if any(s["step"] == "create_vector_bucket" and "HTTP409" in (s.get("error_codes") or {})
                       for s in put):
                    created = False
                    sys.exit(f"vector bucket {bucket} already exists, probably left by a previous run: "
                             "use --cleanup-first to delete it and load again, "
                             "or --no-load to benchmark the vectors it holds")
                sys.exit(2)
            if args.no_wait:
                log("rebuild: wait skipped")
            else:
                results["rebuild"] = common.wait_for_rebuild(args, zone, bucket, index, since)

        for mode in args.modes:
            results["modes"].append(
                run_mode(args, zone, mode, mode_options(args, mode, small_cache_size), bucket))
    except KeyboardInterrupt:
        interrupted = True
        common.kill_container()
        raise
    finally:
        # also when a mode failed: do not leave the RGW with sessions disabled
        cleanup_options(zone)
        if interrupted:
            if created and not args.keep:
                log(f"interrupted: vector bucket {bucket} is left in place. "
                    "use --cleanup-first to delete it in the next run, or --no-load to use it")
        elif created and not args.keep:
            results["cleanup_rc"] = common.s3vtest_cleanup(args, zone)

    steps = [s for r in results["modes"] for s in r["steps"]]
    print()
    print_table(results["modes"],
                f"{args.dataset}: {args.vectors} vectors x {dim} dims ({metric}, "
                f"{raw_bytes / MB:.1f} MB raw), QueryVectors top_k {args.top_k}, "
                f"{args.queries} queries per mode, {args.threads} threads")
    common.write_json(args, results)
    sys.exit(2 if any(s["errors"] for s in steps) else 0)


if __name__ == "__main__":
    common.run_main(main)
