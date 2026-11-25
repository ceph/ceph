#!/usr/bin/env python3
"""
Basic S3 Vectors benchmark against RGW, driven by the s3vtest container image.

The RGW and the credentials come from the configuration file that the
S3VTESTS_CONF environment variable points to, the same file the pytest suite in
this directory uses (see s3vtests.conf.SAMPLE): host, port, zonegroup, cluster
and rgw_client from [DEFAULT], and user_id, access_key and secret_key from
[s3 main]. There are no defaults for them.

Phases, in order:
  1. PutVectors   - s3vtest creates one vector bucket and one index and loads an
                    ann-benchmarks dataset into it (create_vector_bucket,
                    create_index, insert_vectors).
  2. wait         - poll the RGW admin API (GET /admin/vectorbucket/rebuild) until
                    the background index rebuild of that index is finished: no
                    active build, at least one "finish" event, and no new event for
                    --rebuild-quiet seconds. The admin API needs the main user to
                    have the buckets=read cap, which is granted with radosgw-admin
                    first, as the pytest suite does.
  3. QueryVectors - s3vtest runs --queries queries against the built index
                    (query_vectors step, no recall measurement).
  4. GetVectors   - s3vtest fetches --get-percent of the inserted keys in batches
                    of --get-batch keys, with data and metadata (get_vectors step).
  5. cleanup      - s3vtest deletes the vector bucket, index and backing bucket
                    (skipped with --keep).

s3vtest is run with `podman run --rm -i --network host <image> run - --report json`,
the job configuration is passed on standard input. Dataset files are kept in a
named podman volume (default s3vtest-datasets), or in --datasets-dir on the host.
The container is killed when the benchmark is stopped with Ctrl-C. The code that
is shared with the other benchmarks is in s3vtest_benchmark_common.py.

Example:
  S3VTESTS_CONF=s3vtests.conf.SAMPLE ./s3vtest-basic-benchmark.py --threads 16
"""

import argparse
import os
import sys
import time

# the code that the benchmarks of this directory share
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import s3vtest_benchmark_common as common     # noqa: E402
from s3vtest_benchmark_common import log      # noqa: E402


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    w = common.add_workload_args(p, "s3vt-basic-")
    w.add_argument("--queries", type=int, default=1000, help="QueryVectors calls (default %(default)s)")
    w.add_argument("--get-percent", type=float, default=50, help="percent of inserted keys fetched (default %(default)s)")
    w.add_argument("--get-batch", type=int, default=100, help="keys per GetVectors (default %(default)s)")
    common.add_rebuild_args(p)
    common.add_execution_args(p)
    return p.parse_args()


def main():
    args = parse_args()
    common.check_conf()
    zone = common.Zone()
    dim, metric = common.resolve_dataset(args)
    bucket, index = common.bucket_and_index(args)
    log(f"{args.dataset}: {args.vectors} vectors, dim {dim}, {metric}; bucket {bucket} index {index}; "
        f"{args.threads} threads; endpoint {zone.endpoint} user {zone.user_id}; "
        f"backing bucket {zone.backing_bucket}")
    if not args.no_wait:
        zone.grant_admin_caps()

    since = int(time.time()) - 5          # admin API filter, epoch seconds
    results = {"dataset": args.dataset, "vectors": args.vectors, "dim": dim, "metric": metric,
               "threads": args.threads, "endpoint": zone.endpoint, "steps": []}

    put = common.s3vtest_load(args, zone)
    results["steps"] += put
    if any(s["errors"] for s in put):
        common.print_table(put, "put_vectors phase failed; not continuing")
        sys.exit(2)

    if args.no_wait:
        log("rebuild: wait skipped")
    else:
        results["rebuild"] = common.wait_for_rebuild(args, zone, bucket, index, since)

    results["steps"] += common.run_s3vtest(args, zone, "query_vectors",
                                           common.query_flow(args, args.queries))

    get = common.run_s3vtest(args, zone, "get_vectors", [
        {"get_vectors": {"percent": args.get_percent, "batch_size": args.get_batch,
                         "return_data": True, "return_metadata": True}}])
    for s in get:
        s["vectors"] = int(args.vectors * args.get_percent / 100)
    results["steps"] += get

    if not args.keep:
        results["cleanup_rc"] = common.s3vtest_cleanup(args, zone)

    print()
    common.print_table(results["steps"],
                       f"{args.dataset}: {args.vectors} vectors x {dim} dims ({metric}), "
                       f"PutVectors batch {args.batch_size}, QueryVectors top_k {args.top_k}, "
                       f"GetVectors batch {args.get_batch}, {args.threads} threads")
    if "rebuild" in results:
        r = results["rebuild"]
        print(f"index rebuild: {r['finishes']} build(s) {r['results']}, waited {r['wait_secs']:.0f}s")
    common.write_json(args, results)
    sys.exit(2 if any(s["errors"] for s in results["steps"]) else 0)


if __name__ == "__main__":
    common.run_main(main)
