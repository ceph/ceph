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

Example:
  S3VTESTS_CONF=s3vtests.conf.SAMPLE ./s3vtest-basic-benchmark.py --threads 16
"""

import argparse
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
import warnings

warnings.filterwarnings("ignore")
import yaml                                   # noqa: E402
from botocore.auth import S3SigV4Auth         # noqa: E402
from botocore.awsrequest import AWSRequest    # noqa: E402
from botocore.credentials import Credentials  # noqa: E402

# the configuration helpers of the pytest suite, from this directory's __init__.py
test_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(test_dir))
import s3vectors as conf                      # noqa: E402

# name -> (dimension, metric, train vectors); the datasets `s3vtest datasets` lists
DATASETS = {
    "fashion-mnist-784-euclidean": (784, "euclidean", 60000),
    "sift-128-euclidean": (128, "euclidean", 1000000),
    "gist-960-euclidean": (960, "euclidean", 1000000),
    "glove-25-angular": (25, "cosine", 1183514),
    "glove-50-angular": (50, "cosine", 1183514),
    "glove-100-angular": (100, "cosine", 1183514),
    "nytimes-256-angular": (256, "cosine", 290000),
    "deep-image-96-angular": (96, "cosine", 9990000),
}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    w = p.add_argument_group("workload")
    w.add_argument("--dataset", default="fashion-mnist-784-euclidean", choices=sorted(DATASETS),
                   help="ann-benchmarks dataset (default %(default)s)")
    w.add_argument("--vectors", type=int, help="vectors to insert (default: the whole dataset)")
    w.add_argument("--batch-size", type=int, default=500, help="vectors per PutVectors (default %(default)s)")
    w.add_argument("--threads", type=int, default=16, help="in-flight requests in every phase (default %(default)s)")
    w.add_argument("--queries", type=int, default=1000, help="QueryVectors calls (default %(default)s)")
    w.add_argument("--top-k", type=int, default=10)
    w.add_argument("--get-percent", type=float, default=50, help="percent of inserted keys fetched (default %(default)s)")
    w.add_argument("--get-batch", type=int, default=100, help="keys per GetVectors (default %(default)s)")
    w.add_argument("--prefix", default="s3vt-basic-", help="vector bucket prefix (default %(default)s)")
    w.add_argument("--key-prefix", default="vec-")
    w.add_argument("--seed", type=int, default=42)

    r = p.add_argument_group("rebuild wait")
    r.add_argument("--rebuild-quiet", type=float, default=15,
                   help="seconds without new rebuild events before the index counts as built (default %(default)s)")
    r.add_argument("--rebuild-timeout", type=float, default=1800, help="seconds (default %(default)s)")
    r.add_argument("--no-wait", action="store_true", help="skip the rebuild wait")

    e = p.add_argument_group("execution")
    e.add_argument("--image", default="ghcr.io/yuvalif/s3vtest:latest")
    e.add_argument("--datasets-dir", help="host directory holding the .hdf5 files; default: podman volume s3vtest-datasets")
    e.add_argument("--keep", action="store_true", help="leave the vector bucket in place")
    e.add_argument("--json-out", help="write all phase results as JSON to this file")
    return p.parse_args()


# ---------------------------------------------------------------- tested zone

class Zone:
    """The RGW under test and its credentials, from S3VTESTS_CONF."""

    def __init__(self):
        conf.setup()
        self.host = conf.get_config_host()
        self.port = conf.get_config_port()
        self.region = conf.get_config_zonegroup()
        self.cluster = conf.get_config_cluster()
        self.user_id = conf.get_user_id()
        self.access_key = conf.get_access_key()
        self.secret_key = conf.get_secret_key()
        self.backing_bucket = conf.has_backing_bucket()
        for name in ("host", "port", "region", "user_id", "access_key", "secret_key"):
            if getattr(self, name) is None:
                sys.exit(f"{os.environ['S3VTESTS_CONF']}: {name} is not set")
        self.scheme = "https://" if self.port in (443, 8443) else "http://"
        self.endpoint = f"{self.scheme}{self.host}:{self.port}"

    def radosgw_admin(self, args):
        """radosgw-admin against the tested cluster, as the pytest suite runs it:
        directly for an installed cluster, through test-rgw-call.sh for a vstart one."""
        if os.path.exists(f"/etc/ceph/{self.cluster}.conf"):
            cmd = ["radosgw-admin", "--cluster", self.cluster,
                   "-n", conf.get_config_rgw_client() or "client.0"] + args
        else:
            cmd = [os.path.join(test_dir, "..", "test-rgw-call.sh"), "call_rgw_admin", self.cluster] + args
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return res.stdout.decode("utf-8", "replace"), res.returncode

    def grant_admin_caps(self):
        """buckets=read lets the main user call GET /admin/vectorbucket."""
        out, rc = self.radosgw_admin(["caps", "add", "--uid", self.user_id, "--caps", "buckets=read"])
        if rc != 0:
            log(f"warning: could not grant buckets=read to {self.user_id} (rc={rc}): {out.strip()[-300:]}")

    def admin_get(self, path, params):
        # Send the Host header explicitly: botocore signs the host without a
        # default port (80/443) while urllib sends it with the port of the URL,
        # and RGW signs what it received.
        url = f"{self.endpoint}{path}"
        req = AWSRequest(method="GET", url=url, params=params, headers={"Host": f"{self.host}:{self.port}"})
        # S3SigV4Auth, not SigV4Auth: RGW expects x-amz-content-sha256 and S3 canonicalization
        S3SigV4Auth(Credentials(self.access_key, self.secret_key), "s3", self.region).add_auth(req)
        prep = req.prepare()
        ctx = None
        if self.scheme == "https://":
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(urllib.request.Request(prep.url, headers=dict(prep.headers)),
                                    timeout=30, context=ctx) as r:
            return json.loads(r.read())


# ---------------------------------------------------------------- s3vtest via podman

def podman_cmd(args, extra):
    cmd = ["podman", "run", "--rm", "-i", "--network", "host"]
    if args.datasets_dir:
        cmd += ["-v", f"{os.path.abspath(args.datasets_dir)}:/work/datasets:Z"]
    else:
        cmd += ["-v", "s3vtest-datasets:/work/datasets"]
    return cmd + [args.image] + extra


def job_config(args, zone, flow):
    dim, metric, _ = DATASETS[args.dataset]
    return {
        "connection": {
            "endpoint": zone.endpoint,
            "access_key": zone.access_key,
            "secret_key": zone.secret_key,
            "region": zone.region,
            "retries": 0,
        },
        "entities": {
            "vector_buckets": {"count": 1, "prefix": args.prefix, "create_backing_bucket": zone.backing_bucket},
            "indexes_per_bucket": {"count": 1, "prefix": "idx-"},
            "index": {"dimension": dim, "metric": metric},
            "vectors_per_index": args.vectors,
            "insert_batch_size": args.batch_size,
            "key_prefix": args.key_prefix,
            "vectors": {"source": {"dataset": args.dataset}},
        },
        "flow": flow,
        "execution": {"threads": args.threads, "repeat": 1, "seed": args.seed,
                      "datasets_dir": "datasets", "max_logged_errors": 10},
    }


def run_s3vtest(args, zone, name, flow):
    """Run one s3vtest job in the container; return its step summaries (list of dicts)."""
    cfg = yaml.safe_dump(job_config(args, zone, flow), sort_keys=False)
    cmd = podman_cmd(args, ["run", "-", "--report", "json"])
    log(f"{name}: " + " ".join(cmd))
    res = subprocess.run(cmd, input=cfg.encode(), stdout=subprocess.PIPE)
    if res.returncode not in (0, 2) or not res.stdout.strip():
        sys.exit(f"{name}: s3vtest exited with {res.returncode} and no report")
    reports = json.loads(res.stdout)
    if isinstance(reports, dict):
        reports = [reports]
    steps = []
    for rep in reports:
        if rep.get("aborted"):
            log(f"{name}: job aborted: {rep['aborted']}")
        steps += rep["steps"]
    return steps


def s3vtest_cleanup(args, zone):
    cfg = yaml.safe_dump(job_config(args, zone, ["create_vector_bucket"]), sort_keys=False)
    cmd = podman_cmd(args, ["cleanup", "-"])
    log("cleanup: " + " ".join(cmd))
    return subprocess.run(cmd, input=cfg.encode(), stdout=sys.stderr).returncode


# ---------------------------------------------------------------- rebuild wait

def wait_for_rebuild(args, zone, bucket, index, since):
    """Poll the admin API until the index has no active build, at least one finish
    event was recorded since `since`, and no event arrived for --rebuild-quiet seconds."""
    params = {"vectorbucket": bucket, "since": str(since)}
    deadline = time.monotonic() + args.rebuild_timeout
    last_count, last_change = -1, time.monotonic()
    t0 = time.monotonic()
    while True:
        try:
            st = zone.admin_get("/admin/vectorbucket/rebuild", params)
        except urllib.error.HTTPError as e:
            sys.exit(f"admin API GET /admin/vectorbucket/rebuild: HTTP {e.code} {e.read()[:200]!r} "
                     f"(user {zone.user_id} needs the buckets=read cap)")
        active = sum(1 for b in st.get("active_builds", []) if b.get("index") == index)
        finishes, results, count = 0, {}, 0
        for ev in st.get("rebuild_events", []):
            if ev.get("index") != index:
                continue
            count += 1
            if ev["type"] == "finish":
                finishes += 1
                results[ev.get("result", "?")] = results.get(ev.get("result", "?"), 0) + 1
        if count != last_count:
            last_count, last_change = count, time.monotonic()
            log(f"rebuild: events={count} finishes={finishes} results={results} active={active}")
        quiet = time.monotonic() - last_change
        if active == 0 and quiet >= args.rebuild_quiet:
            if finishes == 0:
                log(f"rebuild: no rebuild event in {quiet:.0f}s; assuming none was needed")
            else:
                log(f"rebuild: done, {finishes} build(s) {results}, quiet for {quiet:.0f}s")
            return {"finishes": finishes, "results": results, "wait_secs": time.monotonic() - t0}
        if time.monotonic() > deadline:
            sys.exit(f"rebuild of {bucket}/{index} not finished after {args.rebuild_timeout}s "
                     f"(active={active}, finishes={finishes}, results={results})")
        time.sleep(2)


# ---------------------------------------------------------------- report

def print_table(rows, title):
    cols = [("step", "<", 22), ("ops", ">", 7), ("errors", ">", 6), ("ops/s", ">", 8), ("vec/s", ">", 8),
            ("p50 ms", ">", 9), ("p90 ms", ">", 9), ("p99 ms", ">", 9), ("max ms", ">", 9),
            ("up MB/s", ">", 8), ("down MB/s", ">", 9), ("wall s", ">", 7)]
    print(title)
    print(" ".join(f"{c[0]:{c[1]}{c[2]}}" for c in cols))
    for s in rows:
        up = s.get("upload_bytes_per_sec")
        down = s.get("download_bytes_per_sec")
        # vectors moved by the step, scaled by the share of calls that succeeded
        vec = s["vectors"] * (s["ops"] - s["errors"]) / s["ops"] if s.get("vectors") and s["ops"] else None
        vals = [s["step"], s["ops"], s["errors"], f"{s['ops_per_sec']:.1f}",
                f"{vec / s['wall_secs']:.0f}" if vec and s["wall_secs"] else "-",
                f"{s['p50_ms']:.1f}", f"{s['p90_ms']:.1f}", f"{s['p99_ms']:.1f}", f"{s['max_ms']:.1f}",
                f"{up / 1e6:.2f}" if up is not None else "-",
                f"{down / 1e6:.2f}" if down is not None else "-",
                f"{s['wall_secs']:.1f}"]
        print(" ".join(f"{str(v):{c[1]}{c[2]}}" for v, c in zip(vals, cols)))
        if s.get("error_codes"):
            print(f"    errors: {s['error_codes']}")


def main():
    args = parse_args()
    if "S3VTESTS_CONF" not in os.environ:
        sys.exit("point the S3VTESTS_CONF environment variable to a configuration file "
                 "(see s3vtests.conf.SAMPLE)")
    zone = Zone()
    dim, metric, size = DATASETS[args.dataset]
    if args.vectors is None:
        args.vectors = size
    if args.vectors > size:
        sys.exit(f"{args.dataset} has only {size} vectors")
    bucket, index = f"{args.prefix}0", "idx-0"
    log(f"{args.dataset}: {args.vectors} vectors, dim {dim}, {metric}; bucket {bucket} index {index}; "
        f"{args.threads} threads; endpoint {zone.endpoint} user {zone.user_id}; "
        f"backing bucket {zone.backing_bucket}")
    if not args.no_wait:
        zone.grant_admin_caps()

    since = int(time.time()) - 5          # admin API filter, epoch seconds
    results = {"dataset": args.dataset, "vectors": args.vectors, "dim": dim, "metric": metric,
               "threads": args.threads, "endpoint": zone.endpoint, "steps": []}

    put = run_s3vtest(args, zone, "put_vectors", ["create_vector_bucket", "create_index", "insert_vectors"])
    for s in put:
        if s["step"] == "insert_vectors":
            s["vectors"] = args.vectors
    results["steps"] += put
    if any(s["errors"] for s in put):
        print_table(put, "put_vectors phase failed; not continuing")
        sys.exit(2)

    if args.no_wait:
        log("rebuild: wait skipped")
    else:
        results["rebuild"] = wait_for_rebuild(args, zone, bucket, index, since)

    results["steps"] += run_s3vtest(args, zone, "query_vectors", [
        {"query_vectors": {"count": args.queries, "top_k": args.top_k, "source": "auto",
                           "return_distance": True}}])

    get = run_s3vtest(args, zone, "get_vectors", [
        {"get_vectors": {"percent": args.get_percent, "batch_size": args.get_batch,
                         "return_data": True, "return_metadata": True}}])
    for s in get:
        s["vectors"] = int(args.vectors * args.get_percent / 100)
    results["steps"] += get

    if not args.keep:
        results["cleanup_rc"] = s3vtest_cleanup(args, zone)

    print()
    print_table(results["steps"],
                f"{args.dataset}: {args.vectors} vectors x {dim} dims ({metric}), "
                f"PutVectors batch {args.batch_size}, QueryVectors top_k {args.top_k}, "
                f"GetVectors batch {args.get_batch}, {args.threads} threads")
    if "rebuild" in results:
        r = results["rebuild"]
        print(f"index rebuild: {r['finishes']} build(s) {r['results']}, waited {r['wait_secs']:.0f}s")
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(results, f, indent=1)
        log(f"results written to {args.json_out}")
    sys.exit(2 if any(s["errors"] for s in results["steps"]) else 0)


if __name__ == "__main__":
    main()
