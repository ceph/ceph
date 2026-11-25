"""
Code shared by the S3 Vectors benchmarks of this directory
(s3vtest-basic-benchmark.py and s3vtest-session-benchmark.py), which are driven
by the s3vtest container image:
  - the tested RGW and its credentials, from the configuration file that the
    S3VTESTS_CONF environment variable points to (see s3vtests.conf.SAMPLE)
  - running s3vtest jobs in a podman container, which is killed when the
    benchmark is interrupted (Ctrl-C, or SIGTERM)
  - waiting for the background rebuild of an index
  - the command line arguments and the report table that the benchmarks share

s3vtest is run with `podman run --rm -i --network host <image> run - --report json`,
the job configuration is passed on standard input. Dataset files are kept in a
named podman volume (default s3vtest-datasets), or in --datasets-dir on the host.
"""

import itertools
import json
import os
import signal
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


# ---------------------------------------------------------------- command line

def add_workload_args(p, prefix):
    """The workload arguments of all the benchmarks. returns their group, for the
    arguments that a benchmark adds to it."""
    w = p.add_argument_group("workload")
    w.add_argument("--dataset", default="fashion-mnist-784-euclidean", choices=sorted(DATASETS),
                   help="ann-benchmarks dataset (default %(default)s)")
    w.add_argument("--vectors", type=int, help="vectors to insert (default: the whole dataset)")
    w.add_argument("--batch-size", type=int, default=500, help="vectors per PutVectors (default %(default)s)")
    w.add_argument("--threads", type=int, default=16, help="in-flight requests in every phase (default %(default)s)")
    w.add_argument("--top-k", type=int, default=10)
    w.add_argument("--prefix", default=prefix, help="vector bucket prefix (default %(default)s)")
    w.add_argument("--key-prefix", default="vec-")
    w.add_argument("--seed", type=int, default=42)
    return w


def add_rebuild_args(p):
    r = p.add_argument_group("rebuild wait")
    r.add_argument("--rebuild-quiet", type=float, default=15,
                   help="seconds without new rebuild events before the index counts as built (default %(default)s)")
    r.add_argument("--rebuild-timeout", type=float, default=1800, help="seconds (default %(default)s)")
    r.add_argument("--no-wait", action="store_true", help="skip the rebuild wait")
    return r


def add_execution_args(p):
    """The execution arguments of all the benchmarks. returns their group, for the
    arguments that a benchmark adds to it."""
    e = p.add_argument_group("execution")
    e.add_argument("--image", default="ghcr.io/yuvalif/s3vtest:latest")
    e.add_argument("--datasets-dir", help="host directory holding the .hdf5 files; default: podman volume s3vtest-datasets")
    e.add_argument("--keep", action="store_true", help="leave the vector bucket in place")
    e.add_argument("--json-out", help="write all results as JSON to this file")
    return e


def check_conf():
    if "S3VTESTS_CONF" not in os.environ:
        sys.exit("point the S3VTESTS_CONF environment variable to a configuration file "
                 "(see s3vtests.conf.SAMPLE)")


def resolve_dataset(args):
    """Validates --vectors against the dataset, and defaults it to the whole
    dataset. returns the dimension and the metric of the dataset."""
    dim, metric, size = DATASETS[args.dataset]
    if args.vectors is None:
        args.vectors = size
    if args.vectors > size:
        sys.exit(f"{args.dataset} has only {size} vectors")
    return dim, metric


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
        self.tenant = conf.get_tenant()
        self.access_key = conf.get_access_key()
        self.secret_key = conf.get_secret_key()
        self.backing_bucket = conf.has_backing_bucket()
        for name in ("host", "port", "region", "user_id", "access_key", "secret_key"):
            if getattr(self, name) is None:
                sys.exit(f"{os.environ['S3VTESTS_CONF']}: {name} is not set")
        # the name of the RGW client, as it is known to the cluster. it is set in the
        # configuration when the RGW is not started by "vstart"
        self.rgw_client = conf.get_config_rgw_client() or f"client.rgw.{self.port}"
        self.scheme = "https://" if self.port in (443, 8443) else "http://"
        self.endpoint = f"{self.scheme}{self.host}:{self.port}"

    def _installed(self):
        return os.path.exists(f"/etc/ceph/{self.cluster}.conf")

    @staticmethod
    def _run(cmd):
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return res.stdout.decode("utf-8", "replace"), res.returncode

    def radosgw_admin(self, args):
        """radosgw-admin against the tested cluster, as the pytest suite runs it:
        directly for an installed cluster, through test-rgw-call.sh for a vstart one."""
        if self._installed():
            cmd = ["radosgw-admin", "--cluster", self.cluster,
                   "-n", conf.get_config_rgw_client() or "client.0"] + args
        else:
            cmd = [os.path.join(test_dir, "..", "test-rgw-call.sh"), "call_rgw_admin", self.cluster] + args
        return self._run(cmd)

    def ceph(self, args):
        """ceph command against the tested cluster, as the pytest suite runs it."""
        timeout = ["--connect-timeout", "30"]
        if self._installed():
            cmd = ["ceph", "--cluster", self.cluster] + timeout + args
        else:
            cmd = [os.path.join(test_dir, "..", "test-rgw-call.sh"), "call_ceph", self.cluster] + timeout + args
        return self._run(cmd)

    def grant_admin_caps(self, caps="buckets=read"):
        """buckets=read lets the main user call GET /admin/vectorbucket, and
        buckets=* its other methods as well."""
        out, rc = self.radosgw_admin(["caps", "add", "--uid", self.user_id, "--caps", caps])
        if rc != 0:
            log(f"warning: could not grant {caps} to {self.user_id} (rc={rc}): {out.strip()[-300:]}")

    def admin_request(self, method, path, params):
        """A signed request to the admin API. returns the body of the reply, and
        raises urllib.error.HTTPError when the request fails."""
        # Send the Host header explicitly: botocore signs the host without a
        # default port (80/443) while urllib sends it with the port of the URL,
        # and RGW signs what it received.
        req = AWSRequest(method=method, url=f"{self.endpoint}{path}", params=params,
                         headers={"Host": f"{self.host}:{self.port}"})
        # S3SigV4Auth, not SigV4Auth: RGW expects x-amz-content-sha256 and S3 canonicalization
        S3SigV4Auth(Credentials(self.access_key, self.secret_key), "s3", self.region).add_auth(req)
        prep = req.prepare()
        ctx = None
        if self.scheme == "https://":
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        request = urllib.request.Request(prep.url, headers=dict(prep.headers), method=method)
        with urllib.request.urlopen(request, timeout=30, context=ctx) as r:
            return r.read()

    def admin_get(self, path, params):
        return json.loads(self.admin_request("GET", path, params))


# ---------------------------------------------------------------- s3vtest via podman

# the name of the container that is running now, if any. containers are named so
# that they could be killed when the benchmark is interrupted
_running_container = None
_container_ids = itertools.count()


def kill_container():
    """Kills the s3vtest container that is running now, if any. the container is
    started with "--rm", so it is removed once it is killed."""
    name = _running_container
    if name is None:
        return
    log(f"killing container {name}")
    try:
        subprocess.run(["podman", "kill", name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as e:
        log(f"warning: could not kill container {name}: {e}")


def _sigterm(signum, frame):
    raise KeyboardInterrupt


def run_main(main):
    """Runs the main function of a benchmark. when the benchmark is interrupted
    with Ctrl-C, or is terminated, the s3vtest container is killed: "podman run"
    forwards the signal to s3vtest, which does not have to stop because of it."""
    signal.signal(signal.SIGTERM, _sigterm)
    try:
        main()
    except KeyboardInterrupt:
        kill_container()
        log("interrupted")
        sys.exit(130)


def podman_cmd(args, extra, name=None):
    cmd = ["podman", "run", "--rm", "-i", "--network", "host"]
    if name:
        cmd += ["--name", name]
    if args.datasets_dir:
        cmd += ["-v", f"{os.path.abspath(args.datasets_dir)}:/work/datasets:Z"]
    else:
        cmd += ["-v", "s3vtest-datasets:/work/datasets"]
    return cmd + [args.image] + extra


def run_container(args, what, extra, config, stdout):
    """Runs s3vtest in a container, with a job configuration on its standard
    input. returns its exit code and its standard output (None unless stdout is
    subprocess.PIPE). the container is killed when the benchmark is interrupted."""
    global _running_container
    name = f"s3vtest-benchmark-{os.getpid()}-{next(_container_ids)}"
    cmd = podman_cmd(args, extra, name)
    log(f"{what}: " + " ".join(cmd))
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=stdout)
    _running_container = name
    try:
        out, _ = proc.communicate(config.encode())
        return proc.returncode, out
    except BaseException:
        # the container may not exist yet (e.g. its image is still being pulled),
        # so "podman run" is stopped as well
        kill_container()
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        raise
    finally:
        _running_container = None


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


def bucket_and_index(args):
    """The names of the vector bucket and the index that s3vtest creates."""
    return f"{args.prefix}0", "idx-0"


def run_s3vtest(args, zone, name, flow):
    """Run one s3vtest job in the container; return its step summaries (list of dicts)."""
    cfg = yaml.safe_dump(job_config(args, zone, flow), sort_keys=False)
    rc, out = run_container(args, name, ["run", "-", "--report", "json"], cfg, subprocess.PIPE)
    if rc not in (0, 2) or not out.strip():
        sys.exit(f"{name}: s3vtest exited with {rc} and no report")
    reports = json.loads(out)
    if isinstance(reports, dict):
        reports = [reports]
    steps = []
    for rep in reports:
        if rep.get("aborted"):
            log(f"{name}: job aborted: {rep['aborted']}")
        steps += rep["steps"]
    return steps


def s3vtest_load(args, zone):
    """Create the vector bucket and the index, and load the dataset into the index."""
    put = run_s3vtest(args, zone, "put_vectors", ["create_vector_bucket", "create_index", "insert_vectors"])
    for s in put:
        if s["step"] == "insert_vectors":
            s["vectors"] = args.vectors
    return put


def query_flow(args, count):
    return [{"query_vectors": {"count": count, "top_k": args.top_k, "source": "auto",
                               "return_distance": True}}]


def s3vtest_cleanup(args, zone):
    cfg = yaml.safe_dump(job_config(args, zone, ["create_vector_bucket"]), sort_keys=False)
    rc, _ = run_container(args, "cleanup", ["cleanup", "-"], cfg, sys.stderr)
    return rc


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


def write_json(args, results):
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(results, f, indent=1)
        log(f"results written to {args.json_out}")
