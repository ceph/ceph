#!/usr/bin/env bash

# Enqueue a tailed object through S3 and assert the chain uses
# gc.fifo.* rather than the legacy gc.N queue, then that
# `gc process --include-all` reclaims the tails. The calling suite
# disables RGW GC worker threads so process() is driven here.

set -e

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

mydir=$(dirname "$0")
rgw_host="$(hostname --fqdn)"
export RGW_ACCESS_KEY="0555b35654ad1656d804"
export RGW_SECRET_KEY="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
export RGW_HOST="${RGW_HOST:-$rgw_host}"

echo "INFO: fully qualified domain name: $rgw_host"

python3 -m venv "$mydir"
source "$mydir/bin/activate"
pip install pip --upgrade
pip install boto3
PY="$mydir/bin/python3"

if radosgw-admin user info --access-key "$RGW_ACCESS_KEY" >/dev/null 2>&1; then
  echo INFO: user already exists
else
  echo INFO: creating user
  radosgw-admin user create --uid testid \
    --access-key "$RGW_ACCESS_KEY" \
    --secret "$RGW_SECRET_KEY" \
    --display-name 'M. Tester' \
    --email tester@ceph.com >/dev/null
fi

eval "$(radosgw-admin zone get --format json | $PY -c '
import json, shlex, sys
p = json.load(sys.stdin)["gc_pool"]
if ":" in p:
    pool, ns = p.rsplit(":", 1)
else:
    pool, ns = p, ""
print("GC_POOL=" + shlex.quote(pool))
print("GC_NS=" + shlex.quote(ns))
')"

echo "INFO: gc pool ${GC_POOL}${GC_NS:+:$GC_NS}"

gc_rados() {
  if [ -n "$GC_NS" ]; then
    rados -p "$GC_POOL" --namespace "$GC_NS" "$@"
  else
    rados -p "$GC_POOL" "$@"
  fi
}

# pool in gc list is either "name" or "name:ns"
rados_stat() {
  local pool="$1" ns="$2" oid="$3"
  if [ -n "$ns" ]; then
    rados -p "$pool" --namespace "$ns" stat "$oid"
  else
    rados -p "$pool" stat "$oid"
  fi
}

gc_ls() {
  gc_rados ls 2>/dev/null || true
}

queue_oids() {
  gc_ls | grep -E '^gc\.[0-9]+$' || true
}

fifo_oids() {
  gc_ls | grep -E '^gc\.fifo\.' || true
}

oid_bytes() {
  local total=0 oid sz
  while read -r oid; do
    [ -z "$oid" ] && continue
    sz=$(gc_rados stat "$oid" 2>/dev/null | sed -n 's/.* size \([0-9][0-9]*\).*/\1/p')
    total=$((total + ${sz:-0}))
  done
  echo "$total"
}

s3() {
  $PY - "$1" "$2" "$3" "${4:-0}" <<'PY'
import os, sys
import boto3
import botocore.exceptions

op, bucket, key, size = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
host = os.environ["RGW_HOST"]
ak = os.environ["RGW_ACCESS_KEY"]
sk = os.environ["RGW_SECRET_KEY"]

def connect(url, verify=True):
    kw = dict(endpoint_url=url, aws_access_key_id=ak, aws_secret_access_key=sk)
    if not verify:
        kw["verify"] = False
    return boto3.client("s3", **kw)

client = None
for url, verify in (("http://%s:80" % host, True),
                    ("https://%s:443" % host, False),
                    ("http://%s:8000" % host, True)):
    try:
        c = connect(url, verify)
        c.list_buckets()
        client = c
        break
    except botocore.exceptions.EndpointConnectionError:
        continue
if client is None:
    sys.exit("no rgw endpoint on %s" % host)

try:
    client.create_bucket(Bucket=bucket)
except botocore.exceptions.ClientError:
    pass

if op == "put":
    client.put_object(Bucket=bucket, Key=key, Body=b"\0" * size)
elif op == "multipart":
    # a 5M first part (the S3 minimum) and a last part of `size`
    mp = client.create_multipart_upload(Bucket=bucket, Key=key)
    parts = []
    for num, sz in ((1, 5 * 1024 * 1024), (2, size)):
        r = client.upload_part(Bucket=bucket, Key=key, UploadId=mp["UploadId"],
                               PartNumber=num, Body=b"\0" * sz)
        parts.append({"ETag": r["ETag"], "PartNumber": num})
    client.complete_multipart_upload(Bucket=bucket, Key=key,
                                     UploadId=mp["UploadId"],
                                     MultipartUpload={"Parts": parts})
elif op == "delete":
    client.delete_object(Bucket=bucket, Key=key)
else:
    sys.exit("unknown op %s" % op)
PY
}

gc_json() {
  if [ "${1:-}" = "include-all" ]; then
    radosgw-admin gc list --include-all --format json 2>/dev/null
  else
    radosgw-admin gc list --format json 2>/dev/null
  fi
}

record_gc() {
  queue_bytes=$(queue_oids | oid_bytes)
  fifo_bytes=$(fifo_oids | oid_bytes)
  fifo_list=$(fifo_oids)
}

bucket="gc-fifo-bkt-$$"
tails_file="/tmp/gc-fifo-tails.$$"

record_gc
s3 put "$bucket" tiny 4
s3 delete "$bucket" tiny
tiny_queue=$(queue_oids | oid_bytes)
tiny_fifo=$(fifo_oids | oid_bytes)
[ "$tiny_queue" -eq "$queue_bytes" ] || fail "tiny object grew the gc queue ($queue_bytes -> $tiny_queue)"
[ "$tiny_fifo" -eq "$fifo_bytes" ] || fail "tiny object grew gc fifo ($fifo_bytes -> $tiny_fifo)"
echo "OK: head-only object did not enqueue gc"

record_gc
s3 put "$bucket" big $((8 * 1024 * 1024))
s3 delete "$bucket" big
# overwriting a tailed object retires the old tails
s3 put "$bucket" over $((8 * 1024 * 1024))
s3 put "$bucket" over $((8 * 1024 * 1024))
# every part of a multipart object goes to gc on delete
s3 multipart "$bucket" mp $((1024 * 1024))
s3 delete "$bucket" mp

after_queue=$(queue_oids | oid_bytes)
after_fifo=$(fifo_oids | oid_bytes)
after_fifo_list=$(fifo_oids)
[ "$after_queue" -eq "$queue_bytes" ] || fail "tailed delete grew the legacy gc queue ($queue_bytes -> $after_queue)"
if [ "$after_fifo" -le "$fifo_bytes" ] && [ "$after_fifo_list" = "$fifo_list" ]; then
  fail "tailed delete did not grow gc fifo (bytes $fifo_bytes -> $after_fifo)"
fi
echo "OK: tailed delete enqueued to fifo, not queue"

expired_json=/tmp/gc-expired.$$
all_json=/tmp/gc-all.$$
gc_json >"$expired_json"
gc_json include-all >"$all_json"
$PY - "$tails_file" "$expired_json" "$all_json" <<'PY'
import json, sys

def load(path):
    with open(path) as f:
        return json.loads(f.read() or "[]")

def split_pool(pool):
    if ":" in pool:
        return pool.rsplit(":", 1)
    return pool, ""

def oids_from(entries):
    out = []
    for e in entries:
        for o in e.get("objs", []):
            name, ns = split_pool(o["pool"])
            out.append((name, ns, o["oid"]))
    return out

tails_file, expired_path, all_path = sys.argv[1], sys.argv[2], sys.argv[3]
tails = oids_from(load(all_path))
if not tails:
    sys.exit("gc list --include-all has no tail oids")
expired = {(ns, oid) for _, ns, oid in oids_from(load(expired_path))}
ours = {(ns, oid) for _, ns, oid in tails}
if ours & expired:
    sys.exit("gc list (expired-only) already contains our tails before min_wait")
with open(tails_file, "w") as f:
    for name, ns, oid in tails:
        f.write("%s\t%s\t%s\n" % (name, ns, oid))
print("OK: include-all lists %d tail(s), none of them expired-only" % len(tails))
PY
rm -f "$expired_json" "$all_json"

# what the gc thread runs: only entries past rgw_gc_obj_min_wait, so nothing
radosgw-admin gc process >/dev/null
while IFS=$'\t' read -r pool ns oid; do
  [ -z "$oid" ] && continue
  rados_stat "$pool" "$ns" "$oid" >/dev/null 2>&1 ||
    fail "tail $pool${ns:+:$ns}/$oid removed by gc process before rgw_gc_obj_min_wait"
done < "$tails_file"
echo "OK: gc process without --include-all left unexpired tails alone"

radosgw-admin gc process --include-all >/dev/null

while IFS=$'\t' read -r pool ns oid; do
  [ -z "$oid" ] && continue
  if rados_stat "$pool" "$ns" "$oid" >/dev/null 2>&1; then
    fail "tail $pool${ns:+:$ns}/$oid still exists after gc process"
  fi
done < "$tails_file"
rm -f "$tails_file"
echo "OK: gc process --include-all reclaimed tails"

deactivate
echo "== PASSED =="
exit 0
