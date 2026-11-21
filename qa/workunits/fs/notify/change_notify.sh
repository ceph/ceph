#!/bin/sh -e
#
# CephFS change notification: workunit-level check.
#
# Run on a client node of a cluster whose MDS was started with the file
# endpoint, e.g. in ceph.conf:
#
#   [mds]
#       mds_notify_enable = true
#       mds_notify_file   = /tmp/cephfs-notify.jsonl
#       mds_notify_root   = /
#
# The check drives the six operations the consumer acts on and verifies the
# producer's own accounting ("notify status": queued/sent counters plus the
# config), which works without reading the sink file. The admin socket is on
# the MDS node, so everything goes through `ceph tell`. If NOTIFY_SINK_HOST and
# NOTIFY_SINK are set, the emitted records are fetched with ssh and checked
# against the wire contract as well.
#
# Usage:
#   change_notify.sh [mountpoint]
# Env:
#   NOTIFY_SINK_HOST  host running the MDS (ssh target) - optional
#   NOTIFY_SINK       sink path on that host            - optional

M=${1:-/mnt/cephfs}
: "${NOTIFY_SINK_HOST:=}"
: "${NOTIFY_SINK:=}"
TESTDIR=$M/cephfs_notify_workunit

fail() { echo "FAIL: $*" >&2; exit 1; }

command -v python3 >/dev/null || fail "python3 is required"

echo "== pick the active MDS =="
MDS=$(ceph fs status -f json | python3 -c '
import json, sys
d = json.load(sys.stdin)
for r in d.get("mdsmap", []):
    if str(r.get("state", "")).startswith("active"):
        print(r["name"]); break
') || fail "cannot determine the active MDS from 'ceph fs status'"
[ -n "$MDS" ] || fail "no active MDS"
echo "active MDS: $MDS"

echo "== notify status =="
STATUS=$(ceph tell "mds.$MDS" notify status)
echo "$STATUS"
echo "$STATUS" | python3 -c '
import json, sys
s = json.load(sys.stdin)
s = s.get("change_notifier", s)
assert s["enabled"], "notifier is not enabled"
assert s["endpoint"] != "none", "no endpoint configured"
print("endpoint %s, root %s, queues %s/%s, sent %s" % (
    s["endpoint"], s["root"], s["queued"], s["sent"], s["sent"]))
' || fail "notify status does not show an enabled endpoint"

echo "== enable/disable round trip =="
ceph tell "mds.$MDS" notify disable >/dev/null
ceph tell "mds.$MDS" notify status | python3 -c '
import json, sys
s = json.load(sys.stdin); s = s.get("change_notifier", s)
assert not s["enabled"], "disable did not take"
' || fail "disable did not take effect"
ceph tell "mds.$MDS" notify enable >/dev/null
ceph tell "mds.$MDS" notify status | python3 -c '
import json, sys
s = json.load(sys.stdin); s = s.get("change_notifier", s)
assert s["enabled"], "enable did not take"
' || fail "enable did not take effect"

echo "== workload: the six consumed operations under $TESTDIR =="
rm -rf "$TESTDIR"
mkdir -p "$TESTDIR/projects/alpha"
touch "$TESTDIR/projects/alpha/report.txt"
echo "hello cephfs notify" > "$TESTDIR/projects/alpha/report.txt"
sync "$TESTDIR/projects/alpha/report.txt"
sleep 0.5
mkdir "$TESTDIR/projects/beta"
mv "$TESTDIR/projects/alpha/report.txt" "$TESTDIR/projects/alpha/final.txt"
sleep 0.5
rm "$TESTDIR/projects/alpha/final.txt"
rmdir "$TESTDIR/projects/beta"
sync
sleep 2

echo "== producer accounting =="
ceph tell "mds.$MDS" notify status | python3 -c '
import json, sys
s = json.load(sys.stdin)
s = s.get("change_notifier", s)
# at least: create dir, create file, close_write, mkdir, moved_from/to, delete, rmdir
assert s["sent"] >= 6, "expected at least 6 sent events, got %s" % s["sent"]
assert s["last_error"] == "", "endpoint reported an error: %s" % s["last_error"]
print("sent %s, dropped_queue %s, dropped_endpoint %s" % (
    s["sent"], s["dropped_queue"], s["dropped_endpoint"]))
' || fail "producer accounting does not match the workload"

if [ -n "$NOTIFY_SINK_HOST" ] && [ -n "$NOTIFY_SINK" ]; then
  echo "== records in $NOTIFY_SINK on $NOTIFY_SINK_HOST =="
  ssh "$NOTIFY_SINK_HOST" "cat $NOTIFY_SINK" > /tmp/cephfs-notify-workunit.jsonl || fail "cannot read the sink"
  if ! python3 - "$TESTDIR" <<'PY'
import json, sys
prefix = sys.argv[1].lstrip("/") + "/"
recs = [json.loads(l) for l in open("/tmp/cephfs-notify-workunit.jsonl") if l.strip()]
mine = [r for r in recs if r.get("path", "").startswith(prefix) or
        r.get("src_path", "").startswith(prefix) or r.get("dest_path", "").startswith(prefix)]
paths = [r.get("path") or r.get("dest_path") for r in mine]
expect = {
    "create dir":  any(r["mask"] & 16 and r["mask"] & 65536 for r in mine),
    "create file": any(r["mask"] == 16 for r in mine),
    "close_write": any(r["mask"] & 4 for r in mine),
    "move":        any(r["mask"] == 0 and r.get("dest_mask", 0) & 1024 for r in mine),
    "delete file": any(r["mask"] == 32 for r in mine),
    "delete dir":  any(r["mask"] == 65568 for r in mine),
}
for name, ok in expect.items():
    print("%-12s %s" % (name, "ok" if ok else "MISSING"))
if not all(expect.values()):
    sys.exit(1)
print("%d records for this workload" % len(mine))
PY
  then
    fail "sink records do not match the workload"
  fi
fi

rm -rf "$TESTDIR"
echo "OK"
