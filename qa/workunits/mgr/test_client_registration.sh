#!/bin/bash -ex
#
# A mgr module must not use a RADOS client before the monitor has it in
# the MgrMap: the monitor blocklists mgrmap.clients when it drops the
# active mgr, so a client that has not got there yet keeps running after
# a failover.
#
# https://tracker.ceph.com/issues/58924

cleanup() {
    ceph config rm mgr mgr_debug_client_register_delay || true
    ceph mgr module disable selftest || true
}
trap cleanup EXIT

mgr_field() {
    ceph mgr dump -f json |
        python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"
}

# Is <addr>/<nonce> registered in the MgrMap?
in_mgrmap() {
    ceph mgr dump -f json | python3 -c '
import json, sys
want = sys.argv[1]
for c in json.load(sys.stdin)["active_clients"]:
    for a in c["addrvec"]:
        if "{0}/{1}".format(a["addr"], a["nonce"]) == want:
            sys.exit(0)
sys.exit(1)
' "$1"
}

gid=$(mgr_field active_gid)
ceph mgr module enable selftest --force
# Enabling a module respawns the mgr.
while [ "$(mgr_field active_gid)" = "$gid" ] || [ "$(mgr_field available)" != "True" ]; do
    sleep 2
done

# Registering a client asks for a beacon straight away, which leaves no
# observable gap. Suppress that so the client reaches the MgrMap on the
# next tick instead, which is long enough to check against.
ceph config set mgr mgr_debug_client_register_delay 30000

# The client must be in the MgrMap by the time registration returns.
addr=$(ceph mgr self-test rados-client-register)
if ! in_mgrmap "$addr"; then
    echo "FAIL: selftest is using $addr but it is not in the MgrMap," \
         "so a failover now could not blocklist it"
    exit 1
fi

# Failing the mgr just after a module starts using a client must
# blocklist that client.
addr=$(ceph mgr self-test rados-client-register)
old=$(mgr_field active_name)
ceph mgr fail "$old"
while [ "$(mgr_field active_name)" = "$old" ] || [ "$(mgr_field active_name)" = "" ]; do
    sleep 2
done

for _ in $(seq 30); do
    if ceph osd blocklist ls | grep -q "^$addr "; then
        break
    fi
    sleep 1
done
if ! ceph osd blocklist ls | grep -q "^$addr "; then
    echo "FAIL: $addr was in use by a module but escaped the failover blocklist"
    exit 1
fi

echo OK
