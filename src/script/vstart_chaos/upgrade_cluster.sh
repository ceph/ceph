#!/bin/bash
# Rolling restart of the vstart cluster in $CEPH_BUILD onto the binaries
# of a snapshot directory (bin/ + lib/), e.g. one made by make_snapshot.sh.
# The cluster keeps running from $CEPH_BUILD (its ceph.conf, dev/, out/);
# only the mon and osd executables and their libraries change.
# Source the printed exports before running chaos against the upgraded cluster.
set -e
S=${1:?snapshot dir}
. "$(dirname "$0")/env.sh"
B=$CEPH_BUILD
export LD_LIBRARY_PATH=$S/lib:$LD_LIBRARY_PATH
export CEPH_ARGS="--erasure_code_dir=$S/lib --plugin_dir=$S/lib"
waitpid_gone() { for i in $(seq 1 120); do kill -0 $1 2>/dev/null || return 0; sleep 1; done; return 1; }
clean() {
    for i in $(seq 1 90); do
        bad=$(env -u CEPH_ARGS ceph pg ls 2>/dev/null | awk 'NR>1 && /^[0-9]+\./ && $11 !~ /^active\+clean/' | wc -l)
        [ "$bad" = 0 ] && return 0; sleep 5
    done; echo "not clean"; return 1
}
for m in a b c; do
    pid=$(cat $B/out/mon.$m.pid); kill $pid; waitpid_gone $pid
    for k in $(seq 1 12); do env -u CEPH_KEYRING $S/bin/ceph-mon -i $m -c $B/ceph.conf >/dev/null 2>&1 && break; sleep 10; done
    for i in $(seq 1 60); do [ "$(env -u CEPH_ARGS ceph quorum_status -f json | jq '.quorum_names|length')" = 3 ] && break; sleep 3; done
    echo "mon.$m -> $(readlink /proc/$(cat $B/out/mon.$m.pid)/exe)"
done
for o in $(env -u CEPH_ARGS ceph osd ls); do
    pid=$(cat $B/out/osd.$o.pid); kill $pid; waitpid_gone $pid
    env -u CEPH_KEYRING $S/bin/ceph-osd -i $o -c $B/ceph.conf >/dev/null 2>&1
    sleep 5; clean
    echo "osd.$o -> $(readlink /proc/$(cat $B/out/osd.$o.pid)/exe)"
done
echo "export CHAOS_BIN_DIR=$S/bin LD_LIBRARY_PATH=$S/lib:\$LD_LIBRARY_PATH CEPH_ARGS=\"$CEPH_ARGS\" RADOSC_LIB=$S/lib/librados.so.2"
