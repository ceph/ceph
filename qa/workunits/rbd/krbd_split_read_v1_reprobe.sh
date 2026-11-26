#!/usr/bin/env bash
#
# Recreate for the libceph GET_INTERNAL_VERSIONS_V2 re-probe fix: once a split
# read hits a pre-V2 OSD (-EOPNOTSUPP) the client latches the GET_INTERNAL_VERSIONS
# (v1) fallback, and it must re-probe V2 after the osdmap epoch advances (a rolling
# upgrade) rather than staying on v1 until a remount.
#
# Uses the OSD debug option osd_debug_reject_internal_versions_v2 to make the OSDs
# reject V2 on demand, and reads the per-op split-read mode from the EC_DIRECT_READS
# debugfs counters (needs the ec_split_v1_reads counter kernel).
#
# Before the fix, step 4 fails: the client stays on v1 after the epoch bump.
set -ex

POOL=ecreprobe
MPOOL=ecreprobe-meta
PROFILE=ecreprobep
IMG=reprobe-$$

mountpoint -q /sys/kernel/debug || sudo mount -t debugfs none /sys/kernel/debug

# optimized EC data pool (k=2 m=1, 4K stripe unit): a read over >1 chunk splits
ceph osd erasure-code-profile set $PROFILE k=2 m=1 crush-failure-domain=osd \
    plugin=jerasure technique=reed_sol_van
ceph osd pool create $POOL erasure $PROFILE
ceph osd pool set $POOL allow_ec_overwrites true
ceph osd pool set $POOL allow_ec_optimizations true
ceph osd pool create $MPOOL 8
rbd pool init $MPOOL
for _ in $(seq 30); do
    ceph osd pool ls detail | grep "'$POOL'" | grep -q split_reads && break
    sleep 2
done

rbd create --size 64 --pool $MPOOL --data-pool $POOL $IMG
DEV=$(sudo rbd map --pool $MPOOL --options ms_mode=crc,read_from_replica=balance $IMG)
cleanup() {
    sudo rbd unmap "$DEV" 2>/dev/null || true
    rbd rm $MPOOL/$IMG 2>/dev/null || true
    ceph config rm osd osd_debug_reject_internal_versions_v2 2>/dev/null || true
}
trap cleanup EXIT

# lay down data so the reads return something
sudo dd if=/dev/urandom of="$DEV" bs=1M count=16 oflag=direct 2>/dev/null

# EC_DIRECT_READS <sent> eagain <n> split <n> fallback <n> v1 <n>
field() {
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdc' 2>/dev/null |
        awk -v f="$1" '/^EC_DIRECT_READS/{for(i=1;i<=NF;i++) if($i==f){print $(i+1); exit}}'
}
client_epoch() {
    sudo sh -c 'cat /sys/kernel/debug/ceph/*/osdmap' 2>/dev/null |
        awk '/^epoch/{print $2; exit}'
}
# several multi-chunk direct reads -> split reads
splitread() {
    for i in $(seq 1 8); do
        sudo dd if="$DEV" of=/dev/null bs=64k count=1 iflag=direct skip=$((i * 3)) 2>/dev/null
    done
}

[ -n "$(field split)" ] || { echo "no EC_DIRECT_READS counter: need the counter kernel"; exit 1; }

# 1. baseline: split reads use V2, the v1 counter stays flat
split0=$(field split); v1_0=$(field v1)
splitread
[ "$(field split)" -gt "$split0" ] || { echo "no split reads at baseline"; exit 1; }
[ "$(field v1)" -eq "$v1_0" ] || { echo "unexpected v1 split read at baseline"; exit 1; }

# 2. make the OSDs reject V2 -> the client latches the v1 fallback
ceph config set osd osd_debug_reject_internal_versions_v2 true
sleep 5
v1_1=$(field v1)
splitread   # bounces to the primary on -EOPNOTSUPP and latches v1
splitread   # uses v1
[ "$(field v1)" -gt "$v1_1" ] || { echo "FAIL: client did not fall back to v1"; exit 1; }

# 3. clear the reject and advance the osdmap (rolling-upgrade proxy)
latch_epoch=$(client_epoch)
ceph config set osd osd_debug_reject_internal_versions_v2 false
for _ in $(seq 30); do
    ceph osd set noout; ceph osd unset noout       # each pair bumps the epoch
    [ "$(client_epoch)" -gt "$latch_epoch" ] && break
    sleep 2
done
[ "$(client_epoch)" -gt "$latch_epoch" ] || { echo "client osdmap epoch did not advance"; exit 1; }

# 4. the next split read must re-probe V2: the v1 counter stays flat
split2=$(field split); v1_2=$(field v1)
splitread
[ "$(field split)" -gt "$split2" ] || { echo "no split read after the epoch bump"; exit 1; }
if [ "$(field v1)" -gt "$v1_2" ]; then
    echo "FAIL: split read still used v1 after the osdmap advanced (no V2 re-probe)"
    exit 1
fi
echo "PASS: client re-probed GET_INTERNAL_VERSIONS_V2 after the osdmap epoch change"
