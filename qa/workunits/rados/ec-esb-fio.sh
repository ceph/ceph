#!/bin/bash
# vim: ts=8 sw=2 smarttab
set -ex

# Install FIO
if [[ -f /etc/debian_version ]]; then
    sudo apt-get update
    sudo apt-get install -y git gcc make librados-dev librbd-dev zlib1g-dev libaio-dev
    git clone -b master https://github.com/axboe/fio.git /home/ubuntu/cephtest/fio
    cd /home/ubuntu/cephtest/fio
    ./configure
    make
    sudo make install
    cd -
elif [[ -f /etc/redhat-release ]]; then
    sudo yum install -y fio
else
    echo "Unsupported OS"
    exit 1
fi

sleep 10

ceph config set osd osd_memory_target 939524096
ceph config set osd bluestore_onode_segment_size 0
ceph osd erasure-code-profile set myecprofile k=2 m=1
ceph osd pool create ecpool 16 16 erasure myecprofile
ceph osd pool set ecpool allow_ec_overwrites true

status_log() {
    echo "Cluster status on failure:"
    ceph -s
    ceph health detail
}

cleanup() {
    ceph osd pool rm ecpool ecpool --yes-i-really-really-mean-it || true
    ceph osd erasure-code-profile rm myecprofile || true
    rm -rf /home/ubuntu/cephtest/fio || true
    status_log
}

trap cleanup EXIT INT TERM

echo "[ec-esb-fio] Starting FIO test..."


fio --name=test-ec-esb \
    --ioengine=rados \
    --pool=ecpool \
    --clientname=admin \
    --conf=/etc/ceph/ceph.conf \
    --time_based=1 \
    --runtime=1h \
    --invalidate=0 \
    --direct=1 \
    --touch_objects=0 \
    --iodepth=32 \
    --numjobs=4 \
    --rw=randwrite \
    --file_service_type=pareto:0.20:0 \
    --bssplit=4k/16:8k/10:12k/9:16k/8:20k/7:24k/7 \
    --size=15G \
    --nrfiles=12500 \
    --filename_format=stress_obj.\$jobnum.\$filenum \
    &

FIO_PID=$!

ceph config dump | grep bluestore_elastic_shared_blobs || true
ceph config dump | grep bluestore_onode_segment_size || true
ceph osd dump | grep -A 10 ecpool || true


TIMEOUT=3600
START_TIME=$(date +%s)
while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED -ge $TIMEOUT ]; then
        echo "Reached 1-hour timeout, stopping FIO"
        break
    fi
    if ceph health detail | grep -i "osd.*down"; then
        echo "Detected OSD down state:"
	ceph health detail | grep -i "osd.*down"
        echo "Cleaning up..."
	if [[ -n "$FIO_PID" ]]; then
	  kill -9 $FIO_PID || true
	fi
          exit 1
    fi
    ceph -s
    ceph tell osd.0 perf dump bluestore | grep -A 2 onode || true
    sleep 60
done

echo "[ec-esb-fio] FIO test completed, log checks to follow"
exit 0
