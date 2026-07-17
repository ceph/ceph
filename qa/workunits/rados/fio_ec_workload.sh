#!/bin/bash
# vim: ts=8 sw=2 smarttab
set -ex

POOL_NAME="ecpool"

echo "[fio-workload] Installing FIO..."
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

echo "[fio-workload] Creating erasure-coded pool..."
ceph osd erasure-code-profile set myecprofile k=2 m=1
ceph osd pool create ${POOL_NAME} 16 16 erasure myecprofile
ceph osd pool set ${POOL_NAME} allow_ec_overwrites true

cleanup() {
    echo "[fio-workload] Cleaning up..."
    if [[ -n "$FIO_PID" ]]; then
        kill -9 $FIO_PID 2>/dev/null || true
    fi
    ceph osd pool rm ${POOL_NAME} ${POOL_NAME} --yes-i-really-really-mean-it || true
    ceph osd erasure-code-profile rm myecprofile || true
    rm -rf /home/ubuntu/cephtest/fio || true
    echo "[fio-workload] Cluster status:"
    ceph -s
    ceph health detail
}

trap cleanup EXIT INT TERM

echo "[fio-workload] Starting FIO workload (1hr runtime)..."
fio --name=test-alloc-recovery \
    --ioengine=rados \
    --pool=${POOL_NAME} \
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

echo "[fio-workload] FIO PID: $FIO_PID"
echo "[fio-workload] FIO running in background..."


TIMEOUT=3600
START_TIME=$(date +%s)
while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED -ge $TIMEOUT ]; then
        echo "Reached 1-hour timeout, stopping FIO"
        if [[ -n "$FIO_PID" ]]; then
	        kill -9 $FIO_PID || true
	    fi
        break
    fi
    sleep 60
done

echo "[fio-workload] FIO workload completed successfully"
exit 0
