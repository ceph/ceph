#!/bin/bash

# This script benchmarks RBD image performance using Jerasure vs CLAY EC pools.
# It runs both rbd bench and fio tests (kernel-mapped block device + rados engine)

set -ex

# === CONFIGURABLE PARAMETERS ===
JERASURE_PROFILE="jerasure-ec-profile"
CLAY_PROFILE="clay-ec-profile"

# Pools
JERASURE_DATA_POOL="jerasure_ec_pool"
JERASURE_META_POOL="jerasure_meta"
CLAY_DATA_POOL="clay_ec_pool"
CLAY_META_POOL="clay_meta"

# Images
JERASURE_IMG="test_img_j"
CLAY_IMG="test_img_c"
IMG_SIZE="10G"

# Benchmark params
IO_TYPE="write"
IO_SIZE="1M"
IO_THREADS=4
IO_TOTAL="4G"
PATTERN="seq"

# === FUNCTION TO CLEANUP ON EXIT ===
cleanup() {
  echo "Cleaning up..."
  
  rbd rm ${JERASURE_META_POOL}/${JERASURE_IMG} || true
  rbd rm ${CLAY_META_POOL}/${CLAY_IMG} || true

  ceph osd pool delete ${JERASURE_DATA_POOL} ${JERASURE_DATA_POOL} --yes-i-really-really-mean-it || true
  ceph osd pool delete ${JERASURE_META_POOL} ${JERASURE_META_POOL} --yes-i-really-really-mean-it || true
  ceph osd pool delete ${CLAY_DATA_POOL} ${CLAY_DATA_POOL} --yes-i-really-really-mean-it || true
  ceph osd pool delete ${CLAY_META_POOL} ${CLAY_META_POOL} --yes-i-really-really-mean-it || true

  ceph osd erasure-code-profile rm ${JERASURE_PROFILE} || true
  ceph osd erasure-code-profile rm ${CLAY_PROFILE} || true

  rm -rf /tmp/bench_j /tmp/bench_c
  rm -rf /home/ubuntu/cephtest/fio || true
  echo "Cleanup complete."
}
trap cleanup EXIT

echo "Checking Ceph cluster health..."
if ! ceph health | grep -qE "HEALTH_OK|HEALTH_WARN"; then
  echo "Cluster is not healthy (not HEALTH_OK or HEALTH_WARN). Exiting."
  ceph health detail
  exit 1
fi
echo "Cluster is healthy. Proceeding with benchmark..."

echo "=== STEP 1: Creating EC Profiles ==="

ceph osd erasure-code-profile set $JERASURE_PROFILE \
  plugin=jerasure k=4 m=2 crush-failure-domain=osd || true

ceph osd erasure-code-profile set $CLAY_PROFILE \
  plugin=clay k=4 m=2 d=5 technique=cauchy_good \
  crush-failure-domain=osd || true

echo "=== STEP 2: Creating Pools ==="

# Jerasure
ceph osd pool create $JERASURE_DATA_POOL 64 64 erasure $JERASURE_PROFILE || true
ceph osd pool create $JERASURE_META_POOL 64 64 || true
rbd pool init -p $JERASURE_META_POOL
ceph osd pool set $JERASURE_DATA_POOL allow_ec_overwrites true

# Clay
ceph osd pool create $CLAY_DATA_POOL 64 64 erasure $CLAY_PROFILE || true
ceph osd pool create $CLAY_META_POOL 64 64 || true
rbd pool init -p $CLAY_META_POOL
ceph osd pool set $CLAY_DATA_POOL allow_ec_overwrites true

echo "=== STEP 3: Creating RBD Images ==="

rbd create $JERASURE_META_POOL/$JERASURE_IMG --size $IMG_SIZE
rbd create $CLAY_META_POOL/$CLAY_IMG --size $IMG_SIZE

echo "=== STEP 4: Running rbd bench on Jerasure EC Pool ==="
START_J=$(date +%s)
rbd bench --io-type $IO_TYPE --io-size $IO_SIZE --io-threads $IO_THREADS \
  --io-total $IO_TOTAL --io-pattern $PATTERN $JERASURE_META_POOL/$JERASURE_IMG | tee jerasure_bench.log
END_J=$(date +%s)
J_TIME=$((END_J - START_J))

echo "=== STEP 5: Running rbd bench on CLAY EC Pool ==="
START_C=$(date +%s)
rbd bench --io-type $IO_TYPE --io-size $IO_SIZE --io-threads $IO_THREADS \
  --io-total $IO_TOTAL --io-pattern $PATTERN $CLAY_META_POOL/$CLAY_IMG | tee clay_bench.log
END_C=$(date +%s)
C_TIME=$((END_C - START_C))

# === Install FIO ===
echo "Installing fio..."
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

# === STEP 6: Kernel Map and fio ===
echo "=== STEP 6: Mapping RBD images and running fio ==="
mkdir -p /tmp/bench_j /tmp/bench_c

# Jerasure map and fio
DEV=$(sudo rbd map ${JERASURE_META_POOL}/${JERASURE_IMG} --device-type krbd)
if [ $? -ne 0 ]; then
  echo "Failed to map Jerasure image"
  exit 1
fi
mkfs.ext4 -F $DEV
mount $DEV /tmp/bench_j
START_FIO_J=$(date +%s)
fio --name=jerasure_fio \
  --filename=/tmp/bench_j/testfile \
  --rw=write \
  --bs=1M \
  --numjobs=1 \
  --iodepth=4 \
  --size=2G \
  --runtime=60 \
  --time_based \
  --group_reporting | tee jerasure_fio.log
END_FIO_J=$(date +%s)
J_FIO_TIME=$((END_FIO_J - START_FIO_J))
echo "Jerasure fio time: ${J_FIO_TIME} seconds" | tee -a jerasure_fio.log
umount /tmp/bench_j
sudo rbd unmap $DEV || true

# clay map and fio
DEV=$(sudo rbd map ${CLAY_META_POOL}/${CLAY_IMG} --device-type krbd)
if [ $? -ne 0 ]; then
  echo "Failed to map CLAY image"
  exit 1
fi
mkfs.ext4 -F $DEV
mount $DEV /tmp/bench_c
START_FIO_C=$(date +%s)
fio --name=clay_fio \
  --filename=/tmp/bench_c/testfile \
  --rw=write \
  --bs=1M \
  --numjobs=1 \
  --iodepth=4 \
  --size=2G \
  --runtime=60 \
  --time_based \
  --group_reporting | tee clay_fio.log
END_FIO_C=$(date +%s)
C_FIO_TIME=$((END_FIO_C - START_FIO_C))
echo "CLAY fio time: ${C_FIO_TIME} seconds" | tee -a clay_fio.log
umount /tmp/bench_c
sudo rbd unmap $DEV || true 

# === STEP 8: Results Summary ===
echo ""
echo "=== FINAL RESULT SUMMARY ==="
echo "Write size: $IO_TOTAL | Block size: $IO_SIZE | Threads: $IO_THREADS | Pattern: $PATTERN"
echo ""

echo "Jerasure EC Pool (rbd bench):"
grep "ops/sec" jerasure_bench.log || true
echo "Time taken: $J_TIME seconds"
echo ""

echo "CLAY EC Pool (rbd bench):"
grep "ops/sec" clay_bench.log || true
echo "Time taken: $C_TIME seconds"
echo ""

echo "Jerasure EC Pool (fio):"
grep "IOPS=" jerasure_fio.log || true
echo "Time taken: $J_FIO_TIME seconds"
echo ""

echo "CLAY EC Pool (fio):"
grep "IOPS=" clay_fio.log || true
echo "Time taken: $C_FIO_TIME seconds"
echo ""

if [ "$J_TIME" -gt "$C_TIME" ]; then
  echo "CLAY EC pool (rbd bench) was faster by $((J_TIME - C_TIME)) seconds."
else
  echo "Jerasure EC pool (rbd bench) was faster by $((C_TIME - J_TIME)) seconds."
fi

# FIO results comparison
if [ "$J_FIO_TIME" -gt "$C_FIO_TIME" ]; then
  echo "CLAY EC pool (fio) was faster by $((J_FIO_TIME - C_FIO_TIME)) seconds."
else
  echo "Jerasure EC pool (fio) was faster by $((C_FIO_TIME - J_FIO_TIME)) seconds."
fi

echo ""
echo "Benchmark completed. Cleaning up resources."
exit 0
