#!/usr/bin/bash
TESTDIR=$(dirname $0)
# run for 16 minutes
RUN_FOR=16
EXPECTED_SNAPS=15
RNG_SEED=${RNG_SEED:-$(date +%s)}

echo "TESTDIR=$TESTDIR"
echo "RUN_FOR=$RUN_FOR"
echo "EXPECTED_SNAPS=$EXPECTED_SNAPS"
echo "RNG_SEED=$RNG_SEED"
echo "PYTHON=$(which python3)"

ceph mgr module enable snap_schedule
sleep 2
ceph config set mgr mgr/snap_schedule/allow_m_granularity True
sleep 2
mkdir z
pushd z
fs_path=$(pwd | sed -e "s|^$CEPH_MNT||g")
ceph fs snap-schedule add $fs_path 1M

(python3 -c "import os
import random
import time
from datetime import datetime

random.seed($RNG_SEED)

fd = os.open('./data-file', os.O_WRONLY | os.O_CREAT, 0o777)
assert fd is not None

start = datetime.now()
data = 'A'
while True:
    end = datetime.now()
    elapsed = end - start
    if elapsed.total_seconds() >= int($RUN_FOR * 60):
        break

    s = data * 1024
    os.write(fd, s.encode('ascii'))
    next = ord(data) - ord('A') + 1
    next %= 26
    data = chr(next + ord('A'))
    time.sleep(random.randint(1, 13))

os.close(fd)
") &

bg_pid=$!

# initialize the Random Number Generator
RANDOM=$RNG_SEED

let -i start=$(date +%s)
while true;
do
  let -i now=$(date +%s)
  let -i elapsed=$((now - start))
  if [ $elapsed -ge $(($RUN_FOR * 60)) ]; then
    break
  fi
  ceph fs snap-schedule status $fs_path || echo "$0: snap-schedule status command failed with status $?"
  sleep $((($RANDOM % 13) + 1))
done

wait $bg_pid
bg_status=$?
if [ "$bg_status" != 0 -a "$bg_status" != 127 ]; then
  echo "$0: I/O generation process failed with status $bg_status"
fi

ceph fs snap-schedule deactivate $fs_path 1M
sleep 60

echo 'cephfs:'; ls .snap
let -i count=$(ls .snap | wc -l)
find .snap -maxdepth 1 -mindepth 1 -type d -exec rmdir {} ';'
popd
rm -rf ${CEPH_MNT}/${fs_path}
if [ $bg_status == "0" ]; then
  [ $count -ge $EXPECTED_SNAPS ] && true || false
else
  exit $bg_status
fi
