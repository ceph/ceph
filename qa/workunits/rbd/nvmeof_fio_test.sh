#!/bin/bash -ex

sudo yum -y install fio
sudo yum -y install sysstat

fio_file=$(mktemp -t nvmeof-fio-XXXX)
drives_list=$(sudo nvme list --output-format=json | jq -r '.Devices | .[] | select(.ModelNumber == "SPDK bdev Controller") | .DevicePath')

RUNTIME=${RUNTIME:-600}
# IOSTAT_INTERVAL=10


cat >> $fio_file <<EOF
[nvmeof-fio-test]
ioengine=${IO_ENGINE:-sync}
bsrange=${BS_RANGE:-4k-64k}
numjobs=${NUM_OF_JOBS:-1}
size=${SIZE:-1G}
time_based=1
runtime=$RUNTIME
rw=${RW:-randrw}
filename=$(echo "$drives_list" | tr '\n' ':' | sed 's/:$//')
verify=md5
verify_fatal=1
EOF

fio --showcmd $fio_file
sudo fio $fio_file &

if [ -n "$IOSTAT_INTERVAL" ]; then
    iostat_count=$(( RUNTIME / IOSTAT_INTERVAL ))
    iostat -d $IOSTAT_INTERVAL $iostat_count -h 
fi
wait

echo "[nvmeof] fio test successful!"
