#!/bin/bash -ex

sudo yum -y install fio
sudo yum -y install sysstat

namespace_range_start=
namespace_range_end=
random_devices_count=
rbd_iostat=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --start_ns)
            namespace_range_start=$2
            shift 2
            ;;
        --end_ns)
            namespace_range_end=$2
            shift 2
            ;;
        --random_devices)
            random_devices_count=$2
            shift 2
            ;;
        --rbd_iostat)
            rbd_iostat=true
            shift
            ;;
        *)
            exit 100	# Internal error
            ;;
    esac
done

fio_file=$(mktemp -t nvmeof-fio-XXXX)
all_drives_list=$(sudo nvme list --output-format=json | 
    jq -r '.Devices[].Subsystems[] | select(.Controllers | all(.ModelNumber == "Ceph bdev Controller")) | .Namespaces | sort_by(.NSID) | .[] | .NameSpace')

# When the script is passed --start_ns and --end_ns (example: `nvmeof_fio_test.sh --start_ns 1 --end_ns 3`), 
# then fio runs on namespaces only in the defined range (which is 1 to 3 here). 
# So if `nvme list` has 5 namespaces with "SPDK Controller", then fio will 
# run on first 3 namespaces here.
if [ "$namespace_range_start" ] || [ "$namespace_range_end" ]; then
    selected_drives=$(echo "${all_drives_list[@]}" | sed -n "${namespace_range_start},${namespace_range_end}p")
elif [ "$random_devices_count" ]; then 
    selected_drives=$(echo "${all_drives_list[@]}" | shuf -n $random_devices_count)
else
    selected_drives="${all_drives_list[@]}"
fi


RUNTIME=${RUNTIME:-600}
filename=$(echo "$selected_drives" | sed -z 's/\n/:\/dev\//g' | sed 's/:\/dev\/$//')
filename="/dev/$filename"

cat >> $fio_file <<EOF
[global]
ioengine=${IO_ENGINE:-sync}
bsrange=${BS_RANGE:-4k-64k}
numjobs=${NUM_OF_JOBS:-1}
size=${SIZE:-1G}
time_based=1
runtime=$RUNTIME
rw=${RW:-randrw}
verify=md5
verify_fatal=1
do_verify=1
serialize_overlap=1
group_reporting
direct=1

EOF

for i in $selected_drives; do
  echo "[job-$i]" >> "$fio_file"
  echo "filename=/dev/$i" >> "$fio_file"
  echo "" >> "$fio_file"  # Adds a blank line
done

cat $fio_file

status_log() {
    POOL="${RBD_POOL:-mypool}"
    GROUP="${NVMEOF_GROUP:-mygroup0}"
    ceph -s
    ceph orch host ls
    ceph orch ls 
    ceph orch ps
    ceph health detail
    ceph nvme-gw show $POOL $GROUP
    sudo nvme list
    sudo nvme list | wc -l
    sudo nvme list-subsys
    for device in $selected_drives; do
        echo "Processing device: $device"
        sudo nvme list-subsys /dev/$device
        sudo nvme id-ns /dev/$device
    done
    
}


echo "[nvmeof.fio] starting fio test..."

if [ -n "$IOSTAT_INTERVAL" ]; then
    iostat_count=$(( RUNTIME / IOSTAT_INTERVAL ))
    iostat -d -p $selected_drives $IOSTAT_INTERVAL $iostat_count -h &
fi
if [ "$rbd_iostat" = true  ]; then
    iterations=$(( RUNTIME / 5 ))
    timeout 20 rbd perf image iostat $RBD_POOL --iterations $iterations &
fi
fio --showcmd $fio_file

set +e 
sudo fio $fio_file
if [ $? -ne 0 ]; then
    echo "[nvmeof.fio]: fio failed!" 
    status_log
    exit 1
fi


echo "[nvmeof.fio] fio test successful!"
