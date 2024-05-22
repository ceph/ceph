#!/bin/bash -ex

sudo yum -y install fio
sudo yum -y install sysstat

namespace_range_start=
namespace_range_end=
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
    jq -r '.Devices | sort_by(.NameSpace) | .[] | select(.ModelNumber == "SPDK bdev Controller") | .DevicePath')

# When the script is passed --start_ns and --end_ns (example: `nvmeof_fio_test.sh --start_ns 1 --end_ns 3`), 
# then fio runs on namespaces only in the defined range (which is 1 to 3 here). 
# So if `nvme list` has 5 namespaces with "SPDK Controller", then fio will 
# run on first 3 namespaces here.
if [ "$namespace_range_start" ] || [ "$namespace_range_end" ]; then
    selected_drives=$(echo "${all_drives_list[@]}" | sed -n "${namespace_range_start},${namespace_range_end}p")
else
    selected_drives="${all_drives_list[@]}"
fi


RUNTIME=${RUNTIME:-600}


cat >> $fio_file <<EOF
[nvmeof-fio-test]
ioengine=${IO_ENGINE:-sync}
bsrange=${BS_RANGE:-4k-64k}
numjobs=${NUM_OF_JOBS:-1}
size=${SIZE:-1G}
time_based=1
runtime=$RUNTIME
rw=${RW:-randrw}
filename=$(echo "$selected_drives" | tr '\n' ':' | sed 's/:$//')
verify=md5
verify_fatal=1
direct=1
EOF

echo "[nvmeof] starting fio test..."

if [ -n "$IOSTAT_INTERVAL" ]; then
    iostat_count=$(( RUNTIME / IOSTAT_INTERVAL ))
    iostat -d -p $selected_drives $IOSTAT_INTERVAL $iostat_count -h &
fi
if [ "$rbd_iostat" = true  ]; then
    iterations=$(( RUNTIME / 5 ))
    rbd perf image iostat $RBD_POOL --iterations $iterations &
fi
fio --showcmd $fio_file
sudo fio $fio_file 
wait

echo "[nvmeof] fio test successful!"
