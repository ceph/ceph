set -ex

declare -a disk_storage_minion=$1

function health_ok() {
    until [ "$(ceph health)" == "HEALTH_OK" ] || [[ "$(ceph health)" == *"daemons have recently crashed" ]]
    do
        sleep 30
    done
}

storage_device_name=$(salt $disk_storage_minion cmd.run \
 "pvdisplay | grep -B 1 'VG Name .* ceph' | egrep -v 'ceph-block-dbs|nvme|--' | head -1 | cut -d / -f 3" --output=json | jq -r .[])
salt $disk_storage_minion cmd.run "mkdir /debug; mount debugfs /debug -t debugfs; cd /debug/fail_make_request;\
    echo 10 > interval; echo 100 > probability; echo -1 > times; echo 1 > /sys/block/$storage_device_name/make-it-fail"

rbd create diskfaultinjection/image1 --size 1G

while ! ceph -s | grep ".* osds down"
do
    sleep 30
done

ceph -s

ceph osd tree

health_ok

salt $disk_storage_minion cmd.run "umount /debug; echo 0 > /sys/block/$storage_device_name/make-it-fail; sleep 30; \
    systemctl reset-failed ceph-osd*; systemctl restart ceph-osd.target"

health_ok

ceph osd pool rm diskfaultinjection diskfaultinjection --yes-i-really-really-mean-it
