#!/bin/bash
set -ex

POOL=rbd
IMAGE_PREFIX=image
NUM_IMAGES=20
RUN_TIME=3600

rbd mirror pool enable ${POOL} image
rbd mirror pool peer add ${POOL} dummy

# Create images and schedule their mirror snapshots
for ((i = 1; i <= ${NUM_IMAGES}; i++)); do
    rbd create -s 1G --image-feature exclusive-lock ${POOL}/${IMAGE_PREFIX}$i
    rbd mirror image enable ${POOL}/${IMAGE_PREFIX}$i snapshot
    rbd mirror snapshot schedule add -p ${POOL} --image ${IMAGE_PREFIX}$i 1m
done

# Run fio workloads on images via kclient
# Test the recovery of the rbd_support module and its scheduler from their
# librbd client being blocklisted while a exclusive lock gets passed around
# between their librbd client and a kclient trying to take mirror snapshots
# and perform I/O on the same image.
for ((i = 1; i <= ${NUM_IMAGES}; i++)); do
    DEVS[$i]=$(sudo rbd device map ${POOL}/${IMAGE_PREFIX}$i)
    fio --name=fiotest --filename=${DEVS[$i]} --rw=randrw --bs=4K --direct=1 \
        --ioengine=libaio --iodepth=2 --runtime=43200 --time_based \
        &> /dev/null &
done

# Repeatedly blocklist rbd_support module's client ~10s after the module
# recovers from previous blocklisting
CURRENT_TIME=$(date +%s)
END_TIME=$((CURRENT_TIME + RUN_TIME))
PREV_CLIENT_ADDR=""
CLIENT_ADDR=""
while ((CURRENT_TIME <= END_TIME)); do
    if [[ -n "${CLIENT_ADDR}" ]] &&
       [[ "${CLIENT_ADDR}" != "${PREV_CLIENT_ADDR}" ]]; then
            ceph osd blocklist add ${CLIENT_ADDR}
            # Confirm rbd_support module's client is blocklisted
            ceph osd blocklist ls | grep -q ${CLIENT_ADDR}
            PREV_CLIENT_ADDR=${CLIENT_ADDR}
    fi
    sleep 10
    CLIENT_ADDR=$(ceph mgr dump |
        jq .active_clients[] |
        jq 'select(.name == "rbd_support")' |
        jq -r '[.addrvec[0].addr, "/", .addrvec[0].nonce|tostring] | add')
    CURRENT_TIME=$(date +%s)
done

# Confirm that rbd_support module recovered from repeated blocklisting
# Check that you can add a mirror snapshot schedule after a few retries
for ((i = 1; i <= 24; i++)); do
    rbd mirror snapshot schedule add -p ${POOL} \
        --image ${IMAGE_PREFIX}1 2m && break
    sleep 10
done
rbd mirror snapshot schedule ls -p ${POOL} --image ${IMAGE_PREFIX}1 |
    grep 'every 2m'
# Verify that the schedule present before client blocklisting is preserved
rbd mirror snapshot schedule ls -p ${POOL} --image ${IMAGE_PREFIX}1 |
    grep 'every 1m'
rbd mirror snapshot schedule rm -p ${POOL} --image ${IMAGE_PREFIX}1 2m
for ((i = 1; i <= ${NUM_IMAGES}; i++)); do
    rbd mirror snapshot schedule rm -p ${POOL} --image ${IMAGE_PREFIX}$i 1m
done

# cleanup
killall fio || true
wait
for ((i = 1; i <= ${NUM_IMAGES}; i++)); do
    sudo rbd device unmap ${DEVS[$i]}
done

echo OK
