set -ex

for mode in passive aggressive force
do 
    ceph osd pool set pool_${mode} compression_algorithm zlib
    ceph osd pool set pool_${mode} compression_mode $mode
    rbd create -p pool_${mode} image1 --size 5G
    rbd du -p pool_${mode} image1
    modprobe rbd
    rbd_dev=$(rbd map pool_${mode}/image1)
    parted $rbd_dev mklabel gpt
    parted $rbd_dev unit % mkpart 1 xfs 0 100
    mkfs.xfs ${rbd_dev}p1
    mkdir /mnt/pool_$mode
    mount ${rbd_dev}p1 /mnt/pool_$mode
    dd if=/dev/zero of=/mnt/pool_$mode/file.bin bs=2M count=2048 status=progress oflag=direct
    rbd du -p pool_${mode} image1
    umount /mnt/pool_$mode
    rbd unmap ${rbd_dev}
    ceph osd pool rm pool_$mode pool_$mode --yes-i-really-really-mean-it
done

