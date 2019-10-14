set -ex

declare -a monitor_minions=("$@")

minion_fqdn=${monitor_minions[0]}

secret=$(grep key /etc/ceph/ceph.client.admin.keyring | sed 's/key\ =\ //')
monitors_count=$((${#monitor_minions[@]}))
mon_mounted=0
until [ $mon_mounted -eq $monitors_count ]
do
    mon_mount+="${monitor_minions[$mon_mounted]},"
    let mon_mounted+=1 
done

unset mon_mounted

mkdir /mnt/cephfs

mount -t ceph $(echo $mon_mount | sed 's/.$//'):/ /mnt/cephfs -o name=admin,secret=$(echo $secret | tr -d ' ')

mount | grep "/mnt/cephfs"

dd if=/dev/zero of=/mnt/cephfs/testfile.bin oflag=direct bs=2M count=1000 status=progress

ls -l /mnt/cephfs/testfile.bin

rm -f /mnt/cephfs/testfile.bin

umount /mnt/cephfs
rm -rf /mnt/cephfs

