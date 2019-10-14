set -ex

master=$(hostname -f)
declare -a client="$@"

function wait_for_server () {
    sleep 20
    while ! nc -zv $1 22
    do
        sleep 20
    done
}

function rbd_test () {
    salt $2 cmd.run "systemctl enable rbdmap.service; systemctl start rbdmap.service; modprobe rbd"
    salt $2 cmd.run "rbd create rbd_persistent/image --size 1G"
    rbd_dev=$(salt $2 cmd.run "rbd map rbd_persistent/image" | tail -1 | sed 's/\ //g')
    salt $2 cmd.run "echo \"rbd_persistent/image    id=admin,keyring=/etc/ceph/ceph.client.admin.keyring\" >> /etc/ceph/rbdmap"
    salt $2 cmd.run "parted -s $rbd_dev mklabel gpt unit % mkpart 1 xfs 0 100"
    salt $2 cmd.run "mkfs.xfs ${rbd_dev}p1; mount ${rbd_dev}p1 /mnt; dd if=/dev/zero of=/mnt/testfile.txt bs=1024K count=50"
    salt $2 cmd.run "ls -l /mnt/testfile.txt"
    salt $2 cmd.run "echo \"rbd_persistent/image    id=admin,keyring=/etc/ceph/ceph.client.admin.keyring\" >> /etc/ceph/rbdmap"
    
    salt $2 system.reboot || true
    wait_for_server $2
    
    until [ "$(salt $2 service.status rbdmap.service | tail -1 | sed 's/\ //g')" == "True" ]
    do
        sleep 10
    done
    
    salt $2 cmd.run "lsblk $rbd_dev"
    
    salt $2 cmd.run "rbd unmap $rbd_dev"
    ceph osd pool rm rbd_persistent rbd_persistent --yes-i-really-really-mean-it
}

rbd_test $master $client
