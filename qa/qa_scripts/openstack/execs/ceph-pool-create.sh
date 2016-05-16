#!/bin/bash -f
#
# On the ceph site, make the pools required for Openstack
#

#
# Make a pool, if it does not already exist.
#
function make_pool {
    if [[ -z `sudo ceph osd lspools | grep " $1,"` ]]; then
        echo "making $1"
        sudo ceph osd pool create $1 128
    fi
}

#
# Make sure the pg_num and pgp_num values are good.
#
count=`sudo ceph osd pool get rbd pg_num | sed 's/pg_num: //'`
while [ $count -lt 128 ]; do
    sudo ceph osd pool set rbd pg_num $count
    count=`expr $count + 32`
    sleep 30
done
sudo ceph osd pool set rbd pg_num 128
sleep 30
sudo ceph osd pool set rbd pgp_num 128
sleep 30
make_pool volumes
make_pool images
make_pool backups
make_pool vms
