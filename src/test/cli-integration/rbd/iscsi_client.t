Login to the target
===================
  $ IP=`cat /etc/ceph/iscsi-gateway.cfg |grep 'trusted_ip_list' | awk -F'[, ]' '{print $3}'`
  > sudo iscsiadm -m discovery -t st -p $IP -l 2&> /dev/null
  $ sleep 10
  $ sudo ls /dev/disk/by-path/ |grep 'iscsi-iqn.2003-01.com.redhat.iscsi-gw:ceph-gw' |wc -l
  2

Make filesystem
===============
  $ device=`sudo multipath -l | grep 'LIO-ORG,TCMU device' | awk '{print $1}'`
  > sudo mkfs.xfs /dev/mapper/$device -f | grep 'meta-data=/dev/mapper/mpath' | awk '{print $2}'
  isize=512

Write/Read test
===============
  $ device=`sudo multipath -l | grep 'LIO-ORG,TCMU device' | awk '{print $1}'`
  > sudo dd if=/dev/random of=/tmp/iscsi_tmpfile bs=1 count=1K status=none
  > sudo dd if=/tmp/iscsi_tmpfile of=/dev/mapper/$device bs=1 count=1K status=none
  > sudo dd if=/dev/mapper/$device of=/tmp/iscsi_tmpfile1 bs=1 count=1K status=none
  $ sudo diff /tmp/iscsi_tmpfile /tmp/iscsi_tmpfile1

Logout the targets
==================
  $ IP=`cat /etc/ceph/iscsi-gateway.cfg |grep 'trusted_ip_list' | awk -F'[, ]' '{print $3}'`
  > sudo iscsiadm -m node -T iqn.2003-01.com.redhat.iscsi-gw:ceph-gw -p $IP:3260 -u | grep 'successful' | awk -F']' '{print $2}'
   successful.
  $ IP=`cat /etc/ceph/iscsi-gateway.cfg |grep 'trusted_ip_list' | awk -F'[, ]' '{print $4}'`
  > sudo iscsiadm -m node -T iqn.2003-01.com.redhat.iscsi-gw:ceph-gw -p $IP:3260 -u | grep 'successful' | awk -F']' '{print $2}'
   successful.
