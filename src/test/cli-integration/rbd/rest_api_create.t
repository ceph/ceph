Create a datapool/block1 disk
=============================
  $ sudo curl --user admin:admin -d mode=create -d size=300M -X PUT http://127.0.0.1:5000/api/disk/datapool/block1 -s
  {"message":"disk create/update successful"}

Create the target IQN
=====================
  $ sudo curl --user admin:admin -X PUT http://127.0.0.1:5000/api/target/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw -s
  {"message":"Target defined successfully"}

Create the first gateway
========================
  $ HOST=`python3 -c "import socket; print(socket.getfqdn())"`
  > IP=`hostname -i | awk '{print $1}'`
  > sudo curl --user admin:admin -d ip_address=$IP -X PUT http://127.0.0.1:5000/api/gateway/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/$HOST -s
  {"message":"Gateway creation successful"}

Create the second gateway
========================
  $ IP=`cat /etc/ceph/iscsi-gateway.cfg |grep 'trusted_ip_list' | awk -F'[, ]' '{print $3}'`
  > if [ "$IP" != `hostname -i | awk '{print $1}'` ]; then
  >   HOST=`python3 -c "import socket; print(socket.getfqdn('$IP'))"`
  >   sudo curl --user admin:admin -d ip_address=$IP -X PUT http://127.0.0.1:5000/api/gateway/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/$HOST -s
  > else
  >   IP=`cat /etc/ceph/iscsi-gateway.cfg |grep 'trusted_ip_list' | awk -F'[, ]' '{print $4}'`
  >   HOST=`python3 -c "import socket; print(socket.getfqdn('$IP'))"`
  >   sudo curl --user admin:admin -d ip_address=$IP -X PUT http://127.0.0.1:5000/api/gateway/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/$HOST -s
  > fi
  {"message":"Gateway creation successful"}

Attach the disk
===============
  $ sudo curl --user admin:admin -d disk=datapool/block1 -X PUT http://127.0.0.1:5000/api/targetlun/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw -s
  {"message":"Target LUN mapping updated successfully"}

Create a host
=============
  $ sudo curl --user admin:admin -X PUT http://127.0.0.1:5000/api/client/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/iqn.1994-05.com.redhat:client -s
  {"message":"client create/update successful"}

Map the LUN
===========
  $ sudo curl --user admin:admin -d disk=datapool/block1 -X PUT http://127.0.0.1:5000/api/clientlun/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/iqn.1994-05.com.redhat:client -s
  {"message":"client masking update successful"}
