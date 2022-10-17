Podman find iSCSI container
===========================
  $ ISCSI_CONTAINER=$(sudo podman ps -a | grep -F 'iscsi' | grep -Fv 'tcmu' | awk '{print $1}')

Dismiss the "could not load preferences file .gwcli/prefs.bin" warning
======================================================================
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls >/dev/null 2>&1

Create a datapool/block0 disk
=============================
  $ sudo podman exec $ISCSI_CONTAINER gwcli disks/ create pool=datapool image=block0 size=300M wwn=36001405da17b74481464e9fa968746d3
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls disks/ | grep 'o- disks' | awk -F'[' '{print $2}'
  300M, Disks: 1]
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls disks/ | grep 'o- datapool' | awk -F'[' '{print $2}'
  datapool (300M)]
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls disks/ | grep 'o- block0' | awk -F'[' '{print $2}'
  datapool/block0 (Unknown, 300M)]

Create the target IQN
=====================
  $ sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/ create target_iqn=iqn.2003-01.com.redhat.iscsi-gw:ceph-gw
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- iscsi-targets' | awk -F'[' '{print $2}'
  DiscoveryAuth: None, Targets: 1]
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- iqn.2003-01.com.redhat.iscsi-gw:ceph-gw' | awk -F'[' '{print $2}'
  Auth: None, Gateways: 0]
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- disks' | awk -F'[' '{print $2}'
  Disks: 0]
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- gateways' | awk -F'[' '{print $2}'
  Up: 0/0, Portals: 0]
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- host-groups' | awk -F'[' '{print $2}'
  Groups : 0]
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- hosts' | awk -F'[' '{print $2}'
  Auth: ACL_ENABLED, Hosts: 0]

Create the first gateway
========================
  $ HOST=$(python3 -c "import socket; print(socket.getfqdn())")
  > IP=`hostname -i | awk '{print $1}'`
  > sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/gateways create ip_addresses=$IP gateway_name=$HOST
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- gateways' | awk -F'[' '{print $2}'
  Up: 1/1, Portals: 1]

Create the second gateway
========================
  $ IP=`cat /etc/ceph/iscsi-gateway.cfg |grep 'trusted_ip_list' | awk -F'[, ]' '{print $3}'`
  > if [ "$IP" != `hostname -i | awk '{print $1}'` ]; then
  >   HOST=$(python3 -c "import socket; print(socket.getfqdn('$IP'))")
  >   sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/gateways create ip_addresses=$IP gateway_name=$HOST
  > fi
  $ IP=`cat /etc/ceph/iscsi-gateway.cfg |grep 'trusted_ip_list' | awk -F'[, ]' '{print $4}'`
  > if [ "$IP" != `hostname -i | awk '{print $1}'` ]; then
  >   HOST=$(python3 -c "import socket; print(socket.getfqdn('$IP'))")
  >   sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/gateways create ip_addresses=$IP gateway_name=$HOST
  > fi
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- gateways' | awk -F'[' '{print $2}'
  Up: 2/2, Portals: 2]

Attach the disk
===============
  $ sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/disks/ add disk=datapool/block0
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- disks' | awk -F'[' '{print $2}'
  Disks: 1]

Create a host
=============
  $ sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/hosts create client_iqn=iqn.1994-05.com.redhat:client
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- hosts' | awk -F'[' '{print $2}'
  Auth: ACL_ENABLED, Hosts: 1]
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- iqn.1994-05.com.redhat:client' | awk -F'[' '{print $2}'
  Auth: None, Disks: 0(0.00Y)]

Map the LUN
===========
  $ sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/hosts/iqn.1994-05.com.redhat:client disk disk=datapool/block0
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- hosts' | awk -F'[' '{print $2}'
  Auth: ACL_ENABLED, Hosts: 1]
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- iqn.1994-05.com.redhat:client' | awk -F'[' '{print $2}'
  Auth: None, Disks: 1(300M)]
