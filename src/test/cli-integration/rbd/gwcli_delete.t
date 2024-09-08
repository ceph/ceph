Podman find iSCSI container
===========================
  $ ISCSI_CONTAINER=$(sudo podman ps -a | grep -F 'iscsi' | grep -Fv 'tcmu' | awk '{print $1}')

Dismiss the "could not load preferences file .gwcli/prefs.bin" warning
======================================================================
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls >/dev/null 2>&1

Delete the host
===============
  $ sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/hosts delete client_iqn=iqn.1994-05.com.redhat:client
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- hosts' | awk -F'[' '{print $2}'
  Auth: ACL_ENABLED, Hosts: 0]

Delete the iscsi-targets disk
=============================
  $ sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/disks/ delete disk=datapool/block0
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- disks' | awk -F'[' '{print $2}'
  Disks: 0]

Delete the target IQN
=====================
  $ sudo podman exec $ISCSI_CONTAINER gwcli iscsi-targets/ delete target_iqn=iqn.2003-01.com.redhat.iscsi-gw:ceph-gw
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls iscsi-targets/ | grep 'o- iscsi-targets' | awk -F'[' '{print $2}'
  DiscoveryAuth: None, Targets: 0]

Delete the disks
================
  $ sudo podman exec $ISCSI_CONTAINER gwcli disks/ delete image_id=datapool/block0
  $ sudo podman exec $ISCSI_CONTAINER gwcli ls disks/ | grep 'o- disks' | awk -F'[' '{print $2}'
  0.00Y, Disks: 0]
