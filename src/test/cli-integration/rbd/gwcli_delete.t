Dismiss the "could not load preferences file .gwcli/prefs.bin" warning
======================================================================
  $ sudo gwcli ls >/dev/null 2>&1

Delete the host
===============
  $ sudo gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/hosts delete client_iqn=iqn.1994-05.com.redhat:client
  $ sudo gwcli ls iscsi-targets/ | grep 'o- hosts' | awk -F'[' '{print $2}'
  Auth: ACL_ENABLED, Hosts: 0]

Delete the iscsi-targets disk
=============================
  $ sudo gwcli iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/disks/ delete disk=datapool/block0
  $ sudo gwcli ls iscsi-targets/ | grep 'o- disks' | awk -F'[' '{print $2}'
  Disks: 0]

Delete the target IQN
=====================
  $ sudo gwcli iscsi-targets/ delete target_iqn=iqn.2003-01.com.redhat.iscsi-gw:ceph-gw
  $ sudo gwcli ls iscsi-targets/ | grep 'o- iscsi-targets' | awk -F'[' '{print $2}'
  DiscoveryAuth: None, Targets: 0]

Delete the disks
================
  $ sudo gwcli disks/ delete image_id=datapool/block0
  $ sudo gwcli ls disks/ | grep 'o- disks' | awk -F'[' '{print $2}'
  0.00Y, Disks: 0]
