Delete the host
===============
  $ sudo curl --user admin:admin -X DELETE http://127.0.0.1:5000/api/client/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw/iqn.1994-05.com.redhat:client -s
  {"message":"client delete successful"}

Delete the iscsi-targets disk
=============================
  $ sudo curl --user admin:admin -d disk=datapool/block1 -X DELETE http://127.0.0.1:5000/api/targetlun/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw -s
  {"message":"Target LUN mapping updated successfully"}

Delete the target IQN
=====================
  $ sudo curl --user admin:admin -X DELETE http://127.0.0.1:5000/api/target/iqn.2003-01.com.redhat.iscsi-gw:ceph-gw -s
  {"message":"Target deleted."}

Delete the disks
================
  $ sudo curl --user admin:admin -X DELETE http://127.0.0.1:5000/api/disk/datapool/block1 -s
  {"message":"disk map deletion successful"}
