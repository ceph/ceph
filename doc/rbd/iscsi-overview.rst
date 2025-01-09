.. _ceph-iscsi:

==================
Ceph iSCSI Gateway
==================

The iSCSI Gateway presents a Highly Available (HA) iSCSI target that exports
RADOS Block Device (RBD) images as SCSI disks. The iSCSI protocol allows
clients (initiators) to send SCSI commands to storage devices (targets) over a
TCP/IP network, enabling clients without native Ceph client support to access
Ceph block storage.  

Each iSCSI gateway exploits the Linux IO target kernel subsystem (LIO) to
provide iSCSI protocol support. LIO utilizes userspace passthrough (TCMU) to
interact with Ceph's librbd library and expose RBD images to iSCSI clients.
With Ceph’s iSCSI gateway you can provision a fully integrated block-storage
infrastructure with all the features and benefits of a conventional Storage
Area Network (SAN).

.. ditaa::
                  Cluster Network (optional)
                 +-------------------------------------------+
                 |             |               |             |
             +-------+     +-------+       +-------+     +-------+
             |       |     |       |       |       |     |       |
             | OSD 1 |     | OSD 2 |       | OSD 3 |     | OSD N |
             |    {s}|     |    {s}|       |    {s}|     |    {s}|
             +-------+     +-------+       +-------+     +-------+
                 |             |               |             |
      +--------->|             |  +---------+  |             |<---------+
      :          |             |  |   RBD   |  |             |          :
      |          +----------------|  Image  |----------------+          |
      |           Public Network  |    {d}  |                           |
      |                           +---------+                           |
      |                                                                 |
      |                      +-------------------+                      |
      |   +--------------+   | iSCSI Initiators  |   +--------------+   |
      |   | iSCSI GW     |   |   +-----------+   |   | iSCSI GW     |   |
      +-->|  RBD Module  |<--+   | Various   |   +-->|  RBD Module  |<--+
          |              |   |   | Operating |   |   |              |
          +--------------+   |   | Systems   |   |   +--------------+
                             |   +-----------+   |
                             +-------------------+

.. warning::

   The iSCSI gateway is in maintenance as of November 2022. This means that
   it is no longer in active development and will not be updated to add
   new features. 

.. toctree::
  :maxdepth: 1

  Requirements <iscsi-requirements>
  Configuring the iSCSI Target <iscsi-targets>
  Configuring the iSCSI Initiators <iscsi-initiators>
  Monitoring the iSCSI Gateways <iscsi-monitoring>
