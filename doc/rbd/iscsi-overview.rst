.. _ceph-iscsi:

==================
Ceph iSCSI Gateway
==================

The iSCSI gateway is integrating Ceph Storage with the iSCSI standard to provide
a Highly Available (HA) iSCSI target that exports RADOS Block Device (RBD) images
as SCSI disks. The iSCSI protocol allows clients (initiators) to send SCSI commands
to SCSI storage devices (targets) over a TCP/IP network. This allows for heterogeneous
clients, such as Microsoft Windows, to access the Ceph Storage cluster.

Each iSCSI gateway runs the Linux IO target kernel subsystem (LIO) to provide the
iSCSI protocol support. LIO utilizes a userspace passthrough (TCMU) to interact
with Ceph's librbd library and expose RBD images to iSCSI clients. With Ceph’s
iSCSI gateway you can effectively run a fully integrated block-storage
infrastructure with all the features and benefits of a conventional Storage Area
Network (SAN).

.. ditaa::
                  Cluster Network
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


.. toctree::
  :maxdepth: 1

  Requirements <iscsi-requirements>
  Configuring the iSCSI Target <iscsi-targets>
  Configuring the iSCSI Initiators <iscsi-initiators>
  Monitoring the iSCSI Gateways <iscsi-monitoring>
