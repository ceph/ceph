.. _ceph-nvmeof:

======================
 Ceph NVMe-oF Gateway
======================

The NVMe-oF Gateway presents an NVMe-oF target that exports
RADOS Block Device (RBD) images as NVMe namespaces. The NVMe-oF protocol allows
clients (initiators) to send NVMe commands to storage devices (targets) over a
TCP/IP network, enabling clients without native Ceph client support to access
Ceph block storage.  

Each NVMe-oF gateway consists of an `SPDK <https://spdk.io/>`_ NVMe-oF target
with ``bdev_rbd`` and a control daemon. Ceph’s NVMe-oF gateway can be used to
provision a fully integrated block-storage infrastructure with all the features
and benefits of a conventional Storage Area Network (SAN).

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
      +--------->|             |  +---------+  |             |<----------+
      :          |             |  |   RBD   |  |             |           :
      |          +----------------|  Image  |----------------+           |
      |           Public Network  |    {d}  |                            |
      |                           +---------+                            |
      |                                                                  |
      |                      +--------------------+                      |
      |   +--------------+   | NVMeoF Initiators  |   +--------------+   |
      |   |  NVMe‐oF GW  |   |    +-----------+   |   | NVMe‐oF GW   |   |
      +-->|  RBD Module  |<--+    | Various   |   +-->|  RBD Module  |<--+
          |              |   |    | Operating |   |   |              |
          +--------------+   |    | Systems   |   |   +--------------+
                             |    +-----------+   |
                             +--------------------+

.. toctree::
  :maxdepth: 1

  Requirements <nvmeof-requirements>
  Configuring the NVME-oF Target <nvmeof-target-configure>
  Configuring the NVMe-oF Initiators <nvmeof-initiators>
