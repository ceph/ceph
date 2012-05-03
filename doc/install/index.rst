=================
 Installing Ceph
=================
Storage clusters are the foundation of the Ceph system. Ceph storage hosts
provide object storage. Clients access the Ceph storage cluster directly from
an application (using ``librados``), over an object storage protocol such as
Amazon S3 or OpenStack Swift (using ``radosgw``), or with a block device
(using ``rbd``). To begin using Ceph, you must first set up a storage cluster.

The following sections provide guidance for configuring a storage cluster and
installing Ceph components:

.. toctree::

   Hardware Recommendations <hardware-recommendations>
   Installing Debian/Ubuntu Packages <debian>
   Installing RPM Packages <rpm>
