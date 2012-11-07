==============
 Installation
==============

Storage clusters are the foundation of the Ceph system. Ceph storage hosts
provide object storage. Clients access the Ceph storage cluster directly from
an application (using ``librados``), over an object storage protocol such as
Amazon S3 or OpenStack Swift (using ``radosgw``), or with a block device
(using ``rbd``). To begin using Ceph, you must first set up a storage cluster.

You may deploy Ceph with our ``mkcephfs`` bootstrap utility for development
and test environments. For production environments, we recommend deploying 
Ceph with the Chef cloud management tool.

If your deployment uses OpenStack, you will also need to install OpenStack.

The following sections provide guidance for installing components used with
Ceph:

.. toctree::

   Hardware Recommendations <hardware-recommendations>
   OS Recommendations <os-recommendations>
   Installing Debian/Ubuntu Packages <debian>
   Installing RPM Packages <rpm>
   Installing Chef <chef>
