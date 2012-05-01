==========================
Installing Ceph Components
==========================
Storage clusters are the foundation of the Ceph system. Ceph storage hosts provide object storage. 
Clients access the Ceph storage cluster directly from an application (using ``librados``),
over an object storage protocol such as Amazon S3 or OpenStack Swift (using ``radosgw``), or with a block
device (using ``rbd``). To begin using Ceph, you must first set up a storage cluster.

The following sections provide guidance for configuring a storage cluster and installing Ceph components:

1. :doc:`Hardware Recommendations <hardware_recommendations>`
2. :doc:`File System Recommendations <file_system_recommendations>`
3. :doc:`Host Recommendations <host_recommendations>`
4. :doc:`Download Ceph Packages <download_packages>`
5. :doc:`Building Ceph from Source <building_ceph_from_source>`
6. :doc:`Installing Packages <installing_packages>`

.. toctree::
   :hidden:

   Hardware Recs <hardware_recommendations>
   File System Recs <file_system_recommendations>
   Host Recs <host_recommendations>
   Download Packages <download_packages>
   Build From Source <building_ceph_from_source>
   Install Packages <installing_packages>
