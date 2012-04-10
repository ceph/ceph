=============================
Designing a Storage Cluster
=============================
Storage clusters are the foundation of the Ceph file system, and they can also provide
object storage to clients via ``librados``, ``rbd`` and ``radosgw``. The following sections 
provide guidance for configuring a storage cluster:

1. :doc:`Introduction to RADOS OSDs <introduction_to_rados_osds>`
2. :doc:`Hardware Requirements <hardware_requirements>`
3. :doc:`File System Requirements <file_system_requirements>`
4. :doc:`Build Prerequisites <build_prerequisites>`
5. :doc:`Download Packages <download_packages>`
6. :doc:`Downloading a Ceph Release <downloading_a_ceph_release>`
7. :doc:`Cloning the Ceph Source Code Repository <cloning_the_ceph_source_code_repository>`
8. :doc:`Building Ceph<building_ceph>`
9. :doc:`Installing RADOS Processes and Daemons <installing_rados_processes_and_daemons>`

.. toctree::
   :hidden:

   Introduction <introduction_to_rados_osds>
   Hardware <hardware_requirements>
   File System Reqs <file_system_requirements>
   build_prerequisites
   Download Packages <download_packages>
   Download a Release <downloading_a_ceph_release>
   Clone the Source Code <cloning_the_ceph_source_code_repository>
   building_ceph
   Installation <installing_rados_processes_and_daemons>