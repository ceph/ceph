.. _architecture:

==============
 Architecture
==============

.. meta::
   :description: How Ceph works: the storage cluster, data placement, high availability, erasure coding, and client interfaces.
   :ceph-page-type: assembly
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: rados

Ceph delivers object, block, and file storage from one cluster of commodity
hardware. Its daemons replicate and redistribute data among themselves, so the
cluster scales from a few nodes to thousands and keeps working when hardware
fails. These pages explain how.

.. image:: ../images/stack.png

.. toctree::
   :maxdepth: 2

   The Ceph Storage Cluster <storage-cluster>
   Scalability and High Availability <scalability-high-availability>
   Dynamic Cluster Management <dynamic-cluster-management>
   Erasure Coding <erasure-coding>
   Ceph Clients <ceph-clients>
   Ceph Protocol <ceph-protocol>
   Extending Ceph <extending-ceph>
   Cache Tiering <cache-tiering>

Additional Resources
====================

- :ref:`ceph-cluster-components`
- :doc:`Glossary </glossary>`
- :ref:`hardware-recommendations`
