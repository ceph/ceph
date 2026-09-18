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

These pages explain how Ceph stores data, keeps it available when hardware
fails, and serves it to clients.

.. image:: ../images/stack.png
   :alt: The Ceph stack: RADOS at the base; librados above it; RADOSGW, RBD,
         and CephFS on top, each reached by applications, hosts or virtual
         machines, and clients.

.. toctree::
   :maxdepth: 2
   :hidden:

   The Ceph Storage Cluster <storage-cluster>
   Scalability and High Availability <scalability-high-availability>
   Dynamic Cluster Management <dynamic-cluster-management>
   Erasure Coding <erasure-coding>
   Ceph Clients <ceph-clients>
   Ceph Protocol <ceph-protocol>
   Ceph Object Classes <extending-ceph>
   Cache Tiering (Deprecated) <cache-tiering>

How Ceph Stores Data
====================

- :ref:`The Ceph Storage Cluster <arch-ceph-storage-cluster>`: RADOS, the
  object store under every Ceph service.
- :ref:`Scalability and High Availability
  <arch_scalability_and_high_availability>`: CRUSH, the cluster map, Monitor
  quorum, and authentication.
- :doc:`Dynamic Cluster Management <dynamic-cluster-management>`: pools,
  placement groups, peering, rebalancing, and scrubbing.
- :doc:`Erasure Coding <erasure-coding>`: how chunks are written and read.

How Clients Use It
==================

- :ref:`Ceph Clients <architecture_ceph_clients>`: object, block, and file
  interfaces.
- :doc:`Ceph Protocol <ceph-protocol>`: librados, watch/notify, and striping.

Extending Ceph and Legacy Features
==================================

- :doc:`Ceph Object Classes <extending-ceph>`: extending the OSD with your own
  object methods.
- :doc:`Cache Tiering (Deprecated) <cache-tiering>`: kept for clusters that
  still run it.

Why This Matters for Sizing
===========================

Heartbeats, peering, rebalancing, and recovery run on the OSD hosts, so every
server needs CPU, RAM, and network for them. See
:ref:`hardware-recommendations`.

Next Steps
==========

- :ref:`ceph-cluster-components`
- :ref:`quick-start-cephadm`

Additional Resources
====================

- :doc:`Glossary </glossary>`
- :ref:`hardware-recommendations`
