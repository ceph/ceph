.. _ceph-cluster-components:

=============================
 The Parts of a Ceph Cluster
=============================

.. meta::
   :description: The daemons that make up a Ceph cluster, what each one does, and how many of each a cluster needs.
   :ceph-page-type: concept
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: docs

A Ceph cluster is a set of daemons that run on one or more hosts. Three kinds
are always present: Monitors, Managers, and OSDs. Two more are added for
specific storage interfaces: Metadata Servers for CephFS, and Object Gateways
for object storage.

.. ditaa::

            +------+ +----------+ +----------+ +-------+ +------+
            | OSDs | | Monitors | | Managers | | MDSes | | RGWs |
            +------+ +----------+ +----------+ +-------+ +------+

.. list-table::
   :header-rows: 1
   :widths: 18 14 46 22

   * - Daemon
     - Process
     - What it does
     - How many
   * - :term:`Monitor <Ceph Monitor>`
     - ``ceph-mon``
     - Holds the maps of the cluster state (:ref:`monitor map
       <display-mon-map>`, Manager map, OSD map, MDS map, CRUSH map) that
       daemons use to coordinate, and handles authentication between daemons
       and clients. A majority of Monitors must be up to form a
       :term:`quorum<Quorum>`, and the cluster does not work without one.
     - Three for redundancy. One is enough for a test cluster.
   * - :term:`Manager <Ceph Manager>`
     - ``ceph-mgr``
     - Tracks runtime metrics and cluster state (storage utilization,
       performance, load) and hosts Python modules for orchestration, the
       :ref:`Dashboard <mgr-dashboard>`, data balancing, and non-native
       clients. Taking this work off the Monitors makes the cluster easier
       to scale.
     - Two for high availability, ideally one per Monitor. One is enough for
       a test cluster.
   * - :term:`OSD <Ceph OSD>`
     - ``ceph-osd``
     - Manages one storage device, usually one disk. Stores data as objects,
       replicates, recovers, and rebalances it, and reports on other OSDs by
       checking their heartbeats.
     - At least as many as the number of copies of each object, and three
       for redundancy in production.
   * - :term:`Metadata Server <Ceph Metadata Server>`
     - ``ceph-mds``
     - Stores the metadata of the :term:`Ceph File System` so that clients
       can run commands like ``ls`` and ``find`` without loading the storage
       cluster. Needed only if you use CephFS. See
       :ref:`orchestrator-cli-cephfs` and :ref:`arch-cephfs`.
     - One per file system, plus a standby.
   * - :term:`Object Gateway <Ceph Object Gateway>`
     - ``ceph-radosgw``
     - A RESTful gateway between applications and the cluster. The
       S3-compatible API is the most used; Swift is also available. Needed
       only if you use object storage.
     - One, or more behind a load balancer.

How Data Is Placed
==================

Ceph stores data as objects in logical pools. For each object, the
:term:`CRUSH` algorithm calculates which :term:`placement group<Placement
Groups (PGs)>` (PG) holds it and which OSDs store that PG. Because every
daemon and client can run the same calculation, nothing keeps a central lookup
table, and the cluster can scale, rebalance, and recover on its own.

Additional Resources
====================

- :doc:`Beginner's Guide <beginners-guide>`
- :ref:`Architecture <architecture>`
- :ref:`Deploying a Single-Host Test Cluster <quick-start-cephadm>`
