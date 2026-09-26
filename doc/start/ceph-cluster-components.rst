.. _ceph-cluster-components:

=============================
 The Parts of a Ceph Cluster
=============================

.. meta::
   :description: The daemons that make up a Ceph cluster, what each one does, and how many of each a cluster needs.
   :ceph-page-type: concept

A Ceph cluster is a set of daemons (background programs) that run on one or
more hosts. Three kinds
are always present: Monitors, Managers, and OSDs. Two more are added for
specific storage interfaces: Metadata Servers for CephFS, and Object Gateways
for object storage.

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
       and clients.
     - Three for redundancy; a majority must be up or the cluster stops. One
       is enough for a test cluster.
   * - :term:`Manager <Ceph Manager>`
     - ``ceph-mgr``
     - Tracks runtime metrics and cluster state (storage utilization,
       performance, load) and hosts Python modules for orchestration
       (deploying daemons, through ``ceph orch``), the :ref:`Dashboard
       <mgr-dashboard>`, data balancing, and access for non-native clients
       such as NFS.
     - Two for high availability, ideally one per Monitor; with one, a
       Manager restart pauses the Dashboard and modules. One is enough for a
       test cluster.
   * - :term:`OSD <Ceph OSD>`
     - ``ceph-osd``
     - Manages one storage device, usually one disk. Stores data as objects,
       replicates, recovers, and rebalances it, and reports OSDs that stop
       answering heartbeat checks.
     - At least as many as the number of copies of each object (or of
       chunks, for erasure coding); below that, pools cannot become healthy.
       Three for redundancy in production.
   * - :term:`Metadata Server <Ceph Metadata Server>`
     - ``ceph-mds``
     - Stores the metadata of the :term:`Ceph File System` (directory
       listings, file attributes). Needed only if you use CephFS. See
       :ref:`orchestrator-cli-cephfs` and :ref:`arch-cephfs`.
     - At least one active per file system, plus a standby.
   * - :term:`Object Gateway <Ceph Object Gateway>`
     - ``ceph-radosgw``
     - An HTTP gateway that provides S3-compatible and Swift-compatible APIs.
       Needed only if you use object storage.
     - One, or more behind a load balancer.

How Data Is Placed
==================

Ceph stores data as objects in logical pools. Ceph hashes each object name
to a :term:`placement group<Placement Groups (PGs)>` (PG), and the
:term:`CRUSH` algorithm calculates which OSDs store that PG. Because every
daemon and client can run the same calculation, nothing keeps a central lookup
table, and the cluster can scale, rebalance, and recover on its own.

Additional Resources
====================

- :doc:`Beginner's Guide <beginners-guide>`
- :ref:`Architecture <architecture>`
- :ref:`Deploying a Single-Host Test Cluster <quick-start-cephadm>`
