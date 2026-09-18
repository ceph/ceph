==========================
 Beginner's Guide
==========================

.. meta::
   :description: A plain-language explanation of what Ceph is and what its components do.
   :ceph-page-type: concept
   :ceph-applies-to: squid, tentacle
   :ceph-reviewed: 2026-09
   :ceph-owner: docs

Ceph is a clustered and distributed storage manager. In plain terms: Ceph is
software that stores data on several servers and uses the network to keep
extra copies of that data. In a cluster set up as recommended, the loss of
one disk or one server loses nothing.

What the Words Mean
===================

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Term
     - Meaning
   * - Storage manager
     - Software that puts data on storage devices, usually hard disk drives
       (HDDs) and solid-state drives (SSDs), and gets it back.
   * - Clustered and distributed
     - Runs on several servers that work together as one system. Data and the
       infrastructure that supports it are spread across those servers, unlike
       a traditional storage array, which exposes one logical disk from one
       box.
   * - Data redundancy
     - A second copy of your data, or enough coded pieces of it to rebuild it,
       always exists somewhere else in the cluster.

Storage Interfaces
==================

Ceph offers three ways of storing data. All three are built on the same
object store, :term:`RADOS`; the file system and block device interfaces are
presented on top of it.

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Interface
     - What it gives you
     - Typical use
   * - :term:`CephFS<Ceph File System>`
     - A shared file system that behaves like a local Linux file system
       (POSIX-compatible)
     - Home directories, shared project data, NFS and SMB exports
   * - :term:`RBD`
     - Block devices (virtual disks)
     - Disks for virtual machines and containers
   * - :term:`RGW<Ceph Object Gateway>`
     - An object store with S3-compatible and Swift-compatible APIs
     - Backups, media, data lakes, cloud-native applications

Daemons
=======

A Ceph cluster is run by a small set of daemons (background programs):
:term:`Monitors <Ceph Monitor>`, :term:`Managers <Ceph Manager>`,
:term:`OSDs <Ceph OSD>`, and, depending on the storage interfaces in use,
:term:`Metadata Servers <Ceph Metadata Server>` and :term:`Object Gateways
<Ceph Object Gateway>`. See :ref:`ceph-cluster-components` for what each daemon does and how
many of each a cluster needs.

Pools and Placement Groups
==========================

Objects are stored in :term:`pools<Pools>`. Each pool is either *replicated*,
which keeps whole copies of each object, or *erasure coded*, which splits each
object into data and coding chunks. The method of data protection is set per
pool.

Each pool is divided into :term:`placement groups<Placement Groups (PGs)>`
(PGs). Ceph maps every object to one PG, and each PG to a set of OSDs. This is
how Ceph spreads data across the cluster without keeping a lookup table for
every object.

Additional Resources
====================

- :ref:`The Parts of a Ceph Cluster <ceph-cluster-components>`
- :ref:`Architecture <architecture>`
- :ref:`Deploying a Single-Host Test Cluster <quick-start-cephadm>`
- :doc:`Developer Quick Guide (build from source, vstart) </dev/quick_guide>`
- `Ceph Wiki (requires Ceph Redmine Tracker account) <https://tracker.ceph.com/projects/ceph/wiki>`_
- `Sage Weil's 27 June 2019 "Intro To Ceph" tech talk (1h27m) <https://www.youtube.com/watch?v=PmLPbrf-x9g>`_
- `Sage Weil's 2018 talk "Ceph, the Future of Storage" (27m) <https://www.youtube.com/watch?v=szE4Hg1eXoA>`_
