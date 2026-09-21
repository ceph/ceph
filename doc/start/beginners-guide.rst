==========================
 Beginner's Guide
==========================

.. meta::
   :description: A plain-language explanation of what Ceph is and what its components do.
   :ceph-page-type: concept

Ceph is software that stores data on several servers and keeps extra copies of
it over the network. In a cluster set up as recommended, the loss of one disk
or one server loses nothing.

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
     - Runs on several servers that work together as one system. Data, and
       the services that manage it, are spread across those servers.
   * - Data redundancy
     - A second copy of your data, or enough coded pieces of it to rebuild it,
       always exists somewhere else in the cluster.

Which Interface to Use
======================

Ceph offers three ways of storing data, all on the same object store,
:term:`RADOS`.

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

What Runs the Cluster
=====================

A cluster is run by :term:`Monitors <Ceph Monitor>`, :term:`Managers <Ceph
Manager>`, and :term:`OSDs <Ceph OSD>`, plus :term:`Metadata Servers <Ceph
Metadata Server>` for CephFS and :term:`Object Gateways <Ceph Object Gateway>`
for object storage. See :ref:`ceph-cluster-components` for what each one does
and how many you need.

Pools and Placement Groups
==========================

Objects are stored in :term:`pools<Pools>`. Each pool is either *replicated*,
which keeps whole copies of each object, or *erasure coded*, which splits each
object into data and coding chunks. The method of data protection is set per
pool.

Each pool is divided into :term:`placement groups<Placement Groups (PGs)>`
(PGs). Ceph maps every object to one PG, and each PG to a set of OSDs.

Additional Resources
====================

- :ref:`The Parts of a Ceph Cluster <ceph-cluster-components>`
- :ref:`Architecture <architecture>`
- :ref:`Deploying a Single-Host Test Cluster <quick-start-cephadm>`
- `Ceph Wiki (requires Ceph Redmine Tracker account) <https://tracker.ceph.com/projects/ceph/wiki>`_
- `Sage Weil's 27 June 2019 "Intro To Ceph" tech talk (1h27m) <https://www.youtube.com/watch?v=PmLPbrf-x9g>`_
- `Sage Weil's 2018 talk "Ceph, the Future of Storage" (27m) <https://www.youtube.com/watch?v=szE4Hg1eXoA>`_
