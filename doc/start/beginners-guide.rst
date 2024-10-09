==========================
 Beginner's Guide
==========================

The purpose of A Beginner's Guide to Ceph is to make Ceph comprehensible.

Ceph is a clustered and distributed storage manager. If that's too cryptic,
then just think of Ceph as a computer program that stores data and uses a
network to make sure that there is a backup copy of the data.

Storage Interfaces
------------------

Ceph offers several "storage interfaces", which is another
way of saying "ways of storing data". These storage interfaces include: 
- CephFS (a file system) 
- RBD (block devices) 
- RADOS (an object store).

Deep down, though, all three of these are really RADOS object stores. CephFS
and RBD are just presenting themselves as file systems and block devices.

Storage Manager: What is It?
----------------------------

Ceph is a clustered and distributed storage manager that offers data
redundancy. This sentence might be too cryptic for first-time readers of the
Ceph Beginner's Guide, so let's explain all of the terms in it:

- **Storage Manager.** Ceph is a storage manager. This means that Ceph is
  software that helps storage resources store data. Storage resources come in
  several forms: hard disk drives (HDD), solid-state drives (SSD), magnetic
  tape, floppy disks, punched tape, Hollerith-style punch cards, and magnetic
  drum memory are all forms of storage resources. In this beginner's guide,
  we'll focus on hard disk drives (HDD) and solid-state drives (SSD).
- **Clustered storage manager.** Ceph is a clustered storage manager. That
  means that the storage manager installed not just on a single machine but on
  several machines that work together as a system.
- **Distributed storage manager.** Ceph is a clustered and distributed storage
  manager. That means that the data that is stored and the infrastructure that
  supports it is spread across multiple machines and is not centralized in a
  single machine. To better understand what distributed means in this context,
  it might be helpful to describe what it is not: it is not a system ISCSI,
  which is a system that exposes a single logical disk over the network in a
  1:1 (one-to-one) mapping.
- **Data Redundancy.** Having a second copy of your data somewhere.

Ceph Monitor 
------------

The Ceph Monitor is one of the daemons essential to the functioning of a Ceph
cluster. Monitors know the location of all the data in the Ceph cluster.
Monitors maintain maps of the cluster state, and those maps make it possible
for Ceph daemons to work together. These maps include the monitor map, the OSD
map, the MDS map, and the CRUSH map. Three monitors are required to reach
quorum. Quorum is a state that is necessary for a Ceph cluster to work
properly. Quorum means that a majority of the monitors are in the "up" state.

MANAGER
-------
The manager balances the data in the Ceph cluster, distributing load evenly so
that no part of the cluster gets overloaded. The manager is one of the daemons
essential to the functioning of the Ceph cluster. Managers keep track of
runtime metrics, system utilization, CPU performance, disk load, and they host
the Ceph dashboard web GUI.

OSD
---

Object Storage Daemons (OSDs) store objects.

An OSD is a process that runs on a storage server. The OSD is responsible for
managing a single unit of storage, which is usually a single disk.

POOLS
-----

A pool is an abstraction that can be designated as either "replicated" or
"erasure coded". In Ceph, the method of data protection is set at the pool
level. Ceph offers and supports two types of data protection: replication and
erasure coding. Objects are stored in pools. "A storage pool is a collection of
storage volumes. A storage volume is the basic unit of storage, such as
allocated space on a disk or a single tape cartridge. The server uses the
storage volumes to store backed-up, archived, or space-managed files." (IBM
Tivoli Storage Manager, Version 7.1, "Storage Pools")

PLACEMENT GROUPS
----------------

Placement groups are a part of pools.

MDS
---
A metadata server (MDS) is necessary for the proper functioning of CephFS.
See :ref:`orchestrator-cli-cephfs` and :ref:`arch-cephfs`.

LINKS
-----

#. `Ceph Wiki (requires Ceph Redmine Tracker account) <https://tracker.ceph.com/projects/ceph/wiki>`_
#. `Sage Weil's 27 June 2019 "Intro To Ceph" tech talk (1h27m) <https://www.youtube.com/watch?v=PmLPbrf-x9g>`_
#. `Sage Weil's 2018 talk "Ceph, the Future of Storage" (27m) <https://www.youtube.com/watch?v=szE4Hg1eXoA>`_
