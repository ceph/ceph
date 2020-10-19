.. _ceph-file-system:

=================
 Ceph File System
=================

.. toctree:: 
   :maxdepth: 2
   :hidden:

    Getting Started with CephFS <getting-started>

    Administration <admin-overview>

    Mounting CephFS <mounting-cephfs-overview>
    
    CephFS Concepts <cephfs-concepts-overview>

    Troubleshooting and Disaster Recovery <troubleshooting-overview>
       
    Developer Guide <dev-guides-overview>

    Additional Details <additional-details-overview>

The Ceph File System, or **CephFS**, is a POSIX-compliant file system built on
top of Ceph's distributed object store, **RADOS**. CephFS endeavors to provide
a state-of-the-art, multi-use, highly available, and performant file store for
a variety of applications, including traditional use-cases like shared home
directories, HPC scratch space, and distributed workflow shared storage.

CephFS achieves these goals through the use of some novel architectural
choices.  Notably, file metadata is stored in a separate RADOS pool from file
data and served via a resizable cluster of *Metadata Servers*, or **MDS**,
which may scale to support higher throughput metadata workloads.  Clients of
the file system have direct access to RADOS for reading and writing file data
blocks. For this reason, workloads may linearly scale with the size of the
underlying RADOS object store; that is, there is no gateway or broker mediating
data I/O for clients.

Access to data is coordinated through the cluster of MDS which serve as
authorities for the state of the distributed metadata cache cooperatively
maintained by clients and MDS. Mutations to metadata are aggregated by each MDS
into a series of efficient writes to a journal on RADOS; no metadata state is
stored locally by the MDS. This model allows for coherent and rapid
collaboration between clients within the context of a POSIX file system.

.. image:: cephfs-architecture.svg

CephFS is the subject of numerous academic papers for its novel designs and
contributions to file system research. It is the oldest storage interface in
Ceph and was once the primary use-case for RADOS.  Now it is joined by two
other storage interfaces to form a modern unified storage system: RBD (Ceph
Block Devices) and RGW (Ceph Object Storage Gateway).

.. include:: ./getting-started.rst
