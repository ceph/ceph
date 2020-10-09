.. _ceph-file-system:

=================
 Ceph File System
=================

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


Getting Started with CephFS
^^^^^^^^^^^^^^^^^^^^^^^^^^^

For most deployments of Ceph, setting up a CephFS file system is as simple as:

.. code:: bash

    ceph fs volume create <fs name>

The Ceph `Orchestrator`_  will automatically create and configure MDS for
your file system if the back-end deployment technology supports it (see
`Orchestrator deployment table`_). Otherwise, please `deploy MDS manually
as needed`_.

Finally, to mount CephFS on your client nodes, see `Mount CephFS:
Prerequisites`_ page. Additionally, a command-line shell utility is available
for interactive access or scripting via the `cephfs-shell`_.

.. _cephfs-administration: administration
.. _Orchestrator: ../mgr/orchestrator
.. _deploy MDS manually as needed: add-remove-mds
.. _Orchestrator deployment table: ../mgr/orchestrator/#current-implementation-status
.. _Mount CephFS\: Prerequisites: mount-prerequisites
.. _cephfs-shell: cephfs-shell

.. toctree:: 
   :maxdepth: 2
   :hidden:
    
    Administration <administration>

    Mounting CephFS <mounting-cephfs-overview>

    CephFS Concepts <cephfs-concepts-overview>

    Troubleshooting and Disaster Recovery <troubleshooting-overview>

    Developer Guide <dev-guides-overview>

    Additional Details <additional-details-overview>
