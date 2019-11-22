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

.. note:: If you are evaluating CephFS for the first time, please review
          the best practices for deployment: :doc:`/cephfs/best-practices`



Using CephFS
^^^^^^^^^^^^

Using CephFS with a running Ceph Storage Cluster requires at least one active
:doc:`Metadata Server (MDS) daemon </cephfs/add-remove-mds>`, :doc:`creating
the file system </cephfs/createfs>`, selecting a mount mechanism for clients
(:doc:`FUSE </cephfs/fuse>` or :doc:`kernel driver </cephfs/kernel>`), and
configuring :doc:`authentication credentials </cephfs/client-auth>` for
clients.

For setting up CephFS quickly, checkout the :doc:`CephFS Quick Start document
</start/quick-cephfs>`.

.. raw:: html

   <!---

Metadata Server Setup
^^^^^^^^^^^^^^^^^^^^^

.. raw:: html

   --->

.. toctree:: 
   :maxdepth: 1
   :hidden:

	Provision/Add/Remove MDS(s) <add-remove-mds>
	MDS failover and standby configuration <standby>
	MDS Configuration Settings <mds-config-ref>
	Client Configuration Settings <client-config-ref>
	Journaler Configuration <journaler>
	Manpage ceph-mds <../../man/8/ceph-mds>


.. raw:: html

   <!---

Mounting CephFS
^^^^^^^^^^^^^^^

.. raw:: html

   --->

.. toctree:: 
   :maxdepth: 1
   :hidden:

	Create a CephFS file system <createfs>
	Mount CephFS using Kernel Driver <kernel>
	Mount CephFS using FUSE <fuse>
	Use the CephFS Shell <cephfs-shell>
	Supported Features of Kernel Driver <kernel-features>
	Manpage ceph-fuse <../../man/8/ceph-fuse>
	Manpage mount.ceph <../../man/8/mount.ceph>
	Manpage mount.fuse.ceph <../../man/8/mount.fuse.ceph>


.. raw:: html

   <!---

Additional Details
^^^^^^^^^^^^^^^^^^

.. raw:: html

   --->

.. toctree:: 
   :maxdepth: 1
   :hidden:

    Deployment best practices <best-practices>
    MDS States <mds-states>
    Administrative commands <administration>
    Understanding MDS Cache Size Limits <cache-size-limits>
    POSIX compatibility <posix>
    Experimental Features <experimental-features>
    CephFS Quotas <quota>
    Using Ceph with Hadoop <hadoop>
    MDS Journaling <mds-journaling>
    cephfs-journal-tool <cephfs-journal-tool>
    File layouts <file-layouts>
    Client eviction <eviction>
    Handling full file systems <full>
    Health messages <health-messages>
    Troubleshooting <troubleshooting>
    Disaster recovery <disaster-recovery>
    Client authentication <client-auth>
    Upgrading old file systems <upgrading>
    Configuring directory fragmentation <dirfrags>
    Configuring multiple active MDS daemons <multimds>
    Export over NFS <nfs>
    Application best practices <app-best-practices>
    Scrub <scrub>
    LazyIO <lazyio>
    Distributed Metadata Cache <mdcache>
    FS volume and subvolumes <fs-volumes>
    Dynamic Metadata Management in CephFS <dynamic-metadata-management>
    CephFS IO Path <cephfs-io-path>

.. raw:: html

   <!---

Metadata Repair
^^^^^^^^^^^^^^^

.. raw:: html

   --->

.. toctree:: 
   :hidden:

    Advanced: Metadata repair <disaster-recovery-experts>


.. raw:: html

   <!---

For Developers
^^^^^^^^^^^^^^

.. raw:: html

   --->

.. toctree:: 
   :maxdepth: 1
   :hidden:

    Client's Capabilities <capabilities>
    libcephfs <../../api/libcephfs-java/>
    Mantle <mantle>

