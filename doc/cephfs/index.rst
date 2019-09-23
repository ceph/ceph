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
============

Using the Ceph File System requires at least one :term:`Ceph Metadata Server` in
your Ceph Storage Cluster.



.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Step 1: Metadata Server</h3>

To run the Ceph File System, you must have a running Ceph Storage Cluster with at
least one :term:`Ceph Metadata Server` running.


.. toctree:: 
	:maxdepth: 1

	Provision/Add/Remove MDS(s) <add-remove-mds>
	MDS failover and standby configuration <standby>
	MDS Configuration Settings <mds-config-ref>
	Client Configuration Settings <client-config-ref>
	Journaler Configuration <journaler>
	Manpage ceph-mds <../../man/8/ceph-mds>

.. raw:: html 

	</td><td><h3>Step 2: Mount CephFS</h3>

Once you have a healthy Ceph Storage Cluster with at least
one Ceph Metadata Server, you may create and mount your Ceph File System.
Ensure that your client has network connectivity and the proper
authentication keyring.

.. toctree:: 
	:maxdepth: 1

	Create a CephFS file system <createfs>
	Mount CephFS with the Kernel Driver <kernel>
	Mount CephFS as FUSE <fuse>
	Mount CephFS in fstab <fstab>
	Use the CephFS Shell <cephfs-shell>
	Supported Features of Kernel Driver <kernel-features>
	Manpage ceph-fuse <../../man/8/ceph-fuse>
	Manpage mount.ceph <../../man/8/mount.ceph>
	Manpage mount.fuse.ceph <../../man/8/mount.fuse.ceph>


.. raw:: html 

	</td><td><h3>Additional Details</h3>

.. toctree:: 
    :maxdepth: 1

    Deployment best practices <best-practices>
    MDS States <mds-states>
    Administrative commands <administration>
    Understanding MDS Cache Size Limits <cache-size-limits>
    POSIX compatibility <posix>
    Experimental Features <experimental-features>
    CephFS Quotas <quota>
    Using Ceph with Hadoop <hadoop>
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

.. toctree:: 
   :hidden:

    Advanced: Metadata repair <disaster-recovery-experts>

.. raw:: html

	</td></tr></tbody></table>

For developers
==============

.. toctree:: 
    :maxdepth: 1

    Client's Capabilities <capabilities>
    libcephfs <../../api/libcephfs-java/>
    Mantle <mantle>

