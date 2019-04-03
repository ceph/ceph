.. _ceph-filesystem:

=================
 Ceph Filesystem
=================

The Ceph Filesystem (CephFS) is a POSIX-compliant filesystem that uses
a Ceph Storage Cluster to store its data. The Ceph filesystem uses the same Ceph
Storage Cluster system as Ceph Block Devices, Ceph Object Storage with its S3
and Swift APIs, or native bindings (librados).

.. note:: If you are evaluating CephFS for the first time, please review
          the best practices for deployment: :doc:`/cephfs/best-practices`

.. ditaa::
            +-----------------------+  +------------------------+
            |                       |  |      CephFS FUSE       |
            |                       |  +------------------------+
            |                       |
            |                       |  +------------------------+
            |  CephFS Kernel Object |  |     CephFS Library     |
            |                       |  +------------------------+
            |                       |
            |                       |  +------------------------+
            |                       |  |        librados        |
            +-----------------------+  +------------------------+

            +---------------+ +---------------+ +---------------+
            |      OSDs     | |      MDSs     | |    Monitors   |
            +---------------+ +---------------+ +---------------+


Using CephFS
============

Using the Ceph Filesystem requires at least one :term:`Ceph Metadata Server` in
your Ceph Storage Cluster.



.. raw:: html

	<style type="text/css">div.body h3{margin:5px 0px 0px 0px;}</style>
	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Step 1: Metadata Server</h3>

To run the Ceph Filesystem, you must have a running Ceph Storage Cluster with at
least one :term:`Ceph Metadata Server` running.


.. toctree:: 
	:maxdepth: 1

	Add/Remove MDS(s) <add-remove-mds>
	MDS states <mds-states>
	MDS failover and standby configuration <standby>
	MDS Configuration Settings <mds-config-ref>
	Client Configuration Settings <client-config-ref>
	Journaler Configuration <journaler>
	Manpage ceph-mds <../../man/8/ceph-mds>

.. raw:: html 

	</td><td><h3>Step 2: Mount CephFS</h3>

Once you have a healthy Ceph Storage Cluster with at least
one Ceph Metadata Server, you may create and mount your Ceph Filesystem.
Ensure that your client has network connectivity and the proper
authentication keyring.

.. toctree:: 
	:maxdepth: 1

	Create CephFS <createfs>
	Mount CephFS <kernel>
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
    Handling full filesystems <full>
    Health messages <health-messages>
    Troubleshooting <troubleshooting>
    Disaster recovery <disaster-recovery>
    Client authentication <client-auth>
    Upgrading old filesystems <upgrading>
    Configuring directory fragmentation <dirfrags>
    Configuring multiple active MDS daemons <multimds>
    Export over NFS <nfs>
    Application best practices <app-best-practices>
    Scrub <scrub>
    LazyIO <lazyio>

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

