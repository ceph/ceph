.. _cephfs-multifs:

Multiple Ceph File Systems
==========================


Beginning with the Pacific release, multiple file system support is stable
and ready to use. This functionality allows configuring separate file systems
with full data separation on separate pools.

Existing clusters must set a flag to enable multiple file systems::

    ceph fs flag set enable_multiple true

New Ceph clusters automatically set this.


Creating a new Ceph File System
-------------------------------

The new ``volumes`` plugin interface (see: :doc:`/cephfs/fs-volumes`) automates
most of the work of configuring a new file system. The "volume" concept is
simply a new file system. This can be done via::

    ceph fs volume create <fs_name>

Ceph will create the new pools and automate the deployment of new MDS to
support the new file system. The deployment technology used, e.g. cephadm, will
also configure the MDS affinity (see: :ref:`mds-join-fs`) of new MDS daemons to
operate the new file system.

If the data and metadata pools for the volume are already present, the names of
these pool(s) can be passed as follows::

    ceph fs volume create <vol-name> --meta-pool <meta-pool-name> --data-pool <data-pool-name>



Securing access
---------------

The ``fs authorize`` command allows configuring the client's access to a
particular file system. See also in :ref:`fs-authorize-multifs`. The client will
only have visibility of authorized file systems and the Monitors/MDS will
reject access to clients without authorization.


Other Notes
-----------

Multiple file systems do not share pools. This is particularly important for
snapshots but also because no measures are in place to prevent duplicate
inodes. The Ceph commands prevent this dangerous configuration.

Each file system has its own set of MDS ranks. Consequently, each new file
system requires more MDS daemons to operate and increases operational costs.
This can be useful for increasing metadata throughput by application or user
base but also adds cost to the creation of a file system. Generally, a single
file system with subtree pinning is a better choice for isolating load between
applications.
