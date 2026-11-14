Recovering the file system after catastrophic Monitor store loss
================================================================

During rare occasions, all the Monitor stores of a cluster may get corrupted
or lost. To recover the cluster in such a scenario, you need to rebuild the
Monitor stores using the OSDs (see :ref:`mon-store-recovery-using-osds`),
and get back the pools intact (``active+clean`` state). However, the rebuilt Monitor
stores don't restore the file system maps ("FSMap"). Additional steps are required
to bring back the file system. The steps to recover a multiple active MDS file
system or multiple file systems are yet to be identified. Currently, only the steps
to recover a **single active MDS** file system with no additional file systems
in the cluster have been identified and tested. Briefly the steps are:
recreate the FSMap with basic defaults; and allow MDSs to recover from
the journal/metadata stored in the filesystem's pools. The steps are described
in more detail below.

First up, recreate the file system using the recovered file system pools. The
new FSMap will have the filesystem's default settings. However, the user defined
file system settings such as ``standby_count_wanted`` and ``required_client_features``
are lost and need to be reapplied later. Additional data pools must be added back
before the MDS daemons join the file system, as described below.

::

    ceph fs new <fs_name> <metadata_pool> <data_pool> --force --recover

For ``<data_pool>``, give the file system's default data pool: the data pool
that the file system was created with, not one that was added later.

The ``recover`` flag sets the state of file system's rank 0 to existing but
failed. So when a MDS daemon eventually picks up rank 0, the daemon reads the
existing in-RADOS metadata and doesn't overwrite it. The flag also prevents the
standby MDS daemons to activate the file system.

The file system cluster ID, fscid, of the file system will not be preserved.
This behaviour may not be desirable for certain applications (e.g., Ceph CSI)
that expect the file system to be unchanged across recovery. To fix this, you
can optionally set the ``fscid`` option in the above command (see
:ref:`advanced-cephfs-admin-settings`).

If the file system had additional data pools, for example an erasure-coded
data pool, add them back before you allow MDS daemons to join the file system.
Until a pool is added, the MDS refuses to create files in directories whose
layout names that pool. The rebuilt Monitor stores keep each pool's ``cephfs``
application tag, and ``ceph fs add_data_pool`` refuses a pool that already has
one, so remove the tag from each additional data pool and then add the pool:

::

    ceph osd pool application rm <extra_data_pool> cephfs data
    ceph fs add_data_pool <fs_name> <extra_data_pool>

``ceph fs add_data_pool`` sets the tag again. Client OSD capabilities created
by ``ceph fs authorize`` match data pools by this tag.

Allow standby MDS daemons to join the file system.

::

    ceph fs set <fs_name> joinable true


Check that the file system is no longer in degraded state and has an active
MDS.

::

    ceph fs status

Reapply any other custom file system settings.
