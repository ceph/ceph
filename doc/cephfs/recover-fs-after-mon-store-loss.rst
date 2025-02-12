Recovering the file system after catastrophic Monitor store loss
================================================================

During rare occasions, all the monitor stores of a cluster may get corrupted
or lost. To recover the cluster in such a scenario, you need to rebuild the
monitor stores using the OSDs (see :ref:`mon-store-recovery-using-osds`),
and get back the pools intact (active+clean state). However, the rebuilt monitor
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
file system settings such as ``standby_count_wanted``, ``required_client_features``,
extra data pools, etc., are lost and need to be reapplied later.

::

    ceph fs new <fs_name> <metadata_pool> <data_pool> --force --recover

The ``recover`` flag sets the state of file system's rank 0 to existing but
failed. So when a MDS daemon eventually picks up rank 0, the daemon reads the
existing in-RADOS metadata and doesn't overwrite it. The flag also prevents the
standby MDS daemons to activate the file system.

The file system cluster ID, fscid, of the file system will not be preserved.
This behaviour may not be desirable for certain applications (e.g., Ceph CSI)
that expect the file system to be unchanged across recovery. To fix this, you
can optionally set the ``fscid`` option in the above command (see
:ref:`advanced-cephfs-admin-settings`).

Allow standby MDS daemons to join the file system.

::

    ceph fs set <fs_name> joinable true


Check that the file system is no longer in degraded state and has an active
MDS.

::

    ceph fs status

Reapply any other custom file system settings.
