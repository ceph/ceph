Upgrading the MDS Cluster
=========================

Currently the MDS cluster does not have built-in versioning or file system
flags to support seamless upgrades of the MDSs without potentially causing
assertions or other faults due to incompatible messages or other functional
differences. For this reason, it's necessary during any cluster upgrade to
reduce the number of active MDS for a file system to one first so that two
active MDS do not communicate with different versions.  Further, it's also
necessary to take standbys offline as any new CompatSet flags will propagate
via the MDSMap to all MDS and cause older MDS to suicide.

The proper sequence for upgrading the MDS cluster is:

1. Disable and stop standby-replay daemons.

::

    ceph fs set <fs_name> allow_standby_replay false

In Pacific, the standby-replay daemons are stopped for you after running this
command. Older versions of Ceph require you to stop these daemons manually.

::

    ceph fs dump # find standby-replay daemons
    ceph mds fail mds.<X>


2. Reduce the number of ranks to 1:

::

    ceph fs set <fs_name> max_mds 1

3. Wait for cluster to stop non-zero ranks where only rank 0 is active and the rest are standbys.

::

    ceph status # wait for MDS to finish stopping

4. Take all standbys offline, e.g. using systemctl:

::

    systemctl stop ceph-mds.target

5. Confirm only one MDS is online and is rank 0 for your FS:

::

    ceph status

6. Upgrade the single active MDS, e.g. using systemctl:

::

    # use package manager to update cluster
    systemctl restart ceph-mds.target

7. Upgrade/start the standby daemons.

::

    # use package manager to update cluster
    systemctl restart ceph-mds.target

8. Restore the previous max_mds for your cluster:

::

    ceph fs set <fs_name> max_mds <old_max_mds>

9. Restore setting for ``allow_standby_replay`` (if applicable):

::

    ceph fs set <fs_name> allow_standby_replay true


Upgrading pre-Firefly file systems past Jewel
=============================================

.. tip::

    This advice only applies to users with file systems
    created using versions of Ceph older than *Firefly* (0.80).
    Users creating new file systems may disregard this advice.

Pre-firefly versions of Ceph used a now-deprecated format
for storing CephFS directory objects, called TMAPs.  Support
for reading these in RADOS will be removed after the Jewel
release of Ceph, so for upgrading CephFS users it is important
to ensure that any old directory objects have been converted.

After installing Jewel on all your MDS and OSD servers, and restarting
the services, run the following command:

::
    
    cephfs-data-scan tmap_upgrade <metadata pool name>

This only needs to be run once, and it is not necessary to
stop any other services while it runs.  The command may take some
time to execute, as it iterates overall objects in your metadata
pool.  It is safe to continue using your file system as normal while
it executes.  If the command aborts for any reason, it is safe
to simply run it again.

If you are upgrading a pre-Firefly CephFS file system to a newer Ceph version
than Jewel, you must first upgrade to Jewel and run the ``tmap_upgrade``
command before completing your upgrade to the latest version.

