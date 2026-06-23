.. _upgrade-mds-cluster:

Upgrading the MDS Cluster
=========================

Currently the MDS cluster does not have built-in versioning or file system
flags to support seamless upgrades of the MDSs without potentially causing
assertions or other faults due to incompatible messages or other functional
differences. For this reason, it's necessary during any cluster upgrade to
reduce the number of active MDS for a file system to one first so that two
active MDS do not communicate with different versions.

The proper sequence for upgrading the MDS cluster is:

1. For each file system, disable and stop standby-replay daemons.

::

    ceph fs set <fs_name> allow_standby_replay false

In Pacific, the standby-replay daemons are stopped for you after running this
command. Older versions of Ceph require you to stop these daemons manually.

::

    ceph fs dump # find standby-replay daemons
    ceph mds fail mds.<X>


2. For each file system, reduce the number of ranks to 1:

::

    ceph fs set <fs_name> max_mds 1

3. Wait for cluster to stop non-zero ranks where only rank 0 is active and the rest are standbys.

::

    ceph status # wait for MDS to finish stopping

4. For each MDS, upgrade packages and restart. Note: to reduce failovers, it is
   recommended -- but not strictly necessary -- to first upgrade standby daemons.

::

    # use package manager to update cluster
    systemctl restart ceph-mds.target

5. For each file system, restore the previous max_mds and allow_standby_replay settings for your cluster:

::

    ceph fs set <fs_name> max_mds <old_max_mds>
    ceph fs set <fs_name> allow_standby_replay <old_allow_standby_replay>

