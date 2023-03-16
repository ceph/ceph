=====================
 BlueStore Migration
=====================

Each OSD can run either BlueStore or Filestore, and a single Ceph
cluster can contain a mix of both.  Users who have previously deployed
Filestore OSDs should transition to BlueStore in order to
take advantage of the improved performance and robustness.  Moreover,
Ceph releases beginning with Reef do not support Filestore. There are
several strategies for making such a transition.

An individual OSD cannot be converted in place;
BlueStore and Filestore are simply too different for that to be
feasible.  The conversion process uses either the cluster's normal
replication and healing support or tools and strategies that copy OSD
content from an old (Filestore) device to a new (BlueStore) one.


Deploy new OSDs with BlueStore
==============================

New OSDs (e.g., when the cluster is expanded) should be deployed
using BlueStore.  This is the default behavior so no specific change
is needed.

Similarly, any OSDs that are reprovisioned after replacing a failed drive
should use BlueStore.

Convert existing OSDs
=====================

Mark out and replace
--------------------

The simplest approach is to ensure that the cluster is healthy,
then mark ``out`` each device in turn, wait for
data to replicate across the cluster, reprovision the OSD, and mark
it back ``in`` again.  Proceed to the next OSD when recovery is complete.
This is easy to automate but results in more data migration than
is strictly necessary, which in turn presents additional wear to SSDs and takes
longer to complete.

#. Identify a Filestore OSD to replace::

     ID=<osd-id-number>
     DEVICE=<disk-device>

   You can tell whether a given OSD is Filestore or BlueStore with:

   .. prompt:: bash $

      ceph osd metadata $ID | grep osd_objectstore

   You can get a current count of Filestore and BlueStore OSDs with:

   .. prompt:: bash $

      ceph osd count-metadata osd_objectstore

#. Mark the Filestore OSD ``out``:

   .. prompt:: bash $

      ceph osd out $ID

#. Wait for the data to migrate off the OSD in question:

   .. prompt:: bash $

      while ! ceph osd safe-to-destroy $ID ; do sleep 60 ; done

#. Stop the OSD:

   .. prompt:: bash $

      systemctl kill ceph-osd@$ID

#. Note which device this OSD is using:

   .. prompt:: bash $

      mount | grep /var/lib/ceph/osd/ceph-$ID

#. Unmount the OSD:

   .. prompt:: bash $

      umount /var/lib/ceph/osd/ceph-$ID

#. Destroy the OSD data. Be *EXTREMELY CAREFUL* as this will destroy
   the contents of the device; be certain the data on the device is
   not needed (i.e., that the cluster is healthy) before proceeding:

   .. prompt:: bash $

      ceph-volume lvm zap $DEVICE

#. Tell the cluster the OSD has been destroyed (and a new OSD can be
   reprovisioned with the same ID):

   .. prompt:: bash $

      ceph osd destroy $ID --yes-i-really-mean-it

#. Provision a BlueStore OSD in its place with the same OSD ID.
   This requires you do identify which device to wipe based on what you saw
   mounted above. BE CAREFUL! Also note that hybrid OSDs may require
   adjustments to these commands:

   .. prompt:: bash $

      ceph-volume lvm create --bluestore --data $DEVICE --osd-id $ID

#. Repeat.

You can allow balancing of the replacement OSD to happen
concurrently with the draining of the next OSD, or follow the same
procedure for multiple OSDs in parallel, as long as you ensure the
cluster is fully clean (all data has all replicas) before destroying
any OSDs.  If you reprovision multiple OSDs in parallel, be **very** careful to
only zap / destroy OSDs within a single CRUSH failure domain, e.g. ``host`` or
``rack``.  Failure to do so will reduce the redundancy and availability of
your data and increase the risk of (or even cause) data loss.


Advantages:

* Simple.
* Can be done on a device-by-device basis.
* No spare devices or hosts are required.

Disadvantages:

* Data is copied over the network twice: once to some other OSD in the
  cluster (to maintain the desired number of replicas), and then again
  back to the reprovisioned BlueStore OSD.


Whole host replacement
----------------------

If you have a spare host in the cluster, or have sufficient free space
to evacuate an entire host in order to use it as a spare, then the
conversion can be done on a host-by-host basis with each stored copy of
the data migrating only once.

First, you need an empty host that has no OSDs provisioned.  There are two
ways to do this: either by starting with a new, empty host that isn't yet
part of the cluster, or by offloading data from an existing host in the cluster.

Use a new, empty host
^^^^^^^^^^^^^^^^^^^^^

Ideally the host should have roughly the
same capacity as other hosts you will be converting.
Add the host to the CRUSH hierarchy, but do not attach it to the root:

.. prompt:: bash $

   NEWHOST=<empty-host-name>
   ceph osd crush add-bucket $NEWHOST host

Make sure that Ceph packages are installed on the new host.

Use an existing host
^^^^^^^^^^^^^^^^^^^^

If you would like to use an existing host
that is already part of the cluster, and there is sufficient free
space on that host so that all of its data can be migrated off to
other cluster hosts, you can instead do::


.. prompt:: bash $ 
   
   OLDHOST=<existing-cluster-host-to-offload>
   ceph osd crush unlink $OLDHOST default

where "default" is the immediate ancestor in the CRUSH map. (For
smaller clusters with unmodified configurations this will normally
be "default", but it might also be a rack name.)  You should now
see the host at the top of the OSD tree output with no parent:

.. prompt:: bash $

   bin/ceph osd tree

::

  ID CLASS WEIGHT  TYPE NAME     STATUS REWEIGHT PRI-AFF
  -5             0 host oldhost
  10   ssd 1.00000     osd.10        up  1.00000 1.00000
  11   ssd 1.00000     osd.11        up  1.00000 1.00000
  12   ssd 1.00000     osd.12        up  1.00000 1.00000
  -1       3.00000 root default
  -2       3.00000     host foo
   0   ssd 1.00000         osd.0     up  1.00000 1.00000
   1   ssd 1.00000         osd.1     up  1.00000 1.00000
   2   ssd 1.00000         osd.2     up  1.00000 1.00000
  ...

If everything looks good, jump directly to the "Wait for data
migration to complete" step below and proceed from there to clean up
the old OSDs.

Migration process
^^^^^^^^^^^^^^^^^

If you're using a new host, start at step #1.  For an existing host,
jump to step #5 below.

#. Provision new BlueStore OSDs for all devices:

   .. prompt:: bash $

      ceph-volume lvm create --bluestore --data /dev/$DEVICE

#. Verify OSDs join the cluster with:

   .. prompt:: bash $

      ceph osd tree

   You should see the new host ``$NEWHOST`` with all of the OSDs beneath
   it, but the host should *not* be nested beneath any other node in
   hierarchy (like ``root default``).  For example, if ``newhost`` is
   the empty host, you might see something like::

     $ bin/ceph osd tree
     ID CLASS WEIGHT  TYPE NAME     STATUS REWEIGHT PRI-AFF
     -5             0 host newhost
     10   ssd 1.00000     osd.10        up  1.00000 1.00000
     11   ssd 1.00000     osd.11        up  1.00000 1.00000
     12   ssd 1.00000     osd.12        up  1.00000 1.00000
     -1       3.00000 root default
     -2       3.00000     host oldhost1
      0   ssd 1.00000         osd.0     up  1.00000 1.00000
      1   ssd 1.00000         osd.1     up  1.00000 1.00000
      2   ssd 1.00000         osd.2     up  1.00000 1.00000
     ...

#. Identify the first target host to convert :

   .. prompt:: bash $

      OLDHOST=<existing-cluster-host-to-convert>

#. Swap the new host into the old host's position in the cluster:

   .. prompt:: bash $

      ceph osd crush swap-bucket $NEWHOST $OLDHOST

   At this point all data on ``$OLDHOST`` will start migrating to OSDs
   on ``$NEWHOST``.  If there is a difference in the total capacity of
   the old and new hosts you may also see some data migrate to or from
   other nodes in the cluster, but as long as the hosts are similarly
   sized this will be a relatively small amount of data.

#. Wait for data migration to complete:

   .. prompt:: bash $

      while ! ceph osd safe-to-destroy $(ceph osd ls-tree $OLDHOST); do sleep 60 ; done

#. Stop all old OSDs on the now-empty ``$OLDHOST``:

   .. prompt:: bash $

      ssh $OLDHOST
      systemctl kill ceph-osd.target
      umount /var/lib/ceph/osd/ceph-*

#. Destroy and purge the old OSDs:

   .. prompt:: bash $

      for osd in `ceph osd ls-tree $OLDHOST`; do
         ceph osd purge $osd --yes-i-really-mean-it
      done

#. Wipe the old OSD devices. This requires you do identify which
   devices are to be wiped manually (BE CAREFUL!). For each device:

   .. prompt:: bash $

      ceph-volume lvm zap $DEVICE

#. Use the now-empty host as the new host, and repeat::

   .. prompt:: bash $

      NEWHOST=$OLDHOST

Advantages:

* Data is copied over the network only once.
* Converts an entire host's OSDs at once.
* Can parallelize to converting multiple hosts at a time.
* No spare devices are required on each host.

Disadvantages:

* A spare host is required.
* An entire host's worth of OSDs will be migrating data at a time.  This
  is likely to impact overall cluster performance.
* All migrated data still makes one full hop over the network.


Per-OSD device copy
-------------------

A single logical OSD can be converted by using the ``copy`` function
of ``ceph-objectstore-tool``.  This requires that the host have a free
device (or devices) to provision a new, empty BlueStore OSD.  For
example, if each host in your cluster has twelve OSDs, then you'd need a
thirteenth unused device so that each OSD can be converted in turn before the
old device is reclaimed to convert the next OSD.

Caveats:

* This strategy requires that an empty BlueStore OSD be prepared
  without allocating a new OSD ID, something that the ``ceph-volume``
  tool doesn't support.  More importantly, the setup of *dmcrypt* is
  closely tied to the OSD identity, which means that this approach
  does not work with encrypted OSDs.

* The device must be manually partitioned.

* An unsupported user-contributed script that shows this process may be found at
  https://github.com/ceph/ceph/blob/master/src/script/contrib/ceph-migrate-bluestore.bash

Advantages:

* Little or no data migrates over the network during the conversion, so long as
  the `noout` or `norecover`/`norebalance` flags are set on the OSD or the cluster
  while the process proceeds.

Disadvantages:

* Tooling is not fully implemented, supported, or documented.
* Each host must have an appropriate spare or empty device for staging.
* The OSD is offline during the conversion, which means new writes to PGs
  with the OSD in their acting set may not be ideally redundant until the
  subject OSD comes up and recovers. This increases the risk of data
  loss due to an overlapping failure.  However, if another OSD fails before
  conversion and start-up are complete, the original Filestore OSD can be
  started to provide access to its original data.
