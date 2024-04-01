.. _rados_operations_bluestore_migration:

=====================
 BlueStore Migration
=====================
.. warning:: Filestore has been deprecated in the Reef release and is no longer supported.
	     Please migrate to BlueStore.

Each OSD must be formatted as either Filestore or BlueStore. However, a Ceph
cluster can operate with a mixture of both Filestore OSDs and BlueStore OSDs.
Because BlueStore is superior to Filestore in performance and robustness, and
because Filestore is not supported by Ceph releases beginning with Reef, users
deploying Filestore OSDs should transition to BlueStore. There are several
strategies for making the transition to BlueStore.

BlueStore is so different from Filestore that an individual OSD cannot be
converted in place. Instead, the conversion process must use either (1) the
cluster's normal replication and healing support, or (2) tools and strategies
that copy OSD content from an old (Filestore) device to a new (BlueStore) one.

Deploying new OSDs with BlueStore
=================================

Use BlueStore when deploying new OSDs (for example, when the cluster is
expanded). Because this is the default behavior, no specific change is
needed.

Similarly, use BlueStore for any OSDs that have been reprovisioned after
a failed drive was replaced.

Converting existing OSDs
========================

"Mark-``out``" replacement
--------------------------

The simplest approach is to verify that the cluster is healthy and
then follow these steps for each Filestore OSD in succession: mark the OSD
``out``, wait for the data to replicate across the cluster, reprovision the OSD, 
mark the OSD back ``in``, and wait for recovery to complete before proceeding
to the next OSD. This approach is easy to automate, but it entails unnecessary
data migration that carries costs in time and SSD wear.

#. Identify a Filestore OSD to replace::

     ID=<osd-id-number>
     DEVICE=<disk-device>

   #. Determine whether a given OSD is Filestore or BlueStore:

      .. prompt:: bash $

         ceph osd metadata $ID | grep osd_objectstore

   #. Get a current count of Filestore and BlueStore OSDs:

      .. prompt:: bash $

         ceph osd count-metadata osd_objectstore

#. Mark a Filestore OSD ``out``:

   .. prompt:: bash $

      ceph osd out $ID

#. Wait for the data to migrate off this OSD:

   .. prompt:: bash $

      while ! ceph osd safe-to-destroy $ID ; do sleep 60 ; done

#. Stop the OSD:

   .. prompt:: bash $

      systemctl kill ceph-osd@$ID

   .. _osd_id_retrieval: 

#. Note which device the OSD is using:

   .. prompt:: bash $

      mount | grep /var/lib/ceph/osd/ceph-$ID

#. Unmount the OSD:

   .. prompt:: bash $

      umount /var/lib/ceph/osd/ceph-$ID

#. Destroy the OSD's data. Be *EXTREMELY CAREFUL*! These commands will destroy
   the contents of the device; you must be certain that the data on the device is
   not needed (in other words, that the cluster is healthy) before proceeding:

   .. prompt:: bash $

      ceph-volume lvm zap $DEVICE

#. Tell the cluster that the OSD has been destroyed (and that a new OSD can be
   reprovisioned with the same OSD ID):

   .. prompt:: bash $

      ceph osd destroy $ID --yes-i-really-mean-it

#. Provision a BlueStore OSD in place by using the same OSD ID. This requires
   you to identify which device to wipe, and to make certain that you target
   the correct and intended device, using the information that was retrieved in
   the :ref:`"Note which device the OSD is using" <osd_id_retrieval>` step.  BE
   CAREFUL!  Note that you may need to modify these commands when dealing with
   hybrid OSDs:

   .. prompt:: bash $

      ceph-volume lvm create --bluestore --data $DEVICE --osd-id $ID

#. Repeat.

You may opt to (1) have the balancing of the replacement BlueStore OSD take
place concurrently with the draining of the next Filestore OSD, or instead
(2) follow the same procedure for multiple OSDs in parallel. In either case,
however, you must ensure that the cluster is fully clean (in other words, that
all data has all replicas) before destroying any OSDs. If you opt to reprovision
multiple OSDs in parallel, be **very** careful to destroy OSDs only within a
single CRUSH failure domain (for example, ``host`` or ``rack``). Failure to
satisfy this requirement will reduce the redundancy and availability of your
data and increase the risk of data loss (or even guarantee data loss).

Advantages:

* Simple.
* Can be done on a device-by-device basis.
* No spare devices or hosts are required.

Disadvantages:

* Data is copied over the network twice: once to another OSD in the cluster (to
  maintain the specified number of replicas), and again back to the
  reprovisioned BlueStore OSD.

"Whole host" replacement
------------------------

If you have a spare host in the cluster, or sufficient free space to evacuate
an entire host for use as a spare, then the conversion can be done on a
host-by-host basis so that each stored copy of the data is migrated only once.

To use this approach, you need an empty host that has no OSDs provisioned.
There are two ways to do this: either by using a new, empty host that is not
yet part of the cluster, or by offloading data from an existing host that is
already part of the cluster.

Using a new, empty host
^^^^^^^^^^^^^^^^^^^^^^^

Ideally the host will have roughly the same capacity as each of the other hosts
you will be converting.  Add the host to the CRUSH hierarchy, but do not attach
it to the root:


.. prompt:: bash $

   NEWHOST=<empty-host-name>
   ceph osd crush add-bucket $NEWHOST host

Make sure that Ceph packages are installed on the new host.

Using an existing host
^^^^^^^^^^^^^^^^^^^^^^

If you would like to use an existing host that is already part of the cluster,
and if there is sufficient free space on that host so that all of its data can
be migrated off to other cluster hosts, you can do the following (instead of
using a new, empty host):

.. prompt:: bash $

   OLDHOST=<existing-cluster-host-to-offload>
   ceph osd crush unlink $OLDHOST default

where "default" is the immediate ancestor in the CRUSH map. (For
smaller clusters with unmodified configurations this will normally
be "default", but it might instead be a rack name.) You should now
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

If everything looks good, jump directly to the :ref:`"Wait for the data
migration to complete" <bluestore_data_migration_step>` step below and proceed
from there to clean up the old OSDs.

Migration process
^^^^^^^^^^^^^^^^^

If you're using a new host, start at :ref:`the first step
<bluestore_migration_process_first_step>`. If you're using an existing host,
jump to :ref:`this step <bluestore_data_migration_step>`.

.. _bluestore_migration_process_first_step:

#. Provision new BlueStore OSDs for all devices:

   .. prompt:: bash $

      ceph-volume lvm create --bluestore --data /dev/$DEVICE

#. Verify that the new OSDs have joined the cluster:

   .. prompt:: bash $

      ceph osd tree

   You should see the new host ``$NEWHOST`` with all of the OSDs beneath
   it, but the host should *not* be nested beneath any other node in the
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

   At this point all data on ``$OLDHOST`` will begin migrating to the OSDs on
   ``$NEWHOST``.  If there is a difference between the total capacity of the
   old hosts and the total capacity of the new hosts, you may also see some
   data migrate to or from other nodes in the cluster. Provided that the hosts
   are similarly sized, however, this will be a relatively small amount of
   data.

   .. _bluestore_data_migration_step:

#. Wait for the data migration to complete:

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

#. Wipe the old OSDs. This requires you to identify which devices are to be
   wiped manually. BE CAREFUL! For each device:

   .. prompt:: bash $

      ceph-volume lvm zap $DEVICE

#. Use the now-empty host as the new host, and repeat:

   .. prompt:: bash $

      NEWHOST=$OLDHOST

Advantages:

* Data is copied over the network only once.
* An entire host's OSDs are converted at once.
* Can be parallelized, to make possible the conversion of multiple hosts at the same time.
* No host involved in this process needs to have a spare device.

Disadvantages:

* A spare host is required.
* An entire host's worth of OSDs will be migrating data at a time. This
  is likely to impact overall cluster performance.
* All migrated data still makes one full hop over the network.

Per-OSD device copy
-------------------
A single logical OSD can be converted by using the ``copy`` function
included in ``ceph-objectstore-tool``. This requires that the host have one or more free
devices to provision a new, empty BlueStore OSD. For
example, if each host in your cluster has twelve OSDs, then you need a
thirteenth unused OSD so that each OSD can be converted before the
previous OSD is reclaimed to convert the next OSD.

Caveats:

* This approach requires that we prepare an empty BlueStore OSD but that we do not allocate
  a new OSD ID to it. The ``ceph-volume`` tool does not support such an operation. **IMPORTANT:**
  because the setup of *dmcrypt* is closely tied to the identity of the OSD, this approach does not
  work with encrypted OSDs.

* The device must be manually partitioned.

* An unsupported user-contributed script that demonstrates this process may be found here:
  https://github.com/ceph/ceph/blob/master/src/script/contrib/ceph-migrate-bluestore.bash

Advantages:

* Provided that the 'noout' or the 'norecover'/'norebalance' flags are set on the OSD or the
  cluster while the conversion process is underway, little or no data migrates over the
  network during the conversion.

Disadvantages:

* Tooling is not fully implemented, supported, or documented.
  
* Each host must have an appropriate spare or empty device for staging.
  
* The OSD is offline during the conversion, which means new writes to PGs
  with the OSD in their acting set may not be ideally redundant until the
  subject OSD comes up and recovers. This increases the risk of data
  loss due to an overlapping failure. However, if another OSD fails before
  conversion and startup have completed, the original Filestore OSD can be
  started to provide access to its original data.
