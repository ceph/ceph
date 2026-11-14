.. _ceph-volume-lvm-migrate-block-devices:

``migrate-block-devices``
=========================

Converts partition-backed ``block.db``/``block.wal`` devices of a single
hybrid (bluestore) OSD to LVM volumes, in-place: the OSD id and the data
device are preserved, and no data is rebalanced.

Exactly one OSD is migrated per invocation. Use a shell loop (or external
automation) to migrate several OSDs, one at a time.

The devices to convert are backed up first, then each partition is wiped
and replaced by a ceph-tagged PV/VG/LV (as created by
:ref:`ceph-volume-lvm-prepare`), the bytes are restored into the new
logical volume, the OSD symlinks and LVM tags are updated, and a
``ceph-bluestore-tool`` fsck runs before the OSD is started again.

Moving BlueFS data between existing logical volumes (with no partition
involved) is handled by ``ceph-volume lvm migrate`` instead.

Preflight checks
----------------

The command refuses to proceed when any of the following is true:

* the OSD is not an LVM-based, unencrypted bluestore OSD
* a requested role (``db`` or ``wal``) does not exist, or is not backed by
  a partition
* the LVM-backed db/wal devices on the disk being touched would not form a
  prefix in partition order, i.e. a raw db/wal partition exists below an
  LVM-backed one (this happens when a single OSD was recreated on the same
  disk with newer tooling)
* the backup path is missing, not writable, or does not have enough free
  space for the backup images
* the cluster is not reachable, the OSD is not registered, or cluster
  health is not ``HEALTH_OK`` (this can be bypassed with
  ``--force-unhealthy``)

Migration flow
--------------

#. preflight checks (see above)
#. set the ``noout``, ``norebalance`` and ``nobackfill`` cluster flags
   (unless ``--no-maintenance-flags`` is given; they are unset again only
   after a successful fsck and OSD startup)
#. stop the OSD
#. back up the partitions to the backup path, as raw images plus a JSON
   metadata file
#. wipe each partition and create a ceph-tagged PV/VG/LV on it, using all
   the available space
#. restore the bytes from the backup images into the new logical volumes
#. update the data LV tags and the OSD directory symlinks
#. run ``ceph-bluestore-tool`` fsck (``shallow`` by default)
#. start the OSD again and wait for it to come up

.. note::

   LVM metadata makes the new logical volume slightly smaller than the
   original partition. The restore is trimmed to the size of the logical
   volume, and the fsck/startup gates will catch the (unlikely) case where
   BlueFS had allocated the tail of the partition. The backup images are
   kept after the migration so that the original partitions can be
   restored manually if needed.

On failure the maintenance flags are deliberately left set (unsetting
``noout`` on a broken OSD would trigger rebalancing) and the command
prints the exact instructions to unset them manually.

Running the command again on an already migrated OSD is a no-op that
succeeds immediately.

Examples
--------

Migrate both db and wal of OSD 0::

    ceph-volume lvm migrate-block-devices --osd-id 0 --what db,wal \\
        --backup-path /var/lib/ceph/migrate-backups

Migrate the db device only::

    ceph-volume lvm migrate-block-devices --osd-id 0 --what db \\
        --backup-path /var/lib/ceph/migrate-backups

Print the migration plan without changing anything::

    ceph-volume lvm migrate-block-devices --osd-id 0 --dry-run \\
        --backup-path /var/lib/ceph/migrate-backups

Report a failure
----------------

If the migration fails after the partitions were wiped, the OSD is left
stopped and the backup images at the backup path are the recovery
mechanism: recreate the partitions at their original size and restore the
images into them (the JSON metadata file records the original devices,
sizes and PARTUUIDs), or rebuild the OSD.
