.. _ceph-volume-zfs-prepare:

``prepare``
===========

The ``prepare`` subcommand creates a ZFS pool and zvol on a target device and
tags them with the metadata needed for Ceph OSD operation.

Each OSD gets a dedicated zpool named ``ceph-osd-<id>`` with a zvol named
``osd-block-<osd_fsid>`` inside it. BlueStore uses the zvol as its block
device directly.

Basic usage::

    ceph-volume zfs prepare --data /dev/ada1

With a dedicated DB device::

    ceph-volume zfs prepare --data /dev/ada1 --block.db /dev/ada2

With a dedicated WAL device::

    ceph-volume zfs prepare --data /dev/ada1 --block.wal /dev/ada3

With DB and WAL carved from the same pool as the block device::

    ceph-volume zfs prepare --data /dev/ada1 \
        --block.db same-pool --block-db-size 10G \
        --block.wal same-pool --block-wal-size 2G

Dry run (prints what would be done, touches nothing)::

    ceph-volume zfs prepare -n --data /dev/ada1

Test mode (creates the real zpool/zvol and tags them, but skips Ceph binaries,
monmap fetch, and ``ceph-osd --mkfs``)::

    ceph-volume zfs prepare --test --data /dev/ada1

Options
-------

``--data DEVICE``
    The disk to use for the OSD block device. A whole disk is expected;
    ``prepare`` creates a zpool on it.

``--block.db DEVICE|same-pool``
    Optional BlueStore DB device. Either a dedicated disk (creates a separate
    ``ceph-osd-<id>-db`` pool) or the literal string ``same-pool`` to carve
    the DB zvol from the block pool. Requires ``--block-db-size`` when using
    ``same-pool``.

``--block.wal DEVICE|same-pool``
    Optional BlueStore WAL device. Same forms as ``--block.db``.

``--block-db-size SIZE``
    Size of the DB zvol (e.g. ``10G``). Required when using
    ``--block.db same-pool``.

``--block-wal-size SIZE``
    Size of the WAL zvol. Required when using ``--block.wal same-pool``.

``--thin``
    Create sparse (thinly provisioned) zvols instead of the default
    thick-provisioned ones. Thick provisioning prevents pool oversubscription;
    use ``--thin`` only for testing.

``--crush-device-class CLASS``
    Set the CRUSH device class for the OSD (e.g. ``hdd``, ``ssd``, ``nvme``).

``-n``, ``--dry-run``
    Print what would be done without executing anything.

``--test``
    Create the zpool and zvol and set ZFS properties, but skip all Ceph
    binary calls (``ceph osd new``, monmap fetch, ``ceph-osd --mkfs``).
    Produces a tagged but non-functional OSD, useful for testing the ZFS
    layer in isolation.

Storing metadata
----------------

The following ZFS user properties are set on the pool (via ``zpool set``)::

    ceph:osd_id
    ceph:osd_fsid
    ceph:managed_by   = ceph-volume-zfs
    ceph:type         = block
    ceph:zvol_name

The following properties are set on the block zvol (via ``zfs set``)::

    ceph:osd_id
    ceph:osd_fsid
    ceph:cluster_name
    ceph:cluster_fsid
    ceph:type         = block
    ceph:objectstore  = bluestore
    ceph:crush_device_class
    ceph:block_device

If DB or WAL zvols are created, they also receive ``ceph:type = db`` (or
``wal``) and cross-link properties pointing to each other's device paths.

Summary
-------

To recap the ``prepare`` process:

#. Accept a raw disk as ``--data``.
#. Create a zpool named ``ceph-osd-<id>`` with ``-m none`` on the disk.
#. If DB/WAL devices are specified, create those zvols first (so sizing uses
   genuinely remaining space).
#. Create the block zvol sized at 95% of the pool's available space.
#. Request an OSD ID and fsid from the cluster (unless ``--test``).
#. Set ``ceph:*`` ZFS user properties on the pool and zvol.
#. Create the OSD data directory as a tmpfs mount.
#. Run ``ceph-osd --mkfs`` to initialise the OSD (unless ``--test``).
