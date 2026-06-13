.. _ceph-volume-seastore:

SeaStore Support in ceph-volume
================================

.. note::
   SeaStore is the native object store for Crimson OSD and is currently in
   technology preview. It requires Crimson to be enabled in your Ceph cluster.

Overview
--------

SeaStore is designed with a different architecture than BlueStore:

**BlueStore Architecture** (Classic OSD):
  - Primary device (``block``): Stores object data
  - Optional ``block.db``: RocksDB metadata
  - Optional ``block.wal``: Write-ahead log
  - One OSD process per device set

**SeaStore Architecture** (Crimson OSD):
  - Primary device (``block``): Hot-tier data and journal combined
  - Optional secondary devices (``block.<TYPE>.<ID>/block``): Slower-tier for cold data migration
  - No separate DB/WAL devices
  - One OSD process per OSD ID, using N Seastar reactor threads (``crimson_cpu_num``)

Device Naming Convention
------------------------

SeaStore uses a different naming scheme for secondary devices.  Each secondary
is a **directory** named ``block.<TYPE>.<ID>`` containing a ``block`` symlink
pointing to the device — matching the layout that ``SegmentManager`` opens as
``path + "/block"`` (``segment_manager.cc``)::

    /var/lib/ceph/osd/<cluster>-<id>/block                  # Primary device symlink
    /var/lib/ceph/osd/<cluster>-<id>/block.HDD.1/           # Secondary device directory
    /var/lib/ceph/osd/<cluster>-<id>/block.HDD.1/block      # Secondary device symlink
    /var/lib/ceph/osd/<cluster>-<id>/block.SSD.2/           # Secondary device directory
    /var/lib/ceph/osd/<cluster>-<id>/block.SSD.2/block      # Secondary device symlink

Valid device types for secondary devices:

* ``HDD`` - Hard disk drives (for cold data)
* ``SSD`` - Solid state drives
* ``ZBD`` - Zoned block devices (ZNS SSD or SMR HDD)
* ``RANDOM_BLOCK_SSD`` - Random block SSD

.. important::
   Secondary devices should **NOT be faster** than the primary device.
   When secondary devices are slower (e.g., HDD), SeaStore automatically
   migrates cold data from the fast primary device to slower secondary devices.

Preparing OSDs with LVM
------------------------

Simple Single-Device Setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^

For a basic SeaStore OSD with only a primary device:

.. prompt:: bash #

   ceph-volume lvm prepare --osd-type crimson --objectstore seastore --data /dev/nvme0n1

Or using an existing logical volume:

.. prompt:: bash #

   ceph-volume lvm prepare --osd-type crimson --objectstore seastore --data vg/lv-osd

Multi-Device Setup with Tiering
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For performance optimization with hot/cold tiering:

.. prompt:: bash #

   ceph-volume lvm prepare \
       --osd-type crimson \
       --objectstore seastore \
       --data /dev/nvme0n1 \
       --seastore-secondary /dev/sdb:HDD \
       --seastore-secondary /dev/sdc:HDD

This creates:

* Primary device: NVMe SSD for hot data
* Two cold-tier HDDs for infrequently accessed data

Using LVM for all devices::

    # Create logical volumes first
    lvcreate -L 500G -n osd-primary vg-fast
    lvcreate -L 2T -n osd-cold-1 vg-slow
    lvcreate -L 2T -n osd-cold-2 vg-slow

    # Prepare with SeaStore
    ceph-volume lvm prepare \
        --osd-type crimson \
        --objectstore seastore \
        --data vg-fast/osd-primary \
        --seastore-secondary vg-slow/osd-cold-1:HDD \
        --seastore-secondary vg-slow/osd-cold-2:HDD

Preparing OSDs with RAW
------------------------

Simple Single-Device Setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. prompt:: bash #

   ceph-volume raw prepare --osd-type crimson --objectstore seastore --data /dev/nvme0n1

Multi-Device Setup with Tiering
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. prompt:: bash #

   ceph-volume raw prepare \
       --osd-type crimson \
       --objectstore seastore \
       --data /dev/nvme0n1 \
       --seastore-secondary /dev/sdb:HDD \
       --seastore-secondary /dev/sdc:HDD

Encryption Support
------------------

SeaStore supports encryption via dm-crypt, similar to BlueStore:

.. prompt:: bash #

   ceph-volume lvm prepare \
       --osd-type crimson \
       --objectstore seastore \
       --dmcrypt \
       --data /dev/nvme0n1 \
       --seastore-secondary /dev/sdb:HDD

TPM2 enrollment is also supported:

.. prompt:: bash #

   ceph-volume raw prepare \
       --osd-type crimson \
       --objectstore seastore \
       --dmcrypt \
       --with-tpm \
       --data /dev/nvme0n1

.. note::
   Currently, only the primary device is encrypted. Secondary device
   encryption support may be added in the future.

Activating OSDs
---------------

LVM backend
^^^^^^^^^^^

Activation works the same as BlueStore: OSD identity is read from LVM
tags, so no per-OSD arguments are required.

.. prompt:: bash #

   ceph-volume lvm activate <osd-id> <osd-fsid>

.. prompt:: bash #

   ceph-volume lvm activate --all

RAW backend
^^^^^^^^^^^

.. important::
   For the RAW backend, ``ceph-volume raw activate`` **cannot auto-discover**
   SeaStore OSDs: BlueStore's ``ceph-bluestore-tool show-label`` does not
   understand SeaStore's on-disk format.  Until
   ``crimson-objectstore-tool`` gains a ``dump-superblock`` command
   (tracker `#68106 <https://tracker.ceph.com/issues/68106>`_), every raw
   activate call must supply ``--device``, ``--osd-id``, and
   ``--osd-uuid`` explicitly.  ``ceph-volume raw activate --all`` is
   **not supported** for SeaStore.

.. prompt:: bash #

   ceph-volume raw activate \
       --objectstore seastore \
       --device /dev/nvme0n1 \
       --osd-id <osd-id> \
       --osd-uuid <osd-fsid>

If the OSD has secondary devices, they must also be re-supplied on every
activate, because the secondary device paths are not persisted outside
the primary device's superblock:

.. prompt:: bash #

   ceph-volume raw activate \
       --objectstore seastore \
       --device /dev/nvme0n1 \
       --osd-id <osd-id> \
       --osd-uuid <osd-fsid> \
       --seastore-secondary /dev/sdb:HDD \
       --seastore-secondary /dev/sdc:HDD

Listing OSDs
------------

LVM backend:

.. prompt:: bash #

   ceph-volume lvm list

RAW backend:

.. prompt:: bash #

   ceph-volume raw list

The output will show SeaStore OSDs with their primary and secondary devices.

Migration from BlueStore
------------------------

.. warning::
   Direct migration from BlueStore to SeaStore is **not supported**.
   SeaStore uses a completely different on-disk format.

To migrate:

1. Create new SeaStore OSDs
2. Let Ceph backfill data to new OSDs
3. Remove old BlueStore OSDs
4. Decommission old devices

Troubleshooting
---------------

OSD Fails to Start
^^^^^^^^^^^^^^^^^^

Check that Crimson is enabled in your cluster:

.. prompt:: bash #

   ceph config get global enable_experimental_unrecoverable_data_corrupting_features

The output should include ``crimson``.

.. prompt:: bash #

   ceph osd dump | grep flags

The output should include ``allow_crimson``.

Invalid Secondary Device Type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ensure you're using valid device types::

    Valid: HDD, SSD, ZBD, RANDOM_BLOCK_SSD
    Invalid: hdd, nvme, sata (case-sensitive, must be uppercase)

Example error::

    RuntimeError: Invalid secondary device type: hdd. Valid types: HDD, SSD, ZBD, RANDOM_BLOCK_SSD

Secondary Device Not Detected
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check symlinks in the OSD directory:

.. prompt:: bash #

   ls -la /var/lib/ceph/osd/ceph-0/

The output should include ``block -> /dev/nvme0n1`` (primary device symlink)
and ``block.HDD.1/`` (secondary device directory).

.. prompt:: bash #

   ls -la /var/lib/ceph/osd/ceph-0/block.HDD.1/

The output should include ``block -> /dev/sdb`` (secondary device symlink
inside the directory).

If the directory or inner symlink is missing, the OSD may not have been prepared correctly.

Crimson CPU Allocation
----------------------

.. warning::
   ``crimson_cpu_num`` (or ``crimson_cpu_set``) **must** be configured before
   running ``ceph-volume``.  ``crimson-osd`` reads this value from the Monitor
   during startup and calls ``ceph_abort()`` if neither option is set, causing
   ``ceph-volume``'s ``--mkfs`` step to fail immediately.

Configure CPU allocation before deploying:

.. prompt:: bash #

   ceph config set osd crimson_cpu_num 8

The value is baked into the device superblock at ``--mkfs`` time — SeaStore
partitions the device into ``crimson_cpu_num`` equal shards — and cannot be
changed after deployment without reformatting the device.

When deploying via cephadm, include ``crimson_cpu_num`` in the OSD service
spec's ``config`` block so cephadm pushes it to the Monitor before invoking
``ceph-volume``:

.. code-block:: yaml

    service_type: osd
    service_id: my-crimson-osds
    placement:
      host_pattern: '*'
    osd_type: crimson
    objectstore: seastore
    config:
      crimson_cpu_num: "8"
    spec:
      data_devices:
        paths:
          - /dev/nvme0n1

See Also
--------

* :ref:`crimson_doc` - Crimson OSD overview
* :ref:`seastore` - SeaStore design documentation
* :ref:`ceph-volume-lvm` - LVM backend documentation
