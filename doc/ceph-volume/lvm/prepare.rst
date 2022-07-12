.. _ceph-volume-lvm-prepare:

``prepare``
===========
This subcommand allows a :term:`filestore` or :term:`bluestore` setup. It is
recommended to pre-provision a logical volume before using it with
``ceph-volume lvm``.

Logical volumes are not altered except for adding extra metadata.

.. note:: This is part of a two step process to deploy an OSD. If looking for
          a single-call way, please see :ref:`ceph-volume-lvm-create`

To help identify volumes, the process of preparing a volume (or volumes) to
work with Ceph, the tool will assign a few pieces of metadata information using
:term:`LVM tags`.

:term:`LVM tags` makes volumes easy to discover later, and help identify them as
part of a Ceph system, and what role they have (journal, filestore, bluestore,
etc...)

Although :term:`bluestore` is the default, the back end can be specified with:


* :ref:`--filestore <ceph-volume-lvm-prepare_filestore>`
* :ref:`--bluestore <ceph-volume-lvm-prepare_bluestore>`

.. _ceph-volume-lvm-prepare_bluestore:

``bluestore``
-------------
The :term:`bluestore` objectstore is the default for new OSDs. It offers a bit
more flexibility for devices compared to :term:`filestore`.
Bluestore supports the following configurations:

* A block device, a block.wal, and a block.db device
* A block device and a block.wal device
* A block device and a block.db device
* A single block device

The bluestore subcommand accepts physical block devices, partitions on
physical block devices or logical volumes as arguments for the various device parameters
If a physical device is provided, a logical volume will be created. A volume group will
either be created or reused it its name begins with ``ceph``.
This allows a simpler approach at using LVM but at the cost of flexibility:
there are no options or configurations to change how the LV is created.

The ``block`` is specified with the ``--data`` flag, and in its simplest use
case it looks like::

    ceph-volume lvm prepare --bluestore --data vg/lv

A raw device can be specified in the same way::

    ceph-volume lvm prepare --bluestore --data /path/to/device

For enabling :ref:`encryption <ceph-volume-lvm-encryption>`, the ``--dmcrypt`` flag is required::

    ceph-volume lvm prepare --bluestore --dmcrypt --data vg/lv

If a ``block.db`` or a ``block.wal`` is needed (they are optional for
bluestore) they can be specified with ``--block.db`` and ``--block.wal``
accordingly. These can be a physical device, a partition  or
a logical volume.

For both ``block.db`` and ``block.wal`` partitions aren't made logical volumes
because they can be used as-is.

While creating the OSD directory, the process will use a ``tmpfs`` mount to
place all the files needed for the OSD. These files are initially created by
``ceph-osd --mkfs`` and are fully ephemeral.

A symlink is always created for the ``block`` device, and optionally for
``block.db`` and ``block.wal``. For a cluster with a default name, and an OSD
id of 0, the directory could look like::

    # ls -l /var/lib/ceph/osd/ceph-0
    lrwxrwxrwx. 1 ceph ceph 93 Oct 20 13:05 block -> /dev/ceph-be2b6fbd-bcf2-4c51-b35d-a35a162a02f0/osd-block-25cf0a05-2bc6-44ef-9137-79d65bd7ad62
    lrwxrwxrwx. 1 ceph ceph 93 Oct 20 13:05 block.db -> /dev/sda1
    lrwxrwxrwx. 1 ceph ceph 93 Oct 20 13:05 block.wal -> /dev/ceph/osd-wal-0
    -rw-------. 1 ceph ceph 37 Oct 20 13:05 ceph_fsid
    -rw-------. 1 ceph ceph 37 Oct 20 13:05 fsid
    -rw-------. 1 ceph ceph 55 Oct 20 13:05 keyring
    -rw-------. 1 ceph ceph  6 Oct 20 13:05 ready
    -rw-------. 1 ceph ceph 10 Oct 20 13:05 type
    -rw-------. 1 ceph ceph  2 Oct 20 13:05 whoami

In the above case, a device was used for ``block`` so ``ceph-volume`` create
a volume group and a logical volume using the following convention:

* volume group name: ``ceph-{cluster fsid}`` or if the vg exists already
  ``ceph-{random uuid}``

* logical volume name: ``osd-block-{osd_fsid}``


.. _ceph-volume-lvm-prepare_filestore:

``filestore``
-------------
This is the OSD backend that allows preparation of logical volumes for
a :term:`filestore` objectstore OSD.

It can use a logical volume for the OSD data and a physical device, a partition
or logical volume for the journal. A physical device will have a logical volume
created on it. A volume group will either be created or reused it its name begins
with ``ceph``.  No special preparation is needed for these volumes other than
following the minimum size requirements for data and journal.

The CLI call looks like this of a basic standalone filestore OSD::

    ceph-volume lvm prepare --filestore --data <data block device>

To deploy file store with an external journal::

    ceph-volume lvm prepare --filestore --data <data block device> --journal <journal block device>

For enabling :ref:`encryption <ceph-volume-lvm-encryption>`, the ``--dmcrypt`` flag is required::

    ceph-volume lvm prepare --filestore --dmcrypt --data <data block device> --journal <journal block device>

Both the journal and data block device can take three forms:

* a physical block device
* a partition on a physical block device
* a logical volume

When using logical volumes the value *must* be of the format
``volume_group/logical_volume``. Since logical volume names
are not enforced for uniqueness, this prevents accidentally 
choosing the wrong volume.

When using a partition, it *must* contain a ``PARTUUID``, that can be 
discovered by ``blkid``. THis ensure it can later be identified correctly
regardless of the device name (or path).

For example: passing a logical volume for data and a partition ``/dev/sdc1`` for
the journal::

    ceph-volume lvm prepare --filestore --data volume_group/lv_name --journal /dev/sdc1

Passing a bare device for data and a logical volume ias the journal::

    ceph-volume lvm prepare --filestore --data /dev/sdc --journal volume_group/journal_lv

A generated uuid is used to ask the cluster for a new OSD. These two pieces are
crucial for identifying an OSD and will later be used throughout the
:ref:`ceph-volume-lvm-activate` process.

The OSD data directory is created using the following convention::

    /var/lib/ceph/osd/<cluster name>-<osd id>

At this point the data volume is mounted at this location, and the journal
volume is linked::

      ln -s /path/to/journal /var/lib/ceph/osd/<cluster_name>-<osd-id>/journal

The monmap is fetched using the bootstrap key from the OSD::

      /usr/bin/ceph --cluster ceph --name client.bootstrap-osd
      --keyring /var/lib/ceph/bootstrap-osd/ceph.keyring
      mon getmap -o /var/lib/ceph/osd/<cluster name>-<osd id>/activate.monmap

``ceph-osd`` will be called to populate the OSD directory, that is already
mounted, re-using all the pieces of information from the initial steps::

      ceph-osd --cluster ceph --mkfs --mkkey -i <osd id> \
      --monmap /var/lib/ceph/osd/<cluster name>-<osd id>/activate.monmap --osd-data \
      /var/lib/ceph/osd/<cluster name>-<osd id> --osd-journal /var/lib/ceph/osd/<cluster name>-<osd id>/journal \
      --osd-uuid <osd uuid> --keyring /var/lib/ceph/osd/<cluster name>-<osd id>/keyring \
      --setuser ceph --setgroup ceph


.. _ceph-volume-lvm-partitions:

Partitioning
------------
``ceph-volume lvm`` does not currently create partitions from a whole device.
If using device partitions the only requirement is that they contain the
``PARTUUID`` and that it is discoverable by ``blkid``. Both ``fdisk`` and
``parted`` will create that automatically for a new partition.

For example, using a new, unformatted drive (``/dev/sdd`` in this case) we can
use ``parted`` to create a new partition. First we list the device
information::

    $ parted --script /dev/sdd print
    Model: VBOX HARDDISK (scsi)
    Disk /dev/sdd: 11.5GB
    Sector size (logical/physical): 512B/512B
    Disk Flags:

This device is not even labeled yet, so we can use ``parted`` to create
a ``gpt`` label before we create a partition, and verify again with ``parted
print``::

    $ parted --script /dev/sdd mklabel gpt
    $ parted --script /dev/sdd print
    Model: VBOX HARDDISK (scsi)
    Disk /dev/sdd: 11.5GB
    Sector size (logical/physical): 512B/512B
    Partition Table: gpt
    Disk Flags:

Now lets create a single partition, and verify later if ``blkid`` can find
a ``PARTUUID`` that is needed by ``ceph-volume``::

    $ parted --script /dev/sdd mkpart primary 1 100%
    $ blkid /dev/sdd1
    /dev/sdd1: PARTLABEL="primary" PARTUUID="16399d72-1e1f-467d-96ee-6fe371a7d0d4"


.. _ceph-volume-lvm-existing-osds:

Existing OSDs
-------------
For existing clusters that want to use this new system and have OSDs that are
already running there are a few things to take into account:

.. warning:: this process will forcefully format the data device, destroying
             existing data, if any.

* OSD paths should follow this convention::

     /var/lib/ceph/osd/<cluster name>-<osd id>

* Preferably, no other mechanisms to mount the volume should exist, and should
  be removed (like fstab mount points)

The one time process for an existing OSD, with an ID of 0 and using
a ``"ceph"`` cluster name would look like (the following command will **destroy
any data** in the OSD)::

    ceph-volume lvm prepare --filestore --osd-id 0 --osd-fsid E3D291C1-E7BF-4984-9794-B60D9FA139CB

The command line tool will not contact the monitor to generate an OSD ID and
will format the LVM device in addition to storing the metadata on it so that it
can be started later (for detailed metadata description see
:ref:`ceph-volume-lvm-tags`).


Crush device class
------------------

To set the crush device class for the OSD, use the ``--crush-device-class`` flag. This will
work for both bluestore and filestore OSDs::

    ceph-volume lvm prepare --bluestore --data vg/lv --crush-device-class foo


.. _ceph-volume-lvm-multipath:

``multipath`` support
---------------------
``multipath`` devices are supported if ``lvm`` is configured properly.

**Leave it to LVM**

Most Linux distributions should ship their LVM2 package with
``multipath_component_detection = 1`` in the default configuration. With this
setting ``LVM`` ignores any device that is a multipath component and
``ceph-volume`` will accordingly not touch these devices.

**Using filters**

Should this setting be unavailable, a correct ``filter`` expression must be
provided in ``lvm.conf``. ``ceph-volume`` must not be able to use both the
multipath device and its multipath components.

Storing metadata
----------------
The following tags will get applied as part of the preparation process
regardless of the type of volume (journal or data) or OSD objectstore:

* ``cluster_fsid``
* ``encrypted``
* ``osd_fsid``
* ``osd_id``
* ``crush_device_class``

For :term:`filestore` these tags will be added:

* ``journal_device``
* ``journal_uuid``

For :term:`bluestore` these tags will be added:

* ``block_device``
* ``block_uuid``
* ``db_device``
* ``db_uuid``
* ``wal_device``
* ``wal_uuid``

.. note:: For the complete lvm tag conventions see :ref:`ceph-volume-lvm-tag-api`


Summary
-------
To recap the ``prepare`` process for :term:`bluestore`:

#. Accepts raw physical devices, partitions on physical devices or logical volumes as arguments.
#. Creates logical volumes on any raw physical devices.
#. Generate a UUID for the OSD
#. Ask the monitor get an OSD ID reusing the generated UUID
#. OSD data directory is created on a tmpfs mount.
#. ``block``, ``block.wal``, and ``block.db`` are symlinked if defined.
#. monmap is fetched for activation
#. Data directory is populated by ``ceph-osd``
#. Logical Volumes are assigned all the Ceph metadata using lvm tags


And the ``prepare`` process for :term:`filestore`:

#. Accepts raw physical devices, partitions on physical devices or logical volumes as arguments.
#. Generate a UUID for the OSD
#. Ask the monitor get an OSD ID reusing the generated UUID
#. OSD data directory is created and data volume mounted
#. Journal is symlinked from data volume to journal location
#. monmap is fetched for activation
#. devices is mounted and data directory is populated by ``ceph-osd``
#. data and journal volumes are assigned all the Ceph metadata using lvm tags
