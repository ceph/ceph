.. _ceph-volume-lvm-prepare:

``prepare``
===========
Before you run ``ceph-volume lvm prepare``, we recommend that you provision a
logical volume. Then you can run ``prepare`` on that logical volume. 

``prepare`` adds metadata to logical volumes but does not alter them in any
other way. 

.. note:: This is part of a two-step process to deploy an OSD. If you prefer 
   to deploy an OSD by using only one command, see :ref:`ceph-volume-lvm-create`.

``prepare`` uses :term:`LVM tags` to assign several pieces of metadata to a
logical volume. Volumes tagged in this way are easier to identify and easier to
use with Ceph. :term:`LVM tags` identify logical volumes by the role that they
play in the Ceph cluster (for example: BlueStore data or BlueStore WAL+DB).

:term:`BlueStore<bluestore>` is the default backend. Ceph permits changing
the backend, which can be done by using the following flags and arguments:

* :ref:`--bluestore <ceph-volume-lvm-prepare_bluestore>`

.. _ceph-volume-lvm-prepare_bluestore:

``bluestore``
-------------
:term:`Bluestore<bluestore>` is the default backend for new OSDs.  Bluestore
supports the following configurations:

* a block device, a block.wal device, and a block.db device
* a block device and a block.wal device
* a block device and a block.db device
* a single block device

The ``bluestore`` subcommand accepts physical block devices, partitions on physical
block devices, or logical volumes as arguments for the various device
parameters. If a physical block device is provided, a logical volume will be
created. If the provided volume group's name begins with `ceph`, it will be
created if it does not yet exist and it will be clobbered and reused if it
already exists. This allows for a simpler approach to using LVM but at the
cost of flexibility: no option or configuration can be used to change how the
logical volume is created.

The ``block`` is specified with the ``--data`` flag, and in its simplest use
case it looks like:

.. prompt:: bash #

    ceph-volume lvm prepare --bluestore --data vg/lv

A raw device can be specified in the same way:

.. prompt:: bash #

    ceph-volume lvm prepare --bluestore --data /path/to/device

For enabling :ref:`encryption <ceph-volume-lvm-encryption>`, the ``--dmcrypt`` flag is required:

.. prompt:: bash #

    ceph-volume lvm prepare --bluestore --dmcrypt --data vg/lv

If a ``block.db`` device or a ``block.wal`` device is needed, it can be
specified with ``--block.db`` or ``--block.wal``. These can be physical
devices, partitions, or logical volumes. ``block.db`` and ``block.wal`` are
optional for bluestore.

For both ``block.db`` and ``block.wal``, partitions can be used as-is, and 
therefore are not made into logical volumes.

While creating the OSD directory, the process uses a ``tmpfs`` mount to hold
the files needed for the OSD. These files are created by ``ceph-osd --mkfs``
and are ephemeral.

A symlink is created for the ``block`` device, and is optional for ``block.db``
and ``block.wal``. For a cluster with a default name and an OSD ID of 0, the
directory looks like this::

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

In the above case, a device was used for ``block``, so ``ceph-volume`` created
a volume group and a logical volume using the following conventions:

* volume group name: ``ceph-{cluster fsid}`` (or if the volume group already
  exists: ``ceph-{random uuid}``)

* logical volume name: ``osd-block-{osd_fsid}``


.. _ceph-volume-lvm-prepare_filestore:

``filestore``
-------------
.. warning:: Filestore has been deprecated in the Reef release and is no longer supported.

``Filestore<filestore>`` is the OSD backend that prepares logical volumes for a
`filestore`-backed object-store OSD.


``Filestore<filestore>`` uses a logical volume to store OSD data and it uses
physical devices, partitions, or logical volumes to store the journal.  If a
physical device is used to create a filestore backend, a logical volume will be
created on that physical device. If the provided volume group's name begins
with `ceph`, it will be created if it does not yet exist and it will be
clobbered and reused if it already exists. No special preparation is needed for
these volumes, but be sure to meet the minimum size requirements for OSD data and
for the journal.

Use the following command to create a basic filestore OSD:

.. prompt:: bash #

   ceph-volume lvm prepare --filestore --data <data block device>

Use this command to deploy filestore with an external journal:

.. prompt:: bash #

   ceph-volume lvm prepare --filestore --data <data block device> --journal <journal block device>

Use this command to enable :ref:`encryption <ceph-volume-lvm-encryption>`, and note that the ``--dmcrypt`` flag is required:

.. prompt:: bash #

   ceph-volume lvm prepare --filestore --dmcrypt --data <data block device> --journal <journal block device>

The data block device and the journal can each take one of three forms: 

* a physical block device
* a partition on a physical block device
* a logical volume

If you use a logical volume to deploy filestore, the value that you pass in the
command *must* be of the format ``volume_group/logical_volume_name``. Since logical
volume names are not enforced for uniqueness, using this format is an important 
safeguard against accidentally choosing the wrong volume (and clobbering its data).

If you use a partition to deploy filestore, the partition *must* contain a
``PARTUUID`` that can be discovered by ``blkid``. This ensures that the
partition can be identified correctly regardless of the device's name (or path).

For example, to use a logical volume for OSD data and a partition
(``/dev/sdc1``) for the journal, run a command of this form:

.. prompt:: bash #

   ceph-volume lvm prepare --filestore --data volume_group/logical_volume_name --journal /dev/sdc1

Or, to use a bare device for data and a logical volume for the journal:

.. prompt:: bash #

   ceph-volume lvm prepare --filestore --data /dev/sdc --journal volume_group/journal_lv

A generated UUID is used when asking the cluster for a new OSD. These two
pieces of information (the OSD ID and the OSD UUID) are necessary for
identifying a given OSD and will later be used throughout the
:ref:`activation<ceph-volume-lvm-activate>` process.

The OSD data directory is created using the following convention::

    /var/lib/ceph/osd/<cluster name>-<osd id>

To link the journal volume to the mounted data volume, use this command:

.. prompt:: bash #

   ln -s /path/to/journal /var/lib/ceph/osd/<cluster_name>-<osd-id>/journal

To fetch the monmap by using the bootstrap key from the OSD, use this command:

.. prompt:: bash #

   /usr/bin/ceph --cluster ceph --name client.bootstrap-osd --keyring
   /var/lib/ceph/bootstrap-osd/ceph.keyring mon getmap -o
   /var/lib/ceph/osd/<cluster name>-<osd id>/activate.monmap

To populate the OSD directory (which has already been mounted), use this ``ceph-osd`` command:  
.. prompt:: bash #

   ceph-osd --cluster ceph --mkfs --mkkey -i <osd id> \ --monmap
   /var/lib/ceph/osd/<cluster name>-<osd id>/activate.monmap --osd-data \
   /var/lib/ceph/osd/<cluster name>-<osd id> --osd-journal
   /var/lib/ceph/osd/<cluster name>-<osd id>/journal \ --osd-uuid <osd uuid>
   --keyring /var/lib/ceph/osd/<cluster name>-<osd id>/keyring \ --setuser ceph
   --setgroup ceph

All of the information from the previous steps is used in the above command.      



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

To set the crush device class for the OSD, use the ``--crush-device-class`` flag. 

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
