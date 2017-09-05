.. _ceph-volume-lvm-prepare:

``prepare``
===========
This subcommand allows a :term:`filestore` setup (:term:`bluestore` support is
planned) and currently consumes only logical volumes for both the data and
journal. It will not create or modify the logical volumes except for adding
extra metadata.

.. note:: This is part of a two step process to deploy an OSD. If looking for
          a single-call way, please see :ref:`ceph-volume-lvm-create`

To help identify volumes, the process of preparing a volume (or volumes) to
work with Ceph, the tool will assign a few pieces of metadata information using
:term:`LVM tags`.

:term:`LVM tags` makes volumes easy to discover later, and help identify them as
part of a Ceph system, and what role they have (journal, filestore, bluestore,
etc...)

Although initially :term:`filestore` is supported (and supported by default)
the back end can be specified with:


* :ref:`--filestore <ceph-volume-lvm-prepare_filestore>`
* ``--bluestore``

.. when available, this will need to be updated to:
.. * :ref:`--bluestore <ceph-volume-prepare_bluestore>`

.. _ceph-volume-lvm-prepare_filestore:

``filestore``
-------------
This is the default OSD backend and allows preparation of logical volumes for
a :term:`filestore` OSD.

The process is *very* strict, it requires two logical volumes that are ready to
be used. No special preparation is needed for these volumes other than
following the minimum size requirements for data and journal.

The API call looks like::

    ceph-volume prepare --filestore --data volume_group/lv_name --journal journal

The ``--data`` value *must* be a volume group name and a logical volume name
separated by a ``/``. Since logical volume names are not enforced for
uniqueness, this prevents using the wrong volume. The ``--journal`` can be
either a logical volume *or* a partition.

When using a partition, it *must* contain a ``PARTUUID`` discoverable by
``blkid``, so that it can later be identified correctly regardless of the
device name (or path).

When using a partition, this is how it would look for ``/dev/sdc1``::

    ceph-volume prepare --filestore --data volume_group/lv_name --journal /dev/sdc1

For a logical volume, just like for ``--data``, a volume group and logical
volume name are required::

    ceph-volume prepare --filestore --data volume_group/lv_name --journal volume_group/journal_lv

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
* There is currently no support for encrypted volumes

The one time process for an existing OSD, with an ID of 0 and using
a ``"ceph"`` cluster name would look like (the following command will **destroy
any data** in the OSD)::

    ceph-volume lvm prepare --filestore --osd-id 0 --osd-fsid E3D291C1-E7BF-4984-9794-B60D9FA139CB

The command line tool will not contact the monitor to generate an OSD ID and
will format the LVM device in addition to storing the metadata on it so that it
can later be startednot contact the monitor to generate an OSD ID and will
format the LVM device in addition to storing the metadata on it so that it can
later be started (for detailed metadata description see :ref:`ceph-volume-lvm-tags`).


.. _ceph-volume-lvm-prepare_bluestore:

``bluestore``
-------------
This subcommand is planned but not currently implemented.


Storing metadata
----------------
The following tags will get applied as part of the prepartion process
regardless of the type of volume (journal or data) and also regardless of the
OSD backend:

* ``cluster_fsid``
* ``data_device``
* ``journal_device``
* ``encrypted``
* ``osd_fsid``
* ``osd_id``
* ``block``
* ``db``
* ``wal``
* ``lockbox_device``

.. note:: For the complete lvm tag conventions see :ref:`ceph-volume-lvm-tag-api`


Summary
-------
To recap the ``prepare`` process:

#. Accept only logical volumes for data and journal (both required)
#. Generate a UUID for the OSD
#. Ask the monitor get an OSD ID reusing the generated UUID
#. OSD data directory is created and data volume mounted
#. Journal is symlinked from data volume to journal location
#. monmap is fetched for activation
#. devices is mounted and data directory is populated by ``ceph-osd``
#. data and journal volumes are assigned all the Ceph metadata using lvm tags
