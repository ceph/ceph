
.. _ceph-volume-lvm-api:

LVM
===
The backend of ``ceph-volume lvm`` is LVM, it relies heavily on the usage of
tags, which is a way for LVM to allow extending its volume metadata. These
values can later be queried against devices and it is how they get discovered
later.

.. warning:: These APIs are not meant to be public, but are documented so that
             it is clear what the tool is doing behind the scenes. Do not alter
             any of these values.


.. _ceph-volume-lvm-tag-api:

Tag API
-------
The process of identifying logical volumes as part of Ceph relies on applying
tags on all volumes. It follows a naming convention for the namespace that
looks like::

    ceph.<tag name>=<tag value>

All tags are prefixed by the ``ceph`` keyword do claim ownership of that
namespace and make it easily identifiable. This is how the OSD ID would be used
in the context of lvm tags::

    ceph.osd_id=0


.. _ceph-volume-lvm-tags:

Metadata
--------
The following describes all the metadata from Ceph OSDs that is stored on an
LVM volume:


``type``
--------
Describes if the device is a an OSD or Journal, with the ability to expand to
other types when supported (for example a lockbox)

Example::

    ceph.type=osd


``cluster_fsid``
----------------
Example::

    ceph.cluster_fsid=7146B649-AE00-4157-9F5D-1DBFF1D52C26

``data_device``
---------------
Example::

    ceph.data_device=/dev/ceph/data-0

``journal_device``
------------------
Example::

    ceph.journal_device=/dev/ceph/journal-0

``encrypted``
-------------
Example for enabled encryption with ``luks``::

    ceph.encrypted=luks

For plain dmcrypt::

    ceph.encrypted=dmcrypt

For disabled encryption::

    ceph.encrypted=0

``osd_fsid``
------------
Example::

    ceph.osd_fsid=88ab9018-f84b-4d62-90b4-ce7c076728ff

``osd_id``
----------
Example::

    ceph.osd_id=1

``block``
---------
Just used on :term:`bluestore` backends.

Example::

    ceph.block=/dev/mapper/vg-block-0

``db``
------
Just used on :term:`bluestore` backends.

Example::

    ceph.db=/dev/mapper/vg-db-0

``wal``
-------
Just used on :term:`bluestore` backends.

Example::

    ceph.wal=/dev/mapper/vg-wal-0


``lockbox_device``
------------------
Only used when encryption is enabled, to store keys in an unencrypted
volume.

Example::

    ceph.lockbox_device=/dev/mapper/vg-lockbox-0
