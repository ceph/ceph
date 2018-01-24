
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

All tags are prefixed by the ``ceph`` keyword to claim ownership of that
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


``data_uuid``
-------------
Example::

    ceph.data_uuid=B76418EB-0024-401C-8955-AE6919D45CC3


``journal_device``
------------------
Example::

    ceph.journal_device=/dev/ceph/journal-0


``journal_uuid``
----------------
Example::

    ceph.journal_uuid=2070E121-C544-4F40-9571-0B7F35C6CB2B


``encrypted``
-------------
Example for enabled encryption with ``luks``::

    ceph.encrypted=1

When encryption is not supported or simply disabled::

    ceph.encrypted=0


``osd_fsid``
------------
Example::

    ceph.osd_fsid=88ab9018-f84b-4d62-90b4-ce7c076728ff


``osd_id``
----------
Example::

    ceph.osd_id=1


``block_device``
----------------
Just used on :term:`bluestore` backends. Captures the path to the logical
volume path.

Example::

    ceph.block_device=/dev/mapper/vg-block-0


``block_uuid``
--------------
Just used on :term:`bluestore` backends. Captures either the logical volume UUID or
the partition UUID.

Example::

    ceph.block_uuid=E5F041BB-AAD4-48A8-B3BF-31F7AFD7D73E


``db_device``
-------------
Just used on :term:`bluestore` backends. Captures the path to the logical
volume path.

Example::

    ceph.db_device=/dev/mapper/vg-db-0


``db_uuid``
-----------
Just used on :term:`bluestore` backends. Captures either the logical volume UUID or
the partition UUID.

Example::

    ceph.db_uuid=F9D02CF1-31AB-4910-90A3-6A6302375525


``wal_device``
--------------
Just used on :term:`bluestore` backends. Captures the path to the logical
volume path.

Example::

    ceph.wal_device=/dev/mapper/vg-wal-0


``wal_uuid``
------------
Just used on :term:`bluestore` backends. Captures either the logical volume UUID or
the partition UUID.

Example::

    ceph.wal_uuid=A58D1C68-0D6E-4CB3-8E99-B261AD47CC39
