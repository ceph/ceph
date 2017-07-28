=========
ceph-disk
=========


device-mapper crypt
===================

Settings
--------

``osd_dmcrypt_type``

:Description: this option specifies the mode in which ``cryptsetup`` works. It can be ``luks`` or ``plain``.  It kicks in only if the ``--dmcrypt`` option is passed to ``ceph-disk``. See also `cryptsetup document <https://gitlab.com/cryptsetup/cryptsetup/wikis/DMCrypt#configuration-using-cryptsetup>`_ for more details.

:Type: String
:Default: ``luks``


``osd_dmcrypt_key_size``

:Description: the size of the random string in bytes used as the LUKS key. The string is read from ``/dev/urandom`` and then encoded using base64. It will be stored with the key of ``dm-crypt/osd/$uuid/luks`` using config-key.

:Type: String
:Default: 1024 if ``osd_dmcrypt_type`` is ``luks``, 256 otherwise.

lockbox
-------

``ceph-disk`` supports dmcrypt (device-mapper crypt). If dmcrypt is enabled, the partitions will be encrypted using this machinary. For each OSD device, a lockbox is introduced for holding the information regarding how the dmcrypt key is stored. To prepare a lockbox, ``ceph-disk``

#. creates a dedicated lockbox partition on device, and
#. populates it with a tiny filesystem, then
#. automounts it at ``/var/lib/ceph/osd-lockbox/$uuid``, read-only. where the ``uuid`` is the lockbox's uuid.

under which, settings are stored using plain files:

- key-management-mode: ``ceph-mon v1``
- osd-uuid: the OSD's uuid
- ceph_fsid: the fsid of the cluster
- keyring: the lockbox's allowing one to fetch the LUKS key
- block_uuid: the partition uuid for the block device
- journal_uuid: the partition uuid for the journal device
- block.db_uuid: the partition uuid for the block.db device
- block.wal_uuid: the partition uuid for the block.wal device
- magic: a magic string indicating that this partition is a lockbox. It's not used currently.
- ``${space_uuid}``: symbolic links named after the uuid of space partitions pointing to  ``/var/lib/ceph/osd-lockbox/$uuid``. in the case of FileStore, the space partitions are ``data`` and ``journal`` partitions, for BlueStore, they are ``data``, ``block.db`` and ``block.wal``.

Currently, ``ceph-mon v1`` is the only supported key-management-mode. In that case, the LUKS key is stored using the config-key in the monitor store with the key of ``dm-crypt/osd/$uuid/luks``.


partitions
==========

``ceph-disk`` creates partitions for preparing a device for OSD deployment. Their partition numbers are hardcoded. For instance, data partition's partition number is always *1* :

1. data partition
2. journal partition, if co-located with data
3. block.db for BlueStore, if co-located with data
4. block.wal for BlueStore, if co-located with data
5. lockbox
