=================
 Storage Devices
=================

There are several Ceph daemons in a storage cluster:

.. _rados_configuration_storage-devices_ceph_osd:

* **Ceph OSDs** (Object Storage Daemons) store most of the data
  in Ceph. Usually each OSD is backed by a single storage device.
  This can be a traditional hard disk (HDD) or a solid state disk
  (SSD). OSDs can also be backed by a combination of devices: for
  example, a HDD for most data and an SSD (or partition of an
  SSD) for some metadata. The number of OSDs in a cluster is
  usually a function of the amount of data to be stored, the size
  of each storage device, and the level and type of redundancy
  specified (replication or erasure coding).
* **Ceph Monitor** daemons manage critical cluster state. This
  includes cluster membership and authentication information.
  Small clusters require only a few gigabytes of storage to hold
  the monitor database. In large clusters, however, the monitor
  database can reach sizes of tens of gigabytes to hundreds of
  gigabytes.  
* **Ceph Manager** daemons run alongside monitor daemons, providing
  additional monitoring and providing interfaces to external
  monitoring and management systems.

.. _rados_config_storage_devices_osd_backends:

OSD Back Ends
=============

There are two ways that OSDs manage the data they store.  As of the Luminous
12.2.z release, the default (and recommended) back end is *BlueStore*.  Prior
to the Luminous release, the default (and only) back end was *Filestore*.

.. _rados_config_storage_devices_bluestore:

BlueStore
---------

BlueStore is a special-purpose storage back end designed specifically for
managing data on disk for Ceph OSD workloads.  BlueStore's design is based on
a decade of experience of supporting and managing Filestore OSDs. 

Key BlueStore features include:

* Direct management of storage devices. BlueStore consumes raw block devices or
  partitions. This avoids intervening layers of abstraction (such as local file
  systems like XFS) that can limit performance or add complexity.
* Metadata management with RocksDB. RocksDB's key/value database is embedded
  in order to manage internal metadata, including the mapping of object
  names to block locations on disk.
* Full data and metadata checksumming. By default, all data and
  metadata written to BlueStore is protected by one or more
  checksums. No data or metadata is read from disk or returned
  to the user without being verified.
* Inline compression.  Data can be optionally compressed before being written
  to disk.
* Multi-device metadata tiering. BlueStore allows its internal
  journal (write-ahead log) to be written to a separate, high-speed
  device (like an SSD, NVMe, or NVDIMM) for increased performance.  If
  a significant amount of faster storage is available, internal
  metadata can be stored on the faster device.
* Efficient copy-on-write. RBD and CephFS snapshots rely on a
  copy-on-write *clone* mechanism that is implemented efficiently in
  BlueStore. This results in efficient I/O both for regular snapshots
  and for erasure-coded pools (which rely on cloning to implement
  efficient two-phase commits).

For more information, see :doc:`bluestore-config-ref` and :doc:`/rados/operations/bluestore-migration`.

FileStore
---------
.. warning:: Filestore has been deprecated in the Reef release and is no longer supported.


FileStore is the legacy approach to storing objects in Ceph. It
relies on a standard file system (normally XFS) in combination with a
key/value database (traditionally LevelDB, now RocksDB) for some
metadata.

FileStore is well-tested and widely used in production. However, it
suffers from many performance deficiencies due to its overall design
and its reliance on a traditional file system for object data storage.

Although FileStore is capable of functioning on most POSIX-compatible
file systems (including btrfs and ext4), we recommend that only the
XFS file system be used with Ceph. Both btrfs and ext4 have known bugs and
deficiencies and their use may lead to data loss. By default, all Ceph
provisioning tools use XFS.

For more information, see :doc:`filestore-config-ref`.
