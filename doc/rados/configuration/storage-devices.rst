=================
 Storage Devices
=================

There are two Ceph daemons that store data on devices:

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


OSD Back Ends
=============

There are two ways that OSDs manage the data they store.  As of the Luminous
12.2.z release, the default (and recommended) back end is *BlueStore*.  Prior
to the Luminous release, the default (and only) back end was *Filestore*.

.. _rados_config_storage_devices_bluestore:

BlueStore
---------

<<<<<<< HEAD
BlueStore is a special-purpose storage backend designed specifically
for managing data on disk for Ceph OSD workloads.  It is motivated by
experience supporting and managing OSDs using FileStore over the
last ten years.  Key BlueStore features include:
=======
BlueStore is a special-purpose storage back end designed specifically for
managing data on disk for Ceph OSD workloads.  BlueStore's design is based on
a decade of experience of supporting and managing Filestore OSDs. 
>>>>>>> 28abc6a9a59 (doc/rados: s/backend/back end/)

* Direct management of storage devices.  BlueStore consumes raw block
  devices or partitions.  This avoids any intervening layers of
  abstraction (such as local file systems like XFS) that may limit
  performance or add complexity.
* Metadata management with RocksDB.  We embed RocksDB's key/value database
  in order to manage internal metadata, such as the mapping from object
  names to block locations on disk.
* Full data and metadata checksumming.  By default all data and
  metadata written to BlueStore is protected by one or more
  checksums.  No data or metadata will be read from disk or returned
  to the user without being verified.
* Inline compression.  Data written may be optionally compressed
  before being written to disk.
* Multi-device metadata tiering.  BlueStore allows its internal
  journal (write-ahead log) to be written to a separate, high-speed
  device (like an SSD, NVMe, or NVDIMM) to increased performance.  If
  a significant amount of faster storage is available, internal
  metadata can also be stored on the faster device.
* Efficient copy-on-write.  RBD and CephFS snapshots rely on a
  copy-on-write *clone* mechanism that is implemented efficiently in
  BlueStore.  This results in efficient IO both for regular snapshots
  and for erasure coded pools (which rely on cloning to implement
  efficient two-phase commits).

For more information, see :doc:`bluestore-config-ref` and :doc:`/rados/operations/bluestore-migration`.

FileStore
---------

FileStore is the legacy approach to storing objects in Ceph.  It
relies on a standard file system (normally XFS) in combination with a
key/value database (traditionally LevelDB, now RocksDB) for some
metadata.

FileStore is well-tested and widely used in production but suffers
from many performance deficiencies due to its overall design and
reliance on a traditional file system for storing object data.

Although FileStore is generally capable of functioning on most
POSIX-compatible file systems (including btrfs and ext4), we only
recommend that XFS be used.  Both btrfs and ext4 have known bugs and
deficiencies and their use may lead to data loss.  By default all Ceph
provisioning tools will use XFS.

For more information, see :doc:`filestore-config-ref`.
