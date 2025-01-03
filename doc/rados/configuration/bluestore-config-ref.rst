==================================
 BlueStore Configuration Reference 
==================================

Devices
=======

BlueStore manages either one, two, or in certain cases three storage devices.
These *devices* are "devices" in the Linux/Unix sense. This means that they are
assets listed under ``/dev`` or ``/devices``. Each of these devices may be an
entire storage drive, or a partition of a storage drive, or a logical volume.
BlueStore does not create or mount a conventional file system on devices that
it uses; BlueStore reads and writes to the devices directly in a "raw" fashion.

In the simplest case, BlueStore consumes all of a single storage device. This
device is known as the *primary device*. The primary device is identified by
the ``block`` symlink in the data directory.

The data directory is a ``tmpfs`` mount. When this data directory is booted or
activated by ``ceph-volume``, it is populated with metadata files and links
that hold information about the OSD: for example, the OSD's identifier, the
name of the cluster that the OSD belongs to, and the OSD's private keyring.

In more complicated cases, BlueStore is deployed across one or two additional
devices:

* A *write-ahead log (WAL) device* (identified as ``block.wal`` in the data
  directory) can be used to separate out BlueStore's internal journal or
  write-ahead log. Using a WAL device is advantageous only if the WAL device
  is faster than the primary device (for example, if the WAL device is an SSD
  and the primary device is an HDD).
* A *DB device* (identified as ``block.db`` in the data directory) can be used
  to store BlueStore's internal metadata. BlueStore (or more precisely, the
  embedded RocksDB) will put as much metadata as it can on the DB device in
  order to improve performance. If the DB device becomes full, metadata will
  spill back onto the primary device (where it would have been located in the
  absence of the DB device). Again, it is advantageous to provision a DB device
  only if it is faster than the primary device.

If there is only a small amount of fast storage available (for example, less
than a gigabyte), we recommend using the available space as a WAL device. But
if more fast storage is available, it makes more sense to provision a DB
device. Because the BlueStore journal is always placed on the fastest device
available, using a DB device provides the same benefit that using a WAL device
would, while *also* allowing additional metadata to be stored off the primary
device (provided that it fits). DB devices make this possible because whenever
a DB device is specified but an explicit WAL device is not, the WAL will be
implicitly colocated with the DB on the faster device.

To provision a single-device (colocated) BlueStore OSD, run the following
command:

.. prompt:: bash $

   ceph-volume lvm prepare --bluestore --data <device>

To specify a WAL device or DB device, run the following command:
   
.. prompt:: bash $

   ceph-volume lvm prepare --bluestore --data <device> --block.wal <wal-device> --block.db <db-device>

.. note:: The option ``--data`` can take as its argument any of the the
   following devices: logical volumes specified using *vg/lv* notation,
   existing logical volumes, and GPT partitions.



Provisioning strategies
-----------------------

BlueStore differs from Filestore in that there are several ways to deploy a
BlueStore OSD. However, the overall deployment strategy for BlueStore can be
clarified by examining just these two common arrangements:

.. _bluestore-single-type-device-config:

**block (data) only**
^^^^^^^^^^^^^^^^^^^^^
If all devices are of the same type (for example, they are all HDDs), and if
there are no fast devices available for the storage of metadata, then it makes
sense to specify the block device only and to leave ``block.db`` and
``block.wal`` unseparated. The :ref:`ceph-volume-lvm` command for a single
``/dev/sda`` device is as follows:

.. prompt:: bash $

   ceph-volume lvm create --bluestore --data /dev/sda

If the devices to be used for a BlueStore OSD are pre-created logical volumes,
then the :ref:`ceph-volume-lvm` call for an logical volume named
``ceph-vg/block-lv`` is as follows:

.. prompt:: bash $

   ceph-volume lvm create --bluestore --data ceph-vg/block-lv

.. _bluestore-mixed-device-config:

**block and block.db**
^^^^^^^^^^^^^^^^^^^^^^

If you have a mix of fast and slow devices (for example, SSD or HDD), then we
recommend placing ``block.db`` on the faster device while ``block`` (that is,
the data) is stored on the slower device (that is, the rotational drive).

You must create these volume groups and these logical volumes manually. as The
``ceph-volume`` tool is currently unable to do so [create them?] automatically.

The following procedure illustrates the manual creation of volume groups and
logical volumes.  For this example, we shall assume four rotational drives
(``sda``, ``sdb``, ``sdc``, and ``sdd``) and one (fast) SSD (``sdx``). First,
to create the volume groups, run the following commands:

.. prompt:: bash $

   vgcreate ceph-block-0 /dev/sda
   vgcreate ceph-block-1 /dev/sdb
   vgcreate ceph-block-2 /dev/sdc
   vgcreate ceph-block-3 /dev/sdd

Next, to create the logical volumes for ``block``, run the following commands:

.. prompt:: bash $

   lvcreate -l 100%FREE -n block-0 ceph-block-0
   lvcreate -l 100%FREE -n block-1 ceph-block-1
   lvcreate -l 100%FREE -n block-2 ceph-block-2
   lvcreate -l 100%FREE -n block-3 ceph-block-3

Because there are four HDDs, there will be four OSDs. Supposing that there is a
200GB SSD in ``/dev/sdx``, we can create four 50GB logical volumes by running
the following commands:

.. prompt:: bash $

   vgcreate ceph-db-0 /dev/sdx
   lvcreate -L 50GB -n db-0 ceph-db-0
   lvcreate -L 50GB -n db-1 ceph-db-0
   lvcreate -L 50GB -n db-2 ceph-db-0
   lvcreate -L 50GB -n db-3 ceph-db-0

Finally, to create the four OSDs, run the following commands:

.. prompt:: bash $

   ceph-volume lvm create --bluestore --data ceph-block-0/block-0 --block.db ceph-db-0/db-0
   ceph-volume lvm create --bluestore --data ceph-block-1/block-1 --block.db ceph-db-0/db-1
   ceph-volume lvm create --bluestore --data ceph-block-2/block-2 --block.db ceph-db-0/db-2
   ceph-volume lvm create --bluestore --data ceph-block-3/block-3 --block.db ceph-db-0/db-3

After this procedure is finished, there should be four OSDs, ``block`` should
be on the four HDDs, and each HDD should have a 50GB logical volume
(specifically, a DB device) on the shared SSD.

Sizing
======
When using a :ref:`mixed spinning-and-solid-drive setup
<bluestore-mixed-device-config>`, it is important to make a large enough
``block.db`` logical volume for BlueStore. The logical volumes associated with
``block.db`` should have logical volumes that are *as large as possible*.

It is generally recommended that the size of ``block.db`` be somewhere between
1% and 4% of the size of ``block``. For RGW workloads, it is recommended that
the ``block.db`` be at least 4% of the ``block`` size, because RGW makes heavy
use of ``block.db`` to store metadata (in particular, omap keys). For example,
if the ``block`` size is 1TB, then ``block.db`` should have a size of at least
40GB. For RBD workloads, however, ``block.db`` usually needs no more than 1% to
2% of the ``block`` size.

In older releases, internal level sizes are such that the DB can fully utilize
only those specific partition / logical volume sizes that correspond to sums of
L0, L0+L1, L1+L2, and so on--that is, given default settings, sizes of roughly
3GB, 30GB, 300GB, and so on. Most deployments do not substantially benefit from
sizing that accommodates L3 and higher, though DB compaction can be facilitated
by doubling these figures to 6GB, 60GB, and 600GB.

Improvements in Nautilus 14.2.12, Octopus 15.2.6, and subsequent releases allow
for better utilization of arbitrarily-sized DB devices. Moreover, the Pacific
release brings experimental dynamic-level support. Because of these advances,
users of older releases might want to plan ahead by provisioning larger DB
devices today so that the benefits of scale can be realized when upgrades are
made in the future.

When *not* using a mix of fast and slow devices, there is no requirement to
create separate logical volumes for ``block.db`` or ``block.wal``. BlueStore
will automatically colocate these devices within the space of ``block``.

Automatic Cache Sizing
======================

BlueStore can be configured to automatically resize its caches, provided that
certain conditions are met: TCMalloc must be configured as the memory allocator
and the ``bluestore_cache_autotune`` configuration option must be enabled (note
that it is currently enabled by default). When automatic cache sizing is in
effect, BlueStore attempts to keep OSD heap-memory usage under a certain target
size (as determined by ``osd_memory_target``). This approach makes use of a
best-effort algorithm and caches do not shrink smaller than the size defined by
the value of ``osd_memory_cache_min``. Cache ratios are selected in accordance
with a hierarchy of priorities.  But if priority information is not available,
the values specified in the ``bluestore_cache_meta_ratio`` and
``bluestore_cache_kv_ratio`` options are used as fallback cache ratios.

.. confval:: bluestore_cache_autotune
.. confval:: osd_memory_target
.. confval:: bluestore_cache_autotune_interval
.. confval:: osd_memory_base
.. confval:: osd_memory_expected_fragmentation
.. confval:: osd_memory_cache_min
.. confval:: osd_memory_cache_resize_interval


Manual Cache Sizing
===================

The amount of memory consumed by each OSD to be used for its BlueStore cache is
determined by the ``bluestore_cache_size`` configuration option. If that option
has not been specified (that is, if it remains at 0), then Ceph uses a
different configuration option to determine the default memory budget:
``bluestore_cache_size_hdd`` if the primary device is an HDD, or
``bluestore_cache_size_ssd`` if the primary device is an SSD.

BlueStore and the rest of the Ceph OSD daemon make every effort to work within
this memory budget. Note that in addition to the configured cache size, there
is also memory consumed by the OSD itself. There is additional utilization due
to memory fragmentation and other allocator overhead. 

The configured cache-memory budget can be used to store the following types of
things:

* Key/Value metadata (that is, RocksDB's internal cache)
* BlueStore metadata
* BlueStore data (that is, recently read or recently written object data)

Cache memory usage is governed by the configuration options
``bluestore_cache_meta_ratio`` and ``bluestore_cache_kv_ratio``.  The fraction
of the cache that is reserved for data is governed by both the effective
BlueStore cache size (which depends on the relevant
``bluestore_cache_size[_ssd|_hdd]`` option and the device class of the primary
device) and the "meta" and "kv" ratios.  This data fraction can be calculated
with the following formula: ``<effective_cache_size> * (1 -
bluestore_cache_meta_ratio - bluestore_cache_kv_ratio)``.

.. confval:: bluestore_cache_size
.. confval:: bluestore_cache_size_hdd
.. confval:: bluestore_cache_size_ssd
.. confval:: bluestore_cache_meta_ratio
.. confval:: bluestore_cache_kv_ratio

Checksums
=========

BlueStore checksums all metadata and all data written to disk. Metadata
checksumming is handled by RocksDB and uses the `crc32c` algorithm. By
contrast, data checksumming is handled by BlueStore and can use either
`crc32c`, `xxhash32`, or `xxhash64`. Nonetheless, `crc32c` is the default
checksum algorithm and it is suitable for most purposes.

Full data checksumming increases the amount of metadata that BlueStore must
store and manage. Whenever possible (for example, when clients hint that data
is written and read sequentially), BlueStore will checksum larger blocks. In
many cases, however, it must store a checksum value (usually 4 bytes) for every
4 KB block of data.

It is possible to obtain a smaller checksum value by truncating the checksum to
one or two bytes and reducing the metadata overhead.  A drawback of this
approach is that it increases the probability of a random error going
undetected: about one in four billion given a 32-bit (4 byte) checksum, 1 in
65,536 given a 16-bit (2 byte) checksum, and 1 in 256 given an 8-bit (1 byte)
checksum. To use the smaller checksum values, select `crc32c_16` or `crc32c_8`
as the checksum algorithm.

The *checksum algorithm* can be specified either via a per-pool ``csum_type``
configuration option or via the global configuration option. For example:

.. prompt:: bash $

   ceph osd pool set <pool-name> csum_type <algorithm>

.. confval:: bluestore_csum_type

Inline Compression
==================

BlueStore supports inline compression using `snappy`, `zlib`, `lz4`, or `zstd`. 

Whether data in BlueStore is compressed is determined by two factors: (1) the
*compression mode* and (2) any client hints associated with a write operation.
The compression modes are as follows:

* **none**: Never compress data.
* **passive**: Do not compress data unless the write operation has a
  *compressible* hint set.
* **aggressive**: Do compress data unless the write operation has an
  *incompressible* hint set.
* **force**: Try to compress data no matter what.

For more information about the *compressible* and *incompressible* I/O hints,
see :c:func:`rados_set_alloc_hint`.

Note that data in Bluestore will be compressed only if the data chunk will be
sufficiently reduced in size (as determined by the ``bluestore compression
required ratio`` setting). No matter which compression modes have been used, if
the data chunk is too big, then it will be discarded and the original
(uncompressed) data will be stored instead. For example, if ``bluestore
compression required ratio`` is set to ``.7``, then data compression will take
place only if the size of the compressed data is no more than 70% of the size
of the original data.

The *compression mode*, *compression algorithm*, *compression required ratio*,
*min blob size*, and *max blob size* settings can be specified either via a
per-pool property or via a global config option. To specify pool properties,
run the following commands:

.. prompt:: bash $

   ceph osd pool set <pool-name> compression_algorithm <algorithm>
   ceph osd pool set <pool-name> compression_mode <mode>
   ceph osd pool set <pool-name> compression_required_ratio <ratio>
   ceph osd pool set <pool-name> compression_min_blob_size <size>
   ceph osd pool set <pool-name> compression_max_blob_size <size>

.. confval:: bluestore_compression_algorithm
.. confval:: bluestore_compression_mode
.. confval:: bluestore_compression_required_ratio
.. confval:: bluestore_compression_min_blob_size
.. confval:: bluestore_compression_min_blob_size_hdd
.. confval:: bluestore_compression_min_blob_size_ssd
.. confval:: bluestore_compression_max_blob_size
.. confval:: bluestore_compression_max_blob_size_hdd
.. confval:: bluestore_compression_max_blob_size_ssd

.. _bluestore-rocksdb-sharding:

RocksDB Sharding
================

BlueStore maintains several types of internal key-value data, all of which are
stored in RocksDB. Each data type in BlueStore is assigned a unique prefix.
Prior to the Pacific release, all key-value data was stored in a single RocksDB
column family: 'default'. In Pacific and later releases, however, BlueStore can
divide key-value data into several RocksDB column families. BlueStore achieves
better caching and more precise compaction when keys are similar: specifically,
when keys have similar access frequency, similar modification frequency, and a
similar lifetime.  Under such conditions, performance is improved and less disk
space is required during compaction (because each column family is smaller and
is able to compact independently of the others).

OSDs deployed in Pacific or later releases use RocksDB sharding by default.
However, if Ceph has been upgraded to Pacific or a later version from a
previous version, sharding is disabled on any OSDs that were created before
Pacific.

To enable sharding and apply the Pacific defaults to a specific OSD, stop the
OSD and run the following command:

    .. prompt:: bash #

       ceph-bluestore-tool \
        --path <data path> \
        --sharding="m(3) p(3,0-12) O(3,0-13)=block_cache={type=binned_lru} L P" \
        reshard

.. confval:: bluestore_rocksdb_cf
.. confval:: bluestore_rocksdb_cfs

Throttling
==========

.. confval:: bluestore_throttle_bytes
.. confval:: bluestore_throttle_deferred_bytes
.. confval:: bluestore_throttle_cost_per_io
.. confval:: bluestore_throttle_cost_per_io_hdd
.. confval:: bluestore_throttle_cost_per_io_ssd

SPDK Usage
==========

To use the SPDK driver for NVMe devices, you must first prepare your system.
See `SPDK document`__.

.. __: http://www.spdk.io/doc/getting_started.html#getting_started_examples

SPDK offers a script that will configure the device automatically. Run this
script with root permissions:

.. prompt:: bash $

   sudo src/spdk/scripts/setup.sh

You will need to specify the subject NVMe device's device selector with the
"spdk:" prefix for ``bluestore_block_path``.

In the following example, you first find the device selector of an Intel NVMe
SSD by running the following command:

.. prompt:: bash $

   lspci -mm -n -d -d 8086:0953

The form of the device selector is either ``DDDD:BB:DD.FF`` or
``DDDD.BB.DD.FF``.

Next, supposing that ``0000:01:00.0`` is the device selector found in the
output of the ``lspci`` command, you can specify the device selector by running
the following command::

  bluestore_block_path = "spdk:trtype:pcie traddr:0000:01:00.0"

You may also specify a remote NVMeoF target over the TCP transport, as in the
following example::

  bluestore_block_path = "spdk:trtype:tcp traddr:10.67.110.197 trsvcid:4420 subnqn:nqn.2019-02.io.spdk:cnode1"

To run multiple SPDK instances per node, you must make sure each instance uses
its own DPDK memory by specifying for each instance the amount of DPDK memory
(in MB) that the instance will use.

In most cases, a single device can be used for data, DB, and WAL. We describe
this strategy as *colocating* these components. Be sure to enter the below
settings to ensure that all I/Os are issued through SPDK::

  bluestore_block_db_path = ""
  bluestore_block_db_size = 0
  bluestore_block_wal_path = ""
  bluestore_block_wal_size = 0

If these settings are not entered, then the current implementation will
populate the SPDK map files with kernel file system symbols and will use the
kernel driver to issue DB/WAL I/Os.

Minimum Allocation Size
=======================

There is a configured minimum amount of storage that BlueStore allocates on an
underlying storage device. In practice, this is the least amount of capacity
that even a tiny RADOS object can consume on each OSD's primary device. The
configuration option in question--:confval:`bluestore_min_alloc_size`--derives
its value from the value of either :confval:`bluestore_min_alloc_size_hdd` or
:confval:`bluestore_min_alloc_size_ssd`, depending on the OSD's ``rotational``
attribute. Thus if an OSD is created on an HDD, BlueStore is initialized with
the current value of :confval:`bluestore_min_alloc_size_hdd`; but with SSD OSDs
(including NVMe devices), Bluestore is initialized with the current value of
:confval:`bluestore_min_alloc_size_ssd`.

In Mimic and earlier releases, the default values were 64KB for rotational
media (HDD) and 16KB for non-rotational media (SSD). The Octopus release
changed the the default value for non-rotational media (SSD) to 4KB, and the
Pacific release changed the default value for rotational media (HDD) to 4KB.

These changes were driven by space amplification that was experienced by Ceph
RADOS GateWay (RGW) deployments that hosted large numbers of small files
(S3/Swift objects).

For example, when an RGW client stores a 1 KB S3 object, that object is written
to a single RADOS object. In accordance with the default
:confval:`min_alloc_size` value, 4 KB of underlying drive space is allocated.
This means that roughly 3 KB (that is, 4 KB minus 1 KB) is allocated but never
used: this corresponds to 300% overhead or 25% efficiency. Similarly, a 5 KB
user object will be stored as two RADOS objects, a 4 KB RADOS object and a 1 KB
RADOS object, with the result that 4KB of device capacity is stranded. In this
case, however, the overhead percentage is much smaller. Think of this in terms
of the remainder from a modulus operation. The overhead *percentage* thus
decreases rapidly as object size increases.

There is an additional subtlety that is easily missed: the amplification
phenomenon just described takes place for *each* replica. For example, when
using the default of three copies of data (3R), a 1 KB S3 object actually
strands roughly 9 KB of storage device capacity. If erasure coding (EC) is used
instead of replication, the amplification might be even higher: for a ``k=4,
m=2`` pool, our 1 KB S3 object allocates 24 KB (that is, 4 KB multiplied by 6)
of device capacity.

When an RGW bucket pool contains many relatively large user objects, the effect
of this phenomenon is often negligible. However, with deployments that can
expect a significant fraction of relatively small user objects, the effect
should be taken into consideration.

The 4KB default value aligns well with conventional HDD and SSD devices.
However, certain novel coarse-IU (Indirection Unit) QLC SSDs perform and wear
best when :confval:`bluestore_min_alloc_size_ssd` is specified at OSD creation
to match the device's IU: this might be 8KB, 16KB, or even 64KB.  These novel
storage drives can achieve read performance that is competitive with that of
conventional TLC SSDs and write performance that is faster than that of HDDs,
with higher density and lower cost than TLC SSDs.

Note that when creating OSDs on these novel devices, one must be careful to
apply the non-default value only to appropriate devices, and not to
conventional HDD and SSD devices. Error can be avoided through careful ordering
of OSD creation, with custom OSD device classes, and especially by the use of
central configuration *masks*.

In Quincy and later releases, you can use the
:confval:`bluestore_use_optimal_io_size_for_min_alloc_size` option to allow
automatic discovery of the correct value as each OSD is created. Note that the
use of ``bcache``, ``OpenCAS``, ``dmcrypt``, ``ATA over Ethernet``, `iSCSI`, or
other device-layering and abstraction technologies might confound the
determination of correct values. Moreover, OSDs deployed on top of VMware
storage have sometimes been found to report a ``rotational`` attribute that
does not match the underlying hardware.

We suggest inspecting such OSDs at startup via logs and admin sockets in order
to ensure that their behavior is correct. Be aware that this kind of inspection
might not work as expected with older kernels.  To check for this issue,
examine the presence and value of ``/sys/block/<drive>/queue/optimal_io_size``.

.. note:: When running Reef or a later Ceph release, the ``min_alloc_size``
   baked into each OSD is conveniently reported by ``ceph osd metadata``.

To inspect a specific OSD, run the following command:

.. prompt:: bash #

   ceph osd metadata osd.1701 | egrep rotational\|alloc

This space amplification might manifest as an unusually high ratio of raw to
stored data as reported by ``ceph df``. There might also be ``%USE`` / ``VAR``
values reported by ``ceph osd df`` that are unusually high in comparison to
other, ostensibly identical, OSDs. Finally, there might be unexpected balancer
behavior in pools that use OSDs that have mismatched ``min_alloc_size`` values.

This BlueStore attribute takes effect *only* at OSD creation; if the attribute
is changed later, a specific OSD's behavior will not change unless and until
the OSD is destroyed and redeployed with the appropriate option value(s).
Upgrading to a later Ceph release will *not* change the value used by OSDs that
were deployed under older releases or with other settings.

.. confval:: bluestore_min_alloc_size
.. confval:: bluestore_min_alloc_size_hdd
.. confval:: bluestore_min_alloc_size_ssd
.. confval:: bluestore_use_optimal_io_size_for_min_alloc_size

DSA (Data Streaming Accelerator) Usage
======================================

If you want to use the DML library to drive the DSA device for offloading
read/write operations on persistent memory (PMEM) in BlueStore, you need to
install `DML`_ and the `idxd-config`_ library. This will work only on machines
that have a SPR (Sapphire Rapids) CPU.

.. _dml: https://github.com/intel/dml
.. _idxd-config: https://github.com/intel/idxd-config

After installing the DML software, configure the shared work queues (WQs) with
reference to the following WQ configuration example:

.. prompt:: bash $

   accel-config config-wq --group-id=1 --mode=shared --wq-size=16 --threshold=15 --type=user --name="myapp1" --priority=10 --block-on-fault=1 dsa0/wq0.1
   accel-config config-engine dsa0/engine0.1 --group-id=1
   accel-config enable-device dsa0
   accel-config enable-wq dsa0/wq0.1
