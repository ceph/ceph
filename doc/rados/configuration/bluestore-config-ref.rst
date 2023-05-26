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

inline compression
==================

bluestore supports inline compression using `snappy`, `zlib`, or
`lz4`. please note that the `lz4` compression plugin is not
distributed in the official release.

whether data in bluestore is compressed is determined by a combination
of the *compression mode* and any hints associated with a write
operation.  the modes are:

* **none**: never compress data.
* **passive**: do not compress data unless the write operation has a
  *compressible* hint set.
* **aggressive**: compress data unless the write operation has an
  *incompressible* hint set.
* **force**: try to compress data no matter what.

for more information about the *compressible* and *incompressible* io
hints, see :c:func:`rados_set_alloc_hint`.

note that regardless of the mode, if the size of the data chunk is not
reduced sufficiently it will not be used and the original
(uncompressed) data will be stored.  for example, if the ``bluestore
compression required ratio`` is set to ``.7`` then the compressed data
must be 70% of the size of the original (or smaller).

the *compression mode*, *compression algorithm*, *compression required
ratio*, *min blob size*, and *max blob size* can be set either via a
per-pool property or a global config option.  pool properties can be
set with:

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

rocksdb sharding
================

internally bluestore uses multiple types of key-value data,
stored in rocksdb.  each data type in bluestore is assigned a
unique prefix. until pacific all key-value data was stored in
single rocksdb column family: 'default'.  since pacific,
bluestore can divide this data into multiple rocksdb column
families. when keys have similar access frequency, modification
frequency and lifetime, bluestore benefits from better caching
and more precise compaction. this improves performance, and also
requires less disk space during compaction, since each column
family is smaller and can compact independent of others.

osds deployed in pacific or later use rocksdb sharding by default.
if ceph is upgraded to pacific from a previous version, sharding is off.

to enable sharding and apply the pacific defaults, stop an osd and run

    .. prompt:: bash #

      ceph-bluestore-tool \
        --path <data path> \
        --sharding="m(3) p(3,0-12) o(3,0-13)=block_cache={type=binned_lru} l p" \
        reshard

.. confval:: bluestore_rocksdb_cf
.. confval:: bluestore_rocksdb_cfs

throttling
==========

.. confval:: bluestore_throttle_bytes
.. confval:: bluestore_throttle_deferred_bytes
.. confval:: bluestore_throttle_cost_per_io
.. confval:: bluestore_throttle_cost_per_io_hdd
.. confval:: bluestore_throttle_cost_per_io_ssd

spdk usage
==================

if you want to use the spdk driver for nvme devices, you must prepare your system.
refer to `spdk document`__ for more details.

.. __: http://www.spdk.io/doc/getting_started.html#getting_started_examples

spdk offers a script to configure the device automatically. users can run the
script as root:

.. prompt:: bash $

   sudo src/spdk/scripts/setup.sh

you will need to specify the subject nvme device's device selector with
the "spdk:" prefix for ``bluestore_block_path``.

for example, you can find the device selector of an intel pcie ssd with:

.. prompt:: bash $

   lspci -mm -n -d -d 8086:0953

the device selector always has the form of ``dddd:bb:dd.ff`` or ``dddd.bb.dd.ff``.

and then set::

  bluestore_block_path = "spdk:trtype:pcie traddr:0000:01:00.0"

where ``0000:01:00.0`` is the device selector found in the output of ``lspci``
command above.

you may also specify a remote nvmeof target over the tcp transport as in the
following example::

  bluestore_block_path = "spdk:trtype:tcp traddr:10.67.110.197 trsvcid:4420 subnqn:nqn.2019-02.io.spdk:cnode1"

to run multiple spdk instances per node, you must specify the
amount of dpdk memory in mb that each instance will use, to make sure each
instance uses its own dpdk memory.

in most cases, a single device can be used for data, db, and wal.  we describe
this strategy as *colocating* these components. be sure to enter the below
settings to ensure that all ios are issued through spdk.::

  bluestore_block_db_path = ""
  bluestore_block_db_size = 0
  bluestore_block_wal_path = ""
  bluestore_block_wal_size = 0

otherwise, the current implementation will populate the spdk map files with
kernel file system symbols and will use the kernel driver to issue db/wal io.

minimum allocation size
========================

there is a configured minimum amount of storage that bluestore will allocate on
an osd.  in practice, this is the least amount of capacity that a rados object
can consume.  the value of :confval:`bluestore_min_alloc_size` is derived from the
value of :confval:`bluestore_min_alloc_size_hdd` or :confval:`bluestore_min_alloc_size_ssd`
depending on the osd's ``rotational`` attribute.  this means that when an osd
is created on an hdd, bluestore will be initialized with the current value
of :confval:`bluestore_min_alloc_size_hdd`, and ssd osds (including nvme devices)
with the value of :confval:`bluestore_min_alloc_size_ssd`.

through the mimic release, the default values were 64kb and 16kb for rotational
(hdd) and non-rotational (ssd) media respectively.  octopus changed the default
for ssd (non-rotational) media to 4kb, and pacific changed the default for hdd
(rotational) media to 4kb as well.

these changes were driven by space amplification experienced by ceph rados
gateway (rgw) deployments that host large numbers of small files
(s3/swift objects).

for example, when an rgw client stores a 1kb s3 object, it is written to a
single rados object.  with the default :confval:`min_alloc_size` value, 4kb of
underlying drive space is allocated.  this means that roughly
(4kb - 1kb) == 3kb of that rados object's allocated space is never used, which corresponds to 300%
overhead or 25% efficiency. similarly, a 5kb user object will be stored
as one 4kb and one 1kb rados object, again stranding 4kb of device capacity,
though in this case the overhead is a much smaller percentage.  think of this
in terms of the remainder from a modulus operation. the overhead *percentage*
thus decreases rapidly as user object size increases.

an easily missed additional subtlety is that this
takes place for *each* replica.  so when using the default three copies of
data (3r), a 1kb s3 object actually consumes 12kb of storage device
capacity, with 11kb of overhead.  if erasure coding (ec) is used instead of replication, the
amplification may be even higher: for a ``k=4,m=2`` pool, our 1kb s3 object
will allocate (6 * 4kb) = 24kb of device capacity.

when an rgw bucket pool contains many relatively large user objects, the effect
of this phenomenon is often negligible, but should be considered for deployments
that expect a significant fraction of relatively small objects.

the 4kb default value aligns well with conventional hdd and ssd devices.  some
new coarse-iu (indirection unit) qlc ssds however perform and wear best
when :confval:`bluestore_min_alloc_size_ssd`
is set at osd creation to match the device's iu:. 8kb, 16kb, or even 64kb.
these novel storage drives allow one to achieve read performance competitive
with conventional tlc ssds and write performance faster than hdds, with
high density and lower cost than tlc ssds.

note that when creating osds on these devices, one must carefully apply the
non-default value only to appropriate devices, and not to conventional ssd and
hdd devices.  this may be done through careful ordering of osd creation, custom
osd device classes, and especially by the use of central configuration _masks_.

quincy and later releases add
the :confval:`bluestore_use_optimal_io_size_for_min_alloc_size`
option that enables automatic discovery of the appropriate value as each osd is
created.  note that the use of ``bcache``, ``opencas``, ``dmcrypt``,
``ata over ethernet``, `iscsi`, or other device layering / abstraction
technologies may confound the determination of appropriate values. osds
deployed on top of vmware storage have been reported to also
sometimes report a ``rotational`` attribute that does not match the underlying
hardware.

we suggest inspecting such osds at startup via logs and admin sockets to ensure that
behavior is appropriate.  note that this also may not work as desired with
older kernels.  you can check for this by examining the presence and value
of ``/sys/block/<drive>/queue/optimal_io_size``.

you may also inspect a given osd:

.. prompt:: bash #

   ceph osd metadata osd.1701 | grep rotational

this space amplification may manifest as an unusually high ratio of raw to
stored data reported by ``ceph df``.  ``ceph osd df`` may also report
anomalously high ``%use`` / ``var`` values when
compared to other, ostensibly identical osds.  a pool using osds with
mismatched ``min_alloc_size`` values may experience unexpected balancer
behavior as well.

note that this bluestore attribute takes effect *only* at osd creation; if
changed later, a given osd's behavior will not change unless / until it is
destroyed and redeployed with the appropriate option value(s).  upgrading
to a later ceph release will *not* change the value used by osds deployed
under older releases or with other settings.


.. confval:: bluestore_min_alloc_size
.. confval:: bluestore_min_alloc_size_hdd
.. confval:: bluestore_min_alloc_size_ssd
.. confval:: bluestore_use_optimal_io_size_for_min_alloc_size

dsa (data streaming accelerator usage)
======================================

if you want to use the dml library to drive dsa device for offloading
read/write operations on persist memory in bluestore. you need to install
`dml`_ and `idxd-config`_ library in your machine with spr (sapphire rapids) cpu.

.. _dml: https://github.com/intel/dml
.. _idxd-config: https://github.com/intel/idxd-config

after installing the dml software, you need to configure the shared
work queues (wqs) with the following wq configuration example via accel-config tool:

.. prompt:: bash $

   accel-config config-wq --group-id=1 --mode=shared --wq-size=16 --threshold=15 --type=user --name="myapp1" --priority=10 --block-on-fault=1 dsa0/wq0.1
   accel-config config-engine dsa0/engine0.1 --group-id=1
   accel-config enable-device dsa0
   accel-config enable-wq dsa0/wq0.1
