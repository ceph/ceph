=======================
 librbd Settings
=======================

See `Block Device`_ for additional details.

Cache Settings
=======================

.. sidebar:: Kernel Caching

	The kernel driver for Ceph block devices can use the Linux page cache to 
	improve performance.

The user space implementation of the Ceph block device (i.e., ``librbd``) cannot
take advantage of the Linux page cache, so it includes its own in-memory
caching, called "RBD caching." RBD caching behaves just like well-behaved hard
disk caching.  When the OS sends a barrier or a flush request, all dirty data is
written to the OSDs. This means that using write-back caching is just as safe as
using a well-behaved physical hard disk with a VM that properly sends flushes
(i.e. Linux kernel >= 2.6.32). The cache uses a Least Recently Used (LRU)
algorithm, and in write-back mode it  can coalesce contiguous requests for
better throughput.

.. versionadded:: 0.46

Ceph supports write-back caching for RBD. To enable it, add  ``rbd cache =
true`` to the ``[client]`` section of your ``ceph.conf`` file. By default
``librbd`` does not perform any caching. Writes and reads go directly to the
storage cluster, and writes return only when the data is on disk on all
replicas. With caching enabled, writes return immediately, unless there are more
than ``rbd cache max dirty`` unflushed bytes. In this case, the write triggers
writeback and blocks until enough bytes are flushed.

.. versionadded:: 0.47

Ceph supports write-through caching for RBD. You can set the size of
the cache, and you can set targets and limits to switch from
write-back caching to write through caching. To enable write-through
mode, set ``rbd cache max dirty`` to 0. This means writes return only
when the data is on disk on all replicas, but reads may come from the
cache. The cache is in memory on the client, and each RBD image has
its own.  Since the cache is local to the client, there's no coherency
if there are others accessing the image. Running GFS or OCFS on top of
RBD will not work with caching enabled.

The ``ceph.conf`` file settings for RBD should be set in the ``[client]``
section of your configuration file. The settings include: 


``rbd cache``

:Description: Enable caching for RADOS Block Device (RBD).
:Type: Boolean
:Required: No
:Default: ``true``


``rbd cache size``

:Description: The RBD cache size in bytes.
:Type: 64-bit Integer
:Required: No
:Default: ``32 MiB``


``rbd cache max dirty``

:Description: The ``dirty`` limit in bytes at which the cache triggers write-back.  If ``0``, uses write-through caching.
:Type: 64-bit Integer
:Required: No
:Constraint: Must be less than ``rbd cache size``.
:Default: ``24 MiB``


``rbd cache target dirty``

:Description: The ``dirty target`` before the cache begins writing data to the data storage. Does not block writes to the cache.
:Type: 64-bit Integer
:Required: No
:Constraint: Must be less than ``rbd cache max dirty``.
:Default: ``16 MiB``


``rbd cache max dirty age``

:Description: The number of seconds dirty data is in the cache before writeback starts. 
:Type: Float
:Required: No
:Default: ``1.0``

.. versionadded:: 0.60

``rbd cache writethrough until flush``

:Description: Start out in write-through mode, and switch to write-back after the first flush request is received. Enabling this is a conservative but safe setting in case VMs running on rbd are too old to send flushes, like the virtio driver in Linux before 2.6.32.
:Type: Boolean
:Required: No
:Default: ``true``

.. _Block Device: ../../rbd


Read-ahead Settings
=======================

.. versionadded:: 0.86

RBD supports read-ahead/prefetching to optimize small, sequential reads.
This should normally be handled by the guest OS in the case of a VM,
but boot loaders may not issue efficient reads.
Read-ahead is automatically disabled if caching is disabled.


``rbd readahead trigger requests``

:Description: Number of sequential read requests necessary to trigger read-ahead.
:Type: Integer
:Required: No
:Default: ``10``


``rbd readahead max bytes``

:Description: Maximum size of a read-ahead request.  If zero, read-ahead is disabled.
:Type: 64-bit Integer
:Required: No
:Default: ``512 KiB``


``rbd readahead disable after bytes``

:Description: After this many bytes have been read from an RBD image, read-ahead is disabled for that image until it is closed.  This allows the guest OS to take over read-ahead once it is booted.  If zero, read-ahead stays enabled.
:Type: 64-bit Integer
:Required: No
:Default: ``50 MiB``


RBD Features
============

RBD supports advanced features which can be specified via the command line when creating images or the default features can be specified via Ceph config file via 'rbd_default_features = <sum of feature numeric values>' or 'rbd_default_features = <comma-delimited list of CLI values>'

``Layering``

:Description: Layering enables you to use cloning.
:Internal value: 1
:CLI value: layering
:Added in: v0.70 (Emperor)
:KRBD support: since v3.10
:Default: yes

``Striping v2``

:Description: Striping spreads data across multiple objects. Striping helps with parallelism for sequential read/write workloads.
:Internal value: 2
:CLI value: striping
:Added in: v0.70 (Emperor)
:KRBD support: since v3.10
:Default: yes

``Exclusive locking``

:Description: When enabled, it requires a client to get a lock on an object before making a write. Exclusive lock should only be enabled when a single client is accessing an image at the same time. 
:Internal value: 4
:CLI value: exclusive-lock
:Added in: v0.92 (Hammer)
:KRBD support: since v4.9
:Default: yes

``Object map``

:Description: Object map support depends on exclusive lock support. Block devices are thin provisioned—meaning, they only store data that actually exists. Object map support helps track which objects actually exist (have data stored on a drive). Enabling object map support speeds up I/O operations for cloning; importing and exporting a sparsely populated image; and deleting.
:Internal value: 8
:CLI value: object-map
:Added in: v0.93 (Hammer)
:KRBD support: no
:Default: yes


``Fast-diff``

:Description: Fast-diff support depends on object map support and exclusive lock support. It adds another property to the object map, which makes it much faster to generate diffs between snapshots of an image, and the actual data usage of a snapshot much faster.
:Internal value: 16
:CLI value: fast-diff
:Added in: v9.0.1 (Infernalis)
:KRBD support: no
:Default: yes


``Deep-flatten``

:Description: Deep-flatten makes rbd flatten work on all the snapshots of an image, in addition to the image itself. Without it, snapshots of an image will still rely on the parent, so the parent will not be delete-able until the snapshots are deleted. Deep-flatten makes a parent independent of its clones, even if they have snapshots.
:Internal value: 32
:CLI value: deep-flatten
:Added in: v9.0.2 (Infernalis)
:KRBD support: no
:Default: yes


``Journaling``

:Description: Journaling support depends on exclusive lock support. Journaling records all modifications to an image in the order they occur. RBD mirroring utilizes the journal to replicate a crash consistent image to a remote cluster.
:Internal value: 64
:CLI value: journaling
:Added in: v10.0.1 (Jewel)
:KRBD support: no
:Default: no


``Data pool``

:Description: On erasure-coded pools, the image data block objects need to be stored on a separate pool from the image metadata.
:Internal value: 128
:Added in: v11.1.0 (Kraken)
:KRBD support: since v4.11
:Default: no


``Operations``

:Description: Used to restrict older clients from performing certain maintenance operations against an image (e.g. clone, snap create).
:Internal value: 256
:Added in: v13.0.2 (Mimic)
:KRBD support: since v4.16


``Migrating``

:Description: Used to restrict older clients from opening an image when it is in migration state.
:Internal value: 512
:Added in: v14.0.1 (Nautilus)
:KRBD support: no


RBD QOS Settings
================

RBD supports limiting per image IO, controlled by the following
settings.

``rbd qos iops limit``

:Description: The desired limit of IO operations per second.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos bps limit``

:Description: The desired limit of IO bytes per second.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos read iops limit``

:Description: The desired limit of read operations per second.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos write iops limit``

:Description: The desired limit of write operations per second.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos read bps limit``

:Description: The desired limit of read bytes per second.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos write bps limit``

:Description: The desired limit of write bytes per second.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos iops burst``

:Description: The desired burst limit of IO operations.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos bps burst``

:Description: The desired burst limit of IO bytes.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos read iops burst``

:Description: The desired burst limit of read operations.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos write iops burst``

:Description: The desired burst limit of write operations.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos read bps burst``

:Description: The desired burst limit of read bytes.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos write bps burst``

:Description: The desired burst limit of write bytes.
:Type: Unsigned Integer
:Required: No
:Default: ``0``


``rbd qos schedule tick min``

:Description: The minimum schedule tick (in milliseconds) for QoS.
:Type: Unsigned Integer
:Required: No
:Default: ``50``
