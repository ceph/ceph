===========================
 RBD Cache Config Settings
===========================

RBD caching behaves just like well-behaved hard disk caching.  When the OS sends
a barrier or a flush request, all dirty data is written to the OSDs. This means
that using write-back caching is just as safe as using a well-behaved physical
hard disk with a VMS that properly sends flushes (i.e. Linux kernel >= 2.6.32).

.. versionadded:: 0.46

Ceph supports write-back caching for RBD. To enable it, add  ``rbd cache =
true`` to the ``[global]`` section of your ``ceph.conf`` file. By default
``librbd`` does not perform any caching. Writes and reads go directly to the
storage cluster, and writes return only when the data is on disk on all
replicas. With caching enabled, writes return immediately, unless there are more
than ``rbd cache max dirty`` unflushed bytes. In this case, the write triggers
writeback and blocks until enough bytes are flushed.

.. versionadded:: 0.47

Ceph supports write-through caching for RBD. You can set the size of the 
cache, and you can set targets and limits to switch from write-back
caching to write through caching. To enable write-through mode, set ``rbd cache max dirty`` to 0. This means
writes return only when the data is on disk on all replicas, but reads
may come from the cache. The cache is in memory on the client, and each RBD image has its own.
Since the cache is local to the client, there's no coherency if there are
others accesing the image. Running GFS or OCFS will not work with caching 
enabled.

The ``ceph.conf`` file settings for RBD should be set in the ``[global]``
section of your configuration file. The settings include: 


``rbd cache``

:Description: Enable caching for RADOS Block Device (RBD).
:Type: Boolean
:Required: No
:Default: ``false``


``rbd cache size``

:Description: The RBD cache size in bytes.
:Type: 64-bit Integer
:Required: No
:Default: ``32 MiB``


``rbd cache max dirty``

:Description: The ``dirty`` limit in bytes at which the cache triggers write-back.  If ``0``, uses write-through caching.
:Type: 64-bit Integer
:Required: No
:Default: ``24 MiB``


``rbd cache target dirty``

:Description: The ``dirty target`` before the cache begins writing data to the data storage. Does not block writes to the cache.
:Type: 64-bit Integer
:Required: No
:Default: ``16 MiB``


``rbd cache max dirty age``

:Description: The number of seconds in the cache before writeback starts. 
:Type: Float
:Required: No
:Default: ``1.0``

