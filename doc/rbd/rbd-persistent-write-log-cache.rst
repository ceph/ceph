================================
 RBD Persistent Write Log Cache
================================

.. index:: Ceph Block Device; Persistent Write Log Cache

Persistent Write Log Cache
===========================

The Persistent Write Log Cache (PWL) provides a persistent, fault-tolerant
write-back cache for librbd-based RBD clients.

This cache uses a log-ordered write-back design which maintains checkpoints
internally so that writes that get flushed back to the cluster are always
crash consistent. Even if the client cache is lost entirely, the disk image is
still consistent but the data will appear to be stale.

This cache can be used with PMEM or SSD as a cache device. For PMEM, the cache
mode is called ``replica write log (rwl)``. At present, only local cache is
supported, and the replica function is under development. For SSD, the cache
mode is called ``ssd``.

Usage
=====

The PWL cache manages the cache data in a persistent device. It looks for and
creates cache files in a configured directory, and then caches data in the
file.

The PWL cache depends on the exclusive-lock feature. The cache can be loaded
only after the exclusive lock is acquired.

The cache provides two different persistence modes. In persistent-on-write mode,
the writes are completed only when they are persisted to the cache device and
will be readable after a crash. In persistent-on-flush mode, the writes are
completed as soon as it no longer needs the caller's data buffer to complete
the writes, but does not guarantee that writes will be readable after a crash.
The data is persisted to the cache device when a flush request is received.

Initially it defaults to the persistent-on-write mode and it switches to
persistent-on-flush mode after the first flush request is received.

Enable Cache
========================================

To enable the PWL cache, set the following configuration settings::

        rbd persistent cache mode = {cache-mode}
        rbd plugins = pwl_cache

Value of {cache-mode} can be ``rwl``, ``ssd`` or ``disabled``. By default the
cache is disabled.

Here are some cache configuration settings:

- ``rbd_persistent_cache_path`` A file folder to cache data. This folder must
  have DAX enabled (see `DAX`_) when using ``rwl`` mode to avoid performance
  degradation.

- ``rbd_persistent_cache_size`` The cache size per image. The minimum cache
  size is 1 GB.

- ``rbd_persistent_cache_log_periodic_stats`` This is a debug option. It is
  used to emit periodic perf stats to the debug log if ``debug rbd pwl`` is
  set to ``1`` or higher.

The above configurations can be set per-host, per-pool, per-image etc. Eg, to
set per-host, add the overrides to the appropriate `section`_ in the host's
``ceph.conf`` file. To set per-pool, per-image, etc, please refer to the
``rbd config`` `commands`_.

Cache Status
------------

The PWL cache is enabled when the exclusive lock is acquired,
and it is closed when the exclusive lock is released. To check the cache status,
users may use the command ``rbd status``.  ::

        rbd status {pool-name}/{image-name}

The status of the cache is shown, including present, clean, cache size and the
location. Currently the status is updated only at the time the cache is opened
and closed and therefore may appear to be out of date (e.g. show that the cache
is clean when it is actually dirty).

For example::

        $ rbd status rbd/foo
        Watchers: none
        Image cache state: {"present":"true","empty":"false","clean":"true","cache_type":"ssd","pwl_host":"sceph9","pwl_path":"/tmp/rbd-pwl.rbd.abcdef123456.pool","pwl_size":1073741824}

Discard Cache
-------------

To discard a cache file with ``rbd``, specify the ``image-cache invalidate``
option, the pool name and the image name.  ::

        rbd image-cache invalidate {pool-name}/{image-name}

The command removes the cache metadata of the corresponding image, disable
the cache feature and deletes the local cache file if it exists.

For example::

        $ rbd image-cache invalidate rbd/foo

.. _section: ../../rados/configuration/ceph-conf/#configuration-sections
.. _commands: ../../man/8/rbd#commands
.. _DAX: https://www.kernel.org/doc/Documentation/filesystems/dax.txt
