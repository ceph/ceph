================================
 RBD Persistent Write-back Cache
================================

.. index:: Ceph Block Device; Persistent Write-back Cache

Persistent Write-back Cache
===========================

The persistent write-back cache provides a persistent, fault-tolerant write-back
cache for librbd-based RBD clients.

This cache uses a log-ordered write-back design which maintains checkpoints
internally so that writes that get flushed back to the cluster are always
crash consistent. Even if the client cache is lost entirely, the disk image is
still consistent but the data will appear to be stale.

This cache can be used with PMEM or SSD as a cache device.

Usage
=====

The persistent write-back cache manages the cache data in a persistent device.
It looks for and creates cache files in a configured directory, and then caches
data in the file.

The persistent write-back cache can't be enabled without the exclusive-lock
feature. It tries to enable the write-back cache only when the exclusive lock
is acquired.

The cache provides two different persistence modes. In persistent-on-write mode,
the writes are completed only when they are persisted to the cache device and
will be readable after a crash. In persistent-on-flush mode, the writes are
completed as soon as it no longer needs the caller's data buffer to complete
the writes, but does not guarantee that writes will be readable after a crash.
The data is persisted to the cache device when a flush request is received.

Initially it defaults to the persistent-on-write mode and it switches to
persistent-on-flush mode after the first flush request is received.

Enable Persistent Write-back Cache
========================================

To enable the persistent write-back cache, the following Ceph settings
need to be enabled.::

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

The above configurations can be set per-host, per-pool, per-image etc. Eg, to
set per-host, add the overrides to the appropriate `section`_ in the host's
``ceph.conf`` file. To set per-pool, per-image, etc, please refer to the
``rbd config`` `commands`_.

Cache Status
------------

The persistent write-back cache is enabled when the exclusive lock is acquired,
and it is closed when the exclusive lock is released. To check the cache status,
users may use the command ``rbd status``.  ::

        rbd status {pool-name}/{image-name}

The status of the cache is shown, including present, clean, cache size and the
location as well as some basic metrics.

For example::

        $ rbd status rbd/foo
        Watchers:
                watcher=10.10.0.102:0/1061883624 client.25496 cookie=140338056493088
        Persistent cache state:
                host: sceph9
                path: /mnt/nvme0/rbd-pwl.rbd.101e5824ad9a.pool
                size: 1 GiB
                mode: ssd
                stats_timestamp: Sun Apr 10 13:26:32 2022
                present: true   empty: false    clean: false
                allocated: 509 MiB
                cached: 501 MiB
                dirty: 338 MiB
                free: 515 MiB
                hits_full: 1450 / 61%
                hits_partial: 0 / 0%
                misses: 924
                hit_bytes: 192 MiB / 66%
                miss_bytes: 97 MiB

Invalidate Cache
----------------

To invalidate (discard) a cache file with ``rbd``, specify the
``persistent-cache invalidate`` command, the pool name and the image name.  ::

        rbd persistent-cache invalidate {pool-name}/{image-name}

The command removes the cache metadata of the corresponding image, disables
the cache feature and deletes the local cache file if it exists.

For example::

        $ rbd persistent-cache invalidate rbd/foo

.. _section: ../../rados/configuration/ceph-conf/#configuration-sections
.. _commands: ../../man/8/rbd#commands
.. _DAX: https://www.kernel.org/doc/Documentation/filesystems/dax.txt
