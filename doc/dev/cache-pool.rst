Cache pool
==========

Purpose
-------

Use a pool of fast storage devices (probably SSDs) and use it as a
cache for an existing larger pool.

We should be able to create and add a cache pool to an existing pool
of data, and later remove it, without disrupting service or migrating
data around.

Use cases
---------

Read-write pool, writeback
~~~~~~~~~~~~~~~~~~~~~~~~~~

We have an existing data pool and put a fast cache pool "in front" of it.  Writes will
go to the cache pool and immediately ack.  We flush them back to the data pool based on
some policy.

Read-only pool, weak consistency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We have an existing data pool and add one or more read-only cache
pools.  We copy data to the cache pool(s) on read.  Writes are
forwarded to the original data pool.  Stale data is expired from the
cache pools based on some as-yet undetermined policy.

This is likely only useful for specific applications with specific
data access patterns.  It may be a match for rgw, for example.


Interface
---------

Set up a read/write cache pool foo-hot for pool foo::

 ceph osd tier add foo foo-hot
 ceph osd tier cache-mode foo-hot writeback
 ceph osd tier cache-target-size foo-hot 10G
 ceph osd tier cache-target-dirty foo-hot 1G

Direct all traffic for foo to foo-hot::

 ceph osd tier set-overlay foo foo-hot

Drain the cache in preparation for turning it off::

 ceph osd tier cache-mode foo-hot invalidate+forward
 ceph osd tier cache-target-size foo-hot 0   # do not cache any new items

When cache pool is finally empty, disable it::

 ceph osd tier remove-overlay foo
 ceph osd tier remove foo foo-hot

Read-only pools with lazy consistency::

 ceph osd tier add foo foo-east
 ceph osd tier cache-mode foo-east readonly
 ceph osd tier add foo foo-west
 ceph osd tier cache-mode foo-west readonly

Set up a cold storage tier::

 ceph osd tier add foo foo-cold

