=====================================
 The Logical Bucket Cache in SeaStore
=====================================

This document introduces the LogicalBucketCache mechinary which is
mainly for using SSDs as non-volatile cache for the relatively slow
HDD devices. Specifically, it serves to:

#. Load frequently accesses data from the cold tier to the Logical
   Bucket Cache.
#. Evict relatively cold data from the Logical Bucket Cache to the
   cold tier based on the heat of both data read and write.

The Logical Bucket Cache is enabled when there is at least one cold
tier.

General Approach
================

#. Cache line: a range of continuous laddr address space, which we
   call logical bucket. For now, that continuous laddr range is the
   scope of a RADOS object.
#. Extents are loaded to the cache device when: 1) they are newly
   written by clients and doesn't trigger the write_through mechinary.
   For now, the write_through mechinary is triggered when the newly
   written data is larger than a specific threshhold; 2) they are on
   the slow cold device and read to or evicted from the in-memory Cache.
#. Extents are evicted from the cache device in units of logical buckets,
   that is, extents of the same logical bucket are evicted from the cache
   device altogether.
#. The process that write extents evicted from Cache down to the cache
   device is called promotion.
#. The process that evict extents from the cache device to the cold device
   is called demotion.
#. Extents that are loaded from the cold tier to the Logical Bucket Cache
   are NOT removed from the cold tier, which means they'd have two paddrs,
   one in the Logical Bucket Cache, the other in the cold tier, and the
   promoted data is stored twice. This is designed to make the demote process
   cheap. We call paddrs corresponding to the cold tier shadow paddrs.
   So lba mappings have two paddrs now, the primary paddr and the shadow
   paddr.

Promotion
=========

Promotion is the process that load the extents in the cold tier to the
Logical Bucket Cache.

Extents that are read from the cold tier are first addded to the in-memory
cache; when evicted from the in-memory cache, they are added to the Promoter.

The Promoter write its extents down to the Logical Bucket Cache when the size
of its extents grows larger than ``seastore_cache_promotion_size``.

Demotion
========

Demotion happens when:

#. The usage of the Logical Bucket Cache reaches ``seastore_multiple_tiers_fast_evict_ratio``.
#. The in-memory data index of the Logical Bucket Cache reaches ``seastore_logical_bucket_capacity``.

Demoting extents that are live in both the Logical Bucket Cache and the
cold tier only involves setting the shadow paddr to the primary paddr and
removing the shadow paddr itself.

Demoting extents that are only live in the Logical Bucket Cache would involve
rewriting the extents to the cold tier.

WriteThrough
============

When the newly written data is larger than ``seastore_write_through_size``,
it would be written directly to the cold tier.

Internal Test Workload
=======================

To test the correctness of the whole Logical Bucket Cache mechinary, we also
implemented an internal stress test workload. When turned on, SeaStore would
aggressively load the extents from the cold tier to the Logical Bucket Cache,
and demote extents back to the cold tier. The newly written data may also be
written directly to the cold tier based on ``seastore_test_workload_write_through_probability``.

The internal test workload is turned on by ``crimson_test_workload``.
