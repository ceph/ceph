==========
 SeaStore
==========

This is a rough design doc for a new ObjectStore implementation design
to facilitate higher performance on solid state devices.

Name
====

SeaStore maximizes the opportunity for confusion (seastar? seashore?)
and associated fun.  Alternative suggestions welcome.


Goals
=====

* Target NVMe devices.  Not primarily concerned with pmem or HDD.
* make use of SPDK for user-space driven IO
* Use Seastar futures programming model to facilitate run-to-completion and a sharded memory/processing model
* Allow zero- (or minimal) data copying on read and write paths when combined with a seastar-based messenger using DPDK


Motivation and background
=========================

All flash devices are internally structured in terms of segments that
can be written efficiently but must be erased in their entirety.  The
NVMe device generally has limited knowledge about what data in a
segment is still "live" (hasn't been logically discarded), making the
inevitable garbage collection within the device inefficient.  We can
design an on-disk layout that is friendly to GC at lower layers and
drive garbage collection at higher layers.

In principle a fine-grained discard could communicate our intent to
the device, but in practice discard is poorly implemented in the
device and intervening software layers.

  
Basics
======

The basic idea is that all data will be stream out sequentially to
large segments on the device.  In the SSD hardware, segments are
likely to be on the order of 100's of MB to tens of GB.

SeaStore's logical segments would ideally be perfectly aligned with
the hardware segments.  In practice, it may be challenging to
determine geometry and to sufficiently hint to the device that LBAs
being written should be aligned to the underlying hardware.  In the
worst case, we can structure our logical segments to correspond to
e.g. 5x the physical segment size so that we have about ~20% of our
data misaligned.

When we reach some utilization threshold, we mix cleaning work in with
the ongoing write workload in order to evacuate live data from
previously written segments.  Once they are completely free we can
discard the entire segment so that it can be erased and reclaimed by
the device.

The key is to mix a small bit of cleaning work with every write
transaction to avoid spikes and variance in write latency.


  
Data layout basics
==================

One or more cores/shards will be reading and writing to the device at
once.  Each shard will have its own independent data it is operating
on and stream to its own open segments.  Devices that support streams
can be hinted accordingly so that data from different shards is not
mixed on the underlying media.

Global state
------------

There will be a simple global table of segments and their usage/empty
status.  Each shard will occasionally claim new empty segments for
writing as needed, or return cleaned segments to the global free list.

At a high level, all metadata will be structured as a b-tree.  The
root for the metadata btree will also be stored centrally (along with
the segment allocation table).

This is hand-wavey, but it is probably sufficient to update the root
pointer for the btree either as each segment is sealed or as a new
segment is opened.


Writing segments
----------------

Each segment will be written sequentially as a sequence of
transactions.  Each transaction will be on-disk expression of an
ObjectStore::Transaction.  It will consist of

* new data blocks
* some metadata describing changes to b-tree metadata blocks.  This
  will be written compact as a delta: which keys are removed and which
  keys/values are inserted into the b-tree block.

As each b-tree block is modified, we update the block in memory and
put it on a 'dirty' list.  However, again, only the (compact) delta is journaled
to the segment.

As we approach the end of the segment, the goal is to undirty all of
our dirty blocks in memory.  Based on the number of dirty blocks and
the remaining space, we include a proportional number of dirty blocks
in each transaction write so that we undirty some of the b-tree
blocks.  Eventually, the last transaction written to the segment will
include all of the remaining dirty b-tree blocks.

Segment inventory
-----------------

At the end of each segment, an inventory will be written that includes
any metadata needed to test whether blocks in the segment are still
live.  For data blocks, that means an object id (e.g., ino number) and
offset to test whether the block is still reference.  For metadata
blocks, it would be at least one metadata key that lands in any b-tree
block that is modified (via a delta) in the segment--enough for us to
do a forward lookup in the b-tree to check whether the b-tree block is
still referenced.  Once this is written, the segment is sealed and read-only.

Crash recovery
--------------

On any crash, we simply "replay" the currently open segment in memory.
For any b-tree delta encountered, we load the original block, modify
in memory, and mark it dirty.  Once we continue writing, the normal "write
dirty blocks as we near the end of the segment" behavior will pick up where
we left off.



ObjectStore considerations
==========================

Splits, merges, and sharding
----------------------------

One of the current ObjectStore requirements is to be able to split a
collection (PG) in O(1) time.  Starting in mimic, we also need to be
able to merge two collections into one (i.e., exactly the reverse of a
split).

However, the PGs that we split into would hash to different shards of
the OSD in the current sharding scheme.  One can imagine replacing
that sharding scheme with a temporary mapping directing the smaller
child PG to the right shard since we generally then migrate that PG to
another OSD anyway, but this wouldn't help us in the merge case where
the constituent pieces may start out on different shards and
ultimately need to be handled in the same collection (and be operated
on via single transactions).

This suggests that we likely need a way for data written via one shard
to "switch ownership" and later be read and managed by a different
shard.



