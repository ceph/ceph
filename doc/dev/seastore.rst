==========
 SeaStore
==========

Goals and Basics
================

* Target NVMe devices.  Not primarily concerned with pmem or HDD.
* make use of SPDK for user-space driven IO
* Use Seastar futures programming model to facilitate
  run-to-completion and a sharded memory/processing model
* Allow zero- (or minimal) data copying on read and write paths when
  combined with a seastar-based messenger using DPDK

Motivation and background
-------------------------

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
------------------

One or more cores/shards will be reading and writing to the device at
once.  Each shard will have its own independent data it is operating
on and stream to its own open segments.  Devices that support streams
can be hinted accordingly so that data from different shards is not
mixed on the underlying media.

Persistent Memory
-----------------

As the intial sequential design above matures, we'll introduce
persistent memory support for metadata and caching structures.

Design
======

The design is based heavily on both f2fs and btrfs.  Each reactor
manages its own root.  Prior to reusing a segment, we rewrite any live
blocks to an open segment.

Because we are only writing sequentially to open segments, we must
“clean” one byte of an existing segment for every byte written at
steady state.  Generally, we’ll need to reserve some portion of the
usable capacity in order to ensure that write amplification remains
acceptably low (20% for 2x? -- TODO: find prior work).  As a design
choice, we want to avoid a background gc scheme as it tends to
complicate estimating operation cost and tends to introduce
non-deterministic latency behavior.  Thus, we want a set of structures
that permits us to relocate blocks from existing segments inline with
ongoing client IO.

To that end, at a high level, we’ll maintain 2 basic metadata trees.
First, we need a tree mapping ghobject_t->onode_t (onode_by_hobject).
Second, we need a way to find live blocks within a segment and a way
to decouple internal references from physical locations (lba_tree).

Each onode contains xattrs directly as well as the top of the omap and
extent trees (optimization: we ought to be able to fit small enough
objects into the onode).

Segment Layout
--------------

The backing storage is abstracted into a set of segments.  Each
segment can be in one of 3 states: empty, open, closed.  The byte
contents of a segment are a sequence of records.  A record is prefixed
by a header (including length and checksums) and contains a sequence
of deltas and/or blocks.  Each delta describes a logical mutation for
some block.  Each included block is an aligned extent addressable by
<segment_id_t, segment_offset_t>.  A transaction can be implemented by
constructing a record combining deltas and updated blocks and writing
it to an open segment.

Note that segments will generally be large (something like >=256MB),
so there will not typically be very many of them.

record: [ header | delta | delta... | block | block ... ]
segment: [ record ... ]

See src/crimson/os/seastore/journal.h for Journal implementation
See src/crimson/os/seastore/seastore_types.h for most seastore structures.

Each shard will keep open N segments for writes

- HDD: N is probably 1 on one shard
- NVME/SSD: N is probably 2/shard, one for "journal" and one for
  finished data records as their lifetimes are different.

I think the exact number to keep open and how to partition writes
among them will be a tuning question -- gc/layout should be flexible.
Where practical, the goal is probably to partition blocks by expected
lifetime so that a segment either has long lived or short lived
blocks.

The backing physical layer is exposed via a segment based interface.
See src/crimson/os/seastore/segment_manager.h

Journal and Atomicity
---------------------

One open segment is designated to be the journal.  A transaction is
represented by an atomically written record.  A record will contain
blocks written as part of the transaction as well as deltas which
are logical mutations to existing physical extents.  Transaction deltas
are always written to the journal.  If the transaction is associated
with blocks written to other segments, final record with the deltas
should be written only once the other blocks are persisted.  Crash
recovery is done by finding the segment containing the beginning of
the current journal, loading the root node, replaying the deltas, and
loading blocks into the cache as needed.

See src/crimson/os/seastore/journal.h

Block Cache
-----------

Every block is in one of two states:

- clean: may be in cache or not, reads may cause cache residence or
  not
- dirty: the current version of the record requires overlaying deltas
  from the journal.  Must be fully present in the cache.

Periodically, we need to trim the journal (else, we’d have to replay
journal deltas from the beginning of time).  To do this, we need to
create a checkpoint by rewriting the root blocks and all currently
dirty blocks.  Note, we can do journal checkpoints relatively
infrequently, and they needn’t block the write stream.

Note, deltas may not be byte range modifications.  Consider a btree
node structured with keys to the left and values to the right (common
trick for improving point query/key scan performance).  Inserting a
key/value into that node at the min would involve moving a bunch of
bytes, which would be expensive (or verbose) to express purely as a
sequence of byte operations.  As such, each delta indicates the type
as well as the location of the corresponding extent.  Each block
type can therefore implement CachedExtent::apply_delta as appopriate.

See src/os/crimson/seastore/cached_extent.h.
See src/os/crimson/seastore/cache.h.

GC
---

Prior to reusing a segment, we must relocate all live blocks.  Because
we only write sequentially to empty segments, for every byte we write
to currently open segments, we need to clean a byte of an existing
closed segment.  As a design choice, we’d like to avoid background
work as it complicates estimating operation cost and has a tendency to
create non-deterministic latency spikes.  Thus, under normal operation
each seastore reactor will be inserting enough work to clean a segment
at the same rate as incoming operations.

In order to make this cheap for sparse segments, we need a way to
positively identify dead blocks.  Thus, for every block written, an
entry will be added to the lba tree with a pointer to the previous lba
in the segment.  Any transaction that moves a block or modifies the
reference set of an existing one will include deltas/blocks required
to update the lba tree to update or remove the previous block
allocation.  The gc state thus simply needs to maintain an iterator
(of a sort) into the lba tree segment linked list for segment
currently being cleaned and a pointer to the next record to be
examined -- records not present in the allocation tree may still
contain roots (like allocation tree blocks) and so the record metadata
must be checked for a flag indicating root blocks.

For each transaction, we evaluate a heuristic function of the
currently available space and currently live space in order to
determine whether we need to do cleaning work (could be simply a range
of live/used space ratios).

TODO: there is not yet a GC implementation

Logical Layout
==============

Using the above block and delta semantics, we build two root level trees:
- onode tree: maps hobject_t to onode_t
- lba_tree: maps lba_t to lba_range_t

Each of the above structures is comprised of blocks with mutations
encoded in deltas.  Each node of the above trees maps onto a block.
Each block is either physically addressed (root blocks and the
lba_tree nodes) or is logically addressed (everything else).
Physically addressed blocks are located by a paddr_t: <segment_id_t,
segment_off_t> tuple and are marked as physically addressed in the
record.  Logical blocks are addressed by laddr_t and require a lookup in
the lba_tree to address.

Because the cache/transaction machinery lives below the level of the
lba tree, we can represent atomic mutations of the lba tree and other
structures by simply including both in a transaction.

LBAManager/BtreeLBAManager
--------------------------

Implementations of the LBAManager interface are responsible for managing
the logical->physical mapping -- see crimson/os/seastore/lba_manager.h.

The BtreeLBAManager implements this interface directly on top of
Journal and SegmentManager using a wandering btree approach.

Because SegmentManager does not let us predict the location of a
committed record (a property of both SMR and Zone devices), references
to blocks created within the same transaction will necessarily be
*relative* addresses.  The BtreeLBAManager maintains an invariant by
which the in-memory copy of any block will contain only absolute
addresses when !is_pending() -- on_commit and complete_load fill in
absolute addresses based on the actual block addr and on_delta_write
does so based on the just committed record.  When is_pending(), if
is_initial_pending references in memory are block_relative (because
they will be written to the original block location) and
record_relative otherwise (value will be written to delta).

TransactionManager
------------------

The TransactionManager is responsible for presenting a unified
interface on top of the Journal, SegmentManager, Cache, and
LBAManager.  Users can allocate and mutate extents based on logical
addresses with segment cleaning handled in the background.

See crimson/os/seastore/transaction_manager.h

Next Steps
==========

Journal
-------

- Support for scanning a segment to find physically addressed blocks
- Add support for trimming the journal and releasing segments.

Cache
-----

- Support for replaying block types other than root
- Support for rewriting dirty blocks

  - Need to add support to CachedExtent for finding/updating
    dependent blocks
  - Need to add support for adding dirty block writout to
    try_construct_record

LBAManager
----------

- Add support for pinning
- Fill in replay and misc todos
- Add segment -> laddr for use in GC
- Support for locating remaining used blocks in segments

GC
---

- Initial implementation
- Support in BtreeLBAManager for tracking used blocks in segments
- Heuristic for identifying segments to clean

Other
------

- Add support for periodically generating a journal checkpoint.
- Onode tree
- Extent tree
- Remaining ObjectStore integration

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



