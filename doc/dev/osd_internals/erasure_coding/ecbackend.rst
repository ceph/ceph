=================================
ECBackend Implementation Strategy
=================================

Misc initial design notes
=========================

The initial (and still true for ec pools without the hacky ec
overwrites debug flag enabled) design for ec pools restricted
EC pools to operations which can be easily rolled back:

- CEPH_OSD_OP_APPEND: We can roll back an append locally by
  including the previous object size as part of the PG log event.
- CEPH_OSD_OP_DELETE: The possibility of rolling back a delete
  requires that we retain the deleted object until all replicas have
  persisted the deletion event.  ErasureCoded backend will therefore
  need to store objects with the version at which they were created
  included in the key provided to the filestore.  Old versions of an
  object can be pruned when all replicas have committed up to the log
  event deleting the object.
- CEPH_OSD_OP_(SET|RM)ATTR: If we include the prior value of the attr
  to be set or removed, we can roll back these operations locally.

Log entries contain a structure explaining how to locally undo the
operation represented by the operation
(see osd_types.h:TransactionInfo::LocalRollBack).

PGTemp and Crush
----------------

Primaries are able to request a temp acting set mapping in order to
allow an up-to-date OSD to serve requests while a new primary is
backfilled (and for other reasons).  An erasure coded pg needs to be
able to designate a primary for these reasons without putting it in
the first position of the acting set.  It also needs to be able to
leave holes in the requested acting set.

Core Changes:

- OSDMap::pg_to_*_osds needs to separately return a primary.  For most
  cases, this can continue to be acting[0].
- MOSDPGTemp (and related OSD structures) needs to be able to specify
  a primary as well as an acting set.
- Much of the existing code base assumes that acting[0] is the primary
  and that all elements of acting are valid.  This needs to be cleaned
  up since the acting set may contain holes.

Distinguished acting set positions
----------------------------------

With the replicated strategy, all replicas of a PG are
interchangeable.  With erasure coding, different positions in the
acting set have different pieces of the erasure coding scheme and are
not interchangeable.  Worse, crush might cause chunk 2 to be written
to an OSD which happens already to contain an (old) copy of chunk 4.
This means that the OSD and PG messages need to work in terms of a
type like pair<shard_t, pg_t> in order to distinguish different pg
chunks on a single OSD.

Because the mapping of object name to object in the filestore must
be 1-to-1, we must ensure that the objects in chunk 2 and the objects
in chunk 4 have different names.  To that end, the objectstore must
include the chunk id in the object key.

Core changes:

- The objectstore `ghobject_t needs to also include a chunk id
  <https://github.com/ceph/ceph/blob/firefly/src/common/hobject.h#L241>`_ making it more like
  tuple<hobject_t, gen_t, shard_t>.
- coll_t needs to include a shard_t.
- The OSD pg_map and similar pg mappings need to work in terms of a
  spg_t (essentially
  pair<pg_t, shard_t>).  Similarly, pg->pg messages need to include
  a shard_t
- For client->PG messages, the OSD will need a way to know which PG
  chunk should get the message since the OSD may contain both a
  primary and non-primary chunk for the same pg

Object Classes
--------------

Reads from object classes will return ENOTSUP on ec pools by invoking
a special SYNC read.

Scrub
-----

The main catch, however, for ec pools is that sending a crc32 of the
stored chunk on a replica isn't particularly helpful since the chunks
on different replicas presumably store different data.  Because we
don't support overwrites except via DELETE, however, we have the
option of maintaining a crc32 on each chunk through each append.
Thus, each replica instead simply computes a crc32 of its own stored
chunk and compares it with the locally stored checksum.  The replica
then reports to the primary whether the checksums match.

With overwrites, all scrubs are disabled for now until we work out
what to do (see doc/dev/osd_internals/erasure_coding/proposals.rst).

Crush
-----

If crush is unable to generate a replacement for a down member of an
acting set, the acting set should have a hole at that position rather
than shifting the other elements of the acting set out of position.

=========
ECBackend
=========

MAIN OPERATION OVERVIEW
=======================

A RADOS put operation can span
multiple stripes of a single object. There must be code that
tessellates the application level write into a set of per-stripe write
operations -- some whole-stripes and up to two partial
stripes. Without loss of generality, for the remainder of this
document we will focus exclusively on writing a single stripe (whole
or partial). We will use the symbol "W" to represent the number of
blocks within a stripe that are being written, i.e., W <= K.

There are three data flows for handling a write into an EC stripe. The
choice of which of the three data flows to choose is based on the size
of the write operation and the arithmetic properties of the selected
parity-generation algorithm.

(1) whole stripe is written/overwritten
(2) a read-modify-write operation is performed.

WHOLE STRIPE WRITE
------------------

This is the simple case, and is already performed in the existing code
(for appends, that is). The primary receives all of the data for the
stripe in the RADOS request, computes the appropriate parity blocks
and send the data and parity blocks to their destination shards which
write them. This is essentially the current EC code.

READ-MODIFY-WRITE
-----------------

The primary determines which of the K-W blocks are to be unmodified,
and reads them from the shards. Once all of the data is received it is
combined with the received new data and new parity blocks are
computed. The modified blocks are sent to their respective shards and
written. The RADOS operation is acknowledged.

InPlace vs RollForward
----------------------

An update to a particular object as represented by PGTransaction may
inplace if possible (aligned append, delete, create, etc,
see ECTransaction::requires_inplace), or it may require that it be
executed as a tpc rollforward operation.  Really, both are tpc, but
in the former case, the "rollfoward" phase is really just removing
any rollback objects.

OSD Object Write and Consistency
--------------------------------

Regardless of the algorithm chosen above, writing of the data is a two
phase process: commit and rollforward. The primary sends the log
entries with the operation described (see
osd_types.h:TransactionInfo::(LocalRollForward|LocalRollBack).  
In the RollBack case, the "commit" includes executing the transaction
in place and the "rollfoward" phase simply involves removing any
rollback objects we may have created as part of a delete.  In the
RollForward case, we commit the log entry with the LocalRollForward
data along with the data of the stripes to be updated in a write-aside
object, and the "rollforward" operation takes care of moving the
ranges into place and performing any truncates and xattr updates.

In both cases, once all acting/backfill shards have committed the
initial commit, the write is considered complete (we won't roll
it back since we'll never get a log from this interval which
doesn't contain it).

The rollforward part can be delayed as long as we are able to serve
any reads on the relevant extents without performing an actual read.
(see the next section).  Currently, whenever we send a write, we also
indicate that all previously committed operations should be rolled
forward (see ECBackend::try_reads_to_commit).  If there aren't any
in the pipeline when we arrive at the waiting_rollforward queue,
we start a dummy write to move things along (see the Pipeline section
later on and ECBackend::try_commit_to_rollforwrad).

ExtentCache
-----------

It's pretty important to be able to pipeline writes on the same
object, and we'd like to be able serve reads on a recently
written extent without waiting for the write to rollforward.
For these reasons, there is a cache of extents written by
rollforward operations.  Each extent remains pinned until the
operations referring to it are fully rolled forward.

See ExtentCache.h for a detailed explanation of how the cache
states correspond to the higher level invariants about the conditions
under which cuncurrent operations can refer to the same object.

Pipeline
--------

Reading src/osd/ExtentCache.h should have given a good idea of how
operations might overlap.  There are several states involved in
processing a write operation and an important invariant which
isn't enforced by ReplicatedPG at a higher level which need to be
managed by ECBackend.  The important invariant is that we can't
have inplace and rollforward operations running at the same time
on the same object.  For simplicity, we simply enforce that any
operation which contains an inplace operation must wait until
all in-progress rollforward operations complete and visa versa.
There are a few reasons:

  1. In place operations won't in general commute with rolling
     forward a rollforward operation.
  2. Pipelining a rollforward operation after an inplace operation
     would require that the inplace operation be represented in the
     cache which is really annoying for some operations (clone,
     rename) and inefficient.

There are improvements to be made here in the future.

For more details, see ECBackend::waiting_* and
ECBackend::try_<from>_to_<to>.

