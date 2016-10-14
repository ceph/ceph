===================
PG Backend Proposal
===================

NOTE: the last update of this page is dated 2013, before the Firefly
release. The details of the implementation may be different.

Motivation
----------

The purpose of the `PG Backend interface
<https://github.com/ceph/ceph/blob/firefly/src/osd/PGBackend.h>`_
is to abstract over the differences between replication and erasure
coding as failure recovery mechanisms.

Much of the existing PG logic, particularly that for dealing with
peering, will be common to each.  With both schemes, a log of recent
operations will be used to direct recovery in the event that an OSD is
down or disconnected for a brief period of time.  Similarly, in both
cases it will be necessary to scan a recovered copy of the PG in order
to recover an empty OSD.  The PGBackend abstraction must be
sufficiently expressive for Replicated and ErasureCoded backends to be
treated uniformly in these areas.

However, there are also crucial differences between using replication
and erasure coding which PGBackend must abstract over:

1. The current write strategy would not ensure that a particular
   object could be reconstructed after a failure.
2. Reads on an erasure coded PG require chunks to be read from the
   replicas as well.
3. Object recovery probably involves recovering the primary and
   replica missing copies at the same time to avoid performing extra
   reads of replica shards.
4. Erasure coded PG chunks created for different acting set
   positions are not interchangeable.  In particular, it might make
   sense for a single OSD to hold more than 1 PG copy for different
   acting set positions.
5. Selection of a pgtemp for backfill may differ between replicated
   and erasure coded backends.
6. The set of necessary OSDs from a particular interval required to
   continue peering may differ between replicated and erasure coded
   backends.
7. The selection of the authoritative log may differ between replicated
   and erasure coded backends.

Client Writes
-------------

The current PG implementation performs a write by performing the write
locally while concurrently directing replicas to perform the same
operation.  Once all operations are durable, the operation is
considered durable.  Because these writes may be destructive
overwrites, during peering, a log entry on a replica (or the primary)
may be found to be divergent if that replica remembers a log event
which the authoritative log does not contain.  This can happen if only
1 out of 3 replicas persisted an operation, but was not available in
the next interval to provide an authoritative log.  With replication,
we can repair the divergent object as long as at least 1 replica has a
current copy of the divergent object.  With erasure coding, however,
it might be the case that neither the new version of the object nor
the old version of the object has enough available chunks to be
reconstructed.  This problem is much simpler if we arrange for all
supported operations to be locally roll-back-able.

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

Core Changes:

- Current code should be adapted to use and rollback as appropriate
  APPEND, DELETE, (SET|RM)ATTR log entries.
- The filestore needs to be able to deal with multiply versioned
  hobjects.  This means adapting the filestore internally to
  use a `ghobject <https://github.com/ceph/ceph/blob/firefly/src/common/hobject.h#L238>`_ 
  which is basically a tuple<hobject_t, gen_t,
  shard_t>.  The gen_t + shard_t need to be included in the on-disk
  filename.  gen_t is a unique object identifier to make sure there
  are no name collisions when object N is created +
  deleted + created again. An interface needs to be added to get all
  versions of a particular hobject_t or the most recently versioned
  instance of a particular hobject_t.

PGBackend Interfaces:

- PGBackend::perform_write() : It seems simplest to pass the actual
  ops vector.  The reason for providing an async, callback based
  interface rather than having the PGBackend respond directly is that
  we might want to use this interface for internal operations like
  watch/notify expiration or snap trimming which might not necessarily
  have an external client.
- PGBackend::try_rollback() : Some log entries (all of the ones valid
  for the Erasure coded backend) will support local rollback.  In
  those cases, PGLog can avoid adding objects to the missing set when
  identifying divergent objects.

Peering and PG Logs
-------------------

Currently, we select the log with the newest last_update and the
longest tail to be the authoritative log.  This is fine because we
aren't generally able to roll operations on the other replicas forward
or backwards, instead relying on our ability to re-replicate divergent
objects.  With the write approach discussed in the previous section,
however, the erasure coded backend will rely on being able to roll
back divergent operations since we may not be able to re-replicate
divergent objects.  Thus, we must choose the *oldest* last_update from
the last interval which went active in order to minimize the number of
divergent objects.

The difficulty is that the current code assumes that as long as it has
an info from at least 1 OSD from the prior interval, it can complete
peering.  In order to ensure that we do not end up with an
unrecoverably divergent object, a K+M erasure coded PG must hear from at
least K of the replicas of the last interval to serve writes.  This ensures
that we will select a last_update old enough to roll back at least K
replicas.  If a replica with an older last_update comes along later,
we will be able to provide at least K chunks of any divergent object.

Core Changes:

- PG::choose_acting(), etc. need to be generalized to use PGBackend to
  determine the authoritative log.
- PG::RecoveryState::GetInfo needs to use PGBackend to determine
  whether it has enough infos to continue with authoritative log
  selection.

PGBackend interfaces:

- have_enough_infos() 
- choose_acting()

PGTemp
------

Currently, an OSD is able to request a temp acting set mapping in
order to allow an up-to-date OSD to serve requests while a new primary
is backfilled (and for other reasons).  An erasure coded pg needs to
be able to designate a primary for these reasons without putting it
in the first position of the acting set.  It also needs to be able
to leave holes in the requested acting set.

Core Changes:

- OSDMap::pg_to_*_osds needs to separately return a primary.  For most
  cases, this can continue to be acting[0].
- MOSDPGTemp (and related OSD structures) needs to be able to specify
  a primary as well as an acting set.
- Much of the existing code base assumes that acting[0] is the primary
  and that all elements of acting are valid.  This needs to be cleaned
  up since the acting set may contain holes.

Client Reads
------------

Reads with the replicated strategy can always be satisfied
synchronously out of the primary OSD.  With an erasure coded strategy,
the primary will need to request data from some number of replicas in
order to satisfy a read.  The perform_read() interface for PGBackend
therefore will be async.

PGBackend interfaces:

- perform_read(): as with perform_write() it seems simplest to pass
  the ops vector.  The call to oncomplete will occur once the out_bls
  have been appropriately filled in.

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
in chunk 4 have different names.  To that end, the filestore must
include the chunk id in the object key.

Core changes:

- The filestore `ghobject_t needs to also include a chunk id
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

We probably won't support object classes at first on Erasure coded
backends.

Scrub
-----

We currently have two scrub modes with different default frequencies:

1. [shallow] scrub: compares the set of objects and metadata, but not
   the contents
2. deep scrub: compares the set of objects, metadata, and a crc32 of
   the object contents (including omap)

The primary requests a scrubmap from each replica for a particular
range of objects.  The replica fills out this scrubmap for the range
of objects including, if the scrub is deep, a crc32 of the contents of
each object.  The primary gathers these scrubmaps from each replica
and performs a comparison identifying inconsistent objects.

Most of this can work essentially unchanged with erasure coded PG with
the caveat that the PGBackend implementation must be in charge of
actually doing the scan, and that the PGBackend implementation should
be able to attach arbitrary information to allow PGBackend on the
primary to scrub PGBackend specific metadata.

The main catch, however, for erasure coded PG is that sending a crc32
of the stored chunk on a replica isn't particularly helpful since the
chunks on different replicas presumably store different data.  Because
we don't support overwrites except via DELETE, however, we have the
option of maintaining a crc32 on each chunk through each append.
Thus, each replica instead simply computes a crc32 of its own stored
chunk and compares it with the locally stored checksum.  The replica
then reports to the primary whether the checksums match.

PGBackend interfaces:

- scan()
- scrub()
- compare_scrub_maps()

Crush
-----

If crush is unable to generate a replacement for a down member of an
acting set, the acting set should have a hole at that position rather
than shifting the other elements of the acting set out of position.

Core changes:

- Ensure that crush behaves as above for INDEP.

Recovery
--------

The logic for recovering an object depends on the backend.  With
the current replicated strategy, we first pull the object replica
to the primary and then concurrently push it out to the replicas.
With the erasure coded strategy, we probably want to read the
minimum number of replica chunks required to reconstruct the object
and push out the replacement chunks concurrently.

Another difference is that objects in erasure coded pg may be
unrecoverable without being unfound.  The "unfound" concept
should probably then be renamed to unrecoverable.  Also, the
PGBackend implementation will have to be able to direct the search
for pg replicas with unrecoverable object chunks and to be able
to determine whether a particular object is recoverable.


Core changes:

- s/unfound/unrecoverable

PGBackend interfaces:

- `on_local_recover_start <https://github.com/ceph/ceph/blob/firefly/src/osd/PGBackend.h#L60>`_
- `on_local_recover <https://github.com/ceph/ceph/blob/firefly/src/osd/PGBackend.h#L66>`_
- `on_global_recover <https://github.com/ceph/ceph/blob/firefly/src/osd/PGBackend.h#L78>`_
- `on_peer_recover <https://github.com/ceph/ceph/blob/firefly/src/osd/PGBackend.h#L83>`_
- `begin_peer_recover <https://github.com/ceph/ceph/blob/firefly/src/osd/PGBackend.h#L90>`_

Backfill
--------

For the most part, backfill itself should behave similarly between
replicated and erasure coded pools with a few exceptions:

1. We probably want to be able to backfill multiple OSDs concurrently
   with an erasure coded pool in order to cut down on the read
   overhead.
2. We probably want to avoid having to place the backfill peers in the
   acting set for an erasure coded pg because we might have a good
   temporary pg chunk for that acting set slot.

For 2, we don't really need to place the backfill peer in the acting
set for replicated PGs anyway.
For 1, PGBackend::choose_backfill() should determine which OSDs are
backfilled in a particular interval.

Core changes:

- Backfill should be capable of handling multiple backfill peers
  concurrently even for
  replicated pgs (easier to test for now)
- Backfill peers should not be placed in the acting set.

PGBackend interfaces:

- choose_backfill(): allows the implementation to determine which OSDs
  should be backfilled in a particular interval.
