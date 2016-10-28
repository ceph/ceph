=================================
Proposed Next Steps for ECBackend
=================================

PARITY-DELTA-WRITE
------------------

RMW operations current require 4 network hops (2 round trips).  In
principle, for some codes, we can reduce this to 3 by sending the
update to the replicas holding the data blocks and having them
compute a delta to forward onto the parity blocks.

The primary reads the current values of the "W" blocks and then uses
the new values of the "W" blocks to compute parity-deltas for each of
the parity blocks.  The W blocks and the parity delta-blocks are sent
to their respective shards.

The choice of whether to use a read-modify-write or a
parity-delta-write is complex policy issue that is TBD in the details
and is likely to be heavily dependant on the computational costs
associated with a parity-delta vs. a regular parity-generation
operation. However, it is believed that the parity-delta scheme is
likely to be the preferred choice, when available.

The internal interface to the erasure coding library plug-ins needs to
be extended to support the ability to query if parity-delta
computation is possible for a selected algorithm as well as an
interface to the actual parity-delta computation algorithm when
available.

Stripe Cache
------------

It may be a good idea to extend the current ExtentCache usage to
cache some data past when the pinning operation releases it.
One application pattern that is important to optimize is the small
block sequential write operation (think of the journal of a journaling
file system or a database transaction log). Regardless of the chosen
redundancy algorithm, it is advantageous for the primary to
retain/buffer recently read/written portions of a stripe in order to
reduce network traffic. The dynamic contents of this cache may be used
in the determination of whether a read-modify-write or a
parity-delta-write is performed. The sizing of this cache is TBD, but
we should plan on allowing at least a few full stripes per active
client. Limiting the cache occupancy on a per-client basis will reduce
the noisy neighbor problem.

Recovery and Rollback Details
=============================

Implementing a Rollback-able Prepare Operation
----------------------------------------------

The prepare operation is implemented at each OSD through a simulation
of a versioning or copy-on-write capability for modifying a portion of
an object.

When a prepare operation is performed, the new data is written into a
temporary object. The PG log for the
operation will contain a reference to the temporary object so that it
can be located for recovery purposes as well as a record of all of the
shards which are involved in the operation. 

In order to avoid fragmentation (and hence, future read performance),
creation of the temporary object needs special attention. The name of
the temporary object affects its location within the KV store. Right
now its unclear whether it's desirable for the name to locate near the
base object or whether a separate subset of keyspace should be used
for temporary objects. Sam believes that colocation with the base
object is preferred (he suggests using the generation counter of the
ghobject for temporaries).  Whereas Allen believes that using a
separate subset of keyspace is desirable since these keys are
ephemeral and we don't want to actually colocate them with the base
object keys. Perhaps some modeling here can help resolve this
issue. The data of the temporary object wants to be located as close
to the data of the base object as possible. This may be best performed
by adding a new ObjectStore creation primitive that takes the base
object as an addtional parameter that is a hint to the allocator.

Sam: I think that the short lived thing may be a red herring.  We'll
be updating the donor and primary objects atomically, so it seems like
we'd want them adjacent in the key space, regardless of the donor's
lifecycle.

The apply operation moves the data from the temporary object into the
correct position within the base object and deletes the associated
temporary object. This operation is done using a specialized
ObjectStore primitive. In the current ObjectStore interface, this can
be done using the clonerange function followed by a delete, but can be
done more efficiently with a specialized move primitive.
Implementation of the specialized primitive on FileStore can be done
by copying the data. Some file systems have extensions that might also
be able to implement this operation (like a defrag API that swaps
chunks between files). It is expected that NewStore will be able to
support this efficiently and natively (It has been noted that this
sequence requires that temporary object allocations, which tend to be
small, be efficiently converted into blocks for main objects and that
blocks that were formerly inside of main objects must be reusable with
minimal overhead)

The prepare and apply operations can be separated arbitrarily in
time. If a read operation accesses an object that has been altered by
a prepare operation (but without a corresponding apply operation) it
must return the data after the prepare operation. This is done by
creating an in-memory database of objects which have had a prepare
operation without a corresponding apply operation. All read operations
must consult this in-memory data structure in order to get the correct
data. It should explicitly recognized that it is likely that there
will be multiple prepare operations against a single base object and
the code must handle this case correctly. This code is implemented as
a layer between ObjectStore and all existing readers.  Annoyingly,
we'll want to trash this state when the interval changes, so the first
thing that needs to happen after activation is that the primary and
replicas apply up to last_update so that the empty cache will be
correct.

During peering, it is now obvious that an unapplied prepare operation
can easily be rolled back simply by deleting the associated temporary
object and removing that entry from the in-memory data structure.

Partial Application Peering/Recovery modifications
--------------------------------------------------

Some writes will be small enough to not require updating all of the
shards holding data blocks.  For write amplification minization
reasons, it would be best to avoid writing to those shards at all,
and delay even sending the log entries until the next write which
actually hits that shard.

The delaying (buffering) of the transmission of the prepare and apply
operations for witnessing OSDs creates new situations that peering
must handle. In particular the logic for determining the authoritative
last_update value (and hence the selection of the OSD which has the
authoritative log) must be modified to account for the valid but
missing (i.e., delayed/buffered) pglog entries to which the
authoritative OSD was only a witness to.

Because a partial write might complete without persisting a log entry
on every replica, we have to do a bit more work to determine an
authoritative last_update.  The constraint (as with a replicated PG)
is that last_update >= the most recent log entry for which a commit
was sent to the client (call this actual_last_update).  Secondarily,
we want last_update to be as small as possible since any log entry
past actual_last_update (we do not apply a log entry until we have
sent the commit to the client) must be able to be rolled back.  Thus,
the smaller a last_update we choose, the less recovery will need to
happen (we can always roll back, but rolling a replica forward may
require an object rebuild).  Thus, we will set last_update to 1 before
the oldest log entry we can prove cannot have been committed.  In
current master, this is simply the last_update of the shortest log
from that interval (because that log did not persist any entry past
that point -- a precondition for sending a commit to the client).  For
this design, we must consider the possibility that any log is missing
at its head log entries in which it did not participate.  Thus, we
must determine the most recent interval in which we went active
(essentially, this is what find_best_info currently does).  We then
pull the log from each live osd from that interval back to the minimum
last_update among them.  Then, we extend all logs from the
authoritative interval until each hits an entry in which it should
have participated, but did not record.  The shortest of these extended
logs must therefore contain any log entry for which we sent a commit
to the client -- and the last entry gives us our last_update.

Deep scrub support
------------------

The simple answer here is probably our best bet.  EC pools can't use
the omap namespace at all right now.  The simplest solution would be
to take a prefix of the omap space and pack N M byte L bit checksums
into each key/value.  The prefixing seems like a sensible precaution
against eventually wanting to store something else in the omap space.
It seems like any write will need to read at least the blocks
containing the modified range.  However, with a code able to compute
parity deltas, we may not need to read a whole stripe.  Even without
that, we don't want to have to write to blocks not participating in
the write.  Thus, each shard should store checksums only for itself.
It seems like you'd be able to store checksums for all shards on the
parity blocks, but there may not be distinguished parity blocks which
are modified on all writes (LRC or shec provide two examples).  L
should probably have a fixed number of options (16, 32, 64?) and be
configurable per-pool at pool creation.  N, M should be likewise be
configurable at pool creation with sensible defaults.

We need to handle online upgrade.  I think the right answer is that
the first overwrite to an object with an append only checksum
removes the append only checksum and writes in whatever stripe
checksums actually got written.  The next deep scrub then writes
out the full checksum omap entries.

RADOS Client Acknowledgement Generation Optimization
====================================================

Now that the recovery scheme is understood, we can discuss the
generation of of the RADOS operation acknowledgement (ACK) by the
primary ("sufficient" from above). It is NOT required that the primary
wait for all shards to complete their respective prepare
operations. Using our example where the RADOS operations writes only
"W" chunks of the stripe, the primary will generate and send W+M
prepare operations (possibly including a send-to-self). The primary
need only wait for enough shards to be written to ensure recovery of
the data, Thus after writing W + M chunks you can afford the lost of M
chunks. Hence the primary can generate the RADOS ACK after W+M-M => W
of those prepare operations are completed.

Inconsistent object_info_t versions
===================================

A natural consequence of only writing the blocks which actually
changed is that we don't want to update the object_info_t of the
objects which didn't.  I actually think it would pose a problem to do
so: pg ghobject namespaces are generally large, and unless the osd is
seeing a bunch of overwrites on a small set of objects, I'd expect
each write to be far enough apart in the backing ghobject_t->data
mapping to each constitute a random metadata update.  Thus, we have to
accept that not every shard will have the current version in its
object_info_t.  We can't even bound how old the version on a
particular shard will happen to be.  In particular, the primary does
not necessarily have the current version.  One could argue that the
parity shards would always have the current version, but not every
code necessarily has designated parity shards which see every write
(certainly LRC, iirc shec, and even with a more pedestrian code, it
might be desirable to rotate the shards based on object hash).  Even
if you chose to designate a shard as witnessing all writes, the pg
might be degraded with that particular shard missing.  This is a bit
tricky, currently reads and writes implicitely return the most recent
version of the object written.  On reads, we'd have to read K shards
to answer that question.  We can get around that by adding a "don't
tell me the current version" flag.  Writes are more problematic: we
need an object_info from the most recent write in order to form the
new object_info and log_entry.

A truly terrifying option would be to eliminate version and
prior_version entirely from the object_info_t.  There are a few
specific purposes it serves:
(1) On OSD startup, we prime the missing set by scanning backwards
		from last_update to last_complete comparing the stored object's
		object_info_t to the version of most recent log entry.
(2) During backfill, we compare versions between primary and target
		to avoid some pushes.

We use it elsewhere as well
(3) While pushing and pulling objects, we verify the version.
(4) We return it on reads and writes and allow the librados user to
		assert it atomically on writesto allow the user to deal with write
		races (used extensively by rbd).

Case (3) isn't actually essential, just convenient.  Oh well.  (4)
is more annoying. Writes are easy since we know the version.  Reads
are tricky because we may not need to read from all of the replicas.
Simplest solution is to add a flag to rados operations to just not
return the user version on read.  We can also just not support the
user version assert on ec for now (I think?  Only user is rgw bucket
indices iirc, and those will always be on replicated because they use
omap).

We can avoid (1) by maintaining the missing set explicitely.  It's
already possible for there to be a missing object without a
corresponding log entry (Consider the case where the most recent write
is to an object which has not been updated in weeks.  If that write
becomes divergent, the written object needs to be marked missing based
on the prior_version which is not in the log.)  THe PGLog already has
a way of handling those edge cases (see divergent_priors).  We'd
simply expand that to contain the entire missing set and maintain it
atomically with the log and the objects.  This isn't really an
unreasonable option, the addiitonal keys would be fewer than the
existing log keys + divergent_priors and aren't updated in the fast
write path anyway.

The second case is a bit trickier.  It's really an optimization for
the case where a pg became not in the acting set long enough for the
logs to no longer overlap but not long enough for the PG to have
healed and removed the old copy.  Unfortunately, this describes the
case where a node was taken down for maintenance with noout set. It's
probably not acceptable to re-backfill the whole OSD in such a case,
so we need to be able to quickly determine whether a particular shard
is up to date given a valid acting set of other shards.

Let ordinary writes which do not change the object size not touch the
object_info at all.  That means that the object_info version won't
match the pg log entry version.  Include in the pg_log_entry_t the
current object_info version as well as which shards participated (as
mentioned above).  In addition to the object_info_t attr, record on
each shard s a vector recording for each other shard s' the most
recent write which spanned both s and s'.  Operationally, we maintain
an attr on each shard containing that vector.  A write touching S
updates the version stamp entry for each shard in S on each shard in
S's attribute (and leaves the rest alone).  If we have a valid acting
set during backfill, we must have a witness of every write which
completed -- so taking the max of each entry over all of the acting
set shards must give us the current version for each shard.  During
recovery, we set the attribute on the recovery target to that max
vector (Question: with LRC, we may not need to touch much of the
acting set to recover a particular shard -- can we just use the max of
the shards we used to recovery, or do we need to grab the version
vector from the rest of the acting set as well?  I'm not sure, not a
big deal anyway, I think).

The above lets us perform blind writes without knowing the current
object version (log entry version, that is) while still allowing us to
avoid backfilling up to date objects.  The only catch is that our
backfill scans will can all replicas, not just the primary and the
backfill targets.

It would be worth adding into scrub the ability to check the
consistency of the gathered version vectors -- probably by just
taking 3 random valid subsets and verifying that they generate
the same authoritative version vector.

Implementation Strategy
=======================

It goes without saying that it would be unwise to attempt to do all of
this in one massive PR.  It's also not a good idea to merge code which
isn't being tested.  To that end, it's worth thinking a bit about
which bits can be tested on their own (perhaps with a bit of temporary
scaffolding).

We can implement the overwrite friendly checksumming scheme easily
enough with the current implementation.  We'll want to enable it on a
per-pool basis (probably using a flag which we'll later repurpose for
actual overwrite support).  We can enable it in some of the ec
thrashing tests in the suite.  We can also add a simple test
validating the behavior of turning it on for an existing ec pool
(later, we'll want to be able to convert append-only ec pools to
overwrite ec pools, so that test will simply be expanded as we go).
The flag should be gated by the experimental feature flag since we
won't want to support this as a valid configuration -- testing only.
We need to upgrade append only ones in place during deep scrub.

Similarly, we can implement the unstable extent cache with the current
implementation, it even lets us cut out the readable ack the replicas
send to the primary after the commit which lets it release the lock.
Same deal, implement, gate with experimental flag, add to some of the
automated tests.  I don't really see a reason not to use the same flag
as above.

We can certainly implement the move-range primitive with unit tests
before there are any users.  Adding coverage to the existing
objectstore tests would suffice here.

Explicit missing set can be implemented now, same deal as above --
might as well even use the same feature bit.

The TPC protocol outlined above can actually be implemented an append
only EC pool.  Same deal as above, can even use the same feature bit.

The RADOS flag to suppress the read op user version return can be
implemented immediately.  Mostly just needs unit tests.

The version vector problem is an interesting one.  For append only EC
pools, it would be pointless since all writes increase the size and
therefore update the object_info.  We could do it for replicated pools
though.  It's a bit silly since all "shards" see all writes, but it
would still let us implement and partially test the augmented backfill
code as well as the extra pg log entry fields -- this depends on the
explicit pg log entry branch having already merged.  It's not entirely
clear to me that this one is worth doing seperately.  It's enough code
that I'd really prefer to get it done independently, but it's also a
fair amount of scaffolding that will be later discarded.

PGLog entries need to be able to record the participants and log
comparison needs to be modified to extend logs with entries they
wouldn't have witnessed.  This logic should be abstracted behind
PGLog so it can be unittested -- that would let us test it somewhat
before the actual ec overwrites code merges.

Whatever needs to happen to the ec plugin interface can probably be
done independently of the rest of this (pending resolution of
questions below).

The actual nuts and bolts of performing the ec overwrite it seems to
me can't be productively tested (and therefore implemented) until the
above are complete, so best to get all of the supporting code in
first.

Open Questions
==============

Is there a code we should be using that would let us compute a parity
delta without rereading and reencoding the full stripe?  If so, is it
the kind of thing we need to design for now, or can it be reasonably
put off?

What needs to happen to the EC plugin interface?
