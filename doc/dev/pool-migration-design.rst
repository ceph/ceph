=========================
 Design of Pool Migration
=========================


Background
==========

The objective for pool migration is to be able to migrate all RADOS objects
from one pool to another within the same cluster non-disruptively. This
functionality is planned for the Umbrella release.

Use cases for pool migration:

* This provides the ability to change the erasure code profile (and in
  particular the choice of K and M) non-disruptively. Implementing this as a
  non-disruptive migration between pools is simpler and no less efficient than
  trying to perform this type of transformation in place.
* Converting between replica and erasure coded pools. Changes being made to add
  OMAP and class support to EC pools will remove the need to have a separate
  replica pool for metadata when using RBD, CephFS or RGW, this should make
  these migrations viable in conjunction with this work.
* The general use case of wanting to migrate data between two pools.

By non-disruptive we mean that there will be no time where I/O or applications
need to switch from using the old pool to the new pool, not even a very short
outage at the start or end of the migration (as for example is required by RBD
live migration). The migration will however need to read and write every object
in the pool so there will be a performance impact during the migration -
similar to that when PGs are backfilling. The same techniques and controls that
are used when splitting/merging PGs and backfilling individual PGs will be used
by pool migration to manage this impact.

For the first release we will require that the target pool is empty to begin
with (this means we don't need to worry about objects with the same name).
See the section on avoiding name collisions for more details. Supporting merging of
pools (either where constraints prevent object name collisions or where these
collisions are resolved automatically during the migration) is a possible
future enhancement, but it's not clear what use cases this solves.

During a pool migration restrictions are placed on the source pool, it is not
permitted to modify the number of PGs (to cause splits or merges). See sections
on stopping changes to the number of PGs during migration and on the CLI and UI
for more details. Deletion of the source pool will not be permitted during a
migration. Other actions such as rebalancing are permitted but perhaps should
be discouraged as the data is being moved anyway.

During a pool migration restrictions are placed on the target pool, it is not
permitted to migrate this pool to another (i.e. no daisy chained or cyclical
migrations). Splits, merges and rebalancing are permitted. Deletion of the
target pool will not be permitted during a migration. Once a pool has finished
migrating it is permited to start a new migration of the target pool of a
previous migration.

For the first release there is no option to cancel, suspend or reverse a pool
migration once it has started.

For the first release there is no plan to have the clients update their
references to the pool once the migration has completed, they will continue
to reference the old pool and objector (``librados``) will reroute the request
to the new pool. The ``OSDMap`` will retain stub information for the old pool
redirecting to the new pool.

The feature requires changes to client code; all clients and daemons will
need to be upgraded before a pool migration is permitted. The two main clients,
objector (in ``librados``) and the kernel client will be updated. Updates to
the kernel client are likely to lag the Umbrella release. Where the clients are
integrated into other products (e.g. ``ODF``) these products will need to
incorporate the new clients before the feature can be used.

For the first release there is no plan to support pool migration between Ceph
clusters. Theoretically this could be added later building upon the first
release code but would require substantial extra effort. It would require
clients to be able to update references to the cluster and pool once the
migration had completed and for clients to be able to redirect I/O to a
different cluster. There would be extra authentication challenges as all OSDs
and clients in the source cluster would need to be able to submit requests to
the target cluster.


Design
======


Reuse of Existing Design
------------------------

Let's start by looking at existing code or features that we can copy / reuse
/ refactor / take inspiration from, we don't want to reinvent the wheel or
repeat past mistakes.


Backfill
~~~~~~~~

Backfill is a process run by a PG to recover objects on an OSD that has either
just been added to the PG (starts with no objects) or has been absent from the
PG for a while (has some objects that are up to date, some that are stale, is
probably missing new objects and may have objects that are no longer needed
because they were deleted while it was absent).

Backfill takes a long time so I/O must be permitted to continue while the
backfill happens. It uses the fact that all objects have a hash and that it is
possible to list objects in hash order. This means that backfill can recover
objects in hash order and can simply keep a watermark hash value to track what
progress has been made. I/Os to objects with a hash below the watermark are to
an object that has been recovered and need to update all OSDs including the
backfilling OSD. I/Os to objects with a hash above the watermark can ignore the
backfilling OSDs as the backfill process will recover this object later. The
object(s) currently being recovered by the backfill process are locked to
prevent I/O for the short time it takes to backfill an object.

Another property of backfill is that the process is idempotent, while there are
performance benefits to preserving the watermark there is no correctness issues
if the watermark is reset to the start and the backfill process starts again as
repeating the process will determine that objects have already been recovered.
This simplifies the design because the watermark doesn't have to be rigorously
replicated and checkpointed, although for backfill it is part of ``pg_info_t``
so progress is checkpointed fairly frequently.

Relevance to pool migration:

* Pool migration can list objects in hash order and migrate them to the new pool.
* A watermark can be used to keep track of which objects have been migrated.
  There is no need for the watermark to be persistent.
* Clients can cache a copy of the watermark to help direct I/Os to the correct
  pool and PG.
* The client's cached copy can become stale, if I/Os are misdirected they will
  be failed providing an up-to-date watermark so the I/O can be retried.
* Backfill recovers all parts of a RADOS object - attributes, data and
  OMAP. Large objects are recovered in phases (something like 2MB at a time)
  and utililize a temporary object which is renamed at the end of the recovery
  for atomicity. If a peering cycle interrupts the process, then the
  temporary object is discarded. If pool migration uses this technique it needs
  to be aware that a peering cycle might disrupt the target pool but not the
  source pool and therefore may need to restart the migration of the object if
  the target discards the object.
* Backfill recovers a RADOS head object and its associated snapshots at the
  same time and uses locking (e.g.
  ``PrimaryLogPG::is_degraded_or_backfilling_object`` and
  ``PrimaryLogPG::is_unreadable_object``) to ensure that none of these can be
  accessed while they are being recovered because of dependencies between them.
  Pool migration needs to migrate the head object and the snapshots at the same
  time and needs to ensure we don't process I/O to the object halfway through
  this process.
* Backfill is meant to preserve the space-efficiency of snapshots when
  recovering them using the information in the snapset attribute to work out
  which regions of the snapshots are clones - see ``calc_clone_subsets`` /
  ``calc_head_subsets``. This hasn't been implemented for EC pools yet and
  `tracker 72753 <https://tracker.ceph.com/issues/72753>`_ shows it currently
  isn't working for replica pools either. We will want to use this (and the way
  a ``PushOp`` re-establishes cloned regions) for migration.

Unlike backfill, for migration we want the clients to know the watermark so
they can route I/Os to the old/new pool. We don't care if clients have a
stale watermark - this will just cause a few I/Os to be incorrectly routed to
the old pool which can fail them back to the client and communicate a new
watermark so the I/O can be resubmitted to the new pool.

We deliberately make updating the client's copy of the watermark lazy - there
could be hundreds or thousands of clients so updating them all the time would
be expensive. Putting the watermark into the ``OSDMap`` and issuing new epochs
to distribute it to all the clients would be even more expensive. In contrast
we are thinking about recording which PGs are migrating/have finished migrating
in the ``OSDMap`` - a rule of thumb would be to try and only update the
``OSDMap`` once a second during a migration.

For migration to be able to support direct reads we do need all the OSDs in the
PG to know where the watermark is and for this to be updated as each object is
migrated. Migrating an object involves reading it from the source pool, writing
it to the target pool and then deleting it from the source pool. Other OSDs can
update migration progress as they process the delete request. There will be
some complexity regarding direct reads and migrating an object + its snapshots.
There is already some code that fails direct reads with ``EAGAIN`` (to redirect
these to the primary) when an object + its snapshots have not all been
recovered, we may need to use this when midway through migrating an object
+ snapshots and then have the primary stall the I/O until the object +
snapshots have all been migrated before failing the I/O again for redirection
to the new pool.

The watermark doesn't necessarily need to be checkpointed to disk, it is cheap
to find the object with the lowest hash in a PG so we could do this to
recalculate the watermark whenever peering starts migration.


Scheduling Backfill / Recovery
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Deciding how to prioritize backfill/recovery and how fast to run this process
versus processing I/O from clients is a complex problem. Firstly, a decision
is made as to which PGs should be backfilling/recovering, and which should
wait. This involves messages between OSDs and considers whether I/O is blocked
and how much redundancy the PG has left (for example a replica-3 pool with 2
failures is prioritized over a replica-3 pool with 1 failure). Secondly once a
PG has been selected to backfill/recover the schedule has to decide how
frequently to perform backfill/recovery versus process client I/O. This happens
within the primary OSD using weighted costs.

Relevance to pool migration:

* Pool migration is less critical than backfill or recovery. It needs to fit
  into the same process to determine when a PG should start migrating.
* Once a PG is permitted to start migration the OSD scheduler needs to pace the
  work. The overheads for migrating an object are like the overheads for
  backfilling an object so hopefully we can just copy the backfill scheduling
  for migration.

The objective is to reuse as much of the scheduler (e.g. ``mclock``) as
possible, just teaching it that migration has a lower priority than backfill or
async recovery but higher priority than deep scrub.

``Mclock`` works by assigning a weighting to each backfill / recovery op and
each client I/O request, it also benchmarks OSDs at startup to get some idea
what the maximum performance of the OSD is. This information is then used to
work out when to schedule background work. The same concepts should work for
migration requests. We will need to assign a weighting to migration work;
this should be similar/identical to the weighting for backfills.

We will take a similar approach for supporting clusers running with
``WeightedPriorityQueue`` scheduling.

The expectation is that there should be no need for new tuneable settings
for migration, the existing tuneable settings for backfill/recovery should be
sufficient, we don't want to further complicate this part of the UI.


Statistics
~~~~~~~~~~

I believe there are a few statistics collected about the performance of
backfill/recovery. We should supplement these with similar statistics
about the process of migrations.

We need to consider OSD stats that are gathered by ``Prometheus`` and any
progress summary that is presented via ``HealthCheck`` and/or the UI.


CopyFrom
~~~~~~~~

``CopyFrom`` is a RADOS op that can copy the contents of an object into a new
object. It is sent to the OSD and PG that will store the new object. The OSD
is responsible for reading the source object which involves sending messages
to another OSD and PG and then writing the data it reads to the new object. If
the object being copied is large, then the copy operation is broken up into
multiple stages and this is made atomic by using a temporary object to store
the new data until the last data has been copied at which point the temporary
object can be renamed to become the new object.

Relevance to pool migration:

* Pool migration needs to copy objects from the old pool to a new pool - this
  will involve one OSD and PG reading the object and another OSD and PG writing
  the object.
* Pool migration will want to drive the copy operation from the source side,
  so we probably need a ``CopyTo`` type operation.
* The way messages are sent between OSDs, the way a large object copy is staged
  and the use of a temporary object name when staging are all concepts that can be
  reused.

Alternatively, pool migration might want to copy the recover object
implementation in ``ECBackend`` which is used to recover an object being
recovered or backfilled. This also stages the recovery of large objects
using a temporary object and uses ``PushOp`` messages to send data to the OSDs
being backfilled. It might be possible to use most of the recover object
process without changes, just changing the ``PushOp`` messages to be sent to a
different PG and sending the messages for all shards as the entire object is
being migrated.

Lets consider the differences between the backend recovery op and CopyFrom:

* ``CopyFrom`` is a process that runs in ``PrimaryLogPG`` above either the
  replica or ``ECBackend`` that copies an object from a primary OSD for one PG
  to the primary OSD for another PG. In the case of EC the primary OSD may need
  to issue ``SubOp`` commands to other OSDs to read/write the data.
* ``run_recovery_op`` implemented by replica and EC pools runs on the primary
  OSD and reads data (in the case of EC issuing ``SubOp`` commands to other
  OSDs) but then issues ``PushOp`` commands to write the recovered data to the
  destination OSDs.
* ``CopyFrom`` working at the ``PrimaryLogPG`` level ensures that the copied
  object is included in the PG stats and gets its own PG log entry so the
  update can be rolled forward/backwards and can be recovered by async
  recovery.
* ``run_recovery_op`` is implemented at the ``PGBackend`` level and assumes the
  PG already has stats and a PG log entry for the object, it is just
  responsible for bringing other shards in the PG up to date.
* CopyFrom ends up issuing read and write ops to the PGBackend, it doesn't
  provide techniques for copying a snapshot and preserving its
  space-efficiency.
* ``run_recovery_op`` is meant to preserve space-efficiency of clones (not
  implemented yet for EC pools and replica pools have bugs) – the ``PushOp``
  message includes a way of describing which parts of an object should be
  clones.

For pool migration we probably want a hybrid implementation. We can probably
re-use a lot of the ``run_recovery_op`` code to read the object that we want to
migrate, and ideally handle the space-efficiency in snaps. Instead of issuing
PushOps we probably want to issue a new ``COPY_PUT`` type op to the priamry PG
of the target pool, but passing the same kind of information as a PushOp so we
can keep track of what needs to be cloned. The target pool can then submit a
mixture of write and clone ops to the PGBackend layer to create the object as
well as updating the PG stats and creating a PG log entry.


Splitting PGs
~~~~~~~~~~~~~

Normally a pool has a number of PGs that is a power of 2. This is because we
want each PG to hold roughly the same number of objects, and we use the most
significant N bits of the object hash to select which PG to use. However, when
doubling the number of PGs that a pool has this causes approximately half the
objects in the pool to need to be moved to a new PG. We don't want all this
migration to happen at once; we want it to be paced over time to have less
impact. To deal with this the MGR controls the increase in the number of PGs,
it has a target for how many PGs the pool should have and slowly increases the
number of PGs waiting for PGs to finish recovery before doing further splits.

When a pool has a non-power of 2 number of PGs this means that not all PGs are
the same size. For example, if there are 5 PGs then PGs 0 and 4 will be half
the size of PGs 1 to 3 because the choice between PG 0 and 4 is based on one
extra bit of the object hash. While this is not desirable as a long-term state
it is fine during the splitting process.

Relevance to pool migration:

* Pool migration needs to migrate all the objects in all the PGs in the old
  pool to the new pool. Just like splitting we don't want to overwhelm the
  system while performing the migration.
* Pool migration should therefore migrate one (or a small number) of PGs at
  a time.
* A process needs to monitor the progress of migrations, notice when PGs finish
  migrating and start the next PG. This could either be in the MON (in which case
  it would need to be event driven with OSDs telling the MON when a PG has
  finished migrating - somewhat similar to how PG merges work) or it could be
  implemented in the MGR (in which case the MGR can poll the state of the PGs and
  then tell the MON via a CLI command to start the next PG migration).


Direct I/O / Balanced Reads
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The EC direct I/O feature is making changes to the client to decide which OSD
to send client I/O requests to, it is building on top of the balanced reads
flag for replica pools which tells the client to distribute read I/Os evenly
across all the OSDs in a replica PG rather than sending them all to the
primary.

Relevance to pool migration:

* It's changing code in the client at a similar place to where we want the
  client to implement pool migration deciding which pool (and hence PG and OSD)
  to send I/O to.
* Direct I/O / balanced reads are permitted to be failed by the OSD that
  receives the request with ``EAGAIN`` to deal with corner cases where the OSD
  is unable to process the I/O. In this case the client retries the I/O but
  sends it to the primary OSD. A similar retry mechanism is going to be required
  when a client issue an I/O to the wrong pool because an object has been
  recently migrated. When I/Os are retried, we need to worry about ordering as
  this generates opportunities for I/Os to overtake or be reordered. See section
  Read/Write ordering below.
* Direct I/O is adding extra information to the pg_pool_t structure that is
  part of the ``OSDMap`` that gets sent to every Ceph daemon and client by the
  monitor. This extra information is being used to determine that direct I/O is
  supported and to help work out where to route the I/O request. Pool migration
  will similarly need to add details to ``pg_pool_t`` structure so that clients
  are aware that a migration is happening.


Read/Write Ordering
-------------------

Ceph has some fairly strict read / write ordering rules. Once a write has
completed to the client any read must return the new data. Prior to the write
completing a read is expected to return all old data or all new data (a mixture
is not permitted). If writes A and B are issued concurrently one after another
to the same object then write A is expected to be applied before write B –
ordering of the writes is expected to be preserved through the client,
messenger and the OSD. If write A and read B are issued concurrently then there
is scope for read B to overtake write A. There is a flag ``RWORDERED`` that can
be set that prevents this overtaking from happening.

There are no ordering guarantees when reads or writes are issued to different
objects - these objects are almost certainly stored on different OSDs and even
if they are on the same OSD will be processed by different threads with
different locks so can easily be reordered.

There do not appear to be many uses of the ``RWORDERED`` flag, RBD and RGW do
not use the flag, CephFS uses the flag in MDS ``RecoveryQueue`` (calls
``filer.probe`` which is implemented in ``osdc/Filer.cc``) which I think is
only used in some recovery scenarios.

These rules make it tricky to implement the watermark in the client and use
this to decide which pool to route I/O requests to without using something
equivalent to a new epoch to advance the watermark. The problem is that if the
watermark is advanced without quiescing I/O it is possible that this causes
requests to be reordered.

For example:

* Write A issued to old pool.
* Write B issued to old pool.
* Write A fails with updated watermark and is retried to new pool.
* Read B with ``RWORDERING`` issued to new pool.
* Write B fails and needs to be retried to new pool.

In this example read B has overtaken write B.

Perhaps more concerning is that the rules would also be broken if instead of
Read B we issued another write to B.

The simplest way to prevent reordering violations is to not advance the
watermark while there are outstanding writes (or reads with ``RWORDERING`` flag
set) in flight. This isn't idea as it may result it quite a number of I/Os
being failed for retry before the watermark can be updated.

A more sophisticated implementation stalls issuing new writes to objects
with a hash between the old and new watermark while there are other writes
in flight to objects with a hash between the old and new watermark.


Other Pool Migration Issues
---------------------------

Other topics that we need to think about for pool migration.


Avoiding Name Collisions
~~~~~~~~~~~~~~~~~~~~~~~~

For the first release we will require that the target pool is empty when the
migration starts (by having a UI interface that only starts a migration while
a new pool is being created). We can also protect against objects being written
to the target pool during the migration by adding client code to reject
attempts to initiate requests to the target pool (the client code itself is
still permitted to redirect requests from the source pool to the target pool).
Because we will require a minimum client version to use pool migration this
will ensure that all clients include this extra policing. OSDs cannot
themselves implement the policing so there is no protection against a rouge
client – we probably should have migration halt rather that crash if a name
collision is found.

Post first release if there is a use case for merging pools then it is
theoretically possible to deal with name collisions by additionally using the
pool which the client is accessing the object from to uniquify the name. This
would require extra information in the request from the client to the OSDs.


Stopping Changes to the Number of PGs During Migration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

During a migration we don't really want to be changing the number of PGs in
the source pool. There are three reasons why:

#. We don't really want to be moving objects around in the source pool when we
   are about to migrate them - we are probably better off getting on with the
   migration than trying to fix any imbalance in the source pool.
#. Splitting/merging PGs in the source pool makes it harder to schedule the
   migration. Scheduling is done at two levels - we say how many source PGs are
   migrating at a time and then control the rate of migration within a source PG.
   If we split/merge the source pool this makes selecting which PGs to migrate
   more difficult.
#. If we block splits and merges and migrate the PGs in reverse order (starting
   with the highest numbered PG in the pool) then we can reduce the number of PGs
   in the source pool as PGs finish migrating. This helps keeps the overall number
   of PGs more manageable.

In contrast we don't really care so much about the target pool - we can easily
cope with splits/merges while the migration is in progress. From a performance
perspective we do however want to avoid migrating objects to the target pool
and then having splits/merges occur that copy the objects a second time. That
means that normally we would want to set the number of target pool PGs to be
the same as the source pool at the start of the migrate.

We might also want to default to disable the auto-scaler for the target pool
during the migration as we don't want it seeing a nearly empty target pool
with loads of PGs and thinking that it should reduce the number of PGs.


CLI and UI
~~~~~~~~~~

Pool migration will need a new CLI to start the migration, there will also need
to be a way of monitoring PGs that a migrating and the progress of the
migration. The CLI to start a migration will need to be implemented by the MON
(``OSDMonitor.cc`` already implements most of the pool CLI commands) because
the migration will need to update the ``pg_pool_t`` structures in the ``OSDMap``
to record details of the migration.

The new map will then be distributed to clients and OSDs so that they know that
the migration has started. PGs that have been scheduled to start migration will
need to determine at the end of the peering process that they don't need to
recovery or backfill and that they should attempt to schedule a migration (will
need new PG states ``MIGRATION_WAIT`` and ``MIGRATING``).

We will need to work with the dashboard team to add support for pool migration
to the dashboard and to provide a REST API for starting a migration.

We will want to block some CLIs while a pool migration is taking place:

* We don't want to be able to split/merge PGs in the source pool while it is
  being migrated (see above).
* We don't want the target pool to become the source of another migration
  (no chaining migrations).

Some of these CLIs are issued by MGR, in this case we probably will need to
change the MGR code to either cope with the failures and/or to detect that the
pool is migrating and avoid issuing the CLIs. We probably will need both as
although checking if the pool is migrating before issuing a CLI is probably
more efficient, it is exposed to a race hazard where the migrate may start
between the check and CLI being issued.

We need to look at how the progress of things like backfill and recovery are
reported in the UI (possibly by ``HealthCheck``?) and think about how to report
the progress of a pool migration. We need to think what are the right units for
reporting progress (e.g. number of objects out of total objects, number of PGs
out of total PGs or just a percentage).


Backwards Compatibility / Software Upgrade
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pool migration requires code changes in the Ceph daemons (MON, OSD and possibly
MGR) and to the Ceph clients that issue I/O. We can't allow a pool migration to
happen while any of these are running old code because the old code won't
understand that a pool migration is happening. Old clients won't have any way
of directing I/O to the correct pool, PG and OSD and having OSDs forward all
these requests to the correct OSD would be far too expensive.

Ceph daemons and clients have a set of feature bits indicating what features
they support and there are mechanisms for setting a minimum set of feature
bits that are required by daemons and separately for clients. Once set this
prevents down-level daemons and clients connecting to the cluster. There are
also mechanisms to ensure that once a minimum level has been set that this
cannot be reversed.

Pool migration will need to define a new feature bit and use the existing
mechanisms for setting minimum required levels for daemons and clients. The new
pool migration CLIs will need to fail an attempt to start a migration unless
the minimum levels have been set.


End of Migration
~~~~~~~~~~~~~~~~

When a migration completes, we will have moved all objects from pool A to pool
B, however clients (e.g. RBD, CephFs, RGW, ...) will still have pool A embedded
in their own data structures. We don't want to force all the clients to update
their data structures to point at the new pool, so instead we will retain stub
information about pool A saying that it has been migrated and that all I/O
should now be submitted to pool B.

Retaining a stub ``pg_pool_t`` structure in the ``OSDMap`` is cheap - there
won't be thousands of pools and there isn't that much data stored for the pool.
We will want to ensure that the old pool has no PGs associated with it, we can
do this by reducing the number of PGs it has to 0 and letting the same code
that runs when PGs are merged clean up and delete the old PGs.

We need to think about the consequences of this on the UI interface. While in
the code we start with pool A and create and migrate objects to pool B, from
the perspective of the UI we probably want to show this as a transformation of
pool A and hide the existence of pool B from the user.

An alternative implementation would just show the pool redirection in the UI,
so users would see an RBD image used pool A but would then find that pool A has
been migrated to pool B. This alternative implementation might be better if we
plan to support merging of pools (migration to a non-empty target pool) in the
future.


Walkthrough of how Pool Migration Might Work
--------------------------------------------


Initiating the Pool Migration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. User creates a new pool, perhaps they use a new flag ``--migratefrom`` to
   say they want to start a pool migration.
#. Starting the migration as part of pool creation means we know the pool is
   initially empty.
#. Unless the user specifies a number of PGs we can ensure that the newly
   created pool has the same number of PGs as the source pool. There is no
   requirement that the number of PGs is the same, it just avoids having to
   perform a migration and then perform a second copy of data as the number of
   PGs is adjusted to cater for the eventual number of objects in the pool.
#. The CLI command sets up the ``pg_pool_t`` structures in the ``OSDMap`` to
   indicate that a pool migration is starting. We record that pool A is being
   migrated to pool B, and record which PG(s) we are going to start migrating.
   If we are going to migrate more than one PG at a time, we probably want to
   specify a set of PGs (e.g. 0,1,2,3) that are being migrated. Any PG in the
   set is migrating. Any PG not in the set that is higher than the lowest value
   in the set is assumed to have completed migration, any PG not in the set
   that is lower than the lowest value in the set is assumed to have not
   started migration.
#. We migrate PGs in reverse order - so for example if a pool has PGs 0-15
   then we will start by migrating PG 15.
#. MON publishes new ``OSDMap`` as a new epoch.


Client
~~~~~~

#. Clients use the ``pg_pool_t`` structure in the ``OSDMap`` to work out a
   migration is in progress.
#. From the range of PGs being migrated they can work out which PGs have been
   migrated, which have not started migrating and which are in the process of
   migrating.

   a. If an I/O is submitted to a PG that has been migrated the object hash and
      new pool is used to determine which PG and OSD to route the I/O request
      to.
   b. If an I/O is submitted to a PG that has not started migration the object
      hash and old pool is used to determine which PG and OSD to route the I/O
      request to.
   c. If an I/O is submitted to a PG that is marked as being migrated the client
      checks if it has a cached watermark for this PG. If it does, then it uses
      this to decide whether to route the request to the old or new pool. If it
      has no cached watermark, it guesses and sends the I/O to the old pool.

#. If an I/O is misrouted to the wrong pool the OSD will fail the request
   providing an update to the watermark. The client needs to update its cached
   copy of the watermark and resubmit the I/O.


OSD
~~~

#. OSDs use the ``pg_pool_t`` structure in the ``OSDMap`` to work out if a PG
   needs migrating.
#. At the end of peering if the PG needs migrating and is not performing
   backfill or recovery it sets the PG state to ``MIGRATION_WAIT`` and checks
   with other OSDs whether they have the resources and free capacity to start
   the migration.
#. If everything is good the PG state changes to ``MIGRATING``, sets the
   watermark to 0 and the scheduler is instructed to start scheduling migration
   work.
#. Migration starts by scanning the next range of objects to be migrated
   creating a list of object OIDs.
#. Each object is then migrated, with the watermark being updated after the
   object has been migrated.

   a. The primary reads the object and sends it to the primary of the target
      PG which then writes the object.
   b. If the object is large this is done in stages with the target using a
      temporary object name which is renamed when the last data is written.
   c. Once an object has been migrated it is deleted from the source pool.

#. Client I/O checks the object hash of the client I/O with the watermark. If
   the I/O is below the watermark it is failed for retry to the new pool,
   providing the current watermark for the client to cache.
#. If a PG completes a migration, then it sends a message to the MON telling
   it that the migration has completed.


MON
~~~

#. When MON gets a message from an OSD saying that a migration has completed
   it updates the set in the ``pg_pool_t`` to record that the PG has finished
   migration and that the next PG is starting migration. A new ``OSDMap`` is
   published as a new epoch.
#. Because migrations are scheduled in reverse order and objects are deleted
   as the migration happens, this means that as PG migrations complete that we
   should have empty PGs that can be deleted by simply reducing the number of
   PGs that the source pool has. PG migrations might not complete in the order
   which they are started so we might have a few empty PGs hanging around that
   cannot be deleted until another PG migration completes.
#. At the end of the migration there are no more PGs to start migrating, so
   the set of migrating PGs diminishes. When the set becomes empty we should
   have also reduced the number of PGs for the source pool to zero and at this
   point the migration is complete. The MON can make final updates to the
   ``pg_pool_t`` state to indicate the migration has finished. The
   ``pg_pool_t`` structure needs to be kept so that clients know to direct all
   I/O requests to this pool to the new pool instead.
#. Pools can be migrated more than once, this can result in multiple stub
   ``pg_pool_t`` structures being kept. We do not want to have to recurse
   through these stubs when I/Os are submitted, so at the end of a migration
   the MON should attempt to reduce these redirects to a single level.


Testing and Test Tools
----------------------

The objectives of testing pool migration are:

#. Validate that all the objects in the source pool are migrated to the target
   pool and that their contents (data, attributes and OMAP) are retained.
#. Validate that during a migration object can be read (data, attributes, OMAP)
   for objects that haven't yet been migrated, objects that have been migrated
   and objects in the middle of being migrated.
#. Validate that during a migration objects can be updated (create, delete,
   write, update attributes, update OMAPs) for objects that haven't yet been
   migrated, objects that have been migrated and objects in the middle of being
   migrated.
#. Validating pool migration under error scenarios, including resetting and
   failing OSDs.
#. Validate that snapshots, clones are migrated and can be used during a pool
   migration.
#. Validate the UI for pool migration, including restrictions placed on the UI
   during the migration.
#. Validation of migrating multiple different pools in parallel. Validation of
   migration a single pool multiple times in series.
#. Validation of pool migration with unreadable objects (excessive medium
   errors plus possibly other failures that defeat the redundancy of the
   replica/EC pool without taking it offline).
#. Validation of software upgrade / compatibility for both daemons (OSD, MON,
   MGR) and clients.
#. Validation of performance impact during a migration.

Pool migration makes changes to client code, so all modified clients will need
testing.

Existing tools such as ``ceph_test_rados`` are good for creating and exercising
a set of objects and performing some consistency checking of objects.

A simple script is probably better for creating a large number of objects and
then validating the contents of the objects. Writing a script is probably
better for being able to test attributes and OMAPs as well. If the script has
two phases (create objects and validate objects) then these phases can be run
at different times (before, during, after pool migration) to test different
aspects. The script could use a command line tool such as rados to create and
validate objects, using pseudo random numbers to generate data patterns,
attributes and OMAP data that could then be validated. The script would need to
run many rados commands in parallel to generate a decent I/O workload. There
may be scripts that already exist that can do this, it may be possible to adapt
ceph_test_rados to do this.

Tools such as ``VDBench`` can test data integrity of block volumes, either
creating a data set and then validating it, or can continuously create and
update data keeping a journal so it can be validated at any point. However
block volume tools can only test object data, not attributes or OMAPs.

A tool such as ``FIO`` is best suited for doing performance measurements.

The I/O sequence tool ``ceph_test_rados_io_sequence`` is probably not useful
for testing pool migration - it specializes it testing a very small number of
objects and focuses on boundary conditions within an object (e.g. EC chunk
size, strip size) and data integrity.

The objective should be to use teuthology to perform most of the testing for
pool migration (at a minimum 1 to 5 in the list above). It should be possible
to add pool migration as an option to existing tests in the RADOS suite,
extending the ``thrashOSD`` class to include the option of starting a
migration.
