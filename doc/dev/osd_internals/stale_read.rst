Preventing Stale Reads
======================

We write synchronously to all replicas before sending an ack to the
client, which ensures that we do not introduce potential inconsistency
in the write path.  However, we only read from one replica, and the
client will use whatever OSDMap is has to identify which OSD to read
from.  In most cases, this is fine: either the client map is correct,
or the OSD that we think is the primary for the object knows that it
is not the primary anymore, and can feed the client an updated map
indicating a newer primary.

They key is to ensure that this is *always* true.  In particular, we
need to ensure that an OSD that is fenced off from its peers and has
not learned about a map update does not continue to service read
requests from similarly stale clients at any point after which a new
primary may have been allowed to make a write.

We accomplish this via a mechanism that works much like a read lease.
Each pool may have a ``read_lease_interval`` property which defines
how long this is, although by default we simply set it to
``osd_pool_default_read_lease_ratio`` (default: .8) times the
``osd_heartbeat_grace``.  (This way the lease will generally have
expired by the time we mark a failed OSD down.)

readable_until
--------------

Primary and replica both track a couple of values:

* *readable_until* is how long we are allowed to service (read)
  requests before *our* "lease" expires.
* *readable_until_ub* is an upper bound on *readable_until* for any
  OSD in the acting set.

The primary manages these two values by sending *pg_lease_t* messages
to replicas that increase the upper bound.  Once all acting OSDs have
acknowledged they've seen the higher bound, the primary increases its
own *readable_until* and shares that (in a subsequent *pg_lease_t*
message).  The resulting invariant is that any acting OSDs'
*readable_until* is always <= any acting OSDs' *readable_until_ub*.

In order to avoid any problems with clock skew, we use monotonic
clocks (which are only accurate locally and unaffected by time
adjustments) throughout to manage these leases.  Peer OSDs calculate
upper and lower bounds on the deltas between OSD-local clocks,
allowing the primary to share timestamps based on its local clock
while replicas translate that to an appropriate bound in for their own
local clocks.

Prior Intervals
---------------

Whenever there is an interval change, we need to have an upper bound
on the *readable_until* values for any OSDs in the prior interval.
All OSDs from that interval have this value (*readable_until_ub*), and
share it as part of the pg_history_t during peering.

Because peering may involve OSDs that were not already communicating
before and may not have bounds on their clock deltas, the bound in
*pg_history_t* is shared as a simple duration before the upper bound
expires.  This means that the bound slips forward in time due to the
transit time for the peering message, but that is generally quite
short, and moving the bound later in time is safe since it is an
*upper* bound.

PG "laggy" state
----------------

While the PG is active, *pg_lease_t* and *pg_lease_ack_t* messages are
regularly exchanged.  However, if a client request comes in and the
lease has expired (*readable_until* has passed), the PG will go into a
*LAGGY* state and request will be blocked.  Once the lease is renewed,
the request(s) will be requeued.

PG "wait" state
---------------

If peering completes but the prior interval's OSDs may still be
readable, the PG will go into the *WAIT* state until sufficient time
has passed.  Any OSD requests will block during that period.  Recovery
may proceed while in this state, since the logical, user-visible
content of objects does not change.

Dead OSDs
---------

Generally speaking, we need to wait until prior intervals' OSDs *know*
that they should no longer be readable.  If an OSD is known to have
crashed (e.g., because the process is no longer running, which we may
infer because we get a ECONNREFUSED error), then we can infer that it
is not readable.

Similarly, if an OSD is marked down, gets a map update telling it so,
and then informs the monitor that it knows it was marked down, we can
similarly infer that it is not still serving requests for a prior interval.

When a PG is in the *WAIT* state, it will watch new maps for OSDs'
*dead_epoch* value indicating they are aware of their dead-ness.  If
all down OSDs from prior interval are so aware, we can exit the WAIT
state early.
