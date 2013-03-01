======
Snaps
======

Overview
--------
Rados supports two related snapshotting mechanisms:
  1. *pool snaps*: snapshots are implicitely applied to all objects
     in a pool
  2. *self managed snaps*: the user must provide the current *SnapContext*
     on each write.

These two are mutually exclusive, only one or the other can be used on
a particular pool.

The *SnapContext* is the set of snapshots currently defined for an object
as well as the most recent snapshot (the *seq*) requested from the mon for
sequencing purposes (a *SnapContext* with a newer *seq* is considered to
be more recent).

The difference between *pool snaps* and *self managed snaps* from the
OSD's point of view lies in whether the *SnapContext* comes to the OSD
via the client's MOSDOp or via the most recent OSDMap.

See OSD::make_writeable

Ondisk Structures
-----------------
Each object has in the pg collection a *head* object (or *snapdir*, which we
will come to shortly) and possibly a set of *clone* objects.
Each hobject_t has a snap field.  For the *head* (the only writeable version
of an object), the snap field is set to CEPH_NOSNAP.  For the *clones*, the
snap field is set to the *seq* of the *SnapContext* at their creation.
When the OSD services a write, it first checks whether the most recent
*clone* is tagged with a snapid prior to the most recent snap represented
in the *SnapContext*.  If so, at least one snapshot has occurred between
the time of the write and the time of the last clone.  Therefore, prior
to performing the mutation, the OSD creates a new clone for servicing
reads on snaps between the snapid of the last clone and the most recent
snapid.

The *head* object contains a *SnapSet* encoded in an attribute, which tracks
  1. The full set of snaps defined for the object
  2. The full set of clones which currently exist
  3. Overlapping intervals between clones for tracking space usage
  4. Clone size

If the *head* is deleted while there are still clones, a *snapdir* object
is created instead to house the *SnapSet*.

Addionally, the *object_info_t* on each clone includes a vector of snaps
for which clone is defined.

Snap Removal
------------
To remove a snapshot, a request is made to the *Monitor* cluster to
add the snapshot id to the list of purged snaps (or to remove it from
the set of pool snaps in the case of *pool snaps*).  In either case,
the *PG* adds the snap to its *snaptrimq* for trimming.

A clone can be removed when all of its snaps have been removed.  In
order to determine which clones might need to be removed upon snap
removal, we maintain a mapping from snap to *hobject_t* using the
*snap collections*.  coll_t(<pgid>, <snapid>) constructs a collection
identifier for the <snapid> snap collection for <pgid>.  This contains
a hard link to each clone with <snapid> as either snaps[0] or
snaps[snaps.size() - 1].  Trimming a snap therefore involves scanning
the contents of the corresponding snap collection and updating the
object_info attribute and corresponding snap collection hard links
on each object by filtering out now removed snaps and updating the
snap collections accordingly (removing the ones which now have no
remaining snaps).

See ReplicatedPG::SnapTrimmer

This trimming is performed asynchronously by the snap_trim_wq while the
pg is clean and not scrubbing.
  1. The next snap in PG::snaptrimq is scanned for objects
  2. For each object, we create a log entry and repop updating the
     object info and the snap set (including adjusting the overlaps)
  3. Locally, the primary also updates the snap collection hardlinks.
  4. The log entry containing the modification of the object also
     contains the new set of snaps, which the replica uses to update
		 its hardlinks.
  5. Once the snap is fully trimmed, the primary locally removes the
     snap collection and adds the snap to pg_info_t::purged_snaps.
  6. The primary shares the info with the replica, which uses the
     new items in purged_snaps to update its own snap collections.

Recovery
--------
Because the trim operations are implemented using repops and log entries,
normal pg peering and recovery maintain the snap trimmer operations with
the caveat that push and removal operations need to take care to remove
or create the snap links ensuring that the snap collection links always
match the clone object_info snaps member.  The only thing it doesn't
entirely handle is removing empty snap collections, thus
PG::adjust_local_snaps is called when the replica is recovered to clean
up any now empty snap collections.

Split
-----
The snap collections are split in the same way as the head collection.
