
===================================
Configuring Directory fragmentation
===================================

In CephFS, directories are *fragmented* when they become very large
or very busy.  This splits up the metadata so that it can be shared
between multiple MDS daemons, and between multiple objects in the
metadata pool.

In normal operation, directory fragmentation is invisible to
users and administrators, and all the configuration settings mentioned
here should be left at their default values.

While directory fragmentation enables CephFS to handle very large
numbers of entries in a single directory, application programmers should
remain conservative about creating very large directories, as they still
have a resource cost in situations such as a CephFS client listing
the directory, where all the fragments must be loaded at once.

.. tip:: The root directory cannot be fragmented.

All directories are initially created as a single fragment.  This fragment
may be *split* to divide up the directory into more fragments, and these
fragments may be *merged* to reduce the number of fragments in the directory.

Splitting and merging
=====================

When an MDS identifies a directory fragment to be split, it does not
do the split immediately.  Because splitting interrupts metadata I/O,
a short delay is used to allow short bursts of client I/O to complete
before the split begins.  This delay is configured with
``mds_bal_fragment_interval``, which defaults to 5 seconds.

When the split is done, the directory fragment is broken up into
a power of two number of new fragments.  The number of new
fragments is given by two to the power ``mds_bal_split_bits``, i.e.
if ``mds_bal_split_bits`` is 2, then four new fragments will be
created.  The default setting is 3, i.e. splits create 8 new fragments.

The criteria for initiating a split or a merge are described in the
following sections.

Size thresholds
===============

A directory fragment is eligible for splitting when its size exceeds
``mds_bal_split_size`` (default 10000 directory entries).  Ordinarily this
split is delayed by ``mds_bal_fragment_interval``, but if the fragment size
exceeds a factor of ``mds_bal_fragment_fast_factor`` the split size,
the split will happen immediately (holding up any client metadata
I/O on the directory).

``mds_bal_fragment_size_max`` is the hard limit on the size of
directory fragments.  If it is reached, clients will receive
ENOSPC errors if they try to create files in the fragment.  On
a properly configured system, this limit should never be reached on
ordinary directories, as they will have split long before.  By default,
this is set to 10 times the split size, giving a dirfrag size limit of
100000 directory entries.  Increasing this limit may lead to oversized
directory fragment objects in the metadata pool, which the OSDs may not
be able to handle.

A directory fragment is eligible for merging when its size is less
than ``mds_bal_merge_size``.  There is no merge equivalent of the
"fast splitting" explained above: fast splitting exists to avoid
creating oversized directory fragments, there is no equivalent issue
to avoid when merging.  The default merge size is 50 directory entries.

Size thresholds in bytes
------------------------

A directory fragment is also eligible for splitting when the omap values of
its object in the metadata pool add up to more than ``mds_bal_split_bytes``
(default 512 MiB), however few entries it has.  Snapshots are the usual
cause: after a snapshot, each change to a directory, or to a file with hard
links, keeps a copy of its previous inode, including its extended
attributes, in its existing entry.  The entries grow with every snapshot
while their number stays the same.  Without this threshold, the fragment's
object can grow past ``osd_deep_scrub_large_omap_object_value_sum_threshold``
(default 1 GiB) and raise the ``LARGE_OMAP_OBJECTS`` health warning, so keep
``mds_bal_split_bytes`` below that threshold.  As with entries, a fragment
over ``mds_bal_fragment_fast_factor`` times ``mds_bal_split_bytes`` is split
immediately.  Setting ``mds_bal_split_bytes`` to 0 disables the check.

When ``mds_bal_split_bytes`` is set, a merge must also not produce a fragment
that holds more than ``mds_bal_split_bytes`` divided by two to the power
``mds_bal_split_bits`` (64 MiB by default), which is about what each new
fragment holds after a split on bytes.  A larger merged fragment would undo
the split.

The MDS keeps each fragment's total in the fragment's header, and updates it
as changes are journaled.  The total can be higher than what the fragment's
object holds: for example, when the MDS writes an entry back after the
snapshots it was keeping copies for are removed, it drops those copies
without lowering the total.  A higher total can only make a split happen
earlier.

The total can also be unknown: for fragments written by an older version of
Ceph, and for fragments that the offline recovery tools
(``cephfs-data-scan``, ``cephfs-journal-tool``) write to.  A fragment whose
total is unknown is not split on bytes, and not merged at all, until a scrub
with ``repair`` sets its total (see :ref:`mds-scrub`).  After upgrading,
run a recursive scrub with ``repair`` so that existing directories get their
totals::

    ceph tell mds.<fsname>:0 scrub start / recursive,repair

Activity thresholds
===================

In addition to splitting fragments based
on their size, the MDS may split directory fragments if their
activity exceeds a threshold.

The MDS maintains separate time-decaying load counters for read and write
operations on directory fragments.  The decaying load counters have an
exponential decay based on the ``mds_decay_halflife`` setting.

On writes, the write counter is
incremented, and compared with ``mds_bal_split_wr``, triggering a
split if the threshold is exceeded.  Write operations include metadata I/O
such as renames, unlinks and creations.

The ``mds_bal_split_rd`` threshold is applied based on the read operation
load counter, which tracks readdir operations.

The ``mds_bal_split_rd`` and ``mds_bal_split_wr`` configs represent the
popularity threshold. In the MDS these are measured as "read/write temperatures"
which is closely related to the number of respective read/write operations.
By default, the read threshold is 25000 operations and the write
threshold is 10000 operations, i.e. 2.5x as many reads as writes would be
required to trigger a split.

After fragments are split due to the activity thresholds, they are only
merged based on the size threshold (``mds_bal_merge_size``), so 
a spike in activity may cause a directory to stay fragmented
forever unless some entries are unlinked.

