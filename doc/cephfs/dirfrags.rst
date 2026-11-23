
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

How directories are stored
==========================

Each fragment of a directory is stored as one object in the file system's
metadata pool. The object's name is the directory's inode number and the
fragment ID, both in hexadecimal. For example, an unfragmented directory with
inode number ``0x10000000000`` is stored in the object
``10000000000.00000000``.

Each entry in the fragment is a key in the omap of that object. The key is the
entry's name followed by ``_head`` (or by a snapshot ID, for an entry kept for
a snapshot), and the value holds the entry's inode, or a link to the inode for
a hard link. The fragment's own metadata is stored in the omap header.

The number of entries in a fragment is therefore the number of keys in one
object's omap. An object with a very large omap is slow for the OSDs to read,
recover, and scrub. A deep scrub that finds an object with more than
``osd_deep_scrub_large_omap_object_key_threshold`` keys (200000 by default)
raises the ``LARGE_OMAP_OBJECTS`` health warning, so the warning appears only
after a deep scrub of that object's placement group. Splitting a large
directory into fragments keeps each object small, spreads the directory over
several objects and so over several placement groups and OSDs, and lets
different MDS ranks serve different fragments of a busy directory.
``mds_bal_split_size`` (described below) counts the entries kept for snapshots
as well as the current entries, so a fragment ordinarily splits long before
its object gets that large. ``mds_bal_fragment_size_max``, the hard limit,
counts only the current files and subdirectories.

The cost is that each fragment is a separate object to fetch and to write, and
that operations that need the whole directory, such as listing it, must load
every fragment.

The MDS places each entry in a fragment by a hash of the entry's name. A
fragment covers the hash values that start with a given set of bits. The
fragment ID holds the number of those bits in its top byte and their value in
its lower three bytes. A directory that has never been split has one fragment,
written ``*``, which covers every hash value and has the ID ``00000000``. When
that fragment is split with the default ``mds_bal_split_bits`` of 3, it is
replaced by eight fragments, written ``000*`` through ``111*``, each of which
holds the entries whose name hashes start with those three bits. Each box
below shows a fragment and the object that stores it:

.. mermaid::

   flowchart TB
       dir["Directory, inode 0x10000000000"]
       dir --> f0["000*<br/>10000000000.03000000"]
       dir --> f1["001*<br/>10000000000.03200000"]
       dir --> fx["010* to 110*<br/>10000000000.03400000 to .03c00000"]
       dir --> f7["111*<br/>10000000000.03e00000"]
       f0 --> o0["omap header: fragment metadata<br/>omap keys: name_head"]

To list the fragments of a directory, run the following command against the
MDS rank that holds the directory (rank ``0`` if the file system has one
active MDS). The directory must be in that MDS's cache:

.. prompt:: bash #

   ceph tell mds.<fsname>:<rank> dirfrag ls <path>

Each fragment is listed with its ``value``, its ``bits``, and ``str``, which
is ``<value>/<bits>`` with the value in hexadecimal. After the split above,
the first fragment is listed as ``0/3`` and the last as ``e00000/3``. To find
a fragment's object, put its ``bits`` in the top byte and its ``value`` in the
lower three bytes: ``e00000/3`` is stored in ``10000000000.03e00000``. To list
the entries in that fragment, run the following command:

.. prompt:: bash #

   rados -p <metadata_pool> listomapkeys 10000000000.03e00000

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

