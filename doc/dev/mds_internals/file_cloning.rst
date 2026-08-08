========================================================
Design Document: Fast File Cloning
========================================================

1. Intent and Goals
-------------------
We have two big problems right now in CephFS use cases.

1) The clone operation in mgr/volumes is a persistent sore spot, since users
   expect it to be a constant-time operation (from working with Netapp boxes, I
   think? Or just using rbd?) and it scales in the number of files and
   total data size.
2) For disaster recovery, we offer cephfs-mirror to transfer snapshots over the
   Internet. But no matter how good we make the mirror daemon, and no matter
   how fast a user’s interconnect is, you don’t have a consistent view in the
   live (HEAD) filesystem: we always transfer the snapshot incrementally, and
   once done we start on the next snapshot! So to recover, you have to do a
   full data copy out of the latest completed snapshot into the live filesystem,
   just like with clone.

We would like to fix these problems by introducing a "fast file cloning"
interface. This interface will clone an individual snapshotted file in constant
time, and a snapshotted subtree in time that is either constant or linear in
the number of files (but with a small metadata-only constant, rather than
needing to do work on the data portion of every file).

2. File layering design
----------------------
This system is inspired by rbd's cloning funtionality on a per-file basis, but
is tweaked to handle the needs of a filesystem (with many files, rather than a
single disk), and with new features to enable the necessary metadata management.
When cloning (a file or a subtree), we specify a snapshot source (file FOO at
snapshot BAR, FOO@BAR), and then a new file is layered on top of that source. We
will add and update metadata to indicate snapshots that are used as a clone source
and cannot be deleted, as well as to identify which clones are related to a given
snapshot.

2.1 Tracking Metadata
---------------------
I considered two approaches for relating a CLONE to a SOURCE inode.
1) A CLONE file indicates it clones from SOURCE, and on every open of CLONE, the
   MDS must also open SOURCE (and possibly a SOURCE~, if SOURCE is itself a
   clone.)
2) A CLONE inode contains the metadata needed to read the RADOS object data
   indicated by SOURCE, as well as any further SOURCE~ in the case of a layered
   clone.

These have very different tradeoffs, but ultimately I believe the CLONE inode
should contain all the information needed to read its file: the inode number,
snapid, file layout, and relevant overlap sizes of every ancestral SOURCE. The
downside to this approach is the size of that layering data (which scales with
the number of layers). But the downsides to opening up every layered inode are
worse: any open cloned inode requires an entire extra inode (in the disaster
recovery scenario, this straightforwardly doubles the amount of RAM used by the
MDS per inode, as there will be no further clones to amortize that cost over);
it keeps memory use more consistent per inode and more predictable even as
worklaods and common sources may change; and it minimizes the complexities of
needing to open and coordinate inodes across MDS ranks.

So, when a file is cloned, it tracks new metadata: the inode number and snapid
of its parent layer; the file layout; and the layered file size.
The SOURCE inode may need to track CLONE inodes, but I believe we will only
track that relationship on the SOURCE SnapRealm, for a single entry per clone.
These entries will be written to RADOS, but the in-memory SnapRealm will only
store the number of clones. (It is easy to envision a particular application
volume being cloned thousands of times, which is not a number we want to keep
in memory.)

Users cannot delete snapshots which have any outstanding CLONEs. CLONEs may
become fully promoted (see next section) so that the SOURCE is no longer
required to provide a basis for reads, at which point the CLONE will notify the
SOURCE.

2.2 File layout and I/O
-----------------------
File IO largely follows the pattern established by rbd: reads attempt to access
the file CLONE.<object_number>, and fall through to SOURCE.<object_number> if
the object does not exist. When writing to an offset, we perform a whole-object
COPYUP if <object_number> is not yet present in CLONE. (This COPYUP operation
is written with a blank SnapContext, so we maintain any relevant snapshot data
in the future when reading from that object directly.)
Like rbd, we want to maintain a bitmap of promoted objects, which will be stored
on the first object of the file and accessed as a new object class. When a file
is opened by a single client, that bitmap can be authoritative. But when in MIX
mode with multiple writers, or a writer and multiple readers, that won't work.
So our bitmap is a write-behind data structure -- clients which do a COPYUP
notify the bitmap. When the bitmap is fully populated, it notifies the client in
the message return, and the client is responsible for notifying the MDS. Clients
may read the bitmap and cache it when they have Fc caps on the file, but must
dispose of it when losing those caps.

If a client crashes, the MDS already has a recovery process for each file on
which the client held write capabiliites. This recovery process will be
extended to include checking the bitmap and promotion state from SOURCE.

This solution increases space inflation (as our unit of granularity is an
object rather than the byte range of differences) in exchange for making the
tracking metadata tractable. By storing the bitmap in RADOS and allowing (but
not requiring) clients to cache it, we reduce the memory required on the MDS.

2.2.1 File truncate
-------------------
When truncating a CLONE, we may change the bounds for which we should
logically fall through to SOURCE. If there are no intervening snapshots on
CLONE, we can simply change the overlap to the new size. If there are snapshots,
we still need to make sure we promote up SOURCE's file state when performing
subsequent writes. This should be very natural with the existing truncate
machinery.

3. Cloning operation
--------------------
Cloning operates by pointing at a SnapRealm (referenced as <path>@snapname) and
cloning it into a given new location. The initial implementation will simply
open up the given subtree and recursively generates a clone inode for every
inode it contains. We don't want to lock up the MDS rank (or cluster!) when a
clone request comes in, so this will need to partition the work and allow other
operations to interleave. We can use forward scrub as a model -- the workload
pattern is quite similar!

In the future, we can implement a more sophisticated operation that lazily
generates inodes only as their directory is actually opened, or even that only
generates the inodes as they are opened for write. However, this may not be
necessary and introduces a great deal of complexity. The designs I can come up
with also require allocating inode numbers at clone time, and if we aren't
fully reading the source that requires perfectly accurate recursive statistics
(which we don't have because rstats have lazy updates).

4. Further work
---------------
We will need client interfaces that do not fall through from CLONE to SOURCE so
that informed programs can look at only the relevant data. For instance, the
cephfs-mirror daemon will need to be able to identify and maintain clone
relationships, and access only the data at the top CLONE layer.

5. Weaknesses/Alternatives
-------------
This design accomplishes the core goals I specified up front. But there are two
things I had in mind that it doesn't do:
1) reflink functionality. Linux supports reflink, pointing multiple inodes to
   the same underlying blocks. This doesn't really advance us on that goal, but
   I was unable to make the metadata problem tractable for byte-granularity
   file relationships.
2) In the future, we may want to implement file movement functionality (ie,
   tiering between pools or storage media). This would be easier if we only
   needed to modify the SOURCE inode, but in this model we would also have to
   modify all CLONEs. Luckily, the two-way links mean this is not precluded.
