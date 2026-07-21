============================
Design Document: RADOS fsync
============================

1. The problem with fsync
-------------------------
Fsync requires commiting to the MDS -- sending the MDS a message, waiting for it
to successfully journal to RADOS, and then getting an ack back. Besides the
added latency of going through the MDS, it generates a linear order on cluster
operations because the journal is strictly ordered. And because the MDS wants to
satisfy an fsync operation quickly, it stops batching operations and forces a
journal flush, lowering overall throughput.

In ordinary CephFS usage, this doesn't come up often, because most applications
do not fsync except at specific and direct need. But there are always edge cases,
and here the edge case is NFS. NFS can generate an outsized number of fsyncs in
two specific scenarios we've run across.
First, NFS clients can mount in "sync" mode, where every single write operation
is sent to the server (in Ceph's case, a Ganesha daemon -- but this is not
really specific to Ganesha) with a "stable=2" flag set. This flag means the
server must persist data and metadata reflecting the write to disk before
responding to the client.
Second, if you open a file with O_DIRECT, the Linux NFS client sets that
"stable=2" flag on all operations for the relevant file, in effect promoting
it to O_SYNC. This is extremely common behavior when running benchmarks, and is
also a common operations mode for databases.

2. What does fsync persist to the MDS?
--------------------------------------
It persists all metadata, of course. But the only metadata fields which can be
updated as part of a write() call are mtime, ctime, size and change_attr.

These fields are interesting, because mtime/ctime and size have a special
property in CephFS: we can in large part recover them from the OSDs.

3. MDS Probing
--------------
Whenever a CephFS client crashes, any files it held open and had write caps on
are added to the RecoveryQueue. Because the client may have updated these files
and changed their mtime or their size without informing the MDS, these files must
be "probed" before caps on them can be given to another client.

Probing is implemented in Filer::probe(). When you invoke a prob, it goes out
and invokes stat() on every object which might be part of the file (ie, from the
first object in the file up to the maximum size the client was allowed to expand
the file to). The object stat obviously lets us infer the size of the file and
include object mtime, which we can use to approximate the mtime/ctime of the file
as written by the client.

4. What's different after probing?
----------------------------------
The file size is inferred from the on-disk RADOS objects. As long as the
application actually writes data out to the logical EOF, we will set it correctly.
Happily, any truncate or setattr which changes the size of the file is always
directed synchronously to the MDS, so if we skip synchronizing the size of a file
it is always recoverable.

The mtime and ctime are also inferred from the RADOS-level mtime of the underlying
objects. Unfortunately, these times are NOT the same as the in-memory values
which the client might have returned on a stat(): the Client updates mtime/ctime
in Client::_write_success() after the write has completed (either into a buffer,
or directly to RADOS for synchronous writes).

(RADOS write operations accept an mtime, so we could alter the Client to generate
an mtime up-front, and attach it on the relevant writes.)

The change_attr is, unfortunately, not persisted and not recoverable from RADOS.
But I also don't know if it matters in the slightest -- stx_version is not yet
generally available, and a number of situations where Ganesha could rely on our
internal change_attr it instead substitutes in the ctime. Update rules in the
current implementation are also quite strange (individual clients with shared
caps can separately bump the change_attr, and the MDS tries to reconcile by
further bumping it when it receives those updates, but it's certainly not a
single consistent view like our other metadata is!).

5. The Proposal
---------------
We should have a config option that does not flush MDS capabilities on an fsync,
as long as the only dirty bits are those updated by a simple write() call:
mtime, ctime, size, and change_attr. We can rely on probe() to recover an
accurate size and a reasonable facsimile of the genuine mtime, and Ganesha as an
informed user can enable the config option. This is very straightforward:
in Client::_fsync(), we update the block which invokes check_caps() to simply
skip over if those are the only dirty caps.

There are some available further enhancements:
1) we can generate the mtime stamp upfront when doing a write(), and send that
   timestamp to the OSDs when doing the object write. This will ensure an
   entirely consistent view of the mtime and ctime. This is also pretty
   straightforward: all the places where the Client directly writes to RADOS via
   the ObjectCacher or Objecter, it provides an mtime with ceph::clock::now()!
   The trickiest bit will be updating the ObjectCacher interface to provide the
   mtime -- it internally stores a BufferHead::last_write value which is
   locally generated in ObjectCacher::writex(), but it is easy enough to make
   that an input parameter.
   This enhancment lets us preserve mtime exactly, but still does not preserve
   change_attr or let us avoid the MDS when flushing other metadata.
2) We can extend the Filer::probe() functionality and interfaces to support
   fsyncing directly to a RADOS object, and let the Client simply flush out
   the necessary file data and piggyback any metadata changes on top of that as
   xattr or omap writes to the file data objects which the MDS then recovers on
   Client crash. This would let us scale fsync capability directly with data
   IO capability.
   This enhancement lets us preserve all metadata without needing to do an MDS
   commit, but is significantly more complicated at all layers of the stack.

6. Why don't we already have these behaviors?
---------------------------------------------
To my knowledge, fsync has not itself been a specific bottleneck before. Far
more common were issues with file creates such as untar and rsync workloads, or
workloads which needed to sync across multiple clients. These enhancements would
not generally improve those situations: they specifically focus on the
performance of fsync, and anything that requires multi-client synchronization
will still need to propagate updates to the MDS and wait for it to commit them to
disk. (In fact, the implementations described here may incidentally *decrease*
performance in some multi-client workloads, because completing an fsync does not
mean that a different client will be able to see the update results -- instead,
the other client attempting to access the file will initiate a separate MDS
commit that it must wait for.)
