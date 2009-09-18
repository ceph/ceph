#!/bin/sh

# run me from the root of a _linux_ git tree, and pass ceph tree root.
cephtree=$1
echo ceph tree at $cephtree.
target=$2
echo target is $target
test -d .git || exit 0
test -e include/linux/mm.h || exit 0
test -e $cephtree/src/kernel/super.h || exit 0

# copy into the tree
mkdir -p $target/ceph
mkdir $target/ceph/crush
cp $cephtree/src/kernel/Makefile $target/ceph
cp $cephtree/src/kernel/Kconfig $target/ceph
cp $cephtree/src/kernel/*.[ch] $target/ceph
cp $cephtree/src/kernel/crush/*.[ch] $target/ceph/crush
cp $cephtree/src/kernel/ceph.txt Documentation/filesystems

# build the patch sequence
git branch -D series_start
git branch series_start

# fs/staging
#git cherry-pick 5556036065d8b04b2f7dd439fbf0d710e295cd44

git add Documentation/filesystems/ceph.txt
git commit -s -F - <<EOF
ceph: documentation

Mount options, syntax.

EOF

git add $target/ceph/ceph_fs.h
git add $target/ceph/msgr.h
git add $target/ceph/rados.h
git commit -s -F - <<EOF
ceph: on-wire types

These headers describe the types used to exchange messages between the
Ceph client and various servers.  All types are little-endian and
packed.

Additionally, we define a few magic values to identify the current
version of the protocol(s) in use, so that discrepancies to be
detected on mount.

EOF

git add $target/ceph/types.h
git add $target/ceph/super.h
git add $target/ceph/ceph_ver.h
git add $target/ceph/ceph_debug.h
git commit -s -F - <<EOF
ceph: client types

We first define constants, types, and prototypes for the kernel client
proper.

A few subsystems are defined separately later: the MDS, OSD, and
monitor clients, and the messaging layer.

EOF

git add $target/ceph/buffer.h
git commit -s -F - <<EOF
ceph: ref counted buffer

struct ceph_buffer is a simple ref-counted buffer.  We transparently
choose between kmalloc for small buffers and vmalloc for large ones.

This is used for allocating memory for xattr data, among other things.

EOF

git add $target/ceph/super.c
git commit -s -F - <<EOF
ceph: super.c

Mount option parsing, client setup and teardown, and a few odds and
ends (e.g., statfs).

EOF


git add $target/ceph/inode.c
git commit -s -F - <<EOF
ceph: inode operations

Inode cache and inode operations.  We also include routines to
incorporate metadata structures returned by the MDS into the client
cache, and some helpers to deal with file capabilities and metadata
leases.  The bulk of that work is done by fill_inode() and
fill_trace().

EOF

git add $target/ceph/dir.c
git commit -s -F - <<EOF
ceph: directory operations

Directory operations, including lookup, are defined here.  We take
advantage of lookup intents when possible.  For the most part, we just
need to build the proper requests for the metadata server(s) and
pass things off to the mds_client.  

The results of most operations are normally incorporated into the
client's cache when the reply is parsed by ceph_fill_trace().
However, if the MDS replies without a trace (e.g., when retrying an
update after an MDS failure recovery), some operation-specific cleanup
may be needed.

We can validate cached dentries in two ways.  A per-dentry lease may
be issued by the MDS, or a per-directory cap may be issued that acts
as a lease on the entire directory.  In the latter case, a 'gen' value
is used to determine which dentries belong to the currently leased
directory contents.

We normally prepopulate the dcache and icache with readdir results.
This makes subsequent lookups and getattrs avoid any server
interaction.  It also lets us satisfy readdir operation by peeking at
the dcache IFF we hold the per-directory cap/lease, previously
performed a readdir, and haven't dropped any of the resulting
dentries.

EOF

git add $target/ceph/file.c
git commit -s -F - <<EOF
ceph: file operations

File open and close operations, and read and write methods that ensure
we have obtained the proper capabilities from the MDS cluster before
performing IO on a file.  We take references on held capabilities for
the duration of the read/write to avoid prematurely releasing them
back to the MDS.

We implement two main paths for read and write: one that is buffered
(and uses generic_aio_{read,write}), and one that is fully synchronous
and blocking (operating either on a __user pointer or, if O_DIRECT,
directly on user pages).

EOF

git add $target/ceph/addr.c
git commit -s -F - <<EOF
ceph: address space operations

The ceph address space methods are concerned primarily with managing
the dirty page accounting in the inode, which (among other things)
must keep track of which snapshot context each page was dirtied in,
and ensure that dirty data is written out to the OSDs in snapshort
order.

A writepage() on a page that is not currently writeable due to
snapshot writeback ordering constraints is ignored (it was presumably
called from kswapd).

EOF

git add $target/ceph/mds_client.h
git add $target/ceph/mds_client.c
git add $target/ceph/mdsmap.h
git add $target/ceph/mdsmap.c
git commit -s -F - <<EOF
ceph: MDS client

The MDS (metadata server) client is responsible for submitting
requests to the MDS cluster and parsing the response.  We decide which
MDS to submit each request to based on cached information about the
current partition of the directory hierarchy across the cluster.  A
stateful session is opened with each MDS before we submit requests to
it, and a mutex is used to control the ordering of messages within
each session.

An MDS request may generate two responses.  The first indicates the
operation was a success and returns any result.  A second reply is
sent when the operation commits to disk.  Note that locking on the MDS
ensures that the results of updates are visible only to the updating
client before the operation commits.  Requests are linked to the
containing directory so that an fsync will wait for them to commit.

If an MDS fails and/or recovers, we resubmit requests as needed.  We
also reconnect existing capabilities to a recovering MDS to
reestablish that shared session state.  Old dentry leases are
invalidated.

EOF

git add $target/ceph/osd_client.h
git add $target/ceph/osd_client.c
git add $target/ceph/osdmap.h
git add $target/ceph/osdmap.c
git commit -s -F - <<EOF
ceph: OSD client

The OSD client is responsible for reading and writing data from/to the
object storage pool.  This includes determining where objects are
stored in the cluster, and ensuring that requests are retried or
redirected in the event of a node failure or data migration.

If an OSD does not respond before a timeout expires, keepalive
messages are sent across the lossless, ordered communications channel
to ensure that any break in the TCP is discovered.  If the session
does reset, a reconnection is attempted and affected requests are
resent (by the message transport layer).

EOF

git add $target/ceph/crush/crush.h
git add $target/ceph/crush/crush.c
git add $target/ceph/crush/mapper.h
git add $target/ceph/crush/mapper.c
git add $target/ceph/crush/hash.h
git commit -s -F - <<EOF
ceph: CRUSH mapping algorithm

CRUSH is a pseudorandom data distribution function designed to map
inputs onto a dynamic hierarchy of devices, while minimizing the
extent to which inputs are remapped when the devices are added or
removed.  It includes some features that are specifically useful for
storage, most notably the ability to map each input onto a set of N
devices that are separated across administrator-defined failure
domains.  CRUSH is used to distribute data across the cluster of Ceph
storage nodes.

More information about CRUSH can be found in this paper:

    http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf

EOF

git add $target/ceph/mon_client.h
git add $target/ceph/mon_client.c
git commit -s -F - <<EOF
ceph: monitor client

The monitor cluster is responsible for managing cluster membership
and state.  The monitor client handles what minimal interaction
the Ceph client has with it: checking for updated versions of the
MDS and OSD maps, getting statfs() information, and unmounting.

EOF

git add $target/ceph/caps.c
git commit -s -F - <<EOF
ceph: capability management

The Ceph metadata servers control client access to inode metadata and
file data by issuing capabilities, granting clients permission to read
and/or write both inode field and file data to OSDs (storage nodes).
Each capability consists of a set of bits indicating which operations
are allowed.

If the client holds a *_SHARED cap, the client has a coherent value
that can be safely read from the cached inode.

In the case of a *_EXCL (exclusive) or FILE_WR capabilities, the client
is allowed to change inode attributes (e.g., file size, mtime), note
its dirty state in the ceph_cap, and asynchronously flush that
metadata change to the MDS.

In the event of a conflicting operation (perhaps by another client),
the MDS will revoke the conflicting client capabilities.

In order for a client to cache an inode, it must hold a capability
with at least one MDS server.  When inodes are released, release
notifications are batched and periodically sent en masse to the MDS
cluster to release server state.

EOF

git add $target/ceph/snap.c
git commit -s -F - <<EOF
ceph: snapshot management

Ceph snapshots rely on client cooperation in determining which
operations apply to which snapshots, and appropriately flushing
snapshotted data and metadata back to the OSD and MDS clusters.
Because snapshots apply to subtrees of the file hierarchy and can be
created at any time, there is a fair bit of bookkeeping required to
make this work.

Portions of the hierarchy that belong to the same set of snapshots
are described by a single 'snap realm.'  A 'snap context' describes
the set of snapshots that exist for a given file or directory.

EOF

git add $target/ceph/decode.h
git add $target/ceph/messenger.h
git add $target/ceph/messenger.c
git commit -s -F - <<EOF
ceph: messenger library

A generic message passing library is used to communicate with all
other components in the Ceph file system.  The messenger library
provides ordered, reliable delivery of messages between two nodes in
the system, or notifies the higher layer when it is unable to do so.

This implementation is based on TCP.

EOF

git add $target/ceph/msgpool.h
git add $target/ceph/msgpool.c
git commit -s -F - <<EOF
ceph: message pools

The msgpool is a basic mempool_t-like structure to preallocate
messages we expect to receive over the wire.  This ensures we have the
necessary memory preallocated to process replies to requests, or to
process unsolicited messages from various servers.

EOF

git add $target/ceph/export.c
git commit -s -F - <<EOF
ceph: nfs re-export support

Basic NFS re-export support is included.  This mostly works.  However,
Ceph's MDS design precludes the ability to generate a (small)
filehandle that will be valid forever, so this is of limited utility.

EOF

git apply $cephtree/src/kernel/ioctl-number.patch
git add $target/ceph/ioctl.h
git add $target/ceph/ioctl.c
git commit -s -F - <<EOF
ceph: ioctls

A few Ceph ioctls for getting and setting file layout (striping)
parameters, and learning the identity and network address of the OSD a
given region of a file is stored on.

EOF

git add $target/ceph/debugfs.c
git commit -s -F - <<EOF
ceph: debugfs

Basic state information is available via /sys/kernel/debug/ceph,
including instances of the client, fsids, current monitor, mds and osd
maps, outstanding server requests, and hooks to adjust debug levels.

EOF

#git apply $cephtree/src/kernel/kbuild.staging.patch
git apply $cephtree/src/kernel/kbuild.patch
git add $target/ceph/Makefile
git add $target/ceph/Kconfig
git commit -s -F - <<EOF $target/Kconfig $target/ceph/Kconfig $target/Makefile $target/ceph/Makefile
ceph: Kconfig, Makefile

Kconfig options and Makefile.

EOF


# build the patch files
mkdir out
rm out/*
git-format-patch -s -o out -n series_start..HEAD

cp 0000 out/0000
echo --- >> out/0000
git diff --stat series_start >> out/0000