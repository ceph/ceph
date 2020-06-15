=================================
CephFS Distributed Metadata Cache
=================================
While the data for inodes in a Ceph file system is stored in RADOS and
accessed by the clients directly, inode metadata and directory
information is managed by the Ceph metadata server (MDS). The MDS's
act as mediator for all metadata related activity, storing the resulting
information in a separate RADOS pool from the file data.

CephFS clients can request that the MDS fetch or change inode metadata
on its behalf, but an MDS can also grant the client **capabilities**
(aka **caps**) for each inode (see :doc:`/cephfs/capabilities`).

A capability grants the client the ability to cache and possibly
manipulate some portion of the data or metadata associated with the
inode. When another client needs access to the same information, the MDS
will revoke the capability and the client will eventually return it,
along with an updated version of the inode's metadata (in the event that
it made changes to it while it held the capability).

Clients can request capabilities and will generally get them, but when
there is competing access or memory pressure on the MDS, they may be
**revoked**. When a capability is revoked, the client is responsible for
returning it as soon as it is able. Clients that fail to do so in a
timely fashion may end up **blocklisted** and unable to communicate with
the cluster.

Since the cache is distributed, the MDS must take great care to ensure
that no client holds capabilities that may conflict with other clients'
capabilities, or operations that it does itself. This allows cephfs
clients to rely on much greater cache coherence than a filesystem like
NFS, where the client may cache data and metadata beyond the point where
it has changed on the server.

Client Metadata Requests
------------------------
When a client needs to query/change inode metadata or perform an
operation on a directory, it has two options. It can make a request to
the MDS directly, or serve the information out of its cache. With
CephFS, the latter is only possible if the client has the necessary
caps.

Clients can send simple requests to the MDS to query or request changes
to certain metadata. The replies to these requests may also grant the
client a certain set of caps for the inode, allowing it to perform
subsequent requests without consulting the MDS.

Clients can also request caps directly from the MDS, which is necessary
in order to read or write file data.

Distributed Locks in an MDS Cluster
-----------------------------------
When an MDS wants to read or change information about an inode, it must
gather the appropriate locks for it. The MDS cluster may have a series
of different types of locks on the given inode and each MDS may have
disjoint sets of locks.

If there are outstanding caps that would conflict with these locks, then
they must be revoked before the lock can be acquired. Once the competing
caps are returned to the MDS, then it can get the locks and do the
operation.

On a filesystem served by multiple MDS', the metadata cache is also
distributed among the MDS' in the cluster. For every inode, at any given
time, only one MDS in the cluster is considered **authoritative**. Any
requests to change that inode must be done by the authoritative MDS,
though non-authoritative MDS can forward requests to the authoritative
one.

Non-auth MDS' can also obtain read locks that prevent the auth MDS from
changing the data until the lock is dropped, so that they can serve
inode info to the clients.

The auth MDS for an inode can change over time as well. The MDS' will
actively balance responsibility for the inode cache amongst
themselves, but this can be overridden by **pinning** certain subtrees
to a single MDS.
