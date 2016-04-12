
Experimental Features
=====================

CephFS includes a number of experimental features which are not fully stabilized
or qualified for users to turn on in real deployments. We generally do our best
to clearly demarcate these and fence them off so they can't be used by mistake.

Some of these features are closer to being done than others, though. We describe
each of them with an approximation of how risky they are and briefly describe
what is required to enable them. Note that doing so will *irrevocably* flag maps
in the monitor as having once enabled this flag to improve debugging and
support processes.


Directory Fragmentation
-----------------------
CephFS directories are generally stored within a single RADOS object. But this has
certain negative results once they become large enough. The filesystem is capable
of "fragmenting" these directories into multiple objects. There are no known bugs
with doing so but it is not sufficiently tested to support at this time.

Directory fragmentation has always been off by default and required setting
```mds bal frag = true`` in the MDS' config file. It has been further protected
by requiring the user to set the "allow_dirfrags" flag for Jewel.

Inline data
-----------
By default, all CephFS file data is stored in RADOS objects. The inline data
feature enables small files (generally <2KB) to be stored in the inode
and served out of the MDS. This may improve small-file performance but increases
load on the MDS. It is not sufficiently tested to support at this time, although
failures within it are unlikely to make non-inlined data inaccessible

Inline data has always been off by default and requires setting
the "inline_data" flag.

Multi-MDS filesystem clusters
-----------------------------
CephFS has been designed from the ground up to support fragmenting the metadata
hierarchy across multiple active metadata servers, to allow horizontal scaling
to arbitrary throughput requirements. Unfortunately, doing so requires a lot
more working code than having a single MDS which is authoritative over the
entire filesystem namespace.

Multiple active MDSes are generally stable under trivial workloads, but often
break in the presence of any failure, and do not have enough testing to offer
any stability guarantees. If a filesystem with multiple active MDSes does
experience failure, it will require (generally extensive) manual intervention.
There are serious known bugs.

Multi-MDS filesystems have always required explicitly increasing the "max_mds"
value and have been further protected with the "allow_multimds" flag for Jewel.

Snapshots
---------
Like multiple active MDSes, CephFS is designed from the ground up to support
snapshotting of arbitrary directories. There are no known bugs at the time of
writing, but there is insufficient testing to provide stability guarantees and
every expansion of testing has generally revealed new issues. If you do enable
snapshots and experience failure, manual intervention will be needed.

Snapshotting was blocked off with the "allow_new_snaps" flag prior to Firefly.

Multiple filesystems within a Ceph cluster
------------------------------------------
Code was merged prior to the Jewel release which enables administrators
to create multiple independent CephFS filesystems within a single Ceph cluster.
These independent filesystems have their own set of active MDSes, cluster maps,
and data. But the feature required extensive changes to data structures which
are not yet fully qualified, and has security implications which are not all
apparent nor resolved.

There are no known bugs, but any failures which do result from having multiple
active filesystems in your cluster will require manual intervention and, so far,
will not have been experienced by anybody else -- knowledgeable help will be
extremely limited. You also probably do not have the security or isolation
guarantees you want or think you have upon doing so.

Multiple filesystems were available starting in the Jewel release candidates
but were protected behind the "enable_multiple" flag before the final release.
