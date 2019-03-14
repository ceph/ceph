.. _mds-standby:

Terminology
-----------

A Ceph cluster may have zero or more CephFS *filesystems*.  CephFS
filesystems have a human readable name (set in ``fs new``)
and an integer ID.  The ID is called the filesystem cluster ID,
or *FSCID*.

Each CephFS filesystem has a number of *ranks*, one by default,
which start at zero.  A rank may be thought of as a metadata shard.
Controlling the number of ranks in a filesystem is described
in :doc:`/cephfs/multimds`

Each CephFS ceph-mds process (a *daemon*) initially starts up
without a rank.  It may be assigned one by the monitor cluster.
A daemon may only hold one rank at a time.  Daemons only give up
a rank when the ceph-mds process stops.

If a rank is not associated with a daemon, the rank is
considered *failed*.  Once a rank is assigned to a daemon,
the rank is considered *up*.

A daemon has a *name* that is set statically by the administrator
when the daemon is first configured.  Typical configurations
use the hostname where the daemon runs as the daemon name.

Each time a daemon starts up, it is also assigned a *GID*, which
is unique to this particular process lifetime of the daemon.  The
GID is an integer.

Referring to MDS daemons
------------------------

Most of the administrative commands that refer to an MDS daemon
accept a flexible argument format that may contain a rank, a GID
or a name.

Where a rank is used, this may optionally be qualified with
a leading filesystem name or ID.  If a daemon is a standby (i.e.
it is not currently assigned a rank), then it may only be
referred to by GID or name.

For example, if we had an MDS daemon which was called 'myhost',
had GID 5446, and was assigned rank 0 in the filesystem 'myfs'
which had FSCID 3, then any of the following would be suitable
forms of the 'fail' command:

::

    ceph mds fail 5446     # GID
    ceph mds fail myhost   # Daemon name
    ceph mds fail 0        # Unqualified rank
    ceph mds fail 3:0      # FSCID and rank
    ceph mds fail myfs:0   # Filesystem name and rank

Managing failover
-----------------

If an MDS daemon stops communicating with the monitor, the monitor will wait
``mds_beacon_grace`` seconds (default 15 seconds) before marking the daemon as
*laggy*. If a standby is available, the monitor will immediately replace the
laggy daemon.

Each file system may specify a number of standby daemons to be considered
healthy. This number includes daemons in standby-replay waiting for a rank to
fail (remember that a standby-replay daemon will not be assigned to take over a
failure for another rank or a failure in a another CephFS file system). The
pool of standby daemons not in replay count towards any file system count.
Each file system may set the number of standby daemons wanted using:

::

    ceph fs set <fs name> standby_count_wanted <count>

Setting ``count`` to 0 will disable the health check.


.. _mds-standby-replay:

Configuring standby-replay
--------------------------

Each CephFS file system may be configured to add standby-replay daemons.  These
standby daemons follow the active MDS's metadata journal to reduce failover
time in the event the active MDS becomes unavailable. Each active MDS may have
only one standby-replay daemon following it.

Configuring standby-replay on a file system is done using:

::

    ceph fs set <fs name> allow_standby_replay <bool>

Once set, the monitors will assign available standby daemons to follow the
active MDSs in that file system.

Once an MDS has entered the standby-replay state, it will only be used as a
standby for the rank that it is following. If another rank fails, this
standby-replay daemon will not be used as a replacement, even if no other
standbys are available. For this reason, it is advised that if standby-replay
is used then every active MDS should have a standby-replay daemon.
