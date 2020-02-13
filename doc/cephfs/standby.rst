.. _mds-standby:

Terminology
-----------

A Ceph cluster may have zero or more CephFS *file systems*.  CephFS
file systems have a human readable name (set in ``fs new``)
and an integer ID.  The ID is called the file system cluster ID,
or *FSCID*.

Each CephFS file system has a number of *ranks*, one by default,
which start at zero.  A rank may be thought of as a metadata shard.
Controlling the number of ranks in a file system is described
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

A ceph-mds daemons can be assigned to a particular file system by
setting the `mds_join_fs` configuration option to the file system
name.

Each time a daemon starts up, it is also assigned a *GID*, which
is unique to this particular process lifetime of the daemon.  The
GID is an integer.

Referring to MDS daemons
------------------------

Most of the administrative commands that refer to an MDS daemon
accept a flexible argument format that may contain a rank, a GID
or a name.

Where a rank is used, this may optionally be qualified with
a leading file system name or ID.  If a daemon is a standby (i.e.
it is not currently assigned a rank), then it may only be
referred to by GID or name.

For example, if we had an MDS daemon which was called 'myhost',
had GID 5446, and was assigned rank 0 in the file system 'myfs'
which had FSCID 3, then any of the following would be suitable
forms of the 'fail' command:

::

    ceph mds fail 5446     # GID
    ceph mds fail myhost   # Daemon name
    ceph mds fail 0        # Unqualified rank
    ceph mds fail 3:0      # FSCID and rank
    ceph mds fail myfs:0   # File System name and rank

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

.. _mds-join-fs:

Configuring MDS file system affinity
------------------------------------

You may want to have an MDS used for a particular file system. Or, perhaps you
have larger MDSs on better hardware that should be preferred over a last-resort
standby on lesser or over-provisioned hardware. To express this preference,
CephFS provides a configuration option for MDS called ``mds_join_fs`` which
enforces this `affinity`.

As part of any failover, the Ceph monitors will prefer standby daemons with
``mds_join_fs`` equal to the file system name with the failed rank.  If no
standby exists with ``mds_join_fs`` equal to the file system name, it will
choose a `vanilla` standby (no setting for ``mds_join_fs``) for the replacement
or any other available standby as a last resort. Note, this does not change the
behavior that ``standby-replay`` daemons are always selected before looking at
other standbys.

Even further, the monitors will regularly examine the CephFS file systems when
stable to check if a standby with stronger affinity is available to replace an
MDS with lower affinity. This process is also done for standby-replay daemons:
if a regular standby has stronger affinity than the standby-replay MDS, it will
replace the standby-replay MDS.

For example, given this stable and healthy file system:

::

    $ ceph fs dump
    dumped fsmap epoch 399
    ...
    Filesystem 'cephfs' (27)
    ...
    e399
    max_mds 1
    in      0
    up      {0=20384}
    failed
    damaged
    stopped
    ...
    [mds.a{0:20384} state up:active seq 239 addr [v2:127.0.0.1:6854/966242805,v1:127.0.0.1:6855/966242805]]

    Standby daemons:

    [mds.b{-1:10420} state up:standby seq 2 addr [v2:127.0.0.1:6856/2745199145,v1:127.0.0.1:6857/2745199145]]


You may set ``mds_join_fs`` on the standby to enforce your preference: ::

    $ ceph config set mds.b mds_join_fs cephfs

after automatic failover: ::

    $ ceph fs dump
    dumped fsmap epoch 405
    e405
    ...
    Filesystem 'cephfs' (27)
    ...
    max_mds 1
    in      0
    up      {0=10420}
    failed
    damaged
    stopped
    ...
    [mds.b{0:10420} state up:active seq 274 join_fscid=27 addr [v2:127.0.0.1:6856/2745199145,v1:127.0.0.1:6857/2745199145]]

    Standby daemons:

    [mds.a{-1:10720} state up:standby seq 2 addr [v2:127.0.0.1:6854/1340357658,v1:127.0.0.1:6855/1340357658]]

Note in the above example that ``mds.b`` now has ``join_fscid=27``. In this
output, the file system name from ``mds_join_fs`` is changed to the file system
identifier (27). If the file system is recreated with the same name, the
standby will follow the new file system as expected.

Finally, if the file system is degraded or undersized, no failover will occur
to enforce ``mds_join_fs``.
