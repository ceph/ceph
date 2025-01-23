.. _mds-standby:

Terminology
-----------

A Ceph cluster may have zero or more CephFS *file systems*.  Each CephFS has
a human readable name (set at creation time with ``fs new``) and an integer
ID.  The ID is called the file system cluster ID, or *FSCID*.

Each CephFS file system has a number of *ranks*, numbered beginning with zero.
By default there is one rank per file system.  A rank may be thought of as a
metadata shard.  Management of ranks is described in :doc:`/cephfs/multimds` .

Each CephFS ``ceph-mds`` daemon starts without a rank.  It may be assigned one
by the cluster's monitors. A daemon may only hold one rank at a time, and only
give up a rank when the ``ceph-mds`` process stops.

If a rank is not associated with any daemon, that rank is considered ``failed``.
Once a rank is assigned to a daemon, the rank is considered ``up``.

Each ``ceph-mds`` daemon has a *name* that is assigned statically by the
administrator when the daemon is first configured.  Each daemon's *name* is
typically that of the hostname where the process runs.

A ``ceph-mds`` daemon may be assigned to a specific file system by
setting its ``mds_join_fs`` configuration option to the file system's
``name``.

When a ``ceph-mds`` daemon starts, it is also assigned an integer ``GID``,
which is unique to this current daemon's process.  In other words, when a
``ceph-mds`` daemon is restarted, it runs as a new process and is assigned a
*new* ``GID`` that is different from that of the previous process.

Referring to MDS daemons
------------------------

Most administrative commands that refer to a ``ceph-mds`` daemon (MDS)
accept a flexible argument format that may specify a ``rank``, a ``GID``
or a ``name``.

Where a ``rank`` is used, it  may optionally be qualified by
a leading file system ``name`` or ``GID``.  If a daemon is a standby (i.e.
it is not currently assigned a ``rank``), then it may only be
referred to by ``GID`` or ``name``.

For example, say we have an MDS daemon with ``name`` 'myhost' and
``GID`` 5446, and which is assigned ``rank`` 0 for the file system 'myfs'
with ``FSCID`` 3.  Any of the following are suitable forms of the ``fail``
command:

::

    ceph mds fail 5446     # GID
    ceph mds fail myhost   # Daemon name
    ceph mds fail 0        # Unqualified rank
    ceph mds fail 3:0      # FSCID and rank
    ceph mds fail myfs:0   # File System name and rank

Managing failover
-----------------

If an MDS daemon stops communicating with the cluster's monitors, the monitors
will wait ``mds_beacon_grace`` seconds (default 15) before marking the daemon as
*laggy*.  If a standby MDS is available, the monitor will immediately replace the
laggy daemon.

Each file system may specify a minimum number of standby daemons in order to be
considered healthy. This number includes daemons in the ``standby-replay`` state
waiting for a ``rank`` to fail. (Note, the monitors will not assign a
``standby-replay`` daemon to take over a failure for another ``rank`` or a
failure in a different CephFS file system). The pool of standby daemons not in
``replay`` counts towards any file system count.  Each file system may set the
desired number of standby daemons by:

::

    ceph fs set <fs name> standby_count_wanted <count>

Setting ``count`` to 0 will disable the health check.


.. _mds-standby-replay:

Configuring standby-replay
--------------------------

Each CephFS file system may be configured to add ``standby-replay`` daemons.
These standby daemons follow the active MDS's metadata journal in order to
reduce failover time in the event that the active MDS becomes unavailable. Each
active MDS may have only one ``standby-replay`` daemon following it.

Configuration of ``standby-replay`` on a file system is done using the below:

::

    ceph fs set <fs name> allow_standby_replay <bool>

Once set, the monitors will assign available standby daemons to follow the
active MDSs in that file system.

Once an MDS has entered the ``standby-replay`` state, it will only be used as a
standby for the ``rank`` that it is following. If another ``rank`` fails, this
``standby-replay`` daemon will not be used as a replacement, even if no other
standbys are available. For this reason, it is advised that if ``standby-replay``
is used then *every* active MDS should have a ``standby-replay`` daemon.

.. _mds-join-fs:

Configuring MDS file system affinity
------------------------------------

You might elect to dedicate an MDS to a particular file system. Or, perhaps you
have MDSs that run on better hardware that should be preferred over a last-resort
standby on modest or over-provisioned systems. To configure this preference,
CephFS provides a configuration option for MDS called ``mds_join_fs`` which
enforces this affinity.

When failing over MDS daemons, a cluster's monitors will prefer standby daemons with
``mds_join_fs`` equal to the file system ``name`` with the failed ``rank``.  If no
standby exists with ``mds_join_fs`` equal to the file system ``name``, it will
choose an unqualified standby (no setting for ``mds_join_fs``) for the replacement.
As a last resort, a standby for another filesystem will be chosen, although this
behavior can be disabled:

::

    ceph fs set <fs name> refuse_standby_for_another_fs true

Note, configuring MDS file system affinity does not change the behavior that
``standby-replay`` daemons are always selected before other standbys.

Even further, the monitors will regularly examine the CephFS file systems even when
stable to check if a standby with stronger affinity is available to replace an
MDS with lower affinity. This process is also done for ``standby-replay`` daemons:
if a regular standby has stronger affinity than the ``standby-replay`` MDS, it will
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
