
MDS States
==========


The Metadata Server (MDS) goes through several states during normal operation
in CephFS. For example, some states indicate that the MDS is recovering from a
failover by a previous instance of the MDS. Here we'll document all of these
states and include a state diagram to visualize the transitions.

State Descriptions
------------------

Common states
~~~~~~~~~~~~~~


::

    up:active

This is the normal operating state of the MDS. It indicates that the MDS
and its rank in the file system is available.


::

    up:standby

The MDS is available to takeover for a failed rank (see also :ref:`mds-standby`).
The monitor will automatically assign an MDS in this state to a failed rank
once available.


::

    up:standby_replay

The MDS is following the journal of another ``up:active`` MDS. Should the
active MDS fail, having a standby MDS in replay mode is desirable as the MDS is
replaying the live journal and will more quickly takeover. A downside to having
standby replay MDSs is that they are not available to takeover for any other
MDS that fails, only the MDS they follow.


Less common or transitory states
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


::

    up:boot

This state is broadcast to the Ceph monitors during startup. This state is
never visible as the Monitor immediately assign the MDS to an available rank or
commands the MDS to operate as a standby. The state is documented here for
completeness.


::

    up:creating

The MDS is creating a new rank (perhaps rank 0) by constructing some per-rank
metadata (like the journal) and entering the MDS cluster.


::

    up:starting

The MDS is restarting a stopped rank. It opens associated per-rank metadata
and enters the MDS cluster.


::

    up:stopping

When a rank is stopped, the monitors command an active MDS to enter the
``up:stopping`` state. In this state, the MDS accepts no new client
connections, migrates all subtrees to other ranks in the file system, flush its
metadata journal, and, if the last rank (0), evict all clients and shutdown
(see also :ref:`cephfs-administration`).


::

    up:replay

The MDS taking over a failed rank. This state represents that the MDS is
recovering its journal and other metadata.


::

    up:resolve

The MDS enters this state from ``up:replay`` if the Ceph file system has
multiple ranks (including this one), i.e. it's not a single active MDS cluster.
The MDS is resolving any uncommitted inter-MDS operations. All ranks in the
file system must be in this state or later for progress to be made, i.e. no
rank can be failed/damaged or ``up:replay``.


::

    up:reconnect

An MDS enters this state from ``up:replay`` or ``up:resolve``. This state is to
solicit reconnections from clients. Any client which had a session with this
rank must reconnect during this time, configurable via
``mds_reconnect_timeout``.


::

    up:rejoin

The MDS enters this state from ``up:reconnect``. In this state, the MDS is
rejoining the MDS cluster cache. In particular, all inter-MDS locks on metadata
are reestablished.

If there are no known client requests to be replayed, the MDS directly becomes
``up:active`` from this state.


::

    up:clientreplay

The MDS may enter this state from ``up:rejoin``. The MDS is replaying any
client requests which were replied to but not yet durable (not journaled).
Clients resend these requests during ``up:reconnect`` and the requests are
replayed once again. The MDS enters ``up:active`` after completing replay.


Failed states
~~~~~~~~~~~~~

::

    down:failed

No MDS actually holds this state. Instead, it is applied to the rank in the file system. For example:

::

    $ ceph fs dump
    ...
    max_mds 1
    in      0
    up      {}
    failed  0
    ...

Rank 0 is part of the failed set and is pending to be taken over by a standby
MDS. If this state persists, it indicates no suitable MDS daemons found to be
assigned to this rank. This may be caused by not enough standby daemons, or all
standby daemons have incompatible compat (see also :ref:`upgrade-mds-cluster`).


::

    down:damaged

No MDS actually holds this state. Instead, it is applied to the rank in the file system. For example:

::

    $ ceph fs dump
    ...
    max_mds 1
    in      0
    up      {}
    failed  
    damaged 0
    ...

Rank 0 has become damaged (see also :ref:`cephfs-disaster-recovery`) and placed in
the ``damaged`` set. An MDS which was running as rank 0 found metadata damage
that could not be automatically recovered. Operator intervention is required.


::

    down:stopped
    
No MDS actually holds this state. Instead, it is applied to the rank in the file system. For example:

::

    $ ceph fs dump
    ...
    max_mds 1
    in      0
    up      {}
    failed  
    damaged 
    stopped 1
    ...

The rank has been stopped by reducing ``max_mds`` (see also :ref:`cephfs-multimds`).

State Diagram
-------------

This state diagram shows the possible state transitions for the MDS/rank. The legend is as follows:

Color
~~~~~

- Green: MDS is active.
- Orange: MDS is in transient state trying to become active.
- Red: MDS is indicating a state that causes the rank to be marked failed.
- Purple: MDS and rank is stopping.
- Black: MDS is indicating a state that causes the rank to be marked damaged.

Shape
~~~~~

- Circle: an MDS holds this state.
- Hexagon: no MDS holds this state (it is applied to the rank).

Lines
~~~~~

- A double-lined shape indicates the rank is "in".

.. graphviz:: mds-state-diagram.dot
