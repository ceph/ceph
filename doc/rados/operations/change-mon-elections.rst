.. _changing_monitor_elections:

=======================================
Configuring Monitor Election Strategies
=======================================

By default, the monitors are in ``classic`` mode. We recommend staying in this
mode unless you have a very specific reason.

If you want to switch modes BEFORE constructing the cluster, change the ``mon
election default strategy`` option. This option takes an integer value:

* ``1`` for ``classic``
* ``2`` for ``disallow``
* ``3`` for ``connectivity``

After your cluster has started running, you can change strategies by running a
command of the following form:

  $ ceph mon set election_strategy {classic|disallow|connectivity}

Choosing a mode
===============

The modes other than ``classic`` provide specific features. We recommend staying
in ``classic`` mode if you don't need these extra features because it is the
simplest mode.

.. _rados_operations_disallow_mode:

Disallow Mode
=============

The ``disallow`` mode allows you to mark monitors as disallowed. Disallowed
monitors participate in the quorum and serve clients, but cannot be elected
leader. You might want to use this mode for monitors that are far away from
clients.

To disallow a monitor from being elected leader, run a command of the following
form:

.. prompt:: bash $

   ceph mon add disallowed_leader {name}

To remove a monitor from the disallowed list and allow it to be elected leader,
run a command of the following form:

.. prompt:: bash $

   ceph mon rm disallowed_leader {name}

To see the list of disallowed leaders, examine the output of the following
command:

.. prompt:: bash $

   ceph mon dump

Connectivity Mode
=================

The ``connectivity`` mode evaluates connection scores that are provided by each
monitor for its peers and elects the monitor with the highest score. This mode
is designed to handle network partitioning (also called *net-splits*): network
partitioning might occur if your cluster is stretched across multiple data
centers or otherwise has a non-uniform or unbalanced network topology.

The ``connectivity`` mode also supports disallowing monitors from being elected
leader by using the same commands that were presented in :ref:`Disallow Mode <rados_operations_disallow_mode>`.

Examining connectivity scores
=============================

The monitors maintain connection scores even if they aren't in ``connectivity``
mode. To examine a specific monitor's connection scores, run a command of the
following form:

.. prompt:: bash $

   ceph daemon mon.{name} connection scores dump

Scores for an individual connection range from ``0`` to ``1`` inclusive and
include whether the connection is considered alive or dead (as determined by
whether it returned its latest ping before timeout).

Connectivity scores are expected to remain valid. However, if during
troubleshooting you determine that these scores have for some reason become
invalid, drop the history and reset the scores by running a command of the
following form:

.. prompt:: bash $

   ceph daemon mon.{name} connection scores reset

Resetting connectivity scores carries little risk: monitors will still quickly
determine whether a connection is alive or dead and trend back to the previous
scores if those scores were accurate. Nevertheless, resetting scores ought to
be unnecessary and it is not recommended unless advised by your support team
or by a developer.
