.. _changing_monitor_elections:

=====================================
Configure Monitor Election Strategies
=====================================

By default, the monitors will use the classic option it has always used. We
recommend you stay in this mode unless you require features in the other
modes.

If you want to switch modes BEFORE constructing the cluster, change
the ``mon election default strategy`` option. This option is an integer value:

* 1 for "classic"
* 2 for "disallow"
* 3 for "connectivity"

Once your cluster is running, you can change strategies by running ::

  $ ceph mon set election_strategy {classic|disallow|connectivity}

Choosing a mode
===============
The modes other than classic provide different features. We recommend
you stay in classic mode if you don't need the extra features as it is
the simplest mode.

The disallow Mode
=================
This mode lets you mark monitors as disallowd, in which case they will
participate in the quorum and serve clients, but cannot be elected leader. You
may wish to use this if you have some monitors which are known to be far away
from clients.
You can disallow a leader by running ::

  $ ceph mon add disallowed_leader {name}

You can remove a monitor from the disallowed list, and allow it to become
a leader again, by running ::

  $ ceph mon rm disallowed_leader {name}

The list of disallowed_leaders is included when you run ::

  $ ceph mon dump

The connectivity Mode
=====================
This mode evaluates connection scores provided by each monitor for its
peers and elects the monitor with the highest score. This mode is designed
to handle netsplits, which may happen if your cluster is stretched across
multiple data centers or otherwise susceptible.

This mode also supports disallowing monitors from being the leader
using the same commands as above in disallow.

Examining connectivity scores
=============================
The monitors maintain connection scores even if they aren't in
the connectivity election mode. You can examine the scores a monitor
has by running ::

  ceph daemon mon.{name} connection scores dump

Scores for individual connections range from 0-1 inclusive, and also
include whether the connection is considered alive or dead (determined by
whether it returned its latest ping within the timeout).

While this would be an unexpected occurrence, if for some reason you experience
problems and troubleshooting makes you think your scores have become invalid,
you can forget history and reset them by running ::

  ceph daemon mon.{name} connection scores reset

While resetting scores has low risk (monitors will still quickly determine
if a connection is alive or dead, and trend back to the previous scores if they
were accurate!), it should also not be needed and is not recommended unless
requested by your support team or a developer.
