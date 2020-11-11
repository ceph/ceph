Progress Module
===============

The progress module is primarily used to inform users about the recovery progress
of PGs (Placement Groups) that are affected by events such as OSDs being marked in/out,
pg_autoscaler trying to match the target PG number.

In the ceph -s command you will see a Global Recovery Progress which tells us the
overall recovery progress of the PGs by counting the number of PGs that are active+clean. 

Enabling
--------

The *progress* module is enabled with::

  ceph progress on

Progress can be disabled at anytime with::

  ceph progress off 

Commands
--------

Shows the summary of all the ongoing/completed events and its duration::

  ceph progress

Shows the summary of ongoing/completed events in JSON format::

  ceph progress json


Clear all ongoing/completed events ::

  ceph progress clear

