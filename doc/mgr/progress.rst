Progress Module
===============

The progress module is used to inform users about the recovery progress of PGs
(Placement Groups) that are affected by events such as (1) OSDs being marked
in or out and (2) ``pg_autoscaler`` trying to match the target PG number.

The ``ceph -s`` command returns something called " Global Recovery Progress",
which reports the overall recovery progress of PGs and is based on the number
of PGs that are in the ``active+clean`` state.

Enabling
--------

The *progress* module is enabled by default, but it can be enabled manually by
running the following command:

.. prompt:: bash #

  ceph progress on

The module can be disabled at anytime by running the following command:

.. prompt:: bash #

  ceph progress off

Commands
--------

Show the summary of all the ongoing and completed events and their duration:

.. prompt:: bash #

  ceph progress

Show the summary of ongoing and completed events in JSON format:

.. prompt:: bash #

  ceph progress json

Clear all ongoing and completed events:

.. prompt:: bash #

  ceph progress clear

PG Recovery Event
-----------------

An event for each PG affected by recovery event can be shown in
`ceph progress` This is completely optional, and disabled by default
due to CPU overheard:

.. prompt:: bash #

  ceph config set mgr mgr/progress/allow_pg_recovery_event true
