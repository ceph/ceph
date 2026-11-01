Progress Module
===============

The progress module reports the progress of long-running operations that
other Manager modules start, for example ``pg_autoscaler`` changing the number
of PGs in a pool. When the ``allow_pg_recovery_event`` option is on, it also
reports recovery after an OSD is marked ``in`` or ``out`` (see
`PG Recovery Event`_).

While at least one other progress event is open and some PGs are not in the
``active+clean`` state, the module also opens a "Global Recovery Event", which
``ceph status`` (or ``ceph -s``) shows under ``progress:``. Its progress bar is
based on the number of PGs that are in the ``active+clean`` state.

Enabling
--------

The *progress* module is an always-on Manager module, and its event tracking is
on by default. To turn event tracking back on after it was turned off, run the
following command:

.. prompt:: bash #

   ceph progress on

To turn event tracking off, run the following command. This also clears all
ongoing and completed events, and while tracking is off the module ignores
progress updates from other Manager modules:

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

When the ``allow_pg_recovery_event`` option is on, the module opens one event
for each OSD that is marked ``in`` or ``out``, named "Rebalancing after
osd.<id> marked out" (or "marked in"). The event follows the recovery of the
PGs whose up set changed, and ``ceph progress`` shows it.
This is optional, and is disabled by default due to CPU overhead that may
adversely affect the Monitors:

.. prompt:: bash #

   ceph config set mgr mgr/progress/allow_pg_recovery_event true
