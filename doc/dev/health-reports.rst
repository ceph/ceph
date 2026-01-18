==============
Health Reports
==============


How to Get Reports
==================

In general, there are two channels to retrieve the health reports:

ceph (CLI)
   The ``ceph`` CLI command sends the ``health`` Monitor command for retrieving
   the health status of the cluster.
Manager module
   A Manager module calls the ``mgr.get('health')`` method for the same report
   in the form of a JSON encoded string.


Where are the Reports Generated
===============================

Monitor: Aggregator of Aggregators
----------------------------------

Monitor aggregates health reports from multiple Paxos services:

- ``AuthMonitor``
- ``HealthMonitor``
- ``MDSMonitor``
- ``MgrMonitor``
- ``MgrStatMonitor``
- ``MonmapMonitor``
- ``OSDMonitor``

When each of the Paxos services persist the pending changes in their own domain,
health-related issues are identified and stored into monstore with the prefix ``health``
using the same transaction. For instance:

- ``OSDMonitor`` checks a pending osdmap for possible issues such as
  ``down`` OSDs and a missing scrub flag in a pool and then stores
  the encoded form of the health reports along with the new osdmap. These reports are
  later loaded and decoded, so they can be collected on demand.
- ``MDSMonitor`` persists the health metrics contained in the beacon sent by the MDS daemons
  and prepares health reports when storing the pending changes.

To add a new warning related to CephFS, for example, a good place to
start is ``MDSMonitor::encode_pending()``, where health reports are collected from
the latest ``FSMap`` and the health metrics reported by MDS daemons.

It is noteworthy that ``MgrStatMonitor`` does not prepare health reports. It
receives aggregated reports from the Manager and then persists them to monstore.


Manager: a Delegate Aggregator
------------------------------

Monitor establishes consensus information including osdmap, mdsmap and monmap
which is critical for cluster functioning. Aggregated statistics of the cluster
are crucial for the administrator to understand the status of the cluster but
they are not critical for cluster functioning. For scalability reasons they are
offloaded to Manager which collects and aggregates the metrics.

Manager receives and processes ``MPGStats`` messages from OSDs. Daemons also
report metrics and status periodically to Manager using ``MMgrReport``. An
aggregated report is then sent periodically to the Monitor ``MgrStatMonitor``
service which persists the data to monstore.

