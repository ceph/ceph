==============
Health Reports
==============


How to Get Reports
==================

In general, there are two channels to retrieve the health reports:

ceph (CLI)
   which sends ``health`` mon command for retrieving the health status of the cluster
mgr module
   which calls ``mgr.get('health')`` for the same report in the form of a JSON encoded string

The following diagrams outline the involved parties and how the interact when the clients
query for the reports:


Where are the Reports Generated
===============================

Aggregator of Aggregators
-------------------------

Health reports are aggregated from multiple Paxos services:

- AuthMonitor
- HealthMonitor
- MDSMonitor
- MgrMonitor
- MgrStatMonitor
- MonmapMonitor
- OSDMonitor

When persisting the pending changes in their own domain, each of them identifies the
health related issues and store them into the monstore with the prefix of ``health``
using the same transaction. For instance, ``OSDMonitor`` checks a pending new osdmap
for possible issues, like down OSDs and missing scrub flag in a pool, and then stores
the encoded form of the health reports along with the new osdmap. These reports are
later loaded and decoded, so they can be collected on demand. When it comes to
``MDSMonitor``, it persists the health metrics in the beacon sent by the MDS daemons,
and prepares health reports when storing the pending changes.


So, if we want to add a new warning related to cephfs, probably the best place to
start is ``MDSMonitor::encode_pending()``, where health reports are collected from
the latest ``FSMap`` and the health metrics reported by MDS daemons.

But it's noteworthy that ``MgrStatMonitor`` does *not* prepare the reports by itself,
it just stores whatever the health reports received from mgr!

ceph-mgr -- A Delegate Aggregator
---------------------------------

In Ceph, mgr is created to share the burden of monitor, which is used to establish
the consensus of information which is critical to keep the cluster function.
Apparently, osdmap, mdsmap and monmap fall into this category. But what about the
aggregated statistics of the cluster? They are crucial for the administrator to
understand the status of the cluster, but they might not be that important to keep
the cluster running. To address this scalability issue,  we offloaded the work of
collecting and aggregating the metrics to mgr.

Now, mgr is responsible for receiving and processing the ``MPGStats`` messages from
OSDs. And we also developed a protocol allowing a daemon to periodically report its
metrics and status to mgr using ``MMgrReport``. On the mgr side, it periodically sends
an aggregated report to the ``MgrStatMonitor`` service on mon. As explained earlier,
this service just persists the health reports in the aggregated report to the monstore.

