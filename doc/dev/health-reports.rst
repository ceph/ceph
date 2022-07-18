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

.. seqdiag::

   seqdiag {
     default_note_color = lightblue;
     osd; mon; ceph-cli;
     osd  => mon [ label = "update osdmap service" ];
     osd  => mon [ label = "update osdmap service" ];
     ceph-cli  -> mon [ label = "send 'health' command" ];
     mon -> mon [ leftnote = "gather checks from services" ];
     ceph-cli <-- mon [ label = "checks and mutes" ];
   }

.. seqdiag::

   seqdiag {
     default_note_color = lightblue;
     osd; mon; mgr; mgr-module;
     mgr  -> mon [ label = "subscribe for 'mgrdigest'" ];
     osd  => mon [ label = "update osdmap service" ];
     osd  => mon [ label = "update osdmap service" ];
     mon  -> mgr [ label = "send MMgrDigest" ];
     mgr  -> mgr [ note = "update cluster state" ];
     mon <-- mgr;
     mgr-module  -> mgr [ label = "mgr.get('health')" ];
     mgr-module <-- mgr [ label = "heath reports in json" ];
   }

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

.. seqdiag::

   seqdiag {
     default_note_color = lightblue;
     mds; mon-mds; mon-health; ceph-cli;
     mds  -> mon-mds [ label = "send beacon" ];
     mon-mds -> mon-mds [ note = "store health metrics in beacon" ];
     mds <-- mon-mds;
     mon-mds -> mon-mds [ note = "encode_health(checks)" ];
     ceph-cli -> mon-health [ label = "send 'health' command" ];
     mon-health => mon-mds [ label = "gather health checks" ];
     ceph-cli <-- mon-health [ label = "checks and mutes" ];
   }

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

.. seqdiag::

   seqdiag {
     default_note_color = lightblue;
     service; mgr; mon-mgr-stat; mon-health;
     service -> mgr [ label = "send(open)" ];
     mgr -> mgr [ note = "register the new service" ];
     service <-- mgr;
     mgr => service [ label = "send(configure)" ];
     service -> mgr [ label = "send(report)" ];
     mgr -> mgr [ note = "update/aggregate service metrics" ];
     service <-- mgr;
     service => mgr [ label = "send(report)" ];
     mgr -> mon-mgr-stat [ label = "send(mgr-report)" ];
     mon-mgr-stat -> mon-mgr-stat [ note = "store health checks in the report" ];
     mgr <-- mon-mgr-stat;
     mon-health => mon-mgr-stat [ label = "gather health checks" ];
     service => mgr [ label = "send(report)" ];
     service => mgr [ label = "send(close)" ];
   }
