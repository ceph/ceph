=====================================
 Configuring Monitor/OSD Interaction
=====================================

.. index:: heartbeat

After you have completed your initial Ceph configuration, you may deploy and run
Ceph.  When you execute a command such as ``ceph health`` or ``ceph -s``,  the
:term:`Ceph Monitor` reports on the current state of the :term:`Ceph Storage
Cluster`. The Ceph Monitor knows about the Ceph Storage Cluster by requiring
reports from each :term:`Ceph OSD Daemon`, and by receiving reports from Ceph
OSD Daemons about the status of their neighboring Ceph OSD Daemons. If the Ceph
Monitor doesn't receive reports, or if it receives reports of changes in the
Ceph Storage Cluster, the Ceph Monitor updates the status of the :term:`Ceph
Cluster Map`.

Ceph provides reasonable default settings for Ceph Monitor/Ceph OSD Daemon
interaction. However, you may override the defaults. The following sections
describe how Ceph Monitors and Ceph OSD Daemons interact for the purposes of
monitoring the Ceph Storage Cluster.

.. index:: heartbeat interval

OSDs Check Heartbeats
=====================

Each Ceph OSD Daemon checks the heartbeat of other Ceph OSD Daemons at random
intervals less than every 6 seconds.  If a neighboring Ceph OSD Daemon doesn't
show a heartbeat within a 20 second grace period, the Ceph OSD Daemon may
consider the neighboring Ceph OSD Daemon ``down`` and report it back to a Ceph
Monitor, which will update the Ceph Cluster Map. You may change this grace
period by adding an ``osd heartbeat grace`` setting under the ``[mon]``
and ``[osd]`` or ``[global]`` section of your Ceph configuration file,
or by setting the value at runtime.


.. ditaa::
           +---------+          +---------+
           |  OSD 1  |          |  OSD 2  |
           +---------+          +---------+
                |                    |
                |----+ Heartbeat     |
                |    | Interval      |
                |<---+ Exceeded      |
                |                    |
                |       Check        |
                |     Heartbeat      |
                |------------------->|
                |                    |
                |<-------------------|
                |   Heart Beating    |
                |                    |
                |----+ Heartbeat     |
                |    | Interval      |
                |<---+ Exceeded      |
                |                    |
                |       Check        |
                |     Heartbeat      |
                |------------------->|
                |                    |
                |----+ Grace         |
                |    | Period        |
                |<---+ Exceeded      |
                |                    |
                |----+ Mark          |
                |    | OSD 2         |
                |<---+ Down          |


.. index:: OSD down report

OSDs Report Down OSDs
=====================

By default, two Ceph OSD Daemons from different hosts must report to the Ceph
Monitors that another Ceph OSD Daemon is ``down`` before the Ceph Monitors
acknowledge that the reported Ceph OSD Daemon is ``down``. But there is chance
that all the OSDs reporting the failure are hosted in a rack with a bad switch
which has trouble connecting to another OSD. To avoid this sort of false alarm,
we consider the peers reporting a failure a proxy for a potential "subcluster"
over the overall cluster that is similarly laggy. This is clearly not true in
all cases, but will sometimes help us localize the grace correction to a subset
of the system that is unhappy. ``mon osd reporter subtree level`` is used to
group the peers into the "subcluster" by their common ancestor type in CRUSH
map. By default, only two reports from different subtree are required to report
another Ceph OSD Daemon ``down``. You can change the number of reporters from
unique subtrees and the common ancestor type required to report a Ceph OSD
Daemon ``down`` to a Ceph Monitor by adding an ``mon osd min down reporters``
and ``mon osd reporter subtree level`` settings  under the ``[mon]`` section of
your Ceph configuration file, or by setting the value at runtime.


.. ditaa::

           +---------+     +---------+      +---------+
           |  OSD 1  |     |  OSD 2  |      | Monitor |
           +---------+     +---------+      +---------+
                |               |                |
                | OSD 3 Is Down |                |
                |---------------+--------------->|
                |               |                |
                |               |                |
                |               | OSD 3 Is Down  |
                |               |--------------->|
                |               |                |
                |               |                |
                |               |                |---------+ Mark
                |               |                |         | OSD 3
                |               |                |<--------+ Down


.. index:: peering failure

OSDs Report Peering Failure
===========================

If a Ceph OSD Daemon cannot peer with any of the Ceph OSD Daemons defined in its
Ceph configuration file (or the cluster map), it will ping a Ceph Monitor for
the most recent copy of the cluster map every 30 seconds. You can change the
Ceph Monitor heartbeat interval by adding an ``osd mon heartbeat interval``
setting under the ``[osd]`` section of your Ceph configuration file, or by
setting the value at runtime.

.. ditaa::

           +---------+     +---------+     +-------+     +---------+
           |  OSD 1  |     |  OSD 2  |     | OSD 3 |     | Monitor |
           +---------+     +---------+     +-------+     +---------+
                |               |              |              |
                |  Request To   |              |              |
                |     Peer      |              |              |
                |-------------->|              |              |
                |<--------------|              |              |
                |    Peering                   |              |
                |                              |              |
                |  Request To                  |              |
                |     Peer                     |              |
                |----------------------------->|              |
                |                                             |
                |----+ OSD Monitor                            |
                |    | Heartbeat                              |
                |<---+ Interval Exceeded                      |
                |                                             |
                |         Failed to Peer with OSD 3           |
                |-------------------------------------------->|
                |<--------------------------------------------|
                |          Receive New Cluster Map            |


.. index:: OSD status

OSDs Report Their Status
========================

If an Ceph OSD Daemon doesn't report to a Ceph Monitor, the Ceph Monitor will
consider the Ceph OSD Daemon ``down`` after the  ``mon osd report timeout``
elapses. A Ceph OSD Daemon sends a report to a Ceph Monitor when a reportable
event such as a failure, a change in placement group stats, a change in
``up_thru`` or when it boots within 5 seconds. You can change the Ceph OSD
Daemon minimum report interval by adding an ``osd mon report interval``
setting under the ``[osd]`` section of your Ceph configuration file, or by
setting the value at runtime. A Ceph OSD Daemon sends a report to a Ceph
Monitor every 120 seconds irrespective of whether any notable changes occur.
You can change the Ceph Monitor report interval by adding an ``osd mon report
interval max`` setting under the ``[osd]`` section of your Ceph configuration
file, or by setting the value at runtime.


.. ditaa::

           +---------+          +---------+
           |  OSD 1  |          | Monitor |
           +---------+          +---------+
                |                    |
                |----+ Report Min    |
                |    | Interval      |
                |<---+ Exceeded      |
                |                    |
                |----+ Reportable    |
                |    | Event         |
                |<---+ Occurs        |
                |                    |
                |     Report To      |
                |      Monitor       |
                |------------------->|
                |                    |
                |----+ Report Max    |
                |    | Interval      |
                |<---+ Exceeded      |
                |                    |
                |     Report To      |
                |      Monitor       |
                |------------------->|
                |                    |
                |----+ Monitor       |
                |    | Fails         |
                |<---+               |
                                     +----+ Monitor OSD
                                     |    | Report Timeout
                                     |<---+ Exceeded
                                     |
                                     +----+ Mark
                                     |    | OSD 1
                                     |<---+ Down




Configuration Settings
======================

When modifying heartbeat settings, you should include them in the ``[global]``
section of your configuration file.

.. index:: monitor heartbeat

Monitor Settings
----------------

.. confval:: mon_osd_min_up_ratio
.. confval:: mon_osd_min_in_ratio
.. confval:: mon_osd_laggy_halflife
.. confval:: mon_osd_laggy_weight
.. confval:: mon_osd_laggy_max_interval
.. confval:: mon_osd_adjust_heartbeat_grace
.. confval:: mon_osd_adjust_down_out_interval
.. confval:: mon_osd_auto_mark_in
.. confval:: mon_osd_auto_mark_auto_out_in
.. confval:: mon_osd_auto_mark_new_in
.. confval:: mon_osd_down_out_interval
.. confval:: mon_osd_down_out_subtree_limit
.. confval:: mon_osd_report_timeout
.. confval:: mon_osd_min_down_reporters
.. confval:: mon_osd_reporter_subtree_level

.. index:: OSD heartbeat

OSD Settings
------------

.. confval:: osd_heartbeat_interval
.. confval:: osd_heartbeat_grace
.. confval:: osd_mon_heartbeat_interval
.. confval:: osd_mon_heartbeat_stat_stale
.. confval:: osd_mon_report_interval
