=====================================
 Configuring Monitor/OSD Interaction
=====================================

After you have completed your initial Ceph configuration, you may deploy and run
Ceph.  When you execute a command such as ``ceph health`` or ``ceph -s``,  the
monitor reports on the current state of the cluster. The monitor knows about the
cluster by requiring reports from each OSD, and by receiving reports from OSDs
about the status of their neighboring OSDs. If the monitor doesn't receive
reports, or if it receives reports of changes in the cluster, the monitor
updates the status of the cluster.

Ceph provides reasonable default settings for monitor/OSD interaction. However,
you may override the defaults. The following sections describe how Ceph monitors
and OSDs interact for the purposes of monitoring.


OSDs Check Heartbeats
=====================

Each OSD checks the heartbeat of other OSDs every 6 seconds. You can change the
heartbeat interval by adding an ``osd heartbeat interval`` setting under the
``[osd]`` section of your Ceph configuration file, or by setting the value at
runtime. If an OSD doesn't show a heartbeat within a 20 second grace period, the
cluster may consider the OSD ``down``. You may change this grace period by
adding an ``osd heartbeat grace`` setting under the ``[osd]`` section of your
Ceph configuration file, or by setting the value at runtime.


.. ditaa:: +---------+          +---------+
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
                


OSDs Report Down OSDs
=====================

By default, an OSD must report to the monitors that another OSD is ``down``
three times before the monitors acknowledge that the reported OSD is ``down``.
You can change the minimum number of ``osd down`` reports by adding an ``osd min
down reports`` setting under the ``[osd]`` section of your Ceph configuration
file, or by setting the value at runtime. By default, only one OSD is required
to report another OSD down. You can change the number of OSDs required to report
a monitor down by adding an ``osd min down reporters`` setting under the
``[osd]`` section of your Ceph configuration file, or by setting the value at
runtime.


.. ditaa:: +---------+     +---------+
           |  OSD 1  |     | Monitor |
           +---------+     +---------+
                |               |             
                | OSD 2 Is Down |
                |-------------->|
                |               |             
                | OSD 2 Is Down |
                |-------------->|
                |               |             
                | OSD 2 Is Down |
                |-------------->|
                |               |             
                |               |----------+ Mark
                |               |          | OSD 2                
                |               |<---------+ Down


OSDs Report Peering Failure
===========================

If an OSD cannot peer with any of the OSDs defined in its Ceph configuration
file, it will ping the monitor for the most recent copy of the cluster map every
30 seconds. You can change the monitor heartbeat interval by adding an ``osd mon
heartbeat interval`` setting under the ``[osd]`` section of your Ceph
configuration file, or by setting the value at runtime.

.. ditaa:: +---------+     +---------+     +-------+     +---------+
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
 

OSDs Report Their Status
========================

If an OSD doesn't report to the monitor once at least every 120 seconds, the
monitor will consider the OSD ``down``. You can change the monitor report
interval by adding an ``osd mon report interval max`` setting under the
``[osd]`` section of your Ceph configuration file, or by setting the value at
runtime. The OSD attempts  to report on its status every 30 seconds. You can
change the OSD report interval by adding an ``osd mon report interval min``
setting under the ``[osd]`` section of your Ceph configuration file, or by
setting the value at runtime.


.. ditaa:: +---------+          +---------+
           |  OSD 1  |          | Monitor |
           +---------+          +---------+
                |                    |
                |----+ Report Min    |
                |    | Interval      |
                |<---+ Exceeded      |
                |                    |
                |     Report To      |
                |      Monitor       |
                |------------------->|
                |                    |
                |----+ Report Min    |
                |    | Interval      |
                |<---+ Exceeded      |
                |                    |
                | No Report          |
                                     +----+ Report Max
                                     |    | Interval
                                     |<---+ Exceeded
                                     |
                                     +----+ Mark
                                     |    | OSD 1
                                     |<---+ Down

