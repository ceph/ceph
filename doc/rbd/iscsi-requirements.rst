==========================
iSCSI Gateway Requirements
==========================

To implement the Ceph iSCSI gateway there are a few requirements. It is recommended
to use two to four iSCSI gateway nodes for a highly available Ceph iSCSI gateway
solution.

For hardware recommendations, see :ref:`hardware-recommendations` for more
details.

.. note::
    On the iSCSI gateway nodes, the memory footprint of the RBD images
    can grow to a large size. Plan memory requirements accordingly based
    off the number RBD images mapped.

There are no specific iSCSI gateway options for the Ceph Monitors or
OSDs, but it is important to lower the default timers for detecting
down OSDs to reduce the possibility of initiator timeouts. The following
configuration options are suggested for each OSD node in the storage
cluster::

        [osd]
        osd heartbeat grace = 20
        osd heartbeat interval = 5

-  Online Updating Using the Ceph Monitor

   ::

       ceph tell <daemon_type>.<id> config set <parameter_name> <new_value>

   ::

       ceph tell osd.0 config set osd_heartbeat_grace 20
       ceph tell osd.0 config set osd_heartbeat_interval 5

-  Online Updating on the OSD Node

   ::

       ceph daemon <daemon_type>.<id> config set osd_client_watch_timeout 15

   ::

       ceph daemon osd.0 config set osd_heartbeat_grace 20
       ceph daemon osd.0 config set osd_heartbeat_interval 5

For more details on setting Ceph's configuration options, see
:ref:`configuring-ceph`.
