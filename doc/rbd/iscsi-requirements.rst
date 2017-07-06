==========================
iSCSI Gateway Requirements
==========================

To implement the Ceph iSCSI gateway there are a few requirements. It is recommended
to use two to four iSCSI gateway nodes for a highly available Ceph iSCSI gateway
solution.

For hardware recommendations, see the `Hardware Recommendation page <http://docs.ceph.com/docs/master/start/hardware-recommendations/>`_
for more details.

.. WARNING::
    On the iSCSI gateway nodes, the memory footprint of the RBD images
    can grow to a large size. Plan memory requirements accordingly based
    off the number RBD images mapped. Each RBD image roughly uses 90 MB
    of RAM.

There are no specific iSCSI gateway options for the Ceph Monitors or
OSDs, but changing the ``osd_client_watch_timeout`` value to ``15`` is
required for each OSD node in the storage cluster.

-  Online Updating Using the Ceph Monitor

   ::

       ceph tell <daemon_type>.<id> injectargs '--<parameter_name> <new_value>'

   ::

       # ceph tell osd.0 injectargs '--osd_client_watch_timeout 15'

-  Online Updating on the OSD Node

   ::

       ceph daemon <daemon_type>.<id> config set osd_client_watch_timeout 15

   ::

       # ceph daemon osd.0 config set osd_client_watch_timeout 15

   .. NOTE::
    Update the Ceph configuration file and copy it to all nodes in the
    Ceph storage cluster. For example, the default configuration file is
    ``/etc/ceph/ceph.conf``. Add the following lines to the Ceph
    configuration file:

   ::

        [osd]
        osd_client_watch_timeout = 15

For more details on setting Ceph's configuration options, see the `Configuration page <http://docs.ceph.com/docs/master/rados/configuration/>`_.
