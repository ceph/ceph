=====================
DiskPrediction plugin
=====================

Disk Prediction plugin is used to collect disk information from Ceph OSD and
send DiskPrediction server. The disk information includes the following.

- Ceph status
- I/O operations
- I/O bandwidth
- OSD status
- OSD physical disk smart data
- Storage utilization

Enabling
========

Run the following command to enable *diskprediction* module in the Ceph
environment:

::

    ceph mgr module enable diskprediction

Connection settings
-------------------

The connection settings can be configured on any machine with the proper cephx
credentials; they are usually the monitor node with client.admin keyring.
Run the following command to set up connection between Ceph system and
DiskPrediction server.

::

    ceph diskprediction config-set <diskprediction_server> <diskprediction_user> <diskprediction_password>
	

The ``<diskprediction_server>`` parameter is DiskPrediction server name, and it
could be an IP address if required.

The ``<diskprediction_user>`` and ``<diskprediction_password>`` parameters are the user
id and password logging in to DiskPrediction server.



The connection settings can be shown using the following command:

::

    ceph diskprediction config-show


Receiving predicted health status from Ceph OSD disk drive
==========================================================

You can receive predicted health status from Ceph OSD disk drive by using the
following command.

::

    ceph diskprediction get-predicted-status <osd id>

get-predicted-status response
-----------------------------

::

    {
        "osd.0": {
            "prediction": {
            "sdb": {
                "near_failure": "Good",
                "disk_wwn": "5000011111111111",
                "serial_number": "111111111",
                "predicted": "2018-05-30 18:33:12",
                "device": "sdb"
                }
            }
        }
    }


+--------------------+-----------------------------------------------------+
|Attribute           | Description                                         |
+====================+=====================================================+
|near_failure        | The disk failure prediction state:                  |
|                    | Good/Warning/Bad/Unknown                            |
+--------------------+-----------------------------------------------------+
|disk_wwn            | Disk wwn number                                     |
+--------------------+-----------------------------------------------------+
|serial_number       | Disk serial number                                  |
+--------------------+-----------------------------------------------------+
|predicted           | Predicted date                                      |
+--------------------+-----------------------------------------------------+
|device              | device name on the local system                     |
+--------------------+-----------------------------------------------------+


Debugging
---------

If you want to debug the DiskPrediction module mapping to Ceph logging level,
use the following command.

::

    [mgr]

        debug mgr = 20

With logging set to debug for the manager the plugin will print out logging
message with prefix *mgr[diskprediction]* for easy filtering.

