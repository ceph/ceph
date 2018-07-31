=====================
DiskPrediction plugin
=====================

Disk Prediction plugin is used to collect disk information from Ceph cluster and
send these data to Disk prediction server. Anohter the plugin also receive the physical 
devices healthy prediction data and use the Ceph device command to write the life expectancy 
date.


Enabling
========

Run the following command to enable *diskprediction* module in the Ceph
environment:

::

    ceph mgr module enable diskprediction

Connection settings
-------------------

Run the following command to set up connection between Ceph system and
DiskPrediction server.

::

    ceph diskprediction config-set <diskprediction_server> <diskprediction_user> <diskprediction_password>
	

The ``<diskprediction_server>`` parameter is DiskPrediction server name, and it
could be an IP address if required.

The ``<diskprediction_user>`` and ``<diskprediction_password>`` parameters are the user
id and password logging in to the DiskPrediction server.



The connection settings can be shown using the following command:

::

    ceph diskprediction config-show


Addition optional configuration settings are:

:diskprediction_upload_metrics_interval: Time between reports ceph metrics to the diskprediction server.  Default 10 minutes.
:diskprediction_upload_smart_interval: Time between reports ceph physical device info to the diskprediction server.  Default is 12 hours.
:diskprediction_retrieve_prediction_interval: Time between fetch physical device health prediction data from the server.  Default is 12 hours.



Actively agents
===============

The plugin actively agents send/retrieve information with a Disk prediction server like:


Metrics agent
-------------
- Ceph cluster status
- Ceph mon/osd performance counts
- Ceph pool statistics
- Ceph each objects correlation information
- The plugin agent information
- The plugin agent cluster information
- The plugin agent host information
- Ceph physical device metadata


Smart agent
-----------
- Ceph physical device smart data (by smartctl command)


Prediction agent
----------------
- Retrieve the ceph physical device prediction data
 

Receiving predicted health status from a Ceph OSD disk drive
============================================================

You can receive predicted health status from Ceph OSD disk drive by using the
following command.

::

    ceph diskprediction get-predicted-status <device id>


get-predicted-status response
-----------------------------

::

    {
        "<device id>": {
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
|disk_wwn            | Disk WWN number                                     |
+--------------------+-----------------------------------------------------+
|serial_number       | Disk serial number                                  |
+--------------------+-----------------------------------------------------+
|predicted           | Predicted date                                      |
+--------------------+-----------------------------------------------------+
|device              | device name on the local system                     |
+--------------------+-----------------------------------------------------+

The plugin reference the prediction near_failure state to wite the ceph devcie life expectancy days.

+--------------------+-----------------------------------------------------+
|near_failure        | Life expectancy days                                |
+====================+=====================================================+
|Good                | > 6 weeks                                           |
+--------------------+-----------------------------------------------------+
|Warning             | 2 weeks ~ 6 weeks                                   |
+--------------------+-----------------------------------------------------+
|Bad                 | < 2 weeks                                           |
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

