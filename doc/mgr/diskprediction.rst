=====================
DiskPrediction plugin
=====================

Disk prediction plugin supports two modes: cloud mode and local mode. In cloud mode, disk and Cepth operating status information are collected from Ceph cluster and sent to DiskPrediction server in cloud. DiskPrediction server analyzes the data and provides the prediction result of disk health state for Ceph clusters. 
Local mode does not require a DiskPrediction server in cloud. It uses internal predictor module in the plugin to provide a lite prediction result of disk health state.  
Based on the prediction result,  DiskPrediction plugin reports back the life expectancy days information of disks to Ceph clusters by Ceph device commands. 


Enabling
========

Run the following command to enable *diskprediction* module in the Ceph
environment:

::

    ceph mgr module enable diskprediction


Select the plugin use mode:

::

    ceph diskprediction config-mode <local/cloud>


Local Mode
----------

The Ceph diskprediction plugin uses internal predictor module to provide the device health prediction. It leverages the device health plugin to collect disk health metrics. The local predictor module requires at least six days data of device health metrics to provide the prediction. 


Cloud Mode Connection settings
------------------------------

Registration of a new user is required in Cloud mode. The URL for new user registration is http://federator-ai-homepage.s3-website-us-west-2.amazonaws.com/#/. 
DiskPrediction server and account information will be provided after registration is done. 
Run the following command to set up connection between Ceph system  
and DiskPrediction server.

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

