==================
DiskPrediction plugin
==================

The Disk Prediction Plugin actively sends information to a DiskPrediction server like:

- Ceph status
- I/O operations
- I/O bandwidth
- OSD status
- OSD physical disk smart data
- Storage utilization

Enabling
========

You can enable the *diskprediction* module with:

::

    ceph mgr module enable diskprediction

Configuration
-------------

Below configuration keys are vital for the module to work:

- diskprediction_server
- diskprediction_user
- diskprediction_password

The parameter *diskprediction_server* controls the hostname of the DiskPrediction
server to which the plugin will send the items. This can be a IP-Address if 
required by your installation.

The parameter *diskprediction_user* and *diskprediction_password* controls the user
of the DiskPrediction that can fetch each physical disk predicted health state.

Configuration keys
^^^^^^^^^^^^^^^^^^

Configuration keys can be set on any machine with the proper cephx credentials,
these are usually Monitors where the *client.admin* key is present.

::

    ceph diskprediction config-set <diskprediction_server> <diskprediction_user> <diskprediction_password>

The current configuration of the module can also be shown:

::

    ceph diskprediction config-show

Fetch physical disk of the osd predicted health status
======================================================

User can use command to fetch each physical disk of the osd predicted health status.

::

    ceph diskprediction get-predicted-status <osd id>

get-predicted-status response
::
    {
        "osd.0": {
            "prediction": {
                "sdb": {
                    "replacment_time": null,
                    "near_failure": "Good",
                    "disk_wwn": "5000039ff9eb609c",
                    "serial_number": "44T2E4LAS",
                    "life_expectancy_day": 682,
                    "predicted": "2018-05-30 18:33:12",
                    "device": "sdb"
                }
            }
        }
    }


+--------------------+-----------------------------------------------------+
|Key                 | Description                                         |
+====================+=====================================================+
|replacment_time     | recommend replace physical device time              |
+--------------------+-----------------------------------------------------+
|near_failure        | predicted physical disk result,                     |
|                    | Good/Warning/Bad/Unknown                            |
+--------------------+-----------------------------------------------------+
|disk_wwn            | disk wwn number                                     |
+--------------------+-----------------------------------------------------+
|serial_number       | disk serial number                                  |
+--------------------+-----------------------------------------------------+
|life_expectancy_day | predicted life remaining day of a disk              |
+--------------------+-----------------------------------------------------+
|predicted           | predicted date                                      |
+--------------------+-----------------------------------------------------+
|device              | device name of the local system                     |
+--------------------+-----------------------------------------------------+


Debugging
---------

Should you want to debug the Disk Prediction module increase the logging level for
ceph-mgr and check the logs.

::

    [mgr]

        debug mgr = 20

With logging set to debug for the manager the plugin will print various logging
lines prefixed with *mgr[diskprediction]* for easy filtering.

