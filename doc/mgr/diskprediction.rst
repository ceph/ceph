.. _diskprediction:

=====================
Diskprediction Module
=====================

The *diskprediction* module supports two modes: cloud mode and local mode. In cloud mode, the disk and Ceph operating status information is collected from Ceph cluster and sent to a cloud-based DiskPrediction server over the Internet. DiskPrediction server analyzes the data and provides the analytics and prediction results of performance and disk health states for Ceph clusters. 

Local mode doesn't require any external server for data analysis and output results. In local mode, the *diskprediction* module uses an internal predictor module for disk prediction service, and then returns the disk prediction result to the Ceph system.

| Local predictor: 70% accuracy
| Cloud predictor for free: 95% accuracy

Enabling
========

Run the following command to enable the *diskprediction* module in the Ceph
environment::

    ceph mgr module enable diskprediction_cloud
    ceph mgr module enable diskprediction_local


Select the prediction mode::

    ceph config set global device_failure_prediction_mode local

or::
  
    ceph config set global device_failure_prediction_mode cloud

To disable prediction,::

  ceph config set global device_failure_prediction_mode none


Connection settings
===================
The connection settings are used for connection between Ceph and DiskPrediction server. 

Local Mode
----------

The *diskprediction* module leverages Ceph device health check to collect disk health metrics and uses internal predictor module to produce the disk failure prediction and returns back to Ceph. Thus, no connection settings are required in local mode. The local predictor module requires at least six datasets of device health metrics to implement the prediction.

Run the following command to use local predictor predict device life expectancy.

::

    ceph device predict-life-expectancy <device id>


Cloud Mode 
----------

The user registration is required in cloud mode. The users have to sign up their accounts at https://www.diskprophet.com/#/ to receive the following DiskPrediction server information for connection settings. 

**Certificate file path**: After user registration is confirmed, the system will send a confirmation email including a certificate file download link. Download the certificate file and save it to the Ceph system. Run the following command to verify the file. Without certificate file verification, the connection settings cannot be completed.
	
**DiskPrediction server**: The DiskPrediction server name. It could be an IP address if required. 

**Connection account**: An account name used to set up the connection between Ceph and DiskPrediction server

**Connection password**: The password used to set up the connection between Ceph and DiskPrediction server

Run the following command to complete connection setup.

::

    ceph device set-cloud-prediction-config <diskprediction_server> <connection_account> <connection_password> <certificate file path>
	

You can use the following command to display the connection settings:

::

    ceph device show-prediction-config


Additional optional configuration settings are the following:

:diskprediction_upload_metrics_interval: Indicate the frequency to send Ceph performance metrics to DiskPrediction server regularly at times.  Default is 10 minutes.
:diskprediction_upload_smart_interval: Indicate the frequency to send Ceph physical device info to DiskPrediction server regularly at times.  Default is 12 hours.
:diskprediction_retrieve_prediction_interval: Indicate Ceph that retrieves physical device prediction data from DiskPrediction server regularly at times.  Default is 12 hours.



Diskprediction Data
===================

The *diskprediction* module actively sends/retrieves the following data to/from DiskPrediction server.


Metrics Data
-------------
- Ceph cluster status

+----------------------+-----------------------------------------+
|key                   |Description                              |
+======================+=========================================+
|cluster_health        |Ceph health check status                 |
+----------------------+-----------------------------------------+
|num_mon               |Number of monitor node                   |
+----------------------+-----------------------------------------+
|num_mon_quorum        |Number of monitors in quorum             |
+----------------------+-----------------------------------------+
|num_osd               |Total number of OSD                      |
+----------------------+-----------------------------------------+
|num_osd_up            |Number of OSDs that are up               |
+----------------------+-----------------------------------------+
|num_osd_in            |Number of OSDs that are in cluster       |
+----------------------+-----------------------------------------+
|osd_epoch             |Current epoch of OSD map                 |
+----------------------+-----------------------------------------+
|osd_bytes             |Total capacity of cluster in bytes       |
+----------------------+-----------------------------------------+
|osd_bytes_used        |Number of used bytes on cluster          |
+----------------------+-----------------------------------------+
|osd_bytes_avail       |Number of available bytes on cluster     |
+----------------------+-----------------------------------------+
|num_pool              |Number of pools                          |
+----------------------+-----------------------------------------+
|num_pg                |Total number of placement groups         |
+----------------------+-----------------------------------------+
|num_pg_active_clean   |Number of placement groups in            |
|                      |active+clean state                       |
+----------------------+-----------------------------------------+
|num_pg_active         |Number of placement groups in active     |
|                      |state                                    |
+----------------------+-----------------------------------------+
|num_pg_peering        |Number of placement groups in peering    |
|                      |state                                    |
+----------------------+-----------------------------------------+
|num_object            |Total number of objects on cluster       |
+----------------------+-----------------------------------------+
|num_object_degraded   |Number of degraded (missing replicas)    |
|                      |objects                                  |
+----------------------+-----------------------------------------+
|num_object_misplaced  |Number of misplaced (wrong location in   |
|                      |the cluster) objects                     |
+----------------------+-----------------------------------------+
|num_object_unfound    |Number of unfound objects                |
+----------------------+-----------------------------------------+
|num_bytes             |Total number of bytes of all objects     |
+----------------------+-----------------------------------------+
|num_mds_up            |Number of MDSs that are up               |
+----------------------+-----------------------------------------+
|num_mds_in            |Number of MDS that are in cluster        |
+----------------------+-----------------------------------------+
|num_mds_failed        |Number of failed MDS                     |
+----------------------+-----------------------------------------+
|mds_epoch             |Current epoch of MDS map                 |
+----------------------+-----------------------------------------+


- Ceph mon/osd performance counts

Mon:

+----------------------+-----------------------------------------+
|key                   |Description                              |
+======================+=========================================+
|num_sessions          |Current number of opened monitor sessions|
+----------------------+-----------------------------------------+
|session_add           |Number of created monitor sessions       |
+----------------------+-----------------------------------------+
|session_rm            |Number of remove_session calls in monitor|
+----------------------+-----------------------------------------+
|session_trim          |Number of trimed monitor sessions        |
+----------------------+-----------------------------------------+
|num_elections         |Number of elections monitor took part in |
+----------------------+-----------------------------------------+
|election_call         |Number of elections started by monitor   |
+----------------------+-----------------------------------------+
|election_win          |Number of elections won by monitor       |
+----------------------+-----------------------------------------+
|election_lose         |Number of elections lost by monitor      |
+----------------------+-----------------------------------------+

Osd:

+----------------------+-----------------------------------------+
|key                   |Description                              |
+======================+=========================================+
|op_wip                |Replication operations currently being   |
|                      |processed (primary)                      |
+----------------------+-----------------------------------------+
|op_in_bytes           |Client operations total write size       |
+----------------------+-----------------------------------------+
|op_r                  |Client read operations                   |
+----------------------+-----------------------------------------+
|op_out_bytes          |Client operations total read size        |
+----------------------+-----------------------------------------+
|op_w                  |Client write operations                  |
+----------------------+-----------------------------------------+
|op_latency            |Latency of client operations (including  |
|                      |queue time)                              |
+----------------------+-----------------------------------------+
|op_process_latency    |Latency of client operations (excluding  |
|                      |queue time)                              |
+----------------------+-----------------------------------------+
|op_r_latency          |Latency of read operation (including     |
|                      |queue time)                              |
+----------------------+-----------------------------------------+
|op_r_process_latency  |Latency of read operation (excluding     |
|                      |queue time)                              |
+----------------------+-----------------------------------------+
|op_w_in_bytes         |Client data written                      |
+----------------------+-----------------------------------------+
|op_w_latency          |Latency of write operation (including    |
|                      |queue time)                              |
+----------------------+-----------------------------------------+
|op_w_process_latency  |Latency of write operation (excluding    |
|                      |queue time)                              |
+----------------------+-----------------------------------------+
|op_rw                 |Client read-modify-write operations      |
+----------------------+-----------------------------------------+
|op_rw_in_bytes        |Client read-modify-write operations write|
|                      |in                                       |
+----------------------+-----------------------------------------+
|op_rw_out_bytes       |Client read-modify-write operations read |
|                      |out                                      |
+----------------------+-----------------------------------------+
|op_rw_latency         |Latency of read-modify-write operation   |
|                      |(including queue time)                   |
+----------------------+-----------------------------------------+
|op_rw_process_latency |Latency of read-modify-write operation   |
|                      |(excluding queue time)                   |
+----------------------+-----------------------------------------+


- Ceph pool statistics

+----------------------+-----------------------------------------+
|key                   |Description                              |
+======================+=========================================+
|bytes_used            |Per pool bytes used                      |
+----------------------+-----------------------------------------+
|max_avail             |Max available number of bytes in the pool|
+----------------------+-----------------------------------------+
|objects               |Number of objects in the pool            |
+----------------------+-----------------------------------------+
|wr_bytes              |Number of bytes written in the pool      |
+----------------------+-----------------------------------------+
|dirty                 |Number of bytes dirty in the pool        |
+----------------------+-----------------------------------------+
|rd_bytes              |Number of bytes read in the pool         |
+----------------------+-----------------------------------------+
|stored_raw            |Bytes used in pool including copies made |
+----------------------+-----------------------------------------+

- Ceph physical device metadata

+----------------------+-----------------------------------------+
|key                   |Description                              |
+======================+=========================================+
|disk_domain_id        |Physical device identify id              |
+----------------------+-----------------------------------------+
|disk_name             |Device attachment name                   |
+----------------------+-----------------------------------------+
|disk_wwn              |Device wwn                               |
+----------------------+-----------------------------------------+
|model                 |Device model name                        |
+----------------------+-----------------------------------------+
|serial_number         |Device serial number                     |
+----------------------+-----------------------------------------+
|size                  |Device size                              |
+----------------------+-----------------------------------------+
|vendor                |Device vendor name                       |
+----------------------+-----------------------------------------+

- Ceph each objects correlation information
- The module agent information
- The module agent cluster information
- The module agent host information


SMART Data
-----------
- Ceph physical device SMART data (provided by Ceph *devicehealth* module)


Prediction Data
----------------
- Ceph physical device prediction data
 

Receiving predicted health status from a Ceph OSD disk drive
============================================================

You can receive predicted health status from Ceph OSD disk drive by using the
following command.

::

    ceph device get-predicted-status <device id>


The get-predicted-status command returns:


::

    {
	"near_failure": "Good",
	"disk_wwn": "5000011111111111",
	"serial_number": "111111111",
	"predicted": "2018-05-30 18:33:12",
	"attachment": "sdb"
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
|attachment          | device name on the local system                     |
+--------------------+-----------------------------------------------------+

The *near_failure* attribute for disk failure prediction state indicates disk life expectancy in the following table.

+--------------------+-----------------------------------------------------+
|near_failure        | Life expectancy (weeks)                             |
+====================+=====================================================+
|Good                | > 6 weeks                                           |
+--------------------+-----------------------------------------------------+
|Warning             | 2 weeks ~ 6 weeks                                   |
+--------------------+-----------------------------------------------------+
|Bad                 | < 2 weeks                                           |
+--------------------+-----------------------------------------------------+
 

Debugging
=========

If you want to debug the DiskPrediction module mapping to Ceph logging level,
use the following command.

::

    [mgr]

        debug mgr = 20

With logging set to debug for the manager the module will print out logging
message with prefix *mgr[diskprediction]* for easy filtering.

