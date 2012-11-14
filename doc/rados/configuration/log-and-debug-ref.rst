========================================
 Logging and Debugging Config Reference
========================================

Logging and debugging settings are not required, but you may override default settings 
as needed. Ceph supports the following settings:

Logging
=======

``log file``

:Desription: The location of the logging file for your cluster.
:Type: String
:Required: No
:Default: ``/var/log/ceph/$cluster-$name.log``


``log max new``

:Description: The maximum number of new log files.
:Type: Integer
:Required: No
:Default: ``1000``


``log max recent``

:Description: The maximum number of recent events to include in a log file.
:Type: Integer
:Required:  No
:Default: ``1000000``


``log to stderr``

:Description: Determines if logging messages should appear in ``stderr``.
:Type: Boolean
:Required: No
:Default: ``true``


``err to stderr``

:Description: Determines if error messages should appear in ``stderr``.
:Type: Boolean
:Required: No
:Default: ``true``


``log to syslog``

:Description: Determines if logging messages should appear in ``syslog``.
:Type: Boolean
:Required: No
:Default: ``false``


``err to syslog``

:Description: Determines if error messages should appear in ``syslog``.
:Type: Boolean
:Required: No
:Default: ``false``


``log flush on exit``

:Description: Determines if Ceph should flush the log files after exit.
:Type: Boolean
:Required: No
:Default: ``true``


``clog to monitors``

:Description: Determines if ``clog`` messages should be sent to monitors.
:Type: Boolean
:Required: No
:Default: ``true``


``clog to syslog``

:Description: Determines if ``clog`` messages should be sent to syslog.
:Type: Boolean
:Required: No
:Default: ``false``


``mon cluster log to syslog``

:Description: Determines if the cluster log should be output to the syslog.
:Type: Boolean
:Required: No
:Default: ``false``


``mon cluster log file``

:Description: The location of the cluster's log file. 
:Type: String
:Required: No
:Default: ``/var/log/ceph/$cluster.log``



OSD
===


``osd debug drop ping probability``

:Description: ?
:Type: Double
:Required: No
:Default: 0


``osd debug drop ping duration``

:Description: The duration ?
:Type: Integer
:Required: No
:Default: 0

``osd debug drop pg create probability``

:Description: 
:Type: Integer
:Required: No
:Default: 0

``osd debug drop pg create duration``

:Description: ?
:Type: Double
:Required: No
:Default: 1

``osd preserve trimmed log``

:Description: ?
:Type: Boolean
:Required: No
:Default: ``false``

``osd tmapput sets uses tmap``

:Description: For debug only. ???
:Type: Boolean
:Required: No
:Default: ``false``


``osd min pg log entries``

:Description: The minimum number of log entries for placement groups. 
:Type: 32-bit Unsigned Integer
:Required: No
:Default: 1000

``osd op log threshold``

:Description: How many op log messages to show up in one pass. 
:Type: Integer
:Required: No
:Default: 5



Filestore
=========

``filestore debug omap check``

:Description: Checks the ``omap``. This is an expensive operation.
:Type: Boolean
:Required: No
:Default: 0


MDS
===


``mds debug scatterstat``

:Description: ?
:Type: Boolean
:Required: No
:Default: ``false``


``mds debug frag``

:Description: 
:Type: Boolean
:Required: No
:Default: ``false``


``mds debug auth pins``

:Description: ?
:Type: Boolean
:Required: No
:Default: ``false``


``mds debug subtrees``

:Description: ?
:Type: Boolean
:Required: No
:Default: ``false``



RADOS Gateway
=============


``rgw log nonexistent bucket``

:Description: Should we log a non-existent buckets?
:Type: Boolean
:Required: No
:Default: ``false``


``rgw log object name``

:Description: Should an object's name be logged. // man date to see codes (a subset are supported)
:Type: String
:Required: No
:Default: ``%Y-%m-%d-%H-%i-%n``


``rgw log object name utc``

:Description: Object log name contains UTC?
:Type: Boolean
:Required: No
:Default: ``false``


``rgw enable ops log``

:Description: Enables logging of every RGW operation.
:Type: Boolean
:Required: No
:Default: ``true``


``rgw enable usage log``

:Description: Enable logging of RGW's bandwidth usage.
:Type: Boolean
:Required: No
:Default: ``true``


``rgw usage log flush threshold``

:Description: Threshold to flush pending log data.
:Type: Integer
:Required: No
:Default: ``1024``


``rgw usage log tick interval``

:Description: Flush pending log data every ``s`` seconds.
:Type: Integer
:Required: No
:Default: 30


``rgw intent log object name``

:Description: 
:Type: String
:Required: No
:Default: ``%Y-%m-%d-%i-%n``


``rgw intent log object name utc``

:Description: Include a UTC timestamp in the intent log object name.
:Type: Boolean
:Required: No
:Default: ``false``

``rgw cluster root pool``

:Description: RADOS pool to store radosgw metadata for this instance
:Type: String
:Required: No
:Default: ``.rgw.root``

``rgw gc max objs``

:Description: Number of objects to collect garbage collection data
:Type: 32-bit Integer
:Default: 32

``rgw gc obj min wait``

:Description: Minimum time to wait before object's removal and its processing by the garbage collector
:Type: 32-bit Integer
:Default: 2 hours.  ``2*60*60``

``rgw gc processor max time``

:Description: Max time for a single garbage collection process run
:Type: 32-bit Integer
:Default: 1 hour.  ``60*60``

``rgw gc processor max period``

:Description: Max time between the beginning of two consecutive garbage collection processes run
:Type: 32-bit Integer
:Default: 1 hour.  ``60*60``


