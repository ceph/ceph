==========================
 Monitor Config Reference
==========================

``mon data`` 

:Description: The monitor's data location.
:Type: String
:Default: ``/var/lib/ceph/mon/$cluster-$id``


``mon initial members``

:Description: The IDs of initial monitors in a cluster during startup. If specified, Ceph requires an odd number of monitors to form an initial quorum. 
:Type: String
:Default: None


``mon sync fs threshold`` 

:Description: Synchronize with the filesystem when writing the specified number of objects. Set it to ``0`` to disable it.
:Type: 32-bit Integer
:Default: ``5`` 


``mon tick interval`` 

:Description: A monitor's tick interval in seconds. 
:Type: 32-bit Integer
:Default: ``5`` 


``mon subscribe interval`` 

:Description: 
:Type: Double
:Default: ``300`` 


``mon osd auto mark in`` 

:Description: Ceph will mark any booting OSDs as ``in`` the cluster.
:Type: Boolean
:Default: ``false``


``mon osd auto mark auto out in`` 

:Description: Ceph will mark booting OSDs auto marked ``out`` of the cluster as ``in`` the cluster.
:Type: Boolean
:Default: ``true`` 


``mon osd auto mark new in`` 

:Description: Ceph will mark booting new OSDs as ``in`` the cluster.
:Type: Boolean
:Default: ``true`` 


``mon osd down out interval`` 

:Description: The number of seconds Ceph waits before marking an OSD ``down`` and ``out`` if it doesn't respond.
:Type: 32-bit Integer
:Default: 300 


``mon osd min up ratio``

:Description: The minimum ratio of ``up`` OSDs before Ceph will mark OSDs ``down``.
:Type: Double
:Default: ``.3``


``mon osd min in ratio``

:Description: The minimum ratio of ``in`` OSDs before Ceph will mark OSDs ``out``.


``mon lease`` 

:Description: lease interval
:Type: Float
:Default: ``5`` 


``mon lease renew interval`` 

:Description: on leader | to renew the lease
:Type: Float
:Default: 3 


``mon lease ack timeout`` 

:Description: on leader | if lease isn't acked by all peons
:Type: Float
:Default: 10.0 


``mon clock drift allowed`` 

:Description: The clock drift in seconds allowed between monitors.
:Type: Float
:Default: .050 


``mon clock drift warn backoff`` 

:Description: exponential backoff for clock drift warnings
:Type: Float
:Default: 5 


``mon accept timeout`` 

:Description: on leader | if paxos update isn't accepted
:Type: Float
:Default: 10.0 


``mon pg create interval`` 

:Description: no more than every 30s
:Type: Float
:Default: 30.0 


``mon pg stuck threshold`` 

:Description: number of seconds after which pgs can be considered
:Type: 32-bit Integer
:Default: 300 


``mon osd full ratio`` 

:Description: The percentage of disk space used before an OSD is considered ``full``.
:Type: Float
:Default: .95 


``mon osd nearfull ratio`` 

:Description: The percentage of disk space used before an OSD is considered ``nearfull``.
:Type: Float
:Default: .85 


``mon globalid prealloc`` 

:Description: The number of global IDs to pre-allocate for the cluster.
:Type: 32-bit Integer
:Default: 100 


``mon osd report timeout`` 

:Description: The grace period in seconds before declaring unresponsive OSDs ``down``.
:Type: 32-bit Integer
:Default: 900 


``mon force standby active`` 

:Description: should mons force standby-replay mds to be active
:Type: Boolean
:Default: true 


``mon min osdmap epochs`` 

:Description: 
:Type: 32-bit Integer
:Default: 500 


``mon max pgmap epochs`` 

:Description: 
:Type: 32-bit Integer
:Default: 500 


``mon max log epochs`` 

:Description: 
:Type: 32-bit Integer
:Default: 500


``mon max osd``

:Description: The maximum number of OSDs allowed in the cluster.
:Type: 32-bit Integer
:Default: ``10000``


``mon probe timeout`` 

:Description: 
:Type: Double
:Default: 2.0 


``mon slurp timeout`` 

:Description: 
:Type: Double
:Default: 10.0


``mon slurp bytes``

:Description: Limits the slurp messages to the specified number of bytes.
:Type: 32-bit Integer
:Default: ``256 * 1024``


``mon client bytes``

:Description: The amount of client message data allowed in memory (in bytes).
:Type: 64-bit Integer Unsigned
:Default: ``100ul << 20``


``mon daemon bytes``

:Description: The message memory cap for metadata server and OSD messages (in bytes).
:Type: 64-bit Integer Unsigned
:Default: ``400ul << 20``


``mon max log entries per event``

:Description: The maximum number of log entries per event. 
:Type: Integer
:Default: ``4096``

