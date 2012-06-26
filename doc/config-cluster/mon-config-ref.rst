==========================
 Monitor Config Reference
==========================

``mon data`` 

:Description: 
:Type: String
:Default: "" 

``mon sync fs threshold`` 

:Description: sync when writing this many objects; 0 to disable.
:Type: 32-bit Integer
:Default: 5 

``mon tick interval`` 

:Description: 
:Type: 32-bit Integer
:Default: 5 

``mon subscribe interval`` 

:Description: 
:Type: Double
:Default: 300 

``mon osd auto mark in`` 

:Description: mark any booting osds 'in'
:Type: Boolean
:Default: false 

``mon osd auto mark auto out in`` 

:Description: mark booting auto-marked-out osds 'in'
:Type: Boolean
:Default: true 

``mon osd auto mark new in`` 

:Description: mark booting new osds 'in'
:Type: Boolean
:Default: true 

``mon osd down out interval`` 

:Description: seconds
:Type: 32-bit Integer
:Default: 300 

``mon lease`` 

:Description: lease interval
:Type: Float
:Default: 5 

``mon lease renew interval`` 

:Description: on leader | to renew the lease
:Type: Float
:Default: 3 

``mon lease ack timeout`` 

:Description: on leader | if lease isn't acked by all peons
:Type: Float
:Default: 10.0 

``mon clock drift allowed`` 

:Description: allowed clock drift between monitors
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

:Description: what % full makes an OSD "full"
:Type: Float
:Default: .95 

``mon osd nearfull ratio`` 

:Description: what % full makes an OSD near full
:Type: Float
:Default: .85 

``mon globalid prealloc`` 

:Description: how many globalids to prealloc
:Type: 32-bit Integer
:Default: 100 

``mon osd report timeout`` 

:Description: grace period before declaring unresponsive OSDs dead
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

``mon probe timeout`` 

:Description: 
:Type: Double
:Default: 2.0 

``mon slurp timeout`` 

:Description: 
:Type: Double
:Default: 10.0
