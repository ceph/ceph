======================
 OSD Config Reference
======================

``osd auto upgrade tmap`` 

:Description: Uses ``tmap`` for ``omap`` on old objects.
:Type: Boolean
:Default: True 

``osd tmapput sets users tmap`` 

:Description: Uses ``tmap`` for debugging only.
:Type: Boolean
:Default: False 

``osd data`` 

:Description: 
:Type: String
:Default: None 

``osd journal`` 

:Description: 
:Type: String
:Default: None 

``osd journal size`` 

:Description: The size of the journal in MBs.
:Type: 32-bit Int
:Default: 0 

``osd max write size`` 

:Description: The size of the maximum x to write in MBs.
:Type: 32-bit Int
:Default: 90 

``osd balance reads`` 

:Description: Load balance reads?
:Type: Boolean
:Default: False 

``osd shed reads`` 

:Description: Forward from primary to replica.
:Type: 32-bit Int
:Default: False (0) 

``osd shed reads min latency`` 

:Description: The minimum local latency.
:Type: Double
:Default: .01 

``osd shed reads min latency diff`` 

:Description: Percentage difference from peer. 150% default.
:Type: Double
:Default: 1.5 

``osd client message size cap`` 

:Description: Client data allowed in-memory. 500MB default.
:Type: 64-bit Int Unsigned
:Default: 500*1024L*1024L 

``osd stat refresh interval`` 

:Description: The status refresh interval in seconds.
:Type: 64-bit Int Unsigned
:Default: .5 

``osd pg bits`` 

:Description: Placement group bits per OSD.
:Type: 32-bit Int
:Default: 6 

``osd pgp bits`` 

:Description: Placement group p bits per OSD?
:Type: 32-bit Int
:Default: 4 

``osd pg layout`` 

:Description: Placement Group bits ? per OSD?
:Type: 32-bit Int
:Default: 2 

``osd min rep`` 

:Description: Need a description.
:Type: 32-bit Int
:Default: 1 

``osd max rep`` 

:Description: Need a description.
:Type: 32-bit Int
:Default: 10 

``osd min raid width`` 

:Description: The minimum RAID width.
:Type: 32-bit Int
:Default: 3 

``osd max raid width`` 

:Description: The maximum RAID width.
:Type: 32-bit Int
:Default: 2 

``osd pool default crush rule`` 

:Description: 
:Type: 32-bit Int
:Default: 0 

``osd pool default size`` 

:Description: 
:Type: 32-bit Int
:Default: 2 

``osd pool default pg num`` 

:Description: 
:Type: 32-bit Int
:Default: 8 

``osd pool default pgp num`` 

:Description: 
:Type: 32-bit Int
:Default: 8 

``osd map cache max`` 

:Description: 
:Type: 32-bit Int
:Default: 250 

``osd map message max`` 

:Description: max maps per MOSDMap message
:Type: 32-bit Int
:Default: 100 

``osd op threads`` 

:Description: 0 == no threading
:Type: 32-bit Int
:Default: 2 

``osd disk threads`` 

:Description: 
:Type: 32-bit Int
:Default: 1 

``osd recovery threads`` 

:Description: 
:Type: 32-bit Int
:Default: 1 

``osd recover clone overlap`` 

:Description: preserve clone overlap during rvry/migrat
:Type: Boolean
:Default: false 

``osd backfill scan min`` 

:Description: 
:Type: 32-bit Int
:Default: 64 

``osd backfill scan max`` 

:Description: 
:Type: 32-bit Int
:Default: 512 

``osd op thread timeout`` 

:Description: 
:Type: 32-bit Int
:Default: 30 

``osd backlog thread timeout`` 

:Description: 
:Type: 32-bit Int
:Default: 60*60*1 

``osd recovery thread timeout`` 

:Description: 
:Type: 32-bit Int
:Default: 30 

``osd snap trim thread timeout`` 

:Description: 
:Type: 32-bit Int
:Default: 60*60*1 

``osd scrub thread timeout`` 

:Description: 
:Type: 32-bit Int
:Default: 60 

``osd scrub finalize thread timeout`` 

:Description: 
:Type: 32-bit Int
:Default: 60*10 

``osd remove thread timeout`` 

:Description: 
:Type: 32-bit Int
:Default: 60*60 

``osd command thread timeout`` 

:Description: 
:Type: 32-bit Int
:Default: 10*60 

``osd age`` 

:Description: 
:Type: Float
:Default: .8 

``osd age time`` 

:Description: 
:Type: 32-bit Int
:Default: 0 

``osd heartbeat interval`` 

:Description: 
:Type: 32-bit Int
:Default: 1 

``osd mon heartbeat interval`` 

:Description: if no peers | ping monitor
:Type: 32-bit Int
:Default: 30 

``osd heartbeat grace`` 

:Description: 
:Type: 32-bit Int
:Default: 20 

``osd mon report interval max`` 

:Description: 
:Type: 32-bit Int
:Default: 120 

``osd mon report interval min`` 

:Description: pg stats | failures | up thru | boot.
:Type: 32-bit Int
:Default: 5 

``osd mon ack timeout`` 

:Description: time out a mon if it doesn't ack stats
:Type: 32-bit Int
:Default: 30 

``osd min down reporters`` 

:Description: num OSDs needed to report a down OSD
:Type: 32-bit Int
:Default: 1 

``osd min down reports`` 

:Description: num times a down OSD must be reported
:Type: 32-bit Int
:Default: 3 

``osd default data pool replay window`` 

:Description: 
:Type: 32-bit Int
:Default: 45 

``osd preserve trimmed log`` 

:Description: 
:Type: Boolean
:Default: true 

``osd auto mark unfound lost`` 

:Description: 
:Type: Boolean
:Default: false 

``osd recovery delay start`` 

:Description: 
:Type: Float
:Default: 15 

``osd recovery max active`` 

:Description: 
:Type: 32-bit Int
:Default: 5 

``osd recovery max chunk`` 

:Description: max size of push chunk
:Type: 64-bit Int Unsigned
:Default: 1<<20 

``osd recovery forget lost objects`` 

:Description: off for now
:Type: Boolean
:Default: false 

``osd max scrubs`` 

:Description: 
:Type: 32-bit Int
:Default: 1 

``osd scrub load threshold`` 

:Description: 
:Type: Float
:Default: 0.5 

``osd scrub min interval`` 

:Description: 
:Type: Float
:Default: 300 

``osd scrub max interval`` 

:Description: once a day
:Type: Float
:Default: 60*60*24 

``osd auto weight`` 

:Description: 
:Type: Boolean
:Default: false 

``osd class error timeout`` 

:Description: seconds
:Type: Double
:Default: 60.0 

``osd class timeout`` 

:Description: seconds
:Type: Double
:Default: 60*60.0 

``osd class dir`` 

:Description: where rados plugins are stored
:Type: String
:Default: $libdir/rados-classes 

``osd check for log corruption`` 

:Description: 
:Type: Boolean
:Default: false 

``osd use stale snap`` 

:Description: 
:Type: Boolean
:Default: false 

``osd rollback to cluster snap`` 

:Description: 
:Type: String
:Default: "" 

``osd default notify timeout`` 

:Description: default notify timeout in seconds
:Type: 32-bit Int Unsigned
:Default: 30 

``osd kill backfill at`` 

:Description: 
:Type: 32-bit Int
:Default: 0 

``osd min pg log entries`` 

:Description: num entries to keep in pg log when trimming
:Type: 32-bit Int Unsigned
:Default: 1000 

``osd op complaint time`` 

:Description: how old in secs makes op complaint-worthy
:Type: Float
:Default: 30 

``osd command max records`` 

:Description: 
:Type: 32-bit Int
:Default: 256 
