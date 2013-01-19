======================
 OSD Config Reference
======================



``osd uuid``

:Description: The universally unique identifier (UUID) for the OSD.
:Type: UUID
:Default: None


``osd data`` 

:Description: The path to the OSDs data. You must create the directory. You should mount a data disk at this mount point. We do not recommend changing the default. 
:Type: String
:Default: ``/var/lib/ceph/osd/$cluster-$id``


``osd journal`` 

:Description: The path to the OSD's journal. This may be a path to a file or a block device (such as a partition of an SSD). If it is a file, you must create the directory to contain it.
:Type: String
:Default: ``/var/lib/ceph/osd/$cluster-$id/journal``


``osd journal size`` 

:Description: The size of the journal in megabytes. If this is 0, and the journal is a block device, the entire block device is used. Since v0.54, this is ignored if the journal is a block device, and the entire block device is used.
:Type: 32-bit Integer
:Default: ``1024``
:Recommended: Begin with 1GB. Should be at least twice the product of the expected speed multiplied by ``filestore max sync interval``.


``osd max write size`` 

:Description: The maximum size of a write in megabytes.
:Type: 32-bit Integer
:Default: ``90``


``osd client message size cap`` 

:Description: The largest client data message allowed in memory.
:Type: 64-bit Integer Unsigned
:Default: 500MB default. ``500*1024L*1024L`` 


``osd stat refresh interval`` 

:Description: The status refresh interval in seconds.
:Type: 64-bit Integer Unsigned
:Default: ``.5``


``osd pg bits`` 

:Description: Placement group bits per OSD.
:Type: 32-bit Integer
:Default: ``6`` 


``osd pgp bits`` 

:Description: The number of bits per OSD for PGPs.
:Type: 32-bit Integer
:Default: ``4``


``osd pg layout`` 

:Description: Placement group layout. 
:Type: 32-bit Integer
:Default: ``2``


``osd pool default crush rule`` 

:Description: The default CRUSH rule to use when creating a pool.
:Type: 32-bit Integer
:Default: ``0``


``osd pool default size`` 

:Description: The default size of an OSD pool in gigabytes. The default value is the same as ``--size 2`` with ``mkpool``.
:Type: 32-bit Integer
:Default: ``2`` 


``osd pool default pg num`` 

:Description: The default number of placement groups for a pool. The default value is the same as ``pg_num`` with ``mkpool``.
:Type: 32-bit Integer
:Default: ``8`` 


``osd pool default pgp num`` 

:Description: The default number of placement groups for placement for a pool. The default value is the same as ``pgp_num`` with ``mkpool``. PG and PGP should be equal (for now).
:Type: 32-bit Integer
:Default: ``8``


``osd map dedup``

:Description: Enable removing duplicates in the OSD map. 
:Type: Boolean
:Default: ``true``


``osd map cache size`` 

:Description: The size of the OSD map cache in megabytes.
:Type: 32-bit Integer
:Default: ``500``


``osd map cache bl size``

:Description: The size of the in-memory OSD map cache in OSD daemons. 
:Type: 32-bit Integer
:Default: ``50``


``osd map cache bl inc size``

:Description: The size of the in-memory OSD map cache incrementals in OSD daemons.
:Type: 32-bit Integer
:Default: ``100``


``osd map message max`` 

:Description: The maximum map entries allowed per MOSDMap message.
:Type: 32-bit Integer
:Default: ``100``


``osd op threads`` 

:Description: The number of OSD operation threads. Set to ``0`` to disable it. Increasing the number may increase the request processing rate.
:Type: 32-bit Integer
:Default: ``2`` 


``osd op thread timeout`` 

:Description: The OSD operation thread timeout in seconds.
:Type: 32-bit Integer
:Default: ``30`` 


``osd disk threads`` 

:Description: The number of disk threads, which are used to perform background disk intensive OSD operations such as scrubbing and snap trimming.
:Type: 32-bit Integer
:Default: ``1`` 


``osd recovery threads`` 

:Description: The number of threads for recovering data.
:Type: 32-bit Integer
:Default: ``1``


``osd backfill scan min`` 

:Description: The scan interval in seconds for backfill operations.
:Type: 32-bit Integer
:Default: ``64`` 


``osd backfill scan max`` 

:Description: The maximum scan interval in seconds for backfill operations.
:Type: 32-bit Integer
:Default: ``512`` 


``osd backlog thread timeout`` 

:Description: The maximum time in seconds before timing out a backlog thread.
:Type: 32-bit Integer
:Default: ``60*60*1`` 


``osd max backfills``

:Description: The maximum number of backfills allowed to or from a single OSD.
:Type: 64-bit Unsigned Integer
:Default: 10


``osd backfill full ratio``

:Description: Refuse to accept backfill requests when the OSD's full ratio is above this value.
:Type: Float
:Default: ``0.85``


``osd backfill retry interval``

:Description: The number of seconds to wait before retrying backfill requests.
:Type: Double
:Default: ``10.0``


``osd recovery thread timeout`` 

:Description: The maximum time in seconds before timing out a recovery thread.
:Type: 32-bit Integer
:Default: ``30`` 


``osd snap trim thread timeout`` 

:Description: The maximum time in seconds before timing out a snap trim thread.
:Type: 32-bit Integer
:Default: ``60*60*1`` 


``osd scrub thread timeout`` 

:Description: The maximum time in seconds before timing out a scrub thread.
:Type: 32-bit Integer
:Default: ``60`` 


``osd scrub finalize thread timeout`` 

:Description: The maximum time in seconds before timing out a scrub finalize thread.
:Type: 32-bit Integer
:Default: 60*10 


``osd remove thread timeout`` 

:Description: The maximum time in seconds before timing out a remove OSD thread.
:Type: 32-bit Integer
:Default: 60*60 


``osd command thread timeout`` 

:Description: The maximum time in seconds before timing out a command thread.
:Type: 32-bit Integer
:Default: ``10*60`` 


``osd heartbeat address``

:Description: An OSD's network address for heartbeats. 
:Type: Address
:Default: The host address.


``osd heartbeat interval`` 

:Description: How often an OSD pings its peers (in seconds).
:Type: 32-bit Integer
:Default: ``6``


``osd heartbeat grace`` 

:Description: The elapsed time when an OSD hasn't shown a heartbeat that the cluster considers it ``down``. 
:Type: 32-bit Integer
:Default: ``20``


``osd mon heartbeat interval`` 

:Description: How often the OSD pings a monitor if it has no OSD peers.
:Type: 32-bit Integer
:Default: ``30`` 


``osd mon report interval max`` 

:Description: The maximum time in seconds for an OSD to report to a monitor before the monitor considers the OSD ``down``.
:Type: 32-bit Integer
:Default: ``120`` 


``osd mon report interval min`` 

:Description: The minimum number of seconds for an OSD to report to a monitor to avoid the monitor considering the OSD ``down``.
:Type: 32-bit Integer
:Default: ``5``
:Valid Range: Should be less than ``osd mon report interval max`` 


``osd mon ack timeout`` 

:Description: The number of seconds to wait for a monitor to acknowledge a request for statistics.
:Type: 32-bit Integer
:Default: ``30`` 


``osd min down reporters`` 

:Description: The minimum number of OSDs required to report a ``down`` OSD.
:Type: 32-bit Integer
:Default: ``1``


``osd min down reports`` 

:Description: The minimum number of times an OSD must report that another is ``down``.
:Type: 32-bit Integer
:Default: ``3`` 


``osd recovery delay start`` 

:Description: After peering completes, Ceph will delay for the specified number of seconds before starting to recover objects.
:Type: Float
:Default: ``15`` 


``osd recovery max active`` 

:Description: The number of active recovery requests per OSD at one time. More accelerates recovery, but places an increased load on the cluster.
:Type: 32-bit Integer
:Default: ``5``


``osd recovery max chunk`` 

:Description: The maximum size of a recovered chunk of data to push. 
:Type: 64-bit Integer Unsigned
:Default: ``1 << 20`` 


``osd max scrubs`` 

:Description: The maximum number of scrub operations for an OSD.
:Type: 32-bit Int
:Default: ``1`` 


``osd scrub load threshold`` 

:Description: The maximum CPU load. Ceph will not scrub when the CPU load is higher than this number. Default is 50%.
:Type: Float
:Default: ``0.5`` 


``osd scrub min interval`` 

:Description: The maximum interval in seconds for scrubbing the OSD.
:Type: Float
:Default: 5 minutes. ``300`` 


``osd scrub max interval`` 

:Description: The maximum interval in seconds for scrubbing the OSD.
:Type: Float
:Default: Once per day. ``60*60*24`` 


``osd deep scrub interval``

:Description: The interval for "deep" scrubbing (fully reading all data).
:Type: Float
:Default: Once per week.  ``60*60*24*7``


``osd deep scrub stride``

:Description: Read size when doing a deep scrub.
:Type: 32-bit Int
:Default: 512 KB. ``524288``


``osd class dir`` 

:Description: The class path for RADOS class plug-ins.
:Type: String
:Default: ``$libdir/rados-classes``


``osd check for log corruption`` 

:Description: Check log files for corruption. Can be computationally expensive.
:Type: Boolean
:Default: ``false`` 


``osd default notify timeout`` 

:Description: The OSD default notification timeout (in seconds).
:Type: 32-bit Integer Unsigned
:Default: ``30`` 


``osd min pg log entries`` 

:Description: The minimum number of placement group logs to maintain when trimming log files.
:Type: 32-bit Int Unsigned
:Default: 1000


``osd op complaint time`` 

:Description: An operation becomes complaint worthy after the specified number of seconds have elapsed.
:Type: Float
:Default: ``30`` 


``osd command max records`` 

:Description: Limits the number of lost objects to return. 
:Type: 32-bit Integer
:Default: ``256`` 


``osd auto upgrade tmap`` 

:Description: Uses ``tmap`` for ``omap`` on old objects.
:Type: Boolean
:Default: ``true``
 

``osd tmapput sets users tmap`` 

:Description: Uses ``tmap`` for debugging only.
:Type: Boolean
:Default: ``false`` 

