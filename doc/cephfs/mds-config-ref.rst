======================
 MDS Config Reference
======================

``mds max file size``

:Description: Maximum allowed file size to set when creating a new file system.
:Type:  64-bit Integer Unsigned
:Default:  1ULL << 40

``mds cache size``

:Description: Number of inodes to cache.
:Type:  32-bit Integer
:Default: 100000

``mds cache mid``

:Description: Insertion point for new items in the cache LRU (from the top).
:Type:  Float
:Default: 0.7

``mds dir commit ratio``

:Description: fraction of directory that is dirty before we commit using a full update (intead of partial update)
:Type:  Float
:Default: 0.5

``mds dir max commit size``

:Description: maximum size of a directory update (before we break it into smaller transactions) (MB)
:Type:  32-bit Integer
:Default: 90

``mds decay halflife``

:Description: half-life of mds cache temperature
:Type:  Float
:Default: 5

``mds beacon interval``

:Description: frequency (in seconds) of beacon messages sent to the monitor
:Type:  Float
:Default: 4

``mds beacon grace``

:Description: interval of no beacons before we declare an mds laggy (and possibly replace it)
:Type:  Float
:Default: 15

``mds blacklist interval``

:Description: how long to blacklist failed mds's in the osdmap
:Type:  Float
:Default:  24.0*60.0

``mds session timeout``

:Description: interval (in seconds) of client inactivity before we time out capabilities and leases
:Type:  Float
:Default: 60

``mds session autoclose``

:Description: interval (in seconds) before we close a laggy client's session
:Type:  Float
:Default: 300

``mds reconnect timeout``

:Description: inter (in seconds) to wait for clients to reconnect during mds restart
:Type:  Float
:Default: 45

``mds tick interval``

:Description: how frequently the mds does internal periodic tasks
:Type:  Float
:Default: 5

``mds dirstat min interval``

:Description: minimum interval (in seconds) to try to avoid propagating recursive stats up the tree
:Type:  Float
:Default: 1

``mds scatter nudge interval``

:Description: how quickly dirstat changes propagate up
:Type:  Float
:Default: 5

``mds client prealloc inos``

:Description: number of inode numbers to preallocate per client session
:Type:  32-bit Integer
:Default: 1000

``mds early reply``

:Description: whether the mds should allow clients to see request results before they commit to the journal
:Type:  Boolean
:Default: true

``mds use tmap``

:Description: use trivialmap for dir updates
:Type:  Boolean
:Default: true

``mds default dir hash``

:Description: function to use for hashing files across directory fragments
:Type:  32-bit Integer
:Default: 2 (rjenkins)

``mds log``

:Description: true if the mds should journal metadata updates (disabled for benchmarking only)
:Type:  Boolean
:Default: true

``mds log skip corrupt events``

:Description: whether the mds should try to skip corrupt journal events during journal replay
:Type:  Boolean
:Default:  false

``mds log max events``

:Description: maximum events in the journal before we initiate trimming; -1 to disable limit
:Type:  32-bit Integer
:Default: -1

``mds log max segments``

:Description: maximum segments (objects) in the journal before we initiate trimming; -1 to disable limit
:Type:  32-bit Integer
:Default: 30

``mds log max expiring``

:Description: maximum number of segments to expire in parallel
:Type:  32-bit Integer
:Default: 20

``mds log eopen size``

:Description: maximum number of inodes in an EOpen event
:Type:  32-bit Integer
:Default: 100

``mds bal sample interval``

:Description: how frequently to sample directory temperature (for fragmentation decisions)
:Type:  Float
:Default: 3

``mds bal replicate threshold``

:Description: max temperature before we attempt to replicate metadata to other nodes
:Type:  Float
:Default: 8000

``mds bal unreplicate threshold``

:Description: min temperature before we stop replicating metadata to other nodes
:Type:  Float
:Default: 0

``mds bal frag``

:Description: whether the MDS will fragment directories
:Type:  Boolean
:Default:  false

``mds bal split size``

:Description: maximum directory size before the MDS will split a directory fragment into smaller bits
:Type:  32-bit Integer
:Default: 10000

``mds bal split rd``

:Description: maximum directory read temperature before we split a directory fragment
:Type:  Float
:Default: 25000

``mds bal split wr``

:Description: maximum directory write temperature before we split a directory fragment
:Type:  Float
:Default: 10000

``mds bal split bits``

:Description: number of bits to split a directory fragment by
:Type:  32-bit Integer
:Default: 3

``mds bal merge size``

:Description: minimum directory size before we try to merge adjacent directory fragments
:Type:  32-bit Integer
:Default: 50

``mds bal merge rd``

:Description: minimum read temperature before we merge adjacent directory fragments
:Type:  Float
:Default: 1000

``mds bal merge wr``

:Description: minimum write temperature before we merge adjacent directory fragments
:Type:  Float
:Default: 1000

``mds bal interval``

:Description: frequency (in seconds) of workload exchanges between MDSs
:Type:  32-bit Integer
:Default: 10

``mds bal fragment interval``

:Description: frequency (in seconds) of adjusting directory fragmentation
:Type:  32-bit Integer
:Default: 5

``mds bal idle threshold``

:Description: minimum temperature before we migrate a subtree back to its parent
:Type:  Float
:Default: 0

``mds bal max``

:Description: number of iterations to run balancer before we stop (used for testing purposes only)
:Type:  32-bit Integer
:Default: -1

``mds bal max until``

:Description: number of seconds to run balancer before we stop (used for testing purposes only)
:Type:  32-bit Integer
:Default: -1

``mds bal mode``

:Description: method for calculating MDS load (1 = hybrid, 2 = request rate and latency, 3 = cpu load)
:Type:  32-bit Integer
:Default: 0

``mds bal min rebalance``

:Description: minimum subtree temperature before we migrate
:Type:  Float
:Default: 0.1

``mds bal min start``

:Description: minimum subtree temperature before we search a subtree
:Type:  Float
:Default: 0.2

``mds bal need min``

:Description: minimum fraction of target subtree size to accept
:Type:  Float
:Default: 0.8

``mds bal need max``

:Description: maximum fraction of target subtree size to accept
:Type:  Float
:Default: 1.2

``mds bal midchunk``

:Description: migrate any subtree that is larger than this fraction of the target subtree size
:Type:  Float
:Default: 0.3

``mds bal minchunk``

:Description: ignore any subtree that is smaller than this fraction of the target subtree size
:Type:  Float
:Default: 0.001

``mds bal target removal min``

:Description: min number of balancer iterations before an old MDS target is removed from the mdsmap
:Type:  32-bit Integer
:Default: 5

``mds bal target removal max``

:Description: max number of balancer iteration before an old MDS target is removed from the mdsmap
:Type:  32-bit Integer
:Default: 10

``mds replay interval``

:Description: journal poll interval when in standby-replay ("hot standby") mode
:Type:  Float
:Default: 1

``mds shutdown check``

:Description: interval for polling cache during MDS shutdown
:Type:  32-bit Integer
:Default: 0

``mds thrash exports``

:Description: randomly export subtrees between nodes (testing only)
:Type:  32-bit Integer
:Default: 0

``mds thrash fragments``

:Description: randomly fragment or merge directories
:Type:  32-bit Integer
:Default: 0

``mds dump cache on map``

:Description: dump mds cache contents to a file on each MDSMap
:Type:  Boolean
:Default:  false

``mds dump cache after rejoin``

:Description: dump mds cache contents to a file after rejoining cache (during recovery)
:Type:  Boolean
:Default:  false

``mds verify scatter``

:Description: assert that various scatter/gather invariants are true (dev only)
:Type:  Boolean
:Default:  false

``mds debug scatterstat``

:Description: assert that various recursive stat invariants are true (dev only
:Type:  Boolean
:Default:  false

``mds debug frag``

:Description: verify directory fragmentation invariants when convenient (dev only)
:Type:  Boolean
:Default:  false

``mds debug auth pins``

:Description: debug auth pin invariants (dev only)
:Type:  Boolean
:Default:  false

``mds debug subtrees``

:Description: debug subtree invariants (dev only)
:Type:  Boolean
:Default:  false

``mds kill mdstable at``

:Description: inject mds failure in MDSTable code (dev only)
:Type:  32-bit Integer
:Default: 0

``mds kill export at``

:Description: inject mds failure in subtree export code (dev only)
:Type:  32-bit Integer
:Default: 0

``mds kill import at``

:Description: inject mds failure in subtree import code (dev only)
:Type:  32-bit Integer
:Default: 0

``mds kill link at``

:Description: inject mds failure in hard link code (dev only)
:Type:  32-bit Integer
:Default: 0

``mds kill rename at``

:Description: inject mds failure in rename code (dev only)
:Type:  32-bit Integer
:Default: 0

``mds wipe sessions``

:Description: delete all client sessions on startup (testing only)
:Type:  Boolean
:Default: 0

``mds wipe ino prealloc``

:Description: delete ino preallocation metadata on startup (testing only)
:Type:  Boolean
:Default: 0

``mds skip ino``

:Description: number of inode numbers to skip on startup (testing only)
:Type:  32-bit Integer
:Default: 0

``mds standby for name``

:Description: name of MDS for a ceph-mds daemon to standby for
:Type:  String
:Default:

``mds standby for rank``

:Description: rank of MDS for a ceph-mds daemon to standby for
:Type:  32-bit Integer
:Default: -1

``mds standby replay``

:Description: whether a ceph-mds should poll and replay the log an active mds (hot standby)
:Type:  Boolean
:Default:  false
