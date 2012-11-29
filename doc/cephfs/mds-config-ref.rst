======================
 MDS Config Reference
======================

``mds max file size`` 

:Description: 
:Type:  64-bit Integer Unsigned 
:Default:  1ULL << 40  

``mds cache size`` 

:Description: 
:Type:  32-bit Integer          
:Default: 100000 

``mds cache mid`` 

:Description: 
:Type:  Float                   
:Default: 0.7 

``mds mem max`` 

:Description: // KB
:Type:  32-bit Integer          
:Default: 1048576 

``mds dir commit ratio`` 

:Description: 
:Type:  Float                   
:Default: 0.5 

``mds dir max commit size`` 

:Description: // MB
:Type:  32-bit Integer          
:Default: 90 

``mds decay halflife`` 

:Description: 
:Type:  Float                   
:Default: 5 

``mds beacon interval`` 

:Description: 
:Type:  Float                   
:Default: 4 

``mds beacon grace`` 

:Description: 
:Type:  Float                   
:Default: 15 

``mds blacklist interval`` 

:Description: // how long to blacklist failed nodes
:Type:  Float                   
:Default:  24.0*60.0   

``mds session timeout`` 

:Description: // cap bits and leases time out if client idle
:Type:  Float                   
:Default: 60 

``mds session autoclose`` 

:Description: // autoclose idle session
:Type:  Float                   
:Default: 300 

``mds reconnect timeout`` 

:Description: // secs to wait for clients during mds restart
:Type:  Float                   
:Default: 45 

``mds tick interval`` 

:Description: 
:Type:  Float                   
:Default: 5 

``mds dirstat min interval`` 

:Description: //try to avoid propagating more often than x
:Type:  Float                   
:Default: 1 

``mds scatter nudge interval`` 

:Description: // how quickly dirstat changes propagate up
:Type:  Float                   
:Default: 5 

``mds client prealloc inos`` 

:Description: 
:Type:  32-bit Integer          
:Default: 1000 

``mds early reply`` 

:Description: 
:Type:  Boolean                 
:Default:  true        

``mds use tmap`` 

:Description: // use trivialmap for dir updates
:Type:  Boolean                 
:Default:  true        

``mds default dir hash`` 

:Description: CEPH STR HASH RJENKINS
:Type:  32-bit Integer          
:Default:              

``mds log`` 

:Description: 
:Type:  Boolean                 
:Default:  true        

``mds log skip corrupt events`` 

:Description: 
:Type:  Boolean                 
:Default:  false       

``mds log max events`` 

:Description: 
:Type:  32-bit Integer          
:Default: -1 

``mds log max segments`` 

:Description: // segment size defined by FileLayout  above
:Type:  32-bit Integer          
:Default: 30 

``mds log max expiring`` 

:Description: 
:Type:  32-bit Integer          
:Default: 20 

``mds log eopen size`` 

:Description: // # open inodes per log entry
:Type:  32-bit Integer          
:Default: 100 

``mds bal sample interval`` 

:Description: // every 5 seconds
:Type:  Float                   
:Default: 3 

``mds bal replicate threshold`` 

:Description: 
:Type:  Float                   
:Default: 8000 

``mds bal unreplicate threshold`` 

:Description: 
:Type:  Float                   
:Default: 0 

``mds bal frag`` 

:Description: 
:Type:  Boolean                 
:Default:  false       

``mds bal split size`` 

:Description: 
:Type:  32-bit Integer          
:Default: 10000 

``mds bal split rd`` 

:Description: 
:Type:  Float                   
:Default: 25000 

``mds bal split wr`` 

:Description: 
:Type:  Float                   
:Default: 10000 

``mds bal split bits`` 

:Description: 
:Type:  32-bit Integer          
:Default: 3 

``mds bal merge size`` 

:Description: 
:Type:  32-bit Integer          
:Default: 50 

``mds bal merge rd`` 

:Description: 
:Type:  Float                   
:Default: 1000 

``mds bal merge wr`` 

:Description: 
:Type:  Float                   
:Default: 1000 

``mds bal interval`` 

:Description: // seconds
:Type:  32-bit Integer          
:Default: 10 

``mds bal fragment interval`` 

:Description: // seconds
:Type:  32-bit Integer          
:Default: 5 

``mds bal idle threshold`` 

:Description: 
:Type:  Float                   
:Default: 0 

``mds bal max`` 

:Description: 
:Type:  32-bit Integer          
:Default: -1 

``mds bal max until`` 

:Description: 
:Type:  32-bit Integer          
:Default: -1 

``mds bal mode`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``mds bal min rebalance`` 

:Description: // must be x above avg before we export
:Type:  Float                   
:Default: 0.1 

``mds bal min start`` 

:Description: // if we need less x. we don't do anything
:Type:  Float                   
:Default: 0.2 

``mds bal need min`` 

:Description: // take within this range of what we need
:Type:  Float                   
:Default: 0.8 

``mds bal need max`` 

:Description: 
:Type:  Float                   
:Default: 1.2 

``mds bal midchunk`` 

:Description: // any sub bigger than this taken in full
:Type:  Float                   
:Default: 0.3 

``mds bal minchunk`` 

:Description: // never take anything smaller than this
:Type:  Float                   
:Default: 0.001 

``mds bal target removal min`` 

:Description: // min bal iters before old target is removed
:Type:  32-bit Integer          
:Default: 5 

``mds bal target removal max`` 

:Description: // max bal iters before old target is removed
:Type:  32-bit Integer          
:Default: 10 

``mds replay interval`` 

:Description: // time to wait before starting replay again
:Type:  Float                   
:Default: 1 

``mds shutdown check`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``mds thrash exports`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``mds thrash fragments`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``mds dump cache on map`` 

:Description: 
:Type:  Boolean                 
:Default:  false       

``mds dump cache after rejoin`` 

:Description: 
:Type:  Boolean                 
:Default:  false       

``mds verify scatter`` 

:Description: 
:Type:  Boolean                 
:Default:  false       

``mds debug scatterstat`` 

:Description: 
:Type:  Boolean                 
:Default:  false       

``mds debug frag`` 

:Description: 
:Type:  Boolean                 
:Default:  false       

``mds debug auth pins`` 

:Description: 
:Type:  Boolean                 
:Default:  false       

``mds debug subtrees`` 

:Description: 
:Type:  Boolean                 
:Default:  false       

``mds kill mdstable at`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``mds kill export at`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``mds kill import at`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``mds kill link at`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``mds kill rename at`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``mds wipe sessions`` 

:Description: 
:Type:  Boolean                 
:Default: 0 

``mds wipe ino prealloc`` 

:Description: 
:Type:  Boolean                 
:Default: 0 

``mds skip ino`` 

:Description: 
:Type:  32-bit Integer          
:Default: 0 

``max mds`` 

:Description: 
:Type:  32-bit Integer          
:Default: 1 

``mds standby for name`` 

:Description: 
:Type:  String                  
:Default:           

``mds standby for rank`` 

:Description: 
:Type:  32-bit Integer          
:Default: -1 

``mds standby replay`` 

:Description: 
:Type:  Boolean                 
:Default:  false      