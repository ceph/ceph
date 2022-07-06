  $ ceph rbd -h | sed -n '/^ Monitor commands: $/,$p'
   Monitor commands: 
   =================
  rbd mirror snapshot schedule add        Add rbd mirror snapshot schedule
   <level_spec> <interval> [<start_time>] 
  rbd mirror snapshot schedule list       List rbd mirror snapshot schedule
   [<level_spec>]                         
  rbd mirror snapshot schedule remove     Remove rbd mirror snapshot schedule
   <level_spec> [<interval>] [<start_     
   time>]                                 
  rbd mirror snapshot schedule status     Show rbd mirror snapshot schedule status
   [<level_spec>]                         
  rbd perf image counters [<pool_spec>]   Retrieve current RBD IO performance 
   [<sort_by:write_ops|write_bytes|write_  counters
   latency|read_ops|read_bytes|read_      
   latency>]                              
  rbd perf image stats [<pool_spec>]      Retrieve current RBD IO performance 
   [<sort_by:write_ops|write_bytes|write_  stats
   latency|read_ops|read_bytes|read_      
   latency>]                              
  rbd task add flatten <image_spec>       Flatten a cloned image asynchronously 
                                           in the background
  rbd task add migration abort <image_    Abort a prepared migration 
   spec>                                   asynchronously in the background
  rbd task add migration commit <image_   Commit an executed migration 
   spec>                                   asynchronously in the background
  rbd task add migration execute <image_  Execute an image migration 
   spec>                                   asynchronously in the background
  rbd task add remove <image_spec>        Remove an image asynchronously in the 
                                           background
  rbd task add trash remove <image_id_    Remove an image from the trash 
   spec>                                   asynchronously in the background
  rbd task cancel <task_id>               Cancel a pending or running 
                                           asynchronous task
  rbd task list [<task_id>]               List pending or running asynchronous 
                                           tasks
  rbd trash purge schedule add <level_    Add rbd trash purge schedule
   spec> <interval> [<start_time>]        
  rbd trash purge schedule list [<level_  List rbd trash purge schedule
   spec>]                                 
  rbd trash purge schedule remove <level_ Remove rbd trash purge schedule
   spec> [<interval>] [<start_time>]      
  rbd trash purge schedule status         Show rbd trash purge schedule status
   [<level_spec>]                         
