
#include "include/config.h"

//#define MDS_CACHE_SIZE        4*10000   -> <20mb
//#define MDS_CACHE_SIZE        80000         62mb

#define AVG_PER_INODE_SIZE    400
#define MDS_CACHE_MB_TO_INODES(x) ((x)*1000000/AVG_PER_INODE_SIZE)

#define MDS_CACHE_SIZE       MDS_CACHE_MB_TO_INODES( 100 )
//#define MDS_CACHE_SIZE 25000  // 


md_config_t g_conf = {
  num_mds: 4,
  num_osd: 10,
  num_client: 100,
  
  client_cache_size: 100,
  client_cache_mid: .5,
  client_requests: 50000,
  
  log_messages: true,
  log_interval: 10.0,
  
  mdlog_max_len: 1000,
  mdlog_max_trimming: 16,
  mdlog_read_inc: 4096,

  fake_clock: false,

  debug: 10,

  mdcache_size: MDS_CACHE_SIZE,
  mdcache_mid: .8,
  mdcache_sticky_sync_normal: true,
  mdcache_sticky_sync_softasync: false,
  mdcache_sticky_lock: false,       // sticky is probably a bad idea!

  mdbal_replicate_threshold: 500,
  mdbal_unreplicate_threshold: 200,

  mds_heartbeat_op_interval: 3000
};

