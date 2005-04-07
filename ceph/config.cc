
#include "include/config.h"

//#define MDS_CACHE_SIZE        4*10000   -> <20mb
//#define MDS_CACHE_SIZE        80000         62mb

#define AVG_PER_INODE_SIZE    400
#define MDS_CACHE_MB_TO_INODES(x) ((x)*1000000/AVG_PER_INODE_SIZE)

#define MDS_CACHE_SIZE       MDS_CACHE_MB_TO_INODES( 100 )
//#define MDS_CACHE_SIZE 25000  // 


md_config_t g_conf = {
  num_mds: 33,
  num_osd: 10,
  num_client: 100,

  osd_cow: false, // crashy? true,  

  client_cache_size: 100,
  client_cache_mid: .5,
  client_requests: 1000,
  client_deterministic: false,
  
  log_messages: true,
  log_interval: 10.0,
  
  mdlog_max_len: 1000,
  mdlog_max_trimming: 16,
  mdlog_read_inc: 4096,

  fake_clock: true,
  fakemessenger_serialize: false,

  debug: 10,

  mdcache_size: MDS_CACHE_SIZE,
  mdcache_mid: .8,
  mdcache_sticky_sync_normal: true,
  mdcache_sticky_sync_softasync: false,
  mdcache_sticky_lock: false,       // sticky is probably a bad idea!

  mdbal_replicate_threshold: 500,
  mdbal_unreplicate_threshold: 200,

  mds_heartbeat_op_interval: 2000,
  mds_verify_export_dirauth: true,

  client_op_statfs: false,

  client_op_stat: true,
  client_op_touch: true,
  client_op_utime: false,
  client_op_chmod: true,
  client_op_chown: false,

  client_op_readdir: true,
  client_op_mknod: false,
  client_op_link: false,
  client_op_unlink: false,
  client_op_rename: false,

  client_op_mkdir: false, 
  client_op_rmdir: false,
  client_op_symlink: false,

  client_op_openrd: false,
  client_op_openwr: false,
  client_op_openwrc: false,
  client_op_read: false,
  client_op_write: false,
  client_op_truncate: false,
  client_op_fsync: false,
  client_op_close: false
};

