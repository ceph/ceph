
#include "include/config.h"

//#define MDS_CACHE_SIZE        4*10000   -> <20mb
//#define MDS_CACHE_SIZE        80000         62mb

#define AVG_PER_INODE_SIZE    400
#define MDS_CACHE_MB_TO_INODES(x) ((x)*1000000/AVG_PER_INODE_SIZE)

#define MDS_CACHE_SIZE       MDS_CACHE_MB_TO_INODES( 100 )
//#define MDS_CACHE_SIZE 25000  // 


md_config_t g_conf = {
  num_mds: 13,
  num_osd: 10,
  num_client: 1,

  osd_cow: false, // crashy? true,  

  client_cache_size: 100,
  client_cache_mid: .5,
  client_requests: 10000,
  client_deterministic: false,
  
  log_messages: true,
  log_interval: 10.0,
  
  mdlog_max_len: 1000,
  mdlog_max_trimming: 16,
  mdlog_read_inc: 4096,

  fake_clock: true,
  fakemessenger_serialize: false,

  debug: 15,

  mdcache_size: MDS_CACHE_SIZE,
  mdcache_mid: .8,

  mdbal_replicate_threshold: 500,
  mdbal_unreplicate_threshold: 200,

  mds_heartbeat_op_interval: 2000,
  mds_verify_export_dirauth: true,

  client_op_statfs:  false,

  client_op_stat:    true,
  client_op_touch:   true,
  client_op_utime:   true,   // untested
  client_op_chmod:   true,
  client_op_chown:   true,   // untested

  client_op_readdir: true,
  client_op_mknod:   true,
  client_op_link:    false,
  client_op_unlink:  false,
  client_op_rename:  false,

  client_op_mkdir:   false,//true,  // note: corrupts osddata if we don't shut down & flush logs cleanly
  client_op_rmdir:   false,
  client_op_symlink: true,

  client_op_openrd:   true,
  client_op_openwr:   true,
  client_op_openwrc:  true,
  client_op_read:     false,
  client_op_write:    false,
  client_op_truncate: false,
  client_op_fsync:    false,
  client_op_close:    true
};

