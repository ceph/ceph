
#include "include/config.h"

//#define MDS_CACHE_SIZE        4*10000   -> <20mb
//#define MDS_CACHE_SIZE        80000         62mb

#define AVG_PER_INODE_SIZE    450
#define MDS_CACHE_MB_TO_INODES(x) ((x)*1000000/AVG_PER_INODE_SIZE)

#define MDS_CACHE_SIZE       MDS_CACHE_MB_TO_INODES( 100 )
//#define MDS_CACHE_SIZE 25000  // 


md_config_t g_conf = {
  num_mds: 2,
  num_osd: 5,
  num_client: 1,

  osd_cow: false, // crashy? true,  

  // profiling and debugging
  log_messages: true,
  log_interval: 10.0,

  fake_clock: false,
  fakemessenger_serialize: true,

  debug: 15,
  
  // --- client ---
  client_cache_size: 400,
  client_cache_mid: .5,
  client_cache_stat_ttl: 10, // seconds until cached stat results become invalid
  client_use_random_mds:  false,
  
  // --- mds ---
  mds_cache_size: MDS_CACHE_SIZE,
  mds_cache_mid: .7,

  mds_log_max_len:  MDS_CACHE_SIZE / 3,
  mds_log_max_trimming: 16,
  mds_log_read_inc: 4096,

  mds_bal_replicate_threshold: 500,
  mds_bal_unreplicate_threshold: 200,

  mds_heartbeat_op_interval: 200,
  mds_verify_export_dirauth: true,
  mds_log_before_reply: true,

  // --- fakeclient (mds regression testing) ---
  num_fakeclient: 100,
  fakeclient_requests: 100,
  fakeclient_deterministic: false,

  fakeclient_op_statfs:     false,

  fakeclient_op_stat:     10,
  fakeclient_op_lstat:      false,
  fakeclient_op_utime:    10,   // untested
  fakeclient_op_chmod:    10,
  fakeclient_op_chown:    10,   // untested

  fakeclient_op_readdir:  10,
  fakeclient_op_mknod:    10,
  fakeclient_op_link:     false,
  fakeclient_op_unlink:   10,
  fakeclient_op_rename:   100,

  fakeclient_op_mkdir:    50,
  fakeclient_op_rmdir:    0,  // there's a bug...10,
  fakeclient_op_symlink:  10,

  fakeclient_op_openrd:   100,
  fakeclient_op_openwr:   100,
  fakeclient_op_openwrc:  100,
  fakeclient_op_read:       false,  // osd!
  fakeclient_op_write:      false,  // osd!
  fakeclient_op_truncate:   false,
  fakeclient_op_fsync:      false,
  fakeclient_op_close:    20
};

