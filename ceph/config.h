#ifndef __CONFIG_H
#define __CONFIG_H

struct md_config_t {
  int  num_mds;
  int  num_osd;
  int  num_client;

  bool osd_cow;                      // debug flag?

  int      client_cache_size;
  float    client_cache_mid;
  unsigned client_requests;
  bool     client_deterministic;     // debug flag

  bool log_messages;
  float log_interval;
  
  int mdlog_max_len;
  int mdlog_max_trimming;
  int mdlog_read_inc;
  
  bool fake_clock;
  bool fakemessenger_serialize;

  int  debug;

  int   mdcache_size;
  float mdcache_mid;

  float mdbal_replicate_threshold;
  float mdbal_unreplicate_threshold;

  int   mds_heartbeat_op_interval;
  bool  mds_verify_export_dirauth;     // debug flag

  bool client_use_random_mds;          // debug flag

  int client_op_statfs;

  int client_op_stat;
  int client_op_touch;
  int client_op_utime;
  int client_op_chmod;
  int client_op_chown;

  int client_op_readdir;
  int client_op_mknod;
  int client_op_link;
  int client_op_unlink;
  int client_op_rename;

  int client_op_mkdir;
  int client_op_rmdir;
  int client_op_symlink;

  int client_op_openrd;
  int client_op_openwr;
  int client_op_openwrc;
  int client_op_read;
  int client_op_write;
  int client_op_truncate;
  int client_op_fsync;
  int client_op_close;

};

extern md_config_t g_conf;	 

#define dout(x)  if ((x) <= g_conf.debug) cout
#define dout2(x) if ((x) <= g_conf.debug) cout

#endif
