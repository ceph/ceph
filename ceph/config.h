#ifndef __CONFIG_H
#define __CONFIG_H

struct md_config_t {
  int  num_mds;
  int  num_osd;
  int  num_client;

  bool osd_cow;                      // debug flag?

  // profiling
  bool log_messages;
  float log_interval;

  bool fake_clock;
  bool fakemessenger_serialize;

  int  debug;

 
  // client
  int      client_cache_size;
  float    client_cache_mid;
  int      client_cache_stat_ttl;
  bool     client_use_random_mds;          // debug flag

  // mds
  int mds_log_max_len;
  int mds_log_max_trimming;
  int mds_log_read_inc;
  
  int   mds_cache_size;
  float mds_cache_mid;

  float mds_bal_replicate_threshold;
  float mds_bal_unreplicate_threshold;

  int   mds_heartbeat_op_interval;
  bool  mds_verify_export_dirauth;     // debug flag
  bool  mds_log_before_reply;

  // fake client
  int      num_fakeclient;
  unsigned fakeclient_requests;
  bool     fakeclient_deterministic;     // debug flag

  int fakeclient_op_statfs;

  int fakeclient_op_stat;
  int fakeclient_op_lstat;
  int fakeclient_op_utime;
  int fakeclient_op_chmod;
  int fakeclient_op_chown;

  int fakeclient_op_readdir;
  int fakeclient_op_mknod;
  int fakeclient_op_link;
  int fakeclient_op_unlink;
  int fakeclient_op_rename;

  int fakeclient_op_mkdir;
  int fakeclient_op_rmdir;
  int fakeclient_op_symlink;

  int fakeclient_op_openrd;
  int fakeclient_op_openwr;
  int fakeclient_op_openwrc;
  int fakeclient_op_read;
  int fakeclient_op_write;
  int fakeclient_op_truncate;
  int fakeclient_op_fsync;
  int fakeclient_op_close;

};

extern md_config_t g_conf;	 

#define dout(x)  if ((x) <= g_conf.debug) cout
#define dout2(x) if ((x) <= g_conf.debug) cout

#endif
