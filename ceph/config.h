#ifndef __CONFIG_H
#define __CONFIG_H

struct md_config_t {
  int num_mds;
  int num_osd;
  int num_client;

  int client_cache_size;
  float client_cache_mid;
  unsigned client_requests;

  bool log_messages;
  float log_interval;
  
  int mdlog_max_trimming;
  int mdlog_read_inc;
  
  bool fake_clock;

  int debug;

  int mdcache_size;
  float mdcache_mid;
  bool mdcache_sticky_sync_normal;
  bool mdcache_sticky_sync_softasync;
  bool mdcache_sticky_lock;

  float mdbal_replicate_threshold;
  float mdbal_unreplicate_threshold;

  int mds_heartbeat_op_interval;
};

extern md_config_t g_conf;	 

#define dout(x)  if ((x) <= g_conf.debug) cout
#define dout2(x) if ((x) <= g_conf.debug) cout

#endif
