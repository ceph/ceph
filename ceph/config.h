#ifndef __CONFIG_H
#define __CONFIG_H

extern class FileLayout g_OSD_FileLayout;
extern class FileLayout g_OSD_MDDirLayout;
extern class FileLayout g_OSD_MDLogLayout;

#include <vector>
using namespace std;

struct md_config_t {
  int  num_mds;
  int  num_osd;
  int  num_client;


  // profiling
  bool  log;
  int   log_interval;
  char *log_name;

  bool log_messages;
  bool log_pins;

  bool fake_clock;
  bool fakemessenger_serialize;

  int fake_osdmap_expand;
  bool fake_osd_sync;

  int debug;
  int debug_mds_balancer;
  int debug_mds_log;
  int debug_buffer;
  int debug_filer;
  int debug_client;
  int debug_osd;
  int debug_bdev;

  // client
  int      client_cache_size;
  float    client_cache_mid;
  int      client_cache_stat_ttl;
  bool     client_use_random_mds;          // debug flag

  bool     client_sync_writes;
  bool     client_bcache;
  int      client_bcache_alloc_minsize;
  int      client_bcache_alloc_maxsize;
  int      client_bcache_ttl;
  off_t    client_bcache_size;
  int      client_bcache_lowater;
  int      client_bcache_hiwater;
  size_t   client_bcache_align;

  int      client_trace;
  int      fuse_direct_io;

  // mds
  int   mds_cache_size;
  float mds_cache_mid;

  bool mds_log;
  int mds_log_max_len;
  int mds_log_max_trimming;
  int mds_log_read_inc;
  int mds_log_pad_entry;
  bool  mds_log_before_reply;
  bool  mds_log_flush_on_shutdown;
  
  float mds_bal_replicate_threshold;
  float mds_bal_unreplicate_threshold;
  int   mds_bal_interval;
  float mds_bal_idle_threshold;
  int   mds_bal_max;
  int   mds_bal_max_until;

  bool  mds_commit_on_shutdown;
  bool  mds_verify_export_dirauth;     // debug flag


  // osd
  int   osd_pg_bits;
  int   osd_max_rep;
  bool  osd_fsync;
  bool  osd_writesync;
  int   osd_maxthreads;

  int osd_fakestore_syncthreads;

  off_t ebofs_bc_size;

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

void argv_to_vec(int argc, char **argv,
				 vector<char*>& args);
void vec_to_argv(vector<char*>& args,
				 int& argc, char **&argv);

void parse_config_options(vector<char*>& args);

#endif
