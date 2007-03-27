// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __CONFIG_H
#define __CONFIG_H

extern class FileLayout g_OSD_FileLayout;
extern class FileLayout g_OSD_MDDirLayout;
extern class FileLayout g_OSD_MDLogLayout;

#include <vector>
#include <map>

extern std::map<int,float> g_fake_osd_down;
extern std::map<int,float> g_fake_osd_out;

#define OSD_REP_PRIMARY 0
#define OSD_REP_SPLAY   1
#define OSD_REP_CHAIN   2


#include "msg/msg_types.h"

extern entity_addr_t g_my_addr;

struct md_config_t {
  int  num_mon;
  int  num_mds;
  int  num_osd;
  int  num_client;

  bool mkfs;

  // profiling
  bool  log;
  int   log_interval;
  char *log_name;

  bool log_messages;
  bool log_pins;

  bool fake_clock;
  bool fakemessenger_serialize;

  int fake_osdmap_expand;
  int fake_osdmap_updates;
  int fake_osd_mttf;
  int fake_osd_mttr;

  int osd_remount_at;

  int kill_after;

  int tick;

  int debug;
  int debug_mds;
  int debug_mds_balancer;
  int debug_mds_log;
  int debug_buffer;
  int debug_filer;
  int debug_objecter;
  int debug_objectcacher;
  int debug_client;
  int debug_osd;
  int debug_ebofs;
  int debug_bdev;
  int debug_ns;
  int debug_ms;
  int debug_mon;

  int debug_after;

  // misc
  bool use_abspaths;

  // clock
  bool clock_lock;

  // messenger

  /*bool tcp_skip_rank0;
  bool tcp_overlay_clients;
  bool tcp_log;
  bool tcp_serial_marshall;
  bool tcp_serial_out;
  bool tcp_multi_out;
  bool tcp_multi_dispatch;
  */

  bool ms_single_dispatch;
  bool ms_requeue_on_sender_fail;

  bool ms_stripe_osds;
  bool ms_skip_rank0;
  bool ms_overlay_clients;
  bool ms_die_on_failure;
  bool ms_tcp_nodelay;

  // mon
  int mon_tick_interval;
  int mon_osd_down_out_interval;
  float mon_lease;
  bool mon_stop_with_last_mds;

  // client
  int      client_cache_size;
  float    client_cache_mid;
  int      client_cache_stat_ttl;
  int      client_cache_readdir_ttl;
  bool     client_use_random_mds;          // debug flag

  bool     client_sync_writes;

  bool     client_oc;
  int      client_oc_size;
  int      client_oc_max_dirty;
  size_t   client_oc_max_sync_write;

  

  /*
  bool     client_bcache;
  int      client_bcache_alloc_minsize;
  int      client_bcache_alloc_maxsize;
  int      client_bcache_ttl;
  off_t    client_bcache_size;
  int      client_bcache_lowater;
  int      client_bcache_hiwater;
  size_t   client_bcache_align;
  */

  int      client_trace;
  int      fuse_direct_io;

  // objecter
  bool  objecter_buffer_uncommitted;

  // journaler
  bool  journaler_allow_split_entries;

  // mds
  int   mds_cache_size;
  float mds_cache_mid;
  
  float mds_decay_halflife;

  float mds_beacon_interval;
  float mds_beacon_grace;

  bool mds_log;
  int mds_log_max_len;
  int mds_log_max_trimming;
  int mds_log_read_inc;
  int mds_log_pad_entry;
  bool  mds_log_before_reply;
  bool  mds_log_flush_on_shutdown;
  off_t mds_log_import_map_interval;
  
  float mds_bal_replicate_threshold;
  float mds_bal_unreplicate_threshold;
  float mds_bal_hash_rd;
  float mds_bal_unhash_rd;
  float mds_bal_hash_wr;
  float mds_bal_unhash_wr;
  int   mds_bal_interval;
  int   mds_bal_hash_interval;
  float mds_bal_idle_threshold;
  int   mds_bal_max;
  int   mds_bal_max_until;

  int   mds_bal_mode;
  float mds_bal_min_start;
  float mds_bal_need_min;
  float mds_bal_need_max;
  float mds_bal_midchunk;
  float mds_bal_minchunk;

  bool  mds_commit_on_shutdown;
  int   mds_shutdown_check;
  bool  mds_shutdown_on_last_unmount;
  bool  mds_verify_export_dirauth;     // debug flag

  bool  mds_local_osd;


  // osd
  int   osd_rep;
  bool  osd_balance_reads;
  int   osd_pg_bits;
  int   osd_object_layout;
  int   osd_pg_layout;
  int   osd_max_rep;
  int   osd_maxthreads;
  int   osd_max_opq;
  bool  osd_mkfs;
  float   osd_age;
  int   osd_age_time;
  int   osd_heartbeat_interval;
  int   osd_replay_window;
  int   osd_max_pull;
  bool  osd_pad_pg_log;

  int   fakestore_fake_sync;
  bool  fakestore_fsync;
  bool  fakestore_writesync;
  int   fakestore_syncthreads;   // such crap
  bool  fakestore_fake_attrs;
  bool  fakestore_fake_collections;
  char  *fakestore_dev;

  // ebofs
  int   ebofs;
  bool  ebofs_cloneable;
  bool  ebofs_verify;
  int   ebofs_commit_ms;
  int   ebofs_idle_commit_ms;
  int   ebofs_oc_size;
  int   ebofs_cc_size;
  off_t ebofs_bc_size;
  off_t ebofs_bc_max_dirty;
  unsigned ebofs_max_prefetch;
  bool  ebofs_realloc;

  bool   ebofs_abp_zero;
  size_t ebofs_abp_max_alloc;

  int uofs;
  int uofs_fake_sync;
  int     uofs_cache_size;
  int     uofs_onode_size;
  int     uofs_small_block_size;
  int     uofs_large_block_size;
  int     uofs_segment_size;
  int     uofs_block_meta_ratio;
  int     uofs_sync_write;
  
  int     uofs_nr_hash_buckets;
  int     uofs_flush_interval;
  int     uofs_min_flush_pages;
  int     uofs_delay_allocation;

  // block device
  bool  bdev_lock;
  int   bdev_iothreads;
  int   bdev_idle_kick_after_ms;
  int   bdev_el_fw_max_ms;  
  int   bdev_el_bw_max_ms;
  bool  bdev_el_bidir;
  int   bdev_iov_max;
  bool  bdev_debug_check_io_overlap;
  int   bdev_fake_mb;
  int   bdev_fake_max_mb;

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

  // security (all princiapls)
  //bool secure_io;
  int secure_io;
  int mds_group;
  int mds_collection;
  char *unix_group_file;
  int fix_client_id;
  int renewal;
  int renewal_period;
  char* config_predict;
  int collect_predictions;
  int preload_unix_groups;
  int client_aux;
  int sign_scheme;
  int hash_scheme;
  int crypt_scheme;

#ifdef USE_OSBDB
  bool bdbstore;
  int debug_bdbstore;
  bool bdbstore_btree;
  int bdbstore_ffactor;
  int bdbstore_nelem;
  int bdbstore_pagesize;
  int bdbstore_cachesize;
  bool bdbstore_transactional;
#endif // USE_OSBDB
};

extern md_config_t g_conf;     
extern md_config_t g_debug_after_conf;     

#define dout(x)  if ((x) <= g_conf.debug) std::cout
#define dout2(x) if ((x) <= g_conf.debug) std::cout

void env_to_vec(std::vector<char*>& args);
void argv_to_vec(int argc, char **argv,
                 std::vector<char*>& args);
void vec_to_argv(std::vector<char*>& args,
                 int& argc, char **&argv);

void parse_config_options(std::vector<char*>& args);

extern bool parse_ip_port(const char *s, entity_addr_t& addr);



#endif
