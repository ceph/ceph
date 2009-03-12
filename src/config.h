// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef __CEPH_CONFIG_H
#define __CEPH_CONFIG_H

extern struct ceph_file_layout g_default_file_layout;
extern struct ceph_file_layout g_default_casdata_layout;
extern struct ceph_file_layout g_default_mds_dir_layout;
extern struct ceph_file_layout g_default_mds_log_layout;
extern struct ceph_file_layout g_default_mds_anchortable_layout;

extern const char *get_pool_name(int pool);

#include <vector>
#include <map>

#include "include/assert.h"

#include "common/Mutex.h"

#define OSD_REP_PRIMARY 0
#define OSD_REP_SPLAY   1
#define OSD_REP_CHAIN   2


#include "msg/msg_types.h"

extern entity_addr_t g_my_addr;

struct md_config_t {
  char *type;
  char *id;
  char *name;
  char *alt_name;

  int num_mon;
  int num_mds;
  int num_osd;
  int num_client;

  //bool mkfs;
  
  const char *monmap;
  const char *mon_host;
  bool daemonize;

  // logger (profiling)
  bool logger;
  int logger_interval;
  bool logger_calc_variance;
  const char *logger_subdir;
  const char *logger_dir;

  const char *log_dir;
  const char *log_sym_dir;
  bool log_to_stdout;

  const char *pid_file;

  const char *conf;

  const char *chdir;

  bool fake_clock;
  bool fakemessenger_serialize;

  int kill_after;

  int debug;
  int debug_lockdep;
  int debug_mds;
  int debug_mds_balancer;
  int debug_mds_log;
  int debug_mds_log_expire;
  int debug_mds_migrator;
  int debug_buffer;
  int debug_timer;
  int debug_filer;
  int debug_objecter;
  int debug_journaler;
  int debug_objectcacher;
  int debug_client;
  int debug_osd;
  int debug_ebofs;
  int debug_filestore;
  int debug_journal;
  int debug_bdev;
  int debug_ns;
  int debug_ms;
  int debug_mon;
  int debug_paxos;
  int debug_tp;

  // clock
  bool clock_lock;
  bool clock_tare;

  // messenger

  /*bool tcp_skip_rank0;
  bool tcp_overlay_clients;
  bool tcp_log;
  bool tcp_serial_marshall;
  bool tcp_serial_out;
  bool tcp_multi_out;
  bool tcp_multi_dispatch;
  */

  bool ms_tcp_nodelay;
  double ms_retry_interval;
  double ms_fail_interval;
  bool ms_die_on_failure;
  bool ms_nocrc;

  // mon
  const char *mon_data;
  int mon_tick_interval;
  int mon_osd_down_out_interval;
  float mon_lease;
  float mon_lease_renew_interval;
  float mon_lease_ack_timeout;
  float mon_lease_timeout;
  float mon_accept_timeout;
  bool mon_stop_on_last_unmount;
  bool mon_stop_with_last_mds;
  bool mon_allow_mds_bully;
  float mon_pg_create_interval;

  double paxos_propose_interval;
  double paxos_observer_timeout;

  // client
  int      client_cache_size;
  float    client_cache_mid;
  int      client_cache_stat_ttl;
  int      client_cache_readdir_ttl;
  bool     client_use_random_mds;          // debug flag
  double   client_mount_timeout;
  double   client_tick_interval;
  bool client_hack_balance_reads;
  const char *client_trace;
  long long client_readahead_min;
  long long client_readahead_max_bytes;
  long long client_readahead_max_periods;
  const char *client_snapdir;
  int fuse_direct_io;
  bool fuse_ll;

  // objectcacher
  bool     client_oc;
  int      client_oc_size;
  int      client_oc_max_dirty;
  int      client_oc_target_dirty;
  long long unsigned   client_oc_max_sync_write;

  // objecter
  bool  objecter_buffer_uncommitted;
  double objecter_map_request_interval;
  double objecter_tick_interval;
  double objecter_timeout;

  // journaler
  bool  journaler_allow_split_entries;
  bool  journaler_safe;
  int   journaler_write_head_interval;
  bool  journaler_cache;
  int   journaler_prefetch_periods;
  double journaler_batch_interval;
  long long unsigned journaler_batch_max;
  
  // mds
  int   mds_cache_size;
  float mds_cache_mid;
  
  float mds_decay_halflife;

  float mds_beacon_interval;
  float mds_beacon_grace;
  float mds_blacklist_interval;

  float mds_session_timeout;
  float mds_session_autoclose;
  float mds_client_lease;
  float mds_reconnect_timeout;

  float mds_tick_interval;
  float mds_scatter_nudge_interval;

  int mds_client_prealloc_inos;
  bool mds_early_reply;

  int mds_rdcap_ttl_ms;

  bool mds_log;
  bool mds_log_unsafe;
  int mds_log_max_events;
  int mds_log_max_segments;
  int mds_log_max_expiring;
  int mds_log_pad_entry;
  int mds_log_eopen_size;
  
  float mds_bal_sample_interval;  
  float mds_bal_replicate_threshold;
  float mds_bal_unreplicate_threshold;
  bool mds_bal_frag;
  int mds_bal_split_size;
  float mds_bal_split_rd;
  float mds_bal_split_wr;
  int mds_bal_merge_size;
  float mds_bal_merge_rd;
  float mds_bal_merge_wr;
  int   mds_bal_interval;
  int   mds_bal_fragment_interval;
  float mds_bal_idle_threshold;
  int   mds_bal_max;
  int   mds_bal_max_until;

  int   mds_bal_mode;
  float mds_bal_min_rebalance;
  float mds_bal_min_start;
  float mds_bal_need_min;
  float mds_bal_need_max;
  float mds_bal_midchunk;
  float mds_bal_minchunk;

  bool  mds_trim_on_rejoin;
  int   mds_shutdown_check;

  bool  mds_verify_export_dirauth;     // debug flag

  bool  mds_local_osd;

  int mds_thrash_exports;
  int mds_thrash_fragments;
  bool mds_dump_cache_on_map;
  bool mds_dump_cache_after_rejoin;

  bool mds_hack_log_expire_for_better_stats;

  // osd
  const char *osd_data;
  const char *osd_journal;
  bool osd_balance_reads;
  int osd_flash_crowd_iat_threshold;  // flash crowd interarrival time threshold in ms
  double osd_flash_crowd_iat_alpha;
  double osd_balance_reads_temp;

  int  osd_shed_reads;
  double osd_shed_reads_min_latency;
  double osd_shed_reads_min_latency_diff;
  double osd_shed_reads_min_latency_ratio;

  bool  osd_immediate_read_from_cache;
  bool  osd_exclusive_caching;
  double osd_stat_refresh_interval;

  int osd_min_pg_size_without_alive;

  int   osd_pg_bits;
  int   osd_lpg_bits;
  int   osd_object_layout;
  int   osd_pg_layout;
  int   osd_min_rep;
  int   osd_max_rep;
  int   osd_min_raid_width;
  int   osd_max_raid_width;
  int   osd_maxthreads;
  int   osd_max_opq;
  float   osd_age;
  int   osd_age_time;
  int   osd_heartbeat_interval;  
  int   osd_mon_heartbeat_interval;  
  int   osd_heartbeat_grace;
  int   osd_mon_report_interval;
  int   osd_replay_window;
  int   osd_max_pull;
  bool  osd_preserve_trimmed_log;

  float osd_recovery_delay_start;
  int osd_recovery_max_active;

  bool osd_auto_weight;

  // filestore
  bool filestore;
  double   filestore_max_sync_interval;
  double   filestore_min_sync_interval;
  bool  filestore_fake_attrs;
  bool  filestore_fake_collections;
  const char  *filestore_dev;
  bool filestore_btrfs_trans;
  
  // ebofs
  bool  ebofs;
  bool  ebofs_cloneable;
  bool  ebofs_verify;
  int   ebofs_commit_ms;
  int   ebofs_oc_size;
  int   ebofs_cc_size;
  unsigned long long ebofs_bc_size;
  unsigned long long ebofs_bc_max_dirty;
  unsigned ebofs_max_prefetch;
  bool  ebofs_realloc;
  bool ebofs_verify_csum_on_read;
  
  // journal
  bool journal_dio;
  int journal_max_write_bytes;
  int journal_max_write_entries;

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

typedef enum {
	OPT_NONE, OPT_INT, OPT_LONGLONG, OPT_STR, OPT_DOUBLE, OPT_FLOAT, OPT_BOOL
} opt_type_t;

/**
 * command line / environment argument parsing
 */
void env_to_vec(std::vector<const char*>& args);
void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args);
void vec_to_argv(std::vector<const char*>& args,
                 int& argc, const char **&argv);
void env_to_deq(std::deque<const char*>& args);
void argv_to_deq(int argc, const char **argv,
                 std::deque<const char*>& args);

void parse_startup_config_options(std::vector<const char*>& args, const char *module_type);
void parse_config_options(std::vector<const char*>& args);
void parse_config_option_string(string& s);

extern bool parse_ip_port(const char *s, entity_addr_t& addr, const char **end=0);

void configure_daemon_mode();
void configure_client_mode();

void generic_server_usage();
void generic_client_usage();

class ConfFile;
ConfFile *conf_get_conf_file();

char *conf_post_process_val(const char *val);
int conf_read_key(const char *alt_section, const char *key, opt_type_t type, void *inout);

#include "common/debug.h"

#endif
