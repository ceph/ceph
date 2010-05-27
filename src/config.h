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

#include <vector>
#include <map>

#include "include/assert.h"

#include "common/Mutex.h"

#define OSD_REP_PRIMARY 0
#define OSD_REP_SPLAY   1
#define OSD_REP_CHAIN   2


#include "msg/msg_types.h"

extern entity_addr_t g_my_addr;

extern bool g_daemon;
extern const char *g_default_id;

extern void ceph_set_default_id(const char *id);

struct EntityName;

struct md_config_t {
  char *type;
  char *id;
  char *name;
  char *alt_name;
  char *host;

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
  int debug_rados;
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
  int debug_monc;
  int debug_paxos;
  int debug_tp;
  int debug_auth;
  int debug_finisher;

  // clock
  bool clock_lock;
  bool clock_tare;

  // auth
  char *key;
  char *keyfile;
  char *keyring;
  char *supported_auth;

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
  double ms_initial_backoff;
  double ms_max_backoff;
  bool ms_die_on_failure;
  bool ms_nocrc;
  bool ms_die_on_bad_msg;
  uint64_t ms_waiting_message_bytes;

  // mon
  const char *mon_data;
  int mon_tick_interval;
  double mon_subscribe_interval;
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
  int mon_clientid_prealloc;
  int mon_globalid_prealloc;

  double paxos_propose_interval;
  double paxos_observer_timeout;

  // auth
  double auth_mon_ticket_ttl;
  double auth_service_ticket_ttl;
  int auth_nonce_len;
  EntityName *entity_name;

  // client
  int      client_cache_size;
  float    client_cache_mid;
  int      client_cache_stat_ttl;
  int      client_cache_readdir_ttl;
  bool     client_use_random_mds;          // debug flag
  double   client_mount_timeout;
  double   client_unmount_timeout;
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
  double objecter_mon_retry_interval;
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
  uint64_t mds_max_file_size;
  int   mds_cache_size;
  float mds_cache_mid;
  int   mds_mem_max;
  
  float mds_decay_halflife;

  float mds_beacon_interval;
  float mds_beacon_grace;
  float mds_blacklist_interval;

  float mds_session_timeout;
  float mds_session_autoclose;
  float mds_client_lease;
  float mds_reconnect_timeout;

  float mds_tick_interval;
  float mds_dirstat_min_interval;
  float mds_scatter_nudge_interval;

  int mds_client_prealloc_inos;
  bool mds_early_reply;
  bool mds_short_reply_trace;

  bool mds_use_tmap;

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

  int mds_bal_target_removal_min;
  int mds_bal_target_removal_max;

  bool  mds_trim_on_rejoin;
  int   mds_shutdown_check;

  bool  mds_verify_export_dirauth;     // debug flag

  bool  mds_local_osd;

  int mds_thrash_exports;
  int mds_thrash_fragments;
  bool mds_dump_cache_on_map;
  bool mds_dump_cache_after_rejoin;

  bool mds_hack_log_expire_for_better_stats;

  // set these to non-zero to specify kill points
  int mds_kill_mdstable_at;
  int mds_kill_export_at;
  int mds_kill_import_at;

  // osd
  const char *osd_data;
  const char *osd_journal;
  int osd_journal_size;  // in mb
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
  int   osd_op_threads;
  int   osd_max_opq;
  int   osd_disk_threads;
  int   osd_recovery_threads;

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

  bool osd_class_timeout;

  // filestore
  bool filestore;
  double   filestore_max_sync_interval;
  double   filestore_min_sync_interval;
  bool  filestore_fake_attrs;
  bool  filestore_fake_collections;
  const char  *filestore_dev;
  bool filestore_btrfs_trans;
  bool filestore_btrfs_snap;
  bool filestore_flusher;
  int filestore_flusher_max_fds;
  bool filestore_sync_flush;
  bool filestore_journal_parallel;
  bool filestore_journal_writeahead;
  int filestore_queue_max_ops;
  int filestore_queue_max_bytes;
  int filestore_op_threads;
  
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
  bool journal_block_align;
  int journal_max_write_bytes;
  int journal_max_write_entries;
  int journal_queue_max_ops;
  int journal_queue_max_bytes;

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

void parse_startup_config_options(std::vector<const char*>& args, bool isdaemon, const char *module_type);
void parse_config_options(std::vector<const char*>& args);
void parse_config_option_string(string& s);

extern bool parse_ip_port(const char *s, entity_addr_t& addr, const char **end=0);
extern bool parse_ip_port_vec(const char *s, vector<entity_addr_t>& vec);

void generic_server_usage();
void generic_client_usage();
void generic_usage();

class ConfFile;
ConfFile *conf_get_conf_file();

char *conf_post_process_val(const char *val);
int conf_read_key(const char *alt_section, const char *key, opt_type_t type, void *out, void *def, bool free_old_val = false);
bool conf_set_conf_val(void *field, opt_type_t type, const char *val);
bool conf_cmd_equals(const char *cmd, const char *opt, char char_opt, unsigned int *val_pos);

class ExportControl;
ExportControl *conf_get_export_control();


#define CONF_NEXT_VAL (val_pos ? &args[i][val_pos] : args[++i])

#define CONF_SET_ARG_VAL(dest, type) \
	conf_set_conf_val(dest, type, CONF_NEXT_VAL)

#define CONF_SAFE_SET_ARG_VAL(dest, type) \
	do { \
          if (type == OPT_BOOL) { \
		if (val_pos) { \
			CONF_SET_ARG_VAL(dest, type); \
		} else \
			conf_set_conf_val(dest, type, "true"); \
          } else if (__isarg || val_pos) { \
		CONF_SET_ARG_VAL(dest, type); \
	  } else if (args_usage) \
		args_usage(); \
	} while (0)

#define CONF_SET_BOOL_ARG_VAL(dest) \
	conf_set_conf_val(dest, OPT_BOOL, (val_pos ? &args[i][val_pos] : "true"))

#define CONF_ARG_EQ(str_cmd, char_cmd) \
	conf_cmd_equals(args[i], str_cmd, char_cmd, &val_pos)

#define DEFINE_CONF_VARS(usage_func) \
	unsigned int val_pos; \
	void (*args_usage)() = usage_func; \
	bool __isarg


#define FOR_EACH_ARG(args) \
	__isarg = 1 < args.size(); \
	for (unsigned i=0; i<args.size(); i++, __isarg = i+1 < args.size()) 

#define ARGS_USAGE() args_usage();

#include "common/debug.h"

#endif
