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

#ifndef CEPH_CONFIG_H
#define CEPH_CONFIG_H

extern struct ceph_file_layout g_default_file_layout;

#include <vector>
#include <map>

#include "common/ConfUtils.h"
#include "common/entity_name.h"
#include "common/Mutex.h"
#include "include/assert.h"
#include "msg/msg_types.h"

#define OSD_REP_PRIMARY 0
#define OSD_REP_SPLAY   1
#define OSD_REP_CHAIN   2

class config_option;

extern const char *CEPH_CONF_FILE_DEFAULT;

enum log_to_stderr_t {
  LOG_TO_STDERR_NONE = 0,
  LOG_TO_STDERR_SOME = 1,
  LOG_TO_STDERR_ALL = 2,
};

struct md_config_t
{
public:
  md_config_t();
  ~md_config_t();

  // Parse a config file
  int parse_config_files(const std::list<std::string> &conf_files,
			 std::deque<std::string> *parse_errors);

  // Absorb config settings from the environment
  void parse_env();

  // Absorb config settings from argv
  void parse_argv_part2(std::vector<const char*>& args);
  void parse_argv(std::vector<const char*>& args);

  // Set a configuration value.
  // Metavariables will be expanded.
  int set_val(const char *key, const char *val);

  // Get a configuration value.
  // No metavariables will be returned (they will have already been expanded)
  int get_val(const char *key, char **buf, int len) const;

  // Return a list of all the sections that the current entity is a member of.
  void get_my_sections(std::vector <std::string> &sections);

  // Return a list of all sections
  int get_all_sections(std::vector <std::string> &sections);

  // Get a value from the configuration file that we read earlier.
  // Metavariables will be expanded if emeta is true.
  int get_val_from_conf_file(const std::vector <std::string> &sections,
		   const char *key, std::string &out, bool emeta) const;

  // Perform metavariable expansion on all the data members of md_config_t.
  void expand_all_meta();

  // Expand metavariables in the provided string.
  // Returns true if any metavariables were found and expanded.
  bool expand_meta(std::string &val) const;

private:
  // Private function for setting a default for a config option
  void set_val_from_default(const config_option *opt);

  int set_val_impl(const char *val, const config_option *opt);

  // The configuration file we read, or NULL if we haven't read one.
  ConfFile cf;

public:
  std::string host;

  int num_client;

  //bool mkfs;

  std::string monmap;
  std::string mon_host;
  bool daemonize;

  //profiling
  bool tcmalloc_profiler_run;
  int profiler_allocation_interval;
  int profiler_highwater_interval;

  // profiling logger
  bool profiling_logger;
  int profiling_logger_interval;
  bool profiling_logger_calc_variance;
  std::string profiling_logger_subdir;
  std::string profiling_logger_dir;

  std::string log_file;
  std::string log_dir;
  std::string log_sym_dir;
  int log_sym_history;

  int log_to_stderr;

  bool log_to_syslog;
  bool log_per_instance;

  bool clog_to_monitors;
  bool clog_to_syslog;

  std::string pid_file;

  std::string chdir;

  long long max_open_files;

  int debug;
  int debug_lockdep;
  int debug_context;
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
  int debug_rbd;
  int debug_journaler;
  int debug_objectcacher;
  int debug_client;
  int debug_osd;
  int debug_filestore;
  int debug_journal;
  int debug_bdev;
  int debug_ms;
  int debug_mon;
  int debug_monc;
  int debug_paxos;
  int debug_tp;
  int debug_auth;
  int debug_finisher;

  // auth
  std::string key;
  std::string keyfile;
  std::string keyring;

  // buffer
  bool buffer_track_alloc;

  // messenger

  /*bool tcp_skip_rank0;
  bool tcp_overlay_clients;
  bool tcp_log;
  bool tcp_serial_marshall;
  bool tcp_serial_out;
  bool tcp_multi_out;
  bool tcp_multi_dispatch;
  */
  entity_addr_t public_addr;
  entity_addr_t cluster_addr;

  bool ms_tcp_nodelay;
  double ms_initial_backoff;
  double ms_max_backoff;
  bool ms_nocrc;
  bool ms_die_on_bad_msg;
  uint64_t ms_dispatch_throttle_bytes;
  bool ms_bind_ipv6;
  uint64_t ms_rwthread_stack_bytes;
  uint64_t ms_tcp_read_timeout;
  uint64_t ms_inject_socket_failures;

  // mon
  std::string mon_data;
  int mon_tick_interval;
  double mon_subscribe_interval;
  int mon_osd_down_out_interval;
  float mon_lease;
  float mon_lease_renew_interval;
  float mon_clock_drift_allowed;
  float mon_clock_drift_warn_backoff;
  float mon_lease_ack_timeout;
  float mon_accept_timeout;
  float mon_pg_create_interval;
  int mon_osd_full_ratio;
  int mon_osd_nearfull_ratio;
  int mon_globalid_prealloc;
  int mon_osd_report_timeout;

  bool mon_force_standby_active;

  double paxos_propose_interval;
  double paxos_min_wait;
  double paxos_observer_timeout;

  // auth
  std::string auth_supported;
  double auth_mon_ticket_ttl;
  double auth_service_ticket_ttl;
  EntityName name;

  double mon_client_hunt_interval;
  double mon_client_ping_interval;

  // client
  int      client_cache_size;
  float    client_cache_mid;
  int      client_cache_stat_ttl;
  int      client_cache_readdir_ttl;
  bool     client_use_random_mds;          // debug flag
  double   client_mount_timeout;
  double   client_unmount_timeout;
  double   client_tick_interval;
  std::string client_trace;
  long long client_readahead_min;
  long long client_readahead_max_bytes;
  long long client_readahead_max_periods;
  std::string client_snapdir;
  std::string client_mountpoint;

  // objectcacher
  bool     client_oc;
  int      client_oc_size;
  int      client_oc_max_dirty;
  int      client_oc_target_dirty;
  uint64_t client_oc_max_sync_write;

  int      client_notify_timeout;

  // objecter
  double objecter_mon_retry_interval;
  double objecter_tick_interval;
  double objecter_timeout;
  uint64_t objecter_inflight_op_bytes;

  // journaler
  bool  journaler_allow_split_entries;
  int   journaler_write_head_interval;
  int   journaler_prefetch_periods;
  double journaler_batch_interval;
  uint64_t journaler_batch_max;

  // mds
  uint64_t mds_max_file_size;
  int   mds_cache_size;
  float mds_cache_mid;
  int   mds_mem_max;
  float mds_dir_commit_ratio;
  int   mds_dir_max_commit_size;

  float mds_decay_halflife;

  float mds_beacon_interval;
  float mds_beacon_grace;
  float mds_blacklist_interval;

  float mds_session_timeout;
  float mds_session_autoclose;
  float mds_reconnect_timeout;

  float mds_tick_interval;
  float mds_dirstat_min_interval;
  float mds_scatter_nudge_interval;

  int mds_client_prealloc_inos;
  bool mds_early_reply;

  bool mds_use_tmap;

  int mds_default_dir_hash;

  bool mds_log;
  bool mds_log_skip_corrupt_events;
  int mds_log_max_events;
  int mds_log_max_segments;
  int mds_log_max_expiring;
  int mds_log_eopen_size;

  float mds_bal_sample_interval;
  float mds_bal_replicate_threshold;
  float mds_bal_unreplicate_threshold;
  bool mds_bal_frag;
  int mds_bal_split_size;
  float mds_bal_split_rd;
  float mds_bal_split_wr;
  int mds_bal_split_bits;
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

  float mds_replay_interval;

  int   mds_shutdown_check;

  int mds_thrash_exports;
  int mds_thrash_fragments;
  bool mds_dump_cache_on_map;
  bool mds_dump_cache_after_rejoin;

  // set these to non-zero to specify kill points
  bool mds_verify_scatter;
  bool mds_debug_scatterstat;
  bool mds_debug_frag;
  int mds_kill_mdstable_at;
  int mds_kill_export_at;
  int mds_kill_import_at;
  int mds_kill_rename_at;

  bool mds_wipe_sessions;
  bool mds_wipe_ino_prealloc;
  int mds_skip_ino;
  int max_mds;

  int mds_standby_for_rank;
  std::string mds_standby_for_name;
  bool mds_standby_replay;

  // osd
  std::string osd_data;
  std::string osd_journal;
  int osd_journal_size;  // in mb
  int osd_max_write_size; // in MB
  bool osd_balance_reads;
  int osd_flash_crowd_iat_threshold;  // flash crowd interarrival time threshold in ms
  double osd_flash_crowd_iat_alpha;

  int  osd_shed_reads;
  double osd_shed_reads_min_latency;
  double osd_shed_reads_min_latency_diff;
  double osd_shed_reads_min_latency_ratio;

  uint64_t osd_client_message_size_cap;

  double osd_stat_refresh_interval;

  int   osd_pg_bits;
  int   osd_pgp_bits;
  int   osd_lpg_bits;
  int   osd_pg_layout;
  int   osd_min_rep;
  int   osd_max_rep;
  int   osd_min_raid_width;
  int   osd_max_raid_width;

  int osd_pool_default_crush_rule;
  int osd_pool_default_size;
  int osd_pool_default_pg_num;
  int osd_pool_default_pgp_num;

  int   osd_op_threads;
  int   osd_max_opq;
  int   osd_disk_threads;
  int   osd_recovery_threads;

  float   osd_age;
  int   osd_age_time;
  int   osd_heartbeat_interval;
  int   osd_mon_heartbeat_interval;
  int   osd_heartbeat_grace;
  int   osd_mon_report_interval_max;
  int   osd_mon_report_interval_min;
  int   osd_min_down_reporters;
  int   osd_min_down_reports;
  int   osd_replay_window;
  bool  osd_preserve_trimmed_log;

  float osd_recovery_delay_start;
  int osd_recovery_max_active;
  uint64_t osd_recovery_max_chunk;

  bool osd_recovery_forget_lost_objects;

  bool osd_auto_weight;

  double osd_class_error_timeout;
  double osd_class_timeout;
  std::string osd_class_tmp;

  int osd_max_scrubs;
  float osd_scrub_load_threshold;
  float osd_scrub_min_interval;
  float osd_scrub_max_interval;

  bool osd_check_for_log_corruption;  // bleh

  bool osd_use_stale_snap;

  uint32_t osd_max_notify_timeout;

  // filestore
  bool filestore;
  double   filestore_max_sync_interval;
  double   filestore_min_sync_interval;
  bool  filestore_fake_attrs;
  bool  filestore_fake_collections;
  std::string filestore_dev;
  bool filestore_btrfs_trans;
  bool filestore_btrfs_snap;
  bool filestore_btrfs_clone_range;
  bool filestore_fsync_flushes_journal_data;
  bool filestore_flusher;
  int filestore_flusher_max_fds;
  bool filestore_sync_flush;
  bool filestore_journal_parallel;
  bool filestore_journal_writeahead;
  bool filestore_journal_trailing;
  int filestore_queue_max_ops;
  int filestore_queue_max_bytes;
  int filestore_queue_committing_max_ops;
  int filestore_queue_committing_max_bytes;
  int filestore_op_threads;
  float filestore_commit_timeout;

  // journal
  bool journal_dio;
  bool journal_block_align;
  int journal_max_write_bytes;
  int journal_max_write_entries;
  int journal_queue_max_ops;
  int journal_queue_max_bytes;
  int journal_align_min_size;

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

};

extern md_config_t g_conf;

typedef enum {
	OPT_NONE, OPT_INT, OPT_LONGLONG, OPT_STR, OPT_DOUBLE, OPT_FLOAT, OPT_BOOL,
	OPT_ADDR, OPT_U32, OPT_U64
} opt_type_t;

bool ceph_resolve_file_search(const std::string& filename_list,
			      std::string& result);

struct config_option {
  const char *name;
  size_t md_conf_off;

  const char *def_str;
  long long def_longlong;
  double def_double;

  opt_type_t type;

  // Given a configuration, return a pointer to this option inside
  // that configuration.
  void *conf_ptr(md_config_t *conf) const;

  const void *conf_ptr(const md_config_t *conf) const;
};

#include "common/debug.h"

#endif
