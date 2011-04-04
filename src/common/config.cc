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

#include "auth/Auth.h"
#include "common/BackTrace.h"
#include "common/Clock.h"
#include "common/ConfUtils.h"
#include "common/ProfLogger.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/dyn_snprintf.h"
#include "common/static_assert.h"
#include "common/strtol.h"
#include "common/version.h"
#include "include/atomic.h"
#include "include/str_list.h"
#include "include/types.h"
#include "msg/msg_types.h"
#include "osd/osd_types.h"

#include <errno.h>
#include <fcntl.h>
#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <sys/stat.h>
#include <sys/types.h>

/* Don't use standard Ceph logging in this file.
 * We can't use logging until it's initialized, and a lot of the necessary
 * initialization happens here.
 */
#undef dout
#undef pdout
#undef derr
#undef generic_dout
#undef dendl

const char *CEPH_CONF_FILE_DEFAULT = "/etc/ceph/ceph.conf, ~/.ceph/config, ceph.conf";

/* The Ceph configuration. */
md_config_t g_conf __attribute__((init_priority(103)));

// file layouts
struct ceph_file_layout g_default_file_layout = {
 fl_stripe_unit: init_le32(1<<22),
 fl_stripe_count: init_le32(1),
 fl_object_size: init_le32(1<<22),
 fl_cas_hash: init_le32(0),
 fl_object_stripe_unit: init_le32(0),
 fl_pg_preferred : init_le32(-1),
 fl_pg_pool : init_le32(-1),
};

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

#define TYCHECK(x, ty) STATIC_ASSERT(sizeof(x) == sizeof(ty))

#define OPTION_OPT_STR(section, name, type, def_val) \
       { STRINGIFY(section) + TYCHECK(g_conf.name, std::string), \
	  NULL, STRINGIFY(name), \
	  &g_conf.name, def_val, 0, 0, type }

#define OPTION_OPT_ADDR(section, name, type, def_val) \
       { STRINGIFY(section) + TYCHECK(g_conf.name, entity_addr_t), \
	 NULL, STRINGIFY(name), \
	 &g_conf.name, def_val, 0, 0, type }

#define OPTION_OPT_LONGLONG(section, name, type, def_val) \
       { STRINGIFY(section) + TYCHECK(g_conf.name, long long), \
	 NULL, STRINGIFY(name), \
         &g_conf.name, 0, def_val, 0, type }

#define OPTION_OPT_INT(section, name, type, def_val) \
       { STRINGIFY(section) + TYCHECK(g_conf.name, int), \
	 NULL, STRINGIFY(name), \
         &g_conf.name, 0, def_val, 0, type }

#define OPTION_OPT_BOOL(section, name, type, def_val) \
       { STRINGIFY(section) + TYCHECK(g_conf.name, bool), \
	 NULL, STRINGIFY(name), \
         &g_conf.name, 0, def_val, 0, type }

#define OPTION_OPT_U32(section, name, type, def_val) \
       { STRINGIFY(section) + TYCHECK(g_conf.name, uint32_t), \
	 NULL, STRINGIFY(name), \
         &g_conf.name, 0, def_val, 0, type }

#define OPTION_OPT_U64(section, name, type, def_val) \
       { STRINGIFY(section) + TYCHECK(g_conf.name, uint64_t), \
	 NULL, STRINGIFY(name), \
         &g_conf.name, 0, def_val, 0, type }

#define OPTION_OPT_DOUBLE(section, name, type, def_val) \
       { STRINGIFY(section) + TYCHECK(g_conf.name, double), \
	 NULL, STRINGIFY(name), \
	 &g_conf.name, 0, 0, def_val, type }

#define OPTION_OPT_FLOAT(section, name, type, def_val) \
       { STRINGIFY(section) + TYCHECK(g_conf.name, float), \
	 NULL, STRINGIFY(name), \
	 &g_conf.name, 0, 0, def_val, type }

#define OPTION(name, type, def_val) OPTION_##type("global", name, type, def_val)

struct config_option config_optionsp[] = {
  OPTION(host, OPT_STR, "localhost"),
  OPTION(public_addr, OPT_ADDR, NULL),
  OPTION(cluster_addr, OPT_ADDR, NULL),
  OPTION(num_client, OPT_INT, 1),
  OPTION(monmap, OPT_STR, 0),
  OPTION(mon_host, OPT_STR, 0),
  OPTION(daemonize, OPT_BOOL, false),
  OPTION(tcmalloc_profiler_run, OPT_BOOL, false),
  OPTION(profiler_allocation_interval, OPT_INT, 1073741824),
  OPTION(profiler_highwater_interval, OPT_INT, 104857600),
  OPTION(profiling_logger, OPT_BOOL, false),
  OPTION(profiling_logger_interval, OPT_INT, 1),
  OPTION(profiling_logger_calc_variance, OPT_BOOL, false),
  OPTION(profiling_logger_subdir, OPT_STR, 0),
  OPTION(profiling_logger_dir, OPT_STR, "/var/log/ceph/stat"),
  OPTION(log_file, OPT_STR, 0),
  OPTION(log_dir, OPT_STR, 0),
  OPTION(log_sym_dir, OPT_STR, 0),
  OPTION(log_sym_history, OPT_INT, 10),
  OPTION(log_to_stderr, OPT_INT, LOG_TO_STDERR_ALL),
  OPTION(log_to_syslog, OPT_BOOL, false),
  OPTION(log_per_instance, OPT_BOOL, false),
  OPTION(clog_to_monitors, OPT_BOOL, true),
  OPTION(clog_to_syslog, OPT_BOOL, false),
  OPTION(pid_file, OPT_STR, 0),
  OPTION(chdir, OPT_STR, "/"),
  OPTION(max_open_files, OPT_LONGLONG, 0),
  OPTION(debug, OPT_INT, 0),
  OPTION(debug_lockdep, OPT_INT, 0),
  OPTION(debug_context, OPT_INT, 0),
  OPTION(debug_mds, OPT_INT, 1),
  OPTION(debug_mds_balancer, OPT_INT, 1),
  OPTION(debug_mds_log, OPT_INT, 1),
  OPTION(debug_mds_log_expire, OPT_INT, 1),
  OPTION(debug_mds_migrator, OPT_INT, 1),
  OPTION(debug_buffer, OPT_INT, 0),
  OPTION(debug_timer, OPT_INT, 0),
  OPTION(debug_filer, OPT_INT, 0),
  OPTION(debug_objecter, OPT_INT, 0),
  OPTION(debug_rados, OPT_INT, 0),
  OPTION(debug_rbd, OPT_INT, 0),
  OPTION(debug_journaler, OPT_INT, 0),
  OPTION(debug_objectcacher, OPT_INT, 0),
  OPTION(debug_client, OPT_INT, 0),
  OPTION(debug_osd, OPT_INT, 0),
  OPTION(debug_ebofs, OPT_INT, 1),
  OPTION(debug_filestore, OPT_INT, 1),
  OPTION(debug_journal, OPT_INT, 1),
  OPTION(debug_bdev, OPT_INT, 1),         // block device
  OPTION(debug_ms, OPT_INT, 0),
  OPTION(debug_mon, OPT_INT, 1),
  OPTION(debug_monc, OPT_INT, 0),
  OPTION(debug_paxos, OPT_INT, 0),
  OPTION(debug_tp, OPT_INT, 0),
  OPTION(debug_auth, OPT_INT, 1),
  OPTION(debug_finisher, OPT_INT, 1),
  OPTION(key, OPT_STR, 0),
  OPTION(keyfile, OPT_STR, 0),
  OPTION(keyring, OPT_STR, "/etc/ceph/keyring,/etc/ceph/keyring.bin"),
  OPTION(buffer_track_alloc, OPT_BOOL, false),
  OPTION(ms_tcp_nodelay, OPT_BOOL, true),
  OPTION(ms_initial_backoff, OPT_DOUBLE, .2),
  OPTION(ms_max_backoff, OPT_DOUBLE, 15.0),
  OPTION(ms_nocrc, OPT_BOOL, false),
  OPTION(ms_die_on_bad_msg, OPT_BOOL, false),
  OPTION(ms_dispatch_throttle_bytes, OPT_U64, 100 << 20),
  OPTION(ms_bind_ipv6, OPT_BOOL, false),
  OPTION(ms_rwthread_stack_bytes, OPT_U64, 1024 << 10),
  OPTION(ms_tcp_read_timeout, OPT_U64, 900),
  OPTION(ms_inject_socket_failures, OPT_U64, 0),
  OPTION(mon_data, OPT_STR, 0),
  OPTION(mon_tick_interval, OPT_INT, 5),
  OPTION(mon_subscribe_interval, OPT_DOUBLE, 300),
  OPTION(mon_osd_down_out_interval, OPT_INT, 300), // seconds
  OPTION(mon_lease, OPT_FLOAT, 5),       // lease interval
  OPTION(mon_lease_renew_interval, OPT_FLOAT, 3), // on leader, to renew the lease
  OPTION(mon_lease_ack_timeout, OPT_FLOAT, 10.0), // on leader, if lease isn't acked by all peons
  OPTION(mon_clock_drift_allowed, OPT_FLOAT, .010), // allowed clock drift between monitors
  OPTION(mon_clock_drift_warn_backoff, OPT_FLOAT, 5), // exponential backoff for clock drift warnings
  OPTION(mon_accept_timeout, OPT_FLOAT, 10.0),    // on leader, if paxos update isn't accepted
  OPTION(mon_pg_create_interval, OPT_FLOAT, 30.0), // no more than every 30s
  OPTION(mon_osd_full_ratio, OPT_INT, 95), // what % full makes an OSD "full"
  OPTION(mon_osd_nearfull_ratio, OPT_INT, 85), // what % full makes an OSD near full
  OPTION(mon_globalid_prealloc, OPT_INT, 100),   // how many globalids to prealloc
  OPTION(mon_osd_report_timeout, OPT_INT, 900),    // grace period before declaring unresponsive OSDs dead
  OPTION(mon_force_standby_active, OPT_BOOL, true), // should mons force standby-replay mds to be active
  OPTION(paxos_propose_interval, OPT_DOUBLE, 1.0),  // gather updates for this long before proposing a map update
  OPTION(paxos_min_wait, OPT_DOUBLE, 0.05),  // min time to gather updates for after period of inactivity
  OPTION(paxos_observer_timeout, OPT_DOUBLE, 5*60), // gather updates for this long before proposing a map update
  OPTION(auth_supported, OPT_STR, "none"),
  OPTION(auth_mon_ticket_ttl, OPT_DOUBLE, 60*60*12),
  OPTION(auth_service_ticket_ttl, OPT_DOUBLE, 60*60),
  OPTION(mon_client_hunt_interval, OPT_DOUBLE, 3.0),   // try new mon every N seconds until we connect
  OPTION(mon_client_ping_interval, OPT_DOUBLE, 10.0),  // ping every N seconds
  OPTION(client_cache_size, OPT_INT, 16384),
  OPTION(client_cache_mid, OPT_FLOAT, .75),
  OPTION(client_cache_stat_ttl, OPT_INT, 0), // seconds until cached stat results become invalid
  OPTION(client_cache_readdir_ttl, OPT_INT, 1),  // 1 second only
  OPTION(client_use_random_mds, OPT_BOOL, false),
  OPTION(client_mount_timeout, OPT_DOUBLE, 30.0),
  OPTION(client_unmount_timeout, OPT_DOUBLE, 10.0),
  OPTION(client_tick_interval, OPT_DOUBLE, 1.0),
  OPTION(client_trace, OPT_STR, 0),
  OPTION(client_readahead_min, OPT_LONGLONG, 128*1024),  // readahead at _least_ this much.
  OPTION(client_readahead_max_bytes, OPT_LONGLONG, 0),  //8 * 1024*1024,
  OPTION(client_readahead_max_periods, OPT_LONGLONG, 4),  // as multiple of file layout period (object size * num stripes)
  OPTION(client_snapdir, OPT_STR, ".snap"),
  OPTION(client_mountpoint, OPT_STR, "/"),
  OPTION(client_notify_timeout, OPT_INT, 10), // in seconds
  OPTION(client_oc, OPT_BOOL, true),
  OPTION(client_oc_size, OPT_INT, 1024*1024* 200),    // MB * n
  OPTION(client_oc_max_dirty, OPT_INT, 1024*1024* 100),    // MB * n  (dirty OR tx.. bigish)
  OPTION(client_oc_target_dirty, OPT_INT, 1024*1024* 8), // target dirty (keep this smallish)
  // note: the max amount of "in flight" dirty data is roughly (max - target)
  OPTION(client_oc_max_sync_write, OPT_U64, 128*1024),   // sync writes >= this use wrlock
  OPTION(objecter_tick_interval, OPT_DOUBLE, 5.0),
  OPTION(objecter_mon_retry_interval, OPT_DOUBLE, 5.0),
  OPTION(objecter_timeout, OPT_DOUBLE, 10.0),    // before we ask for a map
  OPTION(objecter_inflight_op_bytes, OPT_U64, 1024*1024*100), //max in-flight data (both directions)
  OPTION(journaler_allow_split_entries, OPT_BOOL, true),
  OPTION(journaler_write_head_interval, OPT_INT, 15),
  OPTION(journaler_prefetch_periods, OPT_INT, 10),   // * journal object size (1~MB? see above)
  OPTION(journaler_batch_interval, OPT_DOUBLE, .001),   // seconds.. max add'l latency we artificially incur
  OPTION(journaler_batch_max, OPT_U64, 0),  // max bytes we'll delay flushing; disable, for now....
  OPTION(mds_max_file_size, OPT_U64, 1ULL << 40),
  OPTION(mds_cache_size, OPT_INT, 100000),
  OPTION(mds_cache_mid, OPT_FLOAT, .7),
  OPTION(mds_mem_max, OPT_INT, 1048576),        // KB
  OPTION(mds_dir_commit_ratio, OPT_FLOAT, .5),
  OPTION(mds_dir_max_commit_size, OPT_INT, 90), // MB
  OPTION(mds_decay_halflife, OPT_FLOAT, 5),
  OPTION(mds_beacon_interval, OPT_FLOAT, 4),
  OPTION(mds_beacon_grace, OPT_FLOAT, 15),
  OPTION(mds_blacklist_interval, OPT_FLOAT, 24.0*60.0),  // how long to blacklist failed nodes
  OPTION(mds_session_timeout, OPT_FLOAT, 60),    // cap bits and leases time out if client idle
  OPTION(mds_session_autoclose, OPT_FLOAT, 300), // autoclose idle session
  OPTION(mds_reconnect_timeout, OPT_FLOAT, 45),  // seconds to wait for clients during mds restart
                //  make it (mds_session_timeout - mds_beacon_grace)
  OPTION(mds_tick_interval, OPT_FLOAT, 5),
  OPTION(mds_dirstat_min_interval, OPT_FLOAT, 1),    // try to avoid propagating more often than this
  OPTION(mds_scatter_nudge_interval, OPT_FLOAT, 5),  // how quickly dirstat changes propagate up the hierarchy
  OPTION(mds_client_prealloc_inos, OPT_INT, 1000),
  OPTION(mds_early_reply, OPT_BOOL, true),
  OPTION(mds_use_tmap, OPT_BOOL, true),        // use trivialmap for dir updates
  OPTION(mds_default_dir_hash, OPT_INT, CEPH_STR_HASH_RJENKINS),
  OPTION(mds_log, OPT_BOOL, true),
  OPTION(mds_log_skip_corrupt_events, OPT_BOOL, false),
  OPTION(mds_log_max_events, OPT_INT, -1),
  OPTION(mds_log_max_segments, OPT_INT, 30),  // segment size defined by FileLayout, above
  OPTION(mds_log_max_expiring, OPT_INT, 20),
  OPTION(mds_log_eopen_size, OPT_INT, 100),   // # open inodes per log entry
  OPTION(mds_bal_sample_interval, OPT_FLOAT, 3.0),  // every 5 seconds
  OPTION(mds_bal_replicate_threshold, OPT_FLOAT, 8000),
  OPTION(mds_bal_unreplicate_threshold, OPT_FLOAT, 0),
  OPTION(mds_bal_frag, OPT_BOOL, false),
  OPTION(mds_bal_split_size, OPT_INT, 10000),
  OPTION(mds_bal_split_rd, OPT_FLOAT, 25000),
  OPTION(mds_bal_split_wr, OPT_FLOAT, 10000),
  OPTION(mds_bal_split_bits, OPT_INT, 3),
  OPTION(mds_bal_merge_size, OPT_INT, 50),
  OPTION(mds_bal_merge_rd, OPT_FLOAT, 1000),
  OPTION(mds_bal_merge_wr, OPT_FLOAT, 1000),
  OPTION(mds_bal_interval, OPT_INT, 10),           // seconds
  OPTION(mds_bal_fragment_interval, OPT_INT, 5),      // seconds
  OPTION(mds_bal_idle_threshold, OPT_FLOAT, 0),
  OPTION(mds_bal_max, OPT_INT, -1),
  OPTION(mds_bal_max_until, OPT_INT, -1),
  OPTION(mds_bal_mode, OPT_INT, 0),
  OPTION(mds_bal_min_rebalance, OPT_FLOAT, .1),  // must be this much above average before we export anything
  OPTION(mds_bal_min_start, OPT_FLOAT, .2),      // if we need less than this, we don't do anything
  OPTION(mds_bal_need_min, OPT_FLOAT, .8),       // take within this range of what we need
  OPTION(mds_bal_need_max, OPT_FLOAT, 1.2),
  OPTION(mds_bal_midchunk, OPT_FLOAT, .3),       // any sub bigger than this taken in full
  OPTION(mds_bal_minchunk, OPT_FLOAT, .001),     // never take anything smaller than this
  OPTION(mds_bal_target_removal_min, OPT_INT, 5), // min balance iterations before old target is removed
  OPTION(mds_bal_target_removal_max, OPT_INT, 10), // max balance iterations before old target is removed
  OPTION(mds_replay_interval, OPT_FLOAT, 1.0), // time to wait before starting replay again
  OPTION(mds_shutdown_check, OPT_INT, 0),
  OPTION(mds_thrash_exports, OPT_INT, 0),
  OPTION(mds_thrash_fragments, OPT_INT, 0),
  OPTION(mds_dump_cache_on_map, OPT_BOOL, false),
  OPTION(mds_dump_cache_after_rejoin, OPT_BOOL, false),
  OPTION(mds_verify_scatter, OPT_BOOL, false),
  OPTION(mds_debug_scatterstat, OPT_BOOL, false),
  OPTION(mds_debug_frag, OPT_BOOL, false),
  OPTION(mds_kill_mdstable_at, OPT_INT, 0),
  OPTION(mds_kill_export_at, OPT_INT, 0),
  OPTION(mds_kill_import_at, OPT_INT, 0),
  OPTION(mds_kill_rename_at, OPT_INT, 0),
  OPTION(mds_wipe_sessions, OPT_BOOL, 0),
  OPTION(mds_wipe_ino_prealloc, OPT_BOOL, 0),
  OPTION(mds_skip_ino, OPT_INT, 0),
  OPTION(max_mds, OPT_INT, 1),
  OPTION(mds_standby_for_name, OPT_STR, 0),
  OPTION(mds_standby_for_rank, OPT_INT, -1),
  OPTION(mds_standby_replay, OPT_BOOL, false),
  OPTION(osd_data, OPT_STR, 0),
  OPTION(osd_journal, OPT_STR, 0),
  OPTION(osd_journal_size, OPT_INT, 0),         // in mb
  OPTION(osd_max_write_size, OPT_INT, 90),
  OPTION(osd_balance_reads, OPT_BOOL, false),
  OPTION(osd_flash_crowd_iat_threshold, OPT_INT, 0),
  OPTION(osd_flash_crowd_iat_alpha, OPT_DOUBLE, 0.125),
  OPTION(osd_shed_reads, OPT_INT, false),     // forward from primary to replica
  OPTION(osd_shed_reads_min_latency, OPT_DOUBLE, .01),       // min local latency
  OPTION(osd_shed_reads_min_latency_diff, OPT_DOUBLE, .01),  // min latency difference
  OPTION(osd_shed_reads_min_latency_ratio, OPT_DOUBLE, 1.5),  // 1.2 == 20% higher than peer
  OPTION(osd_client_message_size_cap, OPT_U64, 500*1024L*1024L), // default to 200MB client data allowed in-memory
  OPTION(osd_stat_refresh_interval, OPT_DOUBLE, .5),
  OPTION(osd_pg_bits, OPT_INT, 6),  // bits per osd
  OPTION(osd_pgp_bits, OPT_INT, 6),  // bits per osd
  OPTION(osd_lpg_bits, OPT_INT, 2),  // bits per osd
  OPTION(osd_pg_layout, OPT_INT, CEPH_PG_LAYOUT_CRUSH),
  OPTION(osd_min_rep, OPT_INT, 1),
  OPTION(osd_max_rep, OPT_INT, 10),
  OPTION(osd_min_raid_width, OPT_INT, 3),
  OPTION(osd_max_raid_width, OPT_INT, 2),
  OPTION(osd_pool_default_crush_rule, OPT_INT, 0),
  OPTION(osd_pool_default_size, OPT_INT, 2),
  OPTION(osd_pool_default_pg_num, OPT_INT, 8),
  OPTION(osd_pool_default_pgp_num, OPT_INT, 8),
  OPTION(osd_op_threads, OPT_INT, 2),    // 0 == no threading
  OPTION(osd_max_opq, OPT_INT, 10),
  OPTION(osd_disk_threads, OPT_INT, 1),
  OPTION(osd_recovery_threads, OPT_INT, 1),
  OPTION(osd_age, OPT_FLOAT, .8),
  OPTION(osd_age_time, OPT_INT, 0),
  OPTION(osd_heartbeat_interval, OPT_INT, 1),
  OPTION(osd_mon_heartbeat_interval, OPT_INT, 30),  // if no peers, ping monitor
  OPTION(osd_heartbeat_grace, OPT_INT, 20),
  OPTION(osd_mon_report_interval_max, OPT_INT, 120),
  OPTION(osd_mon_report_interval_min, OPT_INT, 5),  // pg stats, failures, up_thru, boot.
  OPTION(osd_min_down_reporters, OPT_INT, 1),   // number of OSDs who need to report a down OSD for it to count
  OPTION(osd_min_down_reports, OPT_INT, 3),     // number of times a down OSD must be reported for it to count
  OPTION(osd_replay_window, OPT_INT, 45),
  OPTION(osd_preserve_trimmed_log, OPT_BOOL, true),
  OPTION(osd_recovery_delay_start, OPT_FLOAT, 15),
  OPTION(osd_recovery_max_active, OPT_INT, 5),
  OPTION(osd_recovery_max_chunk, OPT_U64, 1<<20),  // max size of push chunk
  OPTION(osd_recovery_forget_lost_objects, OPT_BOOL, false),   // off for now
  OPTION(osd_max_scrubs, OPT_INT, 1),
  OPTION(osd_scrub_load_threshold, OPT_FLOAT, 0.5),
  OPTION(osd_scrub_min_interval, OPT_FLOAT, 300),
  OPTION(osd_scrub_max_interval, OPT_FLOAT, 60*60*24),   // once a day
  OPTION(osd_auto_weight, OPT_BOOL, false),
  OPTION(osd_class_error_timeout, OPT_DOUBLE, 60.0),  // seconds
  OPTION(osd_class_timeout, OPT_DOUBLE, 60*60.0), // seconds
  OPTION(osd_class_tmp, OPT_STR, "/var/lib/ceph/tmp"),
  OPTION(osd_check_for_log_corruption, OPT_BOOL, false),
  OPTION(osd_use_stale_snap, OPT_BOOL, false),
  OPTION(osd_max_notify_timeout, OPT_U32, 30), // max notify timeout in seconds
  OPTION(filestore, OPT_BOOL, false),
  OPTION(filestore_max_sync_interval, OPT_DOUBLE, 5),    // seconds
  OPTION(filestore_min_sync_interval, OPT_DOUBLE, .01),  // seconds
  OPTION(filestore_fake_attrs, OPT_BOOL, false),
  OPTION(filestore_fake_collections, OPT_BOOL, false),
  OPTION(filestore_dev, OPT_STR, 0),
  OPTION(filestore_btrfs_trans, OPT_BOOL, false),
  OPTION(filestore_btrfs_snap, OPT_BOOL, true),
  OPTION(filestore_btrfs_clone_range, OPT_BOOL, true),
  OPTION(filestore_fsync_flushes_journal_data, OPT_BOOL, false),
  OPTION(filestore_flusher, OPT_BOOL, true),
  OPTION(filestore_flusher_max_fds, OPT_INT, 512),
  OPTION(filestore_sync_flush, OPT_BOOL, false),
  OPTION(filestore_journal_parallel, OPT_BOOL, false),
  OPTION(filestore_journal_writeahead, OPT_BOOL, false),
  OPTION(filestore_journal_trailing, OPT_BOOL, false),
  OPTION(filestore_queue_max_ops, OPT_INT, 500),
  OPTION(filestore_queue_max_bytes, OPT_INT, 100 << 20),
  OPTION(filestore_queue_committing_max_ops, OPT_INT, 500),        // this is ON TOP of filestore_queue_max_*
  OPTION(filestore_queue_committing_max_bytes, OPT_INT, 100 << 20), //  "
  OPTION(filestore_op_threads, OPT_INT, 2),
  OPTION(filestore_commit_timeout, OPT_FLOAT, 600),
  OPTION(ebofs, OPT_BOOL, false),
  OPTION(ebofs_cloneable, OPT_BOOL, true),
  OPTION(ebofs_verify, OPT_BOOL, false),
  OPTION(ebofs_commit_ms, OPT_INT, 200),       // 0 = no forced commit timeout (for debugging/tracing)
  OPTION(ebofs_oc_size, OPT_INT, 10000),      // onode cache
  OPTION(ebofs_cc_size, OPT_INT, 10000),      // cnode cache
  OPTION(ebofs_bc_size, OPT_U64, 50*256), // 4k blocks, *256 for MB
  OPTION(ebofs_bc_max_dirty, OPT_U64, 30*256), // before write() will block
  OPTION(ebofs_max_prefetch, OPT_INT, 1000), // 4k blocks
  OPTION(ebofs_realloc, OPT_BOOL, false),    // hrm, this can cause bad fragmentation, don't use!
  OPTION(ebofs_verify_csum_on_read, OPT_BOOL, true),
  OPTION(journal_dio, OPT_BOOL, true),
  OPTION(journal_block_align, OPT_BOOL, true),
  OPTION(journal_max_write_bytes, OPT_INT, 10 << 20),
  OPTION(journal_max_write_entries, OPT_INT, 100),
  OPTION(journal_queue_max_ops, OPT_INT, 500),
  OPTION(journal_queue_max_bytes, OPT_INT, 100 << 20),
  OPTION(journal_align_min_size, OPT_INT, 64 << 10),  // align data payloads >= this.
  OPTION(bdev_lock, OPT_BOOL, true),
  OPTION(bdev_iothreads, OPT_INT, 1),         // number of ios to queue with kernel
  OPTION(bdev_idle_kick_after_ms, OPT_INT, 100),  // ms
  OPTION(bdev_el_fw_max_ms, OPT_INT, 10000),      // restart elevator at least once every 1000 ms
  OPTION(bdev_el_bw_max_ms, OPT_INT, 3000),       // restart elevator at least once every 300 ms
  OPTION(bdev_el_bidir, OPT_BOOL, false),          // bidirectional elevator?
  OPTION(bdev_iov_max, OPT_INT, 512),            // max # iov's to collect into a single readv()/writev() call
  OPTION(bdev_debug_check_io_overlap, OPT_BOOL, true),  // [DEBUG] check for any pending io overlaps
  OPTION(bdev_fake_mb, OPT_INT, 0),
  OPTION(bdev_fake_max_mb, OPT_INT, 0),
};

const int NUM_CONFIG_OPTIONS = sizeof(config_optionsp) / sizeof(config_option);

static void set_conf_name(config_option *opt)
{
  char *newsection = (char *)opt->section;
  char *newconf = (char *)opt->name;
  int i;

  if (opt->section[0] == 0) {
    newsection = strdup("global");
  }

  if (strncmp(newsection, opt->name, strlen(newsection)) == 0) {
    /* if key starts with the name of the section, remove name of the section
       unless key equals to it */

    if (strcmp(newsection, opt->name) == 0)
      goto done;

    newconf = strdup(&opt->name[strlen(newsection)+1]);
  } else {
    newconf = strdup(opt->name);
  }

  i = 0;
  while (newconf[i]) {
    if (newconf[i] == '_')
      newconf[i] = ' ';

    ++i;
  }

  done:
    opt->section = newsection;
    opt->conf_name = (const char *)newconf;
}

bool is_bool_param(const char *param)
{
  return ((strcasecmp(param, "true")==0) || (strcasecmp(param, "false")==0));
}

bool ceph_resolve_file_search(const std::string& filename_list,
			      std::string& result)
{
  list<string> ls;
  get_str_list(filename_list, ls);

  list<string>::iterator iter;
  for (iter = ls.begin(); iter != ls.end(); ++iter) {
    int fd = ::open(iter->c_str(), O_RDONLY);
    if (fd < 0)
      continue;

    close(fd);
    result = *iter;
    return true;
  }

  return false;
}

md_config_t::
md_config_t()
  : cf(NULL)
{
  //
  // Note: because our md_config_t structure is a global, the memory used to
  // store it will start out zeroed. So there is no need to manually initialize
  // everything to 0 here.
  //
  // However, it's good practice to add your new config option to config_optionsp
  // so that its default value is explicit rather than implicit.
  //
  for (int i = 0; i < NUM_CONFIG_OPTIONS; i++) {
    config_option *opt = config_optionsp + i;
    set_val_from_default(opt);
    set_conf_name(opt);
  }
}

md_config_t::
~md_config_t()
{
  delete cf;
  cf = NULL;
}

int md_config_t::
parse_config_files(const std::list<std::string> &conf_files)
{
  // open new conf
  list<string>::const_iterator c = conf_files.begin();
  if (c == conf_files.end())
    return -EINVAL;
  while (true) {
    if (c == conf_files.end())
      return -EINVAL;
    ConfFile *cf_ = new ConfFile(c->c_str());
    int res = cf_->parse();
    if (res == 0) {
      cf = cf_;
      break;
    }
    delete cf_;
    if (res == -EDOM)
      return -EDOM;
    ++c;
  }

  std::vector <std::string> my_sections;
  get_my_sections(my_sections);
  for (int i = 0; i < NUM_CONFIG_OPTIONS; i++) {
    config_option *opt = &config_optionsp[i];
    std::string val;
    int ret = get_val_from_conf_file(my_sections, opt->conf_name, val);
    if (ret == 0) {
      set_val_impl(val.c_str(), opt);
    }
    else if (ret != -ENOENT) {
      // TODO: complain about parse error
    }
  }

  // FIXME: This bit of global fiddling needs to go somewhere else eventually.
  std::string val;
  g_lockdep =
    ((get_val_from_conf_file(my_sections, "lockdep", val) == 0) &&
      ((strcasecmp(val.c_str(), "true") == 0) || (atoi(val.c_str()) != 0)));
  return 0;
}

void md_config_t::
parse_env()
{
  if (getenv("CEPH_KEYRING"))
    keyring = getenv("CEPH_KEYRING");
}

void md_config_t::
parse_argv(std::vector<const char*>& args)
{
  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "--show_conf", (char*)NULL)) {
      cf->dump();
      _exit(0);
    }
    else if (ceph_argparse_flag(args, i, "--foreground", "-f", (char*)NULL)) {
      daemonize = false;
      pid_file = "";
    }
    else if (ceph_argparse_flag(args, i, "-d", (char*)NULL)) {
      daemonize = false;
      log_dir = "";
      pid_file = "";
      log_sym_dir = "";
      log_sym_history = 0;
      log_to_stderr = LOG_TO_STDERR_ALL;
      log_to_syslog = false;
      log_per_instance = false;
    }
    // Some stuff that we wanted to give universal single-character options for
    // Careful: you can burn through the alphabet pretty quickly by adding
    // to this list.
    else if (ceph_argparse_witharg(args, i, &val, "--monmap", "-M", (char*)NULL)) {
      monmap = val;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--mon_host", "-m", (char*)NULL)) {
      mon_host = val;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--bind", (char*)NULL)) {
      public_addr.parse(val.c_str());
    }
    else if (ceph_argparse_witharg(args, i, &val, "--keyfile", "-K", (char*)NULL)) {
      keyfile = val;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--keyring", "-k", (char*)NULL)) {
      keyring = val;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--client_mountpoint", "-r", (char*)NULL)) {
      client_mountpoint = val;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--lockdep", (char*)NULL)) {
      // FIXME: This bit of global fiddling needs to go somewhere else eventually.
      g_lockdep =
	((strcasecmp(val.c_str(), "true") == 0) || (atoi(val.c_str()) != 0));
    }
    else {
      int o;
      for (o = 0; o < NUM_CONFIG_OPTIONS; ++o) {
	const config_option *opt = config_optionsp + o;
	std::string as_option("--");
	as_option += opt->name;
	if ((opt->type == OPT_BOOL) &&
	    ceph_argparse_flag(args, i, as_option.c_str(), (char*)NULL)) {
	  set_val_impl("true", opt);
	  break;
	}
	else if (ceph_argparse_witharg(args, i, &val,
				       as_option.c_str(), (char*)NULL)) {
	  set_val_impl(val.c_str(), opt);
	  break;
	}
      }
      if (o == NUM_CONFIG_OPTIONS) {
	// ignore
	++i;
      }
    }
  }
}

int md_config_t::
set_val(const char *key, const char *val)
{
  if (!key)
    return -EINVAL;
  if (!val)
    return -EINVAL;
  for (int i = 0; i < NUM_CONFIG_OPTIONS; ++i) {
    config_option *opt = &config_optionsp[i];
    if (strcmp(opt->conf_name, key) == 0)
      return set_val_impl(val, opt);
  }

  // couldn't find a configuration option with key 'key'
  return -ENOENT;
}

int md_config_t::
get_val(const char *key, char **buf, int len) const
{
  if (!key)
    return -EINVAL;
  for (int i = 0; i < NUM_CONFIG_OPTIONS; ++i) {
    const config_option *opt = &config_optionsp[i];
    if (strcmp(opt->conf_name, key))
      continue;

    ostringstream oss;
    switch (opt->type) {
      case OPT_NONE:
        return -ENOSYS;
      case OPT_INT:
        oss << *(int*)opt->val_ptr;
        break;
      case OPT_LONGLONG:
        oss << *(long long*)opt->val_ptr;
        break;
      case OPT_STR:
	oss << *((std::string*)opt->val_ptr);
	break;
      case OPT_FLOAT:
        oss << *(float*)opt->val_ptr;
        break;
      case OPT_DOUBLE:
        oss << *(double*)opt->val_ptr;
        break;
      case OPT_BOOL:
        oss << *(bool*)opt->val_ptr;
        break;
      case OPT_U32:
        oss << *(uint32_t*)opt->val_ptr;
        break;
      case OPT_U64:
        oss << *(uint64_t*)opt->val_ptr;
        break;
      case OPT_ADDR: {
        oss << *(entity_addr_t*)opt->val_ptr;
        break;
      }
    }
    string str(oss.str());
    int l = strlen(str.c_str()) + 1;
    if (len == -1) {
      *buf = (char*)malloc(l);
      strcpy(*buf, str.c_str());
      return 0;
    }
    snprintf(*buf, len, "%s", str.c_str());
    return (l > len) ? -ENAMETOOLONG : 0;
  }
  // couldn't find a configuration option with key 'key'
  return -ENOENT;
}

void md_config_t::
get_my_sections(std::vector <std::string> &sections)
{
  sections.push_back(name->to_str());

  std::string alt_name(name->get_type_name());
  alt_name += name->get_id();
  sections.push_back(alt_name);

  sections.push_back(name->get_type_name());

  sections.push_back("global");
}

// Return a list of all sections
int md_config_t::
get_all_sections(std::vector <std::string> &sections)
{
  if (!cf)
    return -EDOM;
  for (std::list<ConfSection*>::const_iterator p =
	    cf->get_section_list().begin();
       p != cf->get_section_list().end(); ++p)
  {
    sections.push_back((*p)->get_name());
  }
  return 0;
}

bool md_config_t::
have_conf_file() const
{
  return !!cf;
}

int md_config_t::
get_val_from_conf_file(const std::vector <std::string> &sections,
		    const char *key, std::string &out) const
{
  if (!cf)
    return -EDOM;
  std::vector <std::string>::const_iterator s = sections.begin();
  std::vector <std::string>::const_iterator s_end = sections.end();
  for (; s != s_end; ++s) {
    int ret = cf->read(s->c_str(), key, out);
    if (ret == 0) {
      conf_post_process_val(out);
      return 0;
    }
    else if (ret != -ENOENT)
      return ret;
  }
  return -ENOENT;
}

void md_config_t::
set_val_from_default(const config_option *opt)
{
  switch (opt->type) {
    case OPT_INT:
      *(int*)opt->val_ptr = opt->def_longlong;
      break;
    case OPT_LONGLONG:
      *(long long*)opt->val_ptr = opt->def_longlong;
      break;
    case OPT_STR:
      *(std::string *)opt->val_ptr = opt->def_str ? opt->def_str : "";
      break;
    case OPT_FLOAT:
      *(float *)opt->val_ptr = (float)opt->def_double;
      break;
    case OPT_DOUBLE:
      *(double *)opt->val_ptr = opt->def_double;
      break;
    case OPT_BOOL:
      *(bool *)opt->val_ptr = (bool)opt->def_longlong;
      break;
    case OPT_U32:
      *(uint32_t *)opt->val_ptr = (uint32_t)opt->def_longlong;
      break;
    case OPT_U64:
      *(uint64_t *)opt->val_ptr = (uint64_t)opt->def_longlong;
      break;
    case OPT_ADDR: {
      if (!opt->def_str) {
	// entity_addr_t has a default constructor, so we don't need to
	// do anything here.
	break;
      }
      entity_addr_t *addr = (entity_addr_t*)opt->val_ptr;
      if (!addr->parse(opt->def_str)) {
	ostringstream oss;
	oss << "Default value for " << opt->conf_name << " cannot be parsed."
	    << std::endl;
	assert(oss.str() == 0);
      }
      break;
     }
    default:
      assert("unreachable" == 0);
      break;
   }
}

int md_config_t::
set_val_impl(const char *val, const config_option *opt)
{
  switch (opt->type) {
    case OPT_NONE:
      return -ENOSYS;
    case OPT_INT: {
      std::string err;
      int f = strict_strtol(val, 10, &err);
      if (!err.empty())
	return -EINVAL;
      *(int*)opt->val_ptr = f;
      return 0;
    }
    case OPT_LONGLONG: {
      std::string err;
      long long f = strict_strtoll(val, 10, &err);
      if (!err.empty())
	return -EINVAL;
      *(long long*)opt->val_ptr = f;
      return 0;
    }
    case OPT_STR:
      *(std::string*)opt->val_ptr = val ? val : "";
      return 0;
    case OPT_FLOAT:
      *(float*)opt->val_ptr = atof(val);
      return 0;
    case OPT_DOUBLE:
      *(double*)opt->val_ptr = atof(val);
      return 0;
    case OPT_BOOL:
      if (strcasecmp(val, "false") == 0)
	*(bool*)opt->val_ptr = false;
      else if (strcasecmp(val, "true") == 0)
	*(bool*)opt->val_ptr = true;
      else {
	std::string err;
	int b = strict_strtol((const char*)val, 10, &err);
	if (!err.empty())
	  return -EINVAL;
	*(bool*)opt->val_ptr = !!b;
      }
      return 0;
    case OPT_U32: {
      std::string err;
      int f = strict_strtol((const char*)val, 10, &err);
      if (!err.empty())
	return -EINVAL;
      *(uint32_t*)opt->val_ptr = f;
      return 0;
    }
    case OPT_U64: {
      std::string err;
      long long f = strict_strtoll((const char*)val, 10, &err);
      if (!err.empty())
	return -EINVAL;
      *(uint64_t*)opt->val_ptr = f;
      return 0;
    }
    case OPT_ADDR: {
      entity_addr_t *addr = (entity_addr_t*)opt->val_ptr;
      if (!addr->parse(val)) {
	return -EINVAL;
      }
      return 0;
    }
  }
  return -ENOSYS;
}

static const char *CONF_METAVARIABLES[] =
      { "type", "name", "host", "num", "id" };
static const int NUM_CONF_METAVARIABLES =
      (sizeof(CONF_METAVARIABLES) / sizeof(CONF_METAVARIABLES[0]));

void md_config_t::
conf_post_process_val(std::string &val) const
{
  string out;
  string::size_type sz = val.size();
  out.reserve(sz);
  for (string::size_type s = 0; s < sz; ) {
    if (val[s] != '$') {
      out += val[s++];
      continue;
    }
    string::size_type rem = sz - (s + 1);
    int i;
    for (i = 0; i < NUM_CONF_METAVARIABLES; ++i) {
      size_t clen = strlen(CONF_METAVARIABLES[i]);
      if (rem < clen)
	continue;
      if (strncmp(val.c_str() + s + 1, CONF_METAVARIABLES[i], clen))
	continue;
      if (strcmp(CONF_METAVARIABLES[i], "type")==0)
	out += name->get_type_name();
      else if (strcmp(CONF_METAVARIABLES[i], "name")==0)
	out += name->to_cstr();
      else if (strcmp(CONF_METAVARIABLES[i], "host")==0)
	out += host;
      else if (strcmp(CONF_METAVARIABLES[i], "num")==0)
	out += name->get_id().c_str();
      else if (strcmp(CONF_METAVARIABLES[i], "id")==0)
	out += name->get_id().c_str();
      else
	assert(0); // unreachable
      break;
    }
    if (i == NUM_CONF_METAVARIABLES)
      out += val[s++];
    else
      s += strlen(CONF_METAVARIABLES[i]) + 1;
  }
  val = out;
}
