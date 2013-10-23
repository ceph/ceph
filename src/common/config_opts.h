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

/* note: no header guard */
OPTION(host, OPT_STR, "localhost")
OPTION(fsid, OPT_UUID, uuid_d())
OPTION(public_addr, OPT_ADDR, entity_addr_t())
OPTION(cluster_addr, OPT_ADDR, entity_addr_t())
OPTION(public_network, OPT_STR, "")
OPTION(cluster_network, OPT_STR, "")
OPTION(num_client, OPT_INT, 1)
OPTION(monmap, OPT_STR, "")
OPTION(mon_host, OPT_STR, "")
OPTION(lockdep, OPT_BOOL, false)
OPTION(run_dir, OPT_STR, "/var/run/ceph")       // the "/var/run/ceph" dir, created on daemon startup
OPTION(admin_socket, OPT_STR, "$run_dir/$cluster-$name.asok") // default changed by common_preinit()

OPTION(daemonize, OPT_BOOL, false) // default changed by common_preinit()
OPTION(pid_file, OPT_STR, "") // default changed by common_preinit()
OPTION(chdir, OPT_STR, "/")
OPTION(max_open_files, OPT_LONGLONG, 0)
OPTION(restapi_log_level, OPT_STR, "") 	// default set by Python code
OPTION(restapi_base_url, OPT_STR, "")	// "
OPTION(fatal_signal_handlers, OPT_BOOL, true)

OPTION(log_file, OPT_STR, "/var/log/ceph/$cluster-$name.log") // default changed by common_preinit()
OPTION(log_max_new, OPT_INT, 1000) // default changed by common_preinit()
OPTION(log_max_recent, OPT_INT, 10000) // default changed by common_preinit()
OPTION(log_to_stderr, OPT_BOOL, true) // default changed by common_preinit()
OPTION(err_to_stderr, OPT_BOOL, true) // default changed by common_preinit()
OPTION(log_to_syslog, OPT_BOOL, false)
OPTION(err_to_syslog, OPT_BOOL, false)
OPTION(log_flush_on_exit, OPT_BOOL, true) // default changed by common_preinit()
OPTION(log_stop_at_utilization, OPT_FLOAT, .97)  // stop logging at (near) full

OPTION(clog_to_monitors, OPT_BOOL, true)
OPTION(clog_to_syslog, OPT_BOOL, false)
OPTION(clog_to_syslog_level, OPT_STR, "info")         // this level and above
OPTION(clog_to_syslog_facility, OPT_STR, "daemon")

OPTION(mon_cluster_log_to_syslog, OPT_BOOL, false)
OPTION(mon_cluster_log_to_syslog_level, OPT_STR, "info")   // this level and above
OPTION(mon_cluster_log_to_syslog_facility, OPT_STR, "daemon")
OPTION(mon_cluster_log_file, OPT_STR, "/var/log/ceph/$cluster.log")
OPTION(mon_cluster_log_file_level, OPT_STR, "info")

DEFAULT_SUBSYS(0, 5)
SUBSYS(lockdep, 0, 1)
SUBSYS(context, 0, 1)
SUBSYS(crush, 1, 1)
SUBSYS(mds, 1, 5)
SUBSYS(mds_balancer, 1, 5)
SUBSYS(mds_locker, 1, 5)
SUBSYS(mds_log, 1, 5)
SUBSYS(mds_log_expire, 1, 5)
SUBSYS(mds_migrator, 1, 5)
SUBSYS(buffer, 0, 1)
SUBSYS(timer, 0, 1)
SUBSYS(filer, 0, 1)
SUBSYS(striper, 0, 1)
SUBSYS(objecter, 0, 1)
SUBSYS(rados, 0, 5)
SUBSYS(rbd, 0, 5)
SUBSYS(journaler, 0, 5)
SUBSYS(objectcacher, 0, 5)
SUBSYS(client, 0, 5)
SUBSYS(osd, 0, 5)
SUBSYS(optracker, 0, 5)
SUBSYS(objclass, 0, 5)
SUBSYS(filestore, 1, 3)
SUBSYS(journal, 1, 3)
SUBSYS(ms, 0, 5)
SUBSYS(mon, 1, 5)
SUBSYS(monc, 0, 10)
SUBSYS(paxos, 1, 5)
SUBSYS(tp, 0, 5)
SUBSYS(auth, 1, 5)
SUBSYS(crypto, 1, 5)
SUBSYS(finisher, 1, 1)
SUBSYS(heartbeatmap, 1, 5)
SUBSYS(perfcounter, 1, 5)
SUBSYS(rgw, 1, 5)                 // log level for the Rados gateway
SUBSYS(javaclient, 1, 5)
SUBSYS(asok, 1, 5)
SUBSYS(throttle, 1, 1)

OPTION(key, OPT_STR, "")
OPTION(keyfile, OPT_STR, "")
OPTION(keyring, OPT_STR, "/etc/ceph/$cluster.$name.keyring,/etc/ceph/$cluster.keyring,/etc/ceph/keyring,/etc/ceph/keyring.bin") // default changed by common_preinit() for mds and osd
OPTION(heartbeat_interval, OPT_INT, 5)
OPTION(heartbeat_file, OPT_STR, "")
OPTION(heartbeat_inject_failure, OPT_INT, 0)    // force an unhealthy heartbeat for N seconds
OPTION(perf, OPT_BOOL, true)       // enable internal perf counters

OPTION(ms_tcp_nodelay, OPT_BOOL, true)
OPTION(ms_tcp_rcvbuf, OPT_INT, 0)
OPTION(ms_initial_backoff, OPT_DOUBLE, .2)
OPTION(ms_max_backoff, OPT_DOUBLE, 15.0)
OPTION(ms_nocrc, OPT_BOOL, false)
OPTION(ms_die_on_bad_msg, OPT_BOOL, false)
OPTION(ms_die_on_unhandled_msg, OPT_BOOL, false)
OPTION(ms_die_on_old_message, OPT_BOOL, false)     // assert if we get a dup incoming message and shouldn't have (may be triggered by pre-541cd3c64be0dfa04e8a2df39422e0eb9541a428 code)
OPTION(ms_dispatch_throttle_bytes, OPT_U64, 100 << 20)
OPTION(ms_bind_ipv6, OPT_BOOL, false)
OPTION(ms_bind_port_min, OPT_INT, 6800)
OPTION(ms_bind_port_max, OPT_INT, 7300)
OPTION(ms_rwthread_stack_bytes, OPT_U64, 1024 << 10)
OPTION(ms_tcp_read_timeout, OPT_U64, 900)
OPTION(ms_pq_max_tokens_per_priority, OPT_U64, 4194304)
OPTION(ms_pq_min_cost, OPT_U64, 65536)
OPTION(ms_inject_socket_failures, OPT_U64, 0)
OPTION(ms_inject_delay_type, OPT_STR, "")          // "osd mds mon client" allowed
OPTION(ms_inject_delay_max, OPT_DOUBLE, 1)         // seconds
OPTION(ms_inject_delay_probability, OPT_DOUBLE, 0) // range [0, 1]
OPTION(ms_inject_internal_delays, OPT_DOUBLE, 0)   // seconds

OPTION(inject_early_sigterm, OPT_BOOL, false)

OPTION(mon_data, OPT_STR, "/var/lib/ceph/mon/$cluster-$id")
OPTION(mon_initial_members, OPT_STR, "")    // list of initial cluster mon ids; if specified, need majority to form initial quorum and create new cluster
OPTION(mon_sync_fs_threshold, OPT_INT, 5)   // sync() when writing this many objects; 0 to disable.
OPTION(mon_compact_on_start, OPT_BOOL, false)  // compact leveldb on ceph-mon start
OPTION(mon_compact_on_bootstrap, OPT_BOOL, false)  // trigger leveldb compaction on bootstrap
OPTION(mon_compact_on_trim, OPT_BOOL, true)       // compact (a prefix) when we trim old states
OPTION(mon_tick_interval, OPT_INT, 5)
OPTION(mon_subscribe_interval, OPT_DOUBLE, 300)
OPTION(mon_delta_reset_interval, OPT_DOUBLE, 10)   // seconds of inactivity before we reset the pg delta to 0
OPTION(mon_osd_laggy_halflife, OPT_INT, 60*60)        // (seconds) how quickly our laggy estimations decay
OPTION(mon_osd_laggy_weight, OPT_DOUBLE, .3)          // weight for new 'samples's in laggy estimations
OPTION(mon_osd_adjust_heartbeat_grace, OPT_BOOL, true)    // true if we should scale based on laggy estimations
OPTION(mon_osd_adjust_down_out_interval, OPT_BOOL, true)  // true if we should scale based on laggy estimations
OPTION(mon_osd_auto_mark_in, OPT_BOOL, false)         // mark any booting osds 'in'
OPTION(mon_osd_auto_mark_auto_out_in, OPT_BOOL, true) // mark booting auto-marked-out osds 'in'
OPTION(mon_osd_auto_mark_new_in, OPT_BOOL, true)      // mark booting new osds 'in'
OPTION(mon_osd_down_out_interval, OPT_INT, 300) // seconds
OPTION(mon_osd_down_out_subtree_limit, OPT_STR, "rack")   // smallest crush unit/type that we will not automatically mark out
OPTION(mon_osd_min_up_ratio, OPT_DOUBLE, .3)    // min osds required to be up to mark things down
OPTION(mon_osd_min_in_ratio, OPT_DOUBLE, .3)   // min osds required to be in to mark things out
OPTION(mon_osd_max_op_age, OPT_DOUBLE, 32)     // max op age before we get concerned (make it a power of 2)
OPTION(mon_stat_smooth_intervals, OPT_INT, 2)  // smooth stats over last N PGMap maps
OPTION(mon_lease, OPT_FLOAT, 5)       // lease interval
OPTION(mon_lease_renew_interval, OPT_FLOAT, 3) // on leader, to renew the lease
OPTION(mon_lease_ack_timeout, OPT_FLOAT, 10.0) // on leader, if lease isn't acked by all peons
OPTION(mon_clock_drift_allowed, OPT_FLOAT, .050) // allowed clock drift between monitors
OPTION(mon_clock_drift_warn_backoff, OPT_FLOAT, 5) // exponential backoff for clock drift warnings
OPTION(mon_timecheck_interval, OPT_FLOAT, 300.0) // on leader, timecheck (clock drift check) interval (seconds)
OPTION(mon_accept_timeout, OPT_FLOAT, 10.0)    // on leader, if paxos update isn't accepted
OPTION(mon_pg_create_interval, OPT_FLOAT, 30.0) // no more than every 30s
OPTION(mon_pg_stuck_threshold, OPT_INT, 300) // number of seconds after which pgs can be considered inactive, unclean, or stale (see doc/control.rst under dump_stuck for more info)
OPTION(mon_pg_warn_min_per_osd, OPT_INT, 20)  // min # pgs per (in) osd before we warn the admin
OPTION(mon_pg_warn_max_object_skew, OPT_FLOAT, 10.0) // max skew few average in objects per pg
OPTION(mon_pg_warn_min_objects, OPT_INT, 10000)  // do not warn below this object #
OPTION(mon_pg_warn_min_pool_objects, OPT_INT, 1000)  // do not warn on pools below this object #
OPTION(mon_osd_full_ratio, OPT_FLOAT, .95) // what % full makes an OSD "full"
OPTION(mon_osd_nearfull_ratio, OPT_FLOAT, .85) // what % full makes an OSD near full
OPTION(mon_globalid_prealloc, OPT_INT, 100)   // how many globalids to prealloc
OPTION(mon_osd_report_timeout, OPT_INT, 900)    // grace period before declaring unresponsive OSDs dead
OPTION(mon_force_standby_active, OPT_BOOL, true) // should mons force standby-replay mds to be active
OPTION(mon_min_osdmap_epochs, OPT_INT, 500)
OPTION(mon_max_pgmap_epochs, OPT_INT, 500)
OPTION(mon_max_log_epochs, OPT_INT, 500)
OPTION(mon_max_osd, OPT_INT, 10000)
OPTION(mon_probe_timeout, OPT_DOUBLE, 2.0)
OPTION(mon_slurp_timeout, OPT_DOUBLE, 10.0)
OPTION(mon_slurp_bytes, OPT_INT, 256*1024)    // limit size of slurp messages
OPTION(mon_client_bytes, OPT_U64, 100ul << 20)  // client msg data allowed in memory (in bytes)
OPTION(mon_daemon_bytes, OPT_U64, 400ul << 20)  // mds, osd message memory cap (in bytes)
OPTION(mon_max_log_entries_per_event, OPT_INT, 4096)
OPTION(mon_health_data_update_interval, OPT_FLOAT, 60.0)
OPTION(mon_data_avail_crit, OPT_INT, 5)
OPTION(mon_data_avail_warn, OPT_INT, 30)
OPTION(mon_config_key_max_entry_size, OPT_INT, 4096) // max num bytes per config-key entry
OPTION(mon_sync_timeout, OPT_DOUBLE, 60.0)
OPTION(mon_sync_max_payload_size, OPT_U32, 1048576) // max size for a sync chunk payload (say, 1MB)
OPTION(mon_sync_debug, OPT_BOOL, false) // enable sync-specific debug
OPTION(mon_sync_debug_leader, OPT_INT, -1) // monitor to be used as the sync leader
OPTION(mon_sync_debug_provider, OPT_INT, -1) // monitor to be used as the sync provider
OPTION(mon_sync_debug_provider_fallback, OPT_INT, -1) // monitor to be used as fallback if sync provider fails
OPTION(mon_inject_sync_get_chunk_delay, OPT_DOUBLE, 0)  // inject N second delay on each get_chunk request
OPTION(mon_osd_min_down_reporters, OPT_INT, 1)   // number of OSDs who need to report a down OSD for it to count
OPTION(mon_osd_min_down_reports, OPT_INT, 3)     // number of times a down OSD must be reported for it to count
OPTION(mon_osd_force_trim_to, OPT_INT, 0)   // force mon to trim maps to this point, regardless of min_last_epoch_clean (dangerous, use with care)

// dump transactions
OPTION(mon_debug_dump_transactions, OPT_BOOL, false)
OPTION(mon_debug_dump_location, OPT_STR, "/var/log/ceph/$cluster-$name.tdump")

OPTION(mon_sync_provider_kill_at, OPT_INT, 0)  // kill the sync provider at a specific point in the work flow
OPTION(mon_sync_requester_kill_at, OPT_INT, 0) // kill the sync requester at a specific point in the work flow
OPTION(mon_leveldb_write_buffer_size, OPT_U64, 32*1024*1024) // monitor's leveldb write buffer size
OPTION(mon_leveldb_cache_size, OPT_U64, 512*1024*1024) // monitor's leveldb cache size
OPTION(mon_leveldb_block_size, OPT_U64, 64*1024) // monitor's leveldb block size
OPTION(mon_leveldb_bloom_size, OPT_INT, 0) // monitor's leveldb bloom bits per entry
OPTION(mon_leveldb_max_open_files, OPT_INT, 0) // monitor's leveldb max open files
OPTION(mon_leveldb_compression, OPT_BOOL, false) // monitor's leveldb uses compression
OPTION(mon_leveldb_paranoid, OPT_BOOL, false)   // monitor's leveldb paranoid flag
OPTION(mon_leveldb_log, OPT_STR, "/dev/null")
OPTION(mon_leveldb_size_warn, OPT_U64, 40*1024*1024*1024) // issue a warning when the monitor's leveldb goes over 40GB (in bytes)
OPTION(paxos_stash_full_interval, OPT_INT, 25)   // how often (in commits) to stash a full copy of the PaxosService state
OPTION(paxos_max_join_drift, OPT_INT, 10) // max paxos iterations before we must first sync the monitor stores
OPTION(paxos_propose_interval, OPT_DOUBLE, 1.0)  // gather updates for this long before proposing a map update
OPTION(paxos_min_wait, OPT_DOUBLE, 0.05)  // min time to gather updates for after period of inactivity
OPTION(paxos_min, OPT_INT, 500)       // minimum number of paxos states to keep around
OPTION(paxos_trim_min, OPT_INT, 250)  // number of extra proposals tolerated before trimming
OPTION(paxos_trim_max, OPT_INT, 500) // max number of extra proposals to trim at a time
OPTION(paxos_service_trim_min, OPT_INT, 250) // minimum amount of versions to trigger a trim (0 disables it)
OPTION(paxos_service_trim_max, OPT_INT, 500) // maximum amount of versions to trim during a single proposal (0 disables it)
OPTION(paxos_kill_at, OPT_INT, 0)
OPTION(clock_offset, OPT_DOUBLE, 0) // how much to offset the system clock in Clock.cc
OPTION(auth_cluster_required, OPT_STR, "cephx")   // required of mon, mds, osd daemons
OPTION(auth_service_required, OPT_STR, "cephx")   // required by daemons of clients
OPTION(auth_client_required, OPT_STR, "cephx, none")     // what clients require of daemons
OPTION(auth_supported, OPT_STR, "")               // deprecated; default value for above if they are not defined.
OPTION(cephx_require_signatures, OPT_BOOL, false) //  If true, don't talk to Cephx partners if they don't support message signing; off by default
OPTION(cephx_cluster_require_signatures, OPT_BOOL, false)
OPTION(cephx_service_require_signatures, OPT_BOOL, false)
OPTION(cephx_sign_messages, OPT_BOOL, true)  // Default to signing session messages if supported
OPTION(auth_mon_ticket_ttl, OPT_DOUBLE, 60*60*12)
OPTION(auth_service_ticket_ttl, OPT_DOUBLE, 60*60)
OPTION(auth_debug, OPT_BOOL, false)          // if true, assert when weird things happen
OPTION(mon_client_hunt_interval, OPT_DOUBLE, 3.0)   // try new mon every N seconds until we connect
OPTION(mon_client_ping_interval, OPT_DOUBLE, 10.0)  // ping every N seconds
OPTION(mon_client_max_log_entries_per_message, OPT_INT, 1000)
OPTION(mon_max_pool_pg_num, OPT_INT, 65536)
OPTION(mon_pool_quota_warn_threshold, OPT_INT, 0) // percent of quota at which to issue warnings
OPTION(mon_pool_quota_crit_threshold, OPT_INT, 0) // percent of quota at which to issue errors
OPTION(client_cache_size, OPT_INT, 16384)
OPTION(client_cache_mid, OPT_FLOAT, .75)
OPTION(client_use_random_mds, OPT_BOOL, false)
OPTION(client_mount_timeout, OPT_DOUBLE, 300.0)
OPTION(client_tick_interval, OPT_DOUBLE, 1.0)
OPTION(client_trace, OPT_STR, "")
OPTION(client_readahead_min, OPT_LONGLONG, 128*1024)  // readahead at _least_ this much.
OPTION(client_readahead_max_bytes, OPT_LONGLONG, 0)  //8 * 1024*1024
OPTION(client_readahead_max_periods, OPT_LONGLONG, 4)  // as multiple of file layout period (object size * num stripes)
OPTION(client_snapdir, OPT_STR, ".snap")
OPTION(client_mountpoint, OPT_STR, "/")
OPTION(client_notify_timeout, OPT_INT, 10) // in seconds
OPTION(osd_client_watch_timeout, OPT_INT, 30) // in seconds
OPTION(client_caps_release_delay, OPT_INT, 5) // in seconds
OPTION(client_oc, OPT_BOOL, true)
OPTION(client_oc_size, OPT_INT, 1024*1024* 200)    // MB * n
OPTION(client_oc_max_dirty, OPT_INT, 1024*1024* 100)    // MB * n  (dirty OR tx.. bigish)
OPTION(client_oc_target_dirty, OPT_INT, 1024*1024* 8) // target dirty (keep this smallish)
OPTION(client_oc_max_dirty_age, OPT_DOUBLE, 5.0)      // max age in cache before writeback
OPTION(client_oc_max_objects, OPT_INT, 1000)      // max objects in cache
OPTION(client_debug_force_sync_read, OPT_BOOL, false)     // always read synchronously (go to osds)
OPTION(client_debug_inject_tick_delay, OPT_INT, 0) // delay the client tick for a number of seconds
// note: the max amount of "in flight" dirty data is roughly (max - target)
OPTION(fuse_use_invalidate_cb, OPT_BOOL, false) // use fuse 2.8+ invalidate callback to keep page cache consistent
OPTION(fuse_allow_other, OPT_BOOL, true)
OPTION(fuse_default_permissions, OPT_BOOL, true)
OPTION(fuse_big_writes, OPT_BOOL, true)
OPTION(fuse_atomic_o_trunc, OPT_BOOL, true)
OPTION(fuse_debug, OPT_BOOL, false)
OPTION(objecter_tick_interval, OPT_DOUBLE, 5.0)
OPTION(objecter_timeout, OPT_DOUBLE, 10.0)    // before we ask for a map
OPTION(objecter_inflight_op_bytes, OPT_U64, 1024*1024*100) // max in-flight data (both directions)
OPTION(objecter_inflight_ops, OPT_U64, 1024)               // max in-flight ios
OPTION(journaler_allow_split_entries, OPT_BOOL, true)
OPTION(journaler_write_head_interval, OPT_INT, 15)
OPTION(journaler_prefetch_periods, OPT_INT, 10)   // * journal object size
OPTION(journaler_prezero_periods, OPT_INT, 5)     // * journal object size
OPTION(journaler_batch_interval, OPT_DOUBLE, .001)   // seconds.. max add'l latency we artificially incur
OPTION(journaler_batch_max, OPT_U64, 0)  // max bytes we'll delay flushing; disable, for now....
OPTION(mds_data, OPT_STR, "/var/lib/ceph/mds/$cluster-$id")
OPTION(mds_max_file_size, OPT_U64, 1ULL << 40)
OPTION(mds_cache_size, OPT_INT, 100000)
OPTION(mds_cache_mid, OPT_FLOAT, .7)
OPTION(mds_mem_max, OPT_INT, 1048576)        // KB
OPTION(mds_dir_commit_ratio, OPT_FLOAT, .5)
OPTION(mds_dir_max_commit_size, OPT_INT, 90) // MB
OPTION(mds_decay_halflife, OPT_FLOAT, 5)
OPTION(mds_beacon_interval, OPT_FLOAT, 4)
OPTION(mds_beacon_grace, OPT_FLOAT, 15)
OPTION(mds_enforce_unique_name, OPT_BOOL, true)
OPTION(mds_blacklist_interval, OPT_FLOAT, 24.0*60.0)  // how long to blacklist failed nodes
OPTION(mds_session_timeout, OPT_FLOAT, 60)    // cap bits and leases time out if client idle
OPTION(mds_session_autoclose, OPT_FLOAT, 300) // autoclose idle session
OPTION(mds_reconnect_timeout, OPT_FLOAT, 45)  // seconds to wait for clients during mds restart
	      //  make it (mds_session_timeout - mds_beacon_grace)
OPTION(mds_tick_interval, OPT_FLOAT, 5)
OPTION(mds_dirstat_min_interval, OPT_FLOAT, 1)    // try to avoid propagating more often than this
OPTION(mds_scatter_nudge_interval, OPT_FLOAT, 5)  // how quickly dirstat changes propagate up the hierarchy
OPTION(mds_client_prealloc_inos, OPT_INT, 1000)
OPTION(mds_early_reply, OPT_BOOL, true)
OPTION(mds_use_tmap, OPT_BOOL, true)        // use trivialmap for dir updates
OPTION(mds_default_dir_hash, OPT_INT, CEPH_STR_HASH_RJENKINS)
OPTION(mds_log, OPT_BOOL, true)
OPTION(mds_log_skip_corrupt_events, OPT_BOOL, false)
OPTION(mds_log_max_events, OPT_INT, -1)
OPTION(mds_log_segment_size, OPT_INT, 0)  // segment size for mds log,
	      // defaults to g_default_file_layout.fl_object_size (4MB)
OPTION(mds_log_max_segments, OPT_INT, 30)
OPTION(mds_log_max_expiring, OPT_INT, 20)
OPTION(mds_bal_sample_interval, OPT_FLOAT, 3.0)  // every 5 seconds
OPTION(mds_bal_replicate_threshold, OPT_FLOAT, 8000)
OPTION(mds_bal_unreplicate_threshold, OPT_FLOAT, 0)
OPTION(mds_bal_frag, OPT_BOOL, false)
OPTION(mds_bal_split_size, OPT_INT, 10000)
OPTION(mds_bal_split_rd, OPT_FLOAT, 25000)
OPTION(mds_bal_split_wr, OPT_FLOAT, 10000)
OPTION(mds_bal_split_bits, OPT_INT, 3)
OPTION(mds_bal_merge_size, OPT_INT, 50)
OPTION(mds_bal_merge_rd, OPT_FLOAT, 1000)
OPTION(mds_bal_merge_wr, OPT_FLOAT, 1000)
OPTION(mds_bal_interval, OPT_INT, 10)           // seconds
OPTION(mds_bal_fragment_interval, OPT_INT, 5)      // seconds
OPTION(mds_bal_idle_threshold, OPT_FLOAT, 0)
OPTION(mds_bal_max, OPT_INT, -1)
OPTION(mds_bal_max_until, OPT_INT, -1)
OPTION(mds_bal_mode, OPT_INT, 0)
OPTION(mds_bal_min_rebalance, OPT_FLOAT, .1)  // must be this much above average before we export anything
OPTION(mds_bal_min_start, OPT_FLOAT, .2)      // if we need less than this, we don't do anything
OPTION(mds_bal_need_min, OPT_FLOAT, .8)       // take within this range of what we need
OPTION(mds_bal_need_max, OPT_FLOAT, 1.2)
OPTION(mds_bal_midchunk, OPT_FLOAT, .3)       // any sub bigger than this taken in full
OPTION(mds_bal_minchunk, OPT_FLOAT, .001)     // never take anything smaller than this
OPTION(mds_bal_target_removal_min, OPT_INT, 5) // min balance iterations before old target is removed
OPTION(mds_bal_target_removal_max, OPT_INT, 10) // max balance iterations before old target is removed
OPTION(mds_replay_interval, OPT_FLOAT, 1.0) // time to wait before starting replay again
OPTION(mds_shutdown_check, OPT_INT, 0)
OPTION(mds_thrash_exports, OPT_INT, 0)
OPTION(mds_thrash_fragments, OPT_INT, 0)
OPTION(mds_dump_cache_on_map, OPT_BOOL, false)
OPTION(mds_dump_cache_after_rejoin, OPT_BOOL, false)
OPTION(mds_verify_scatter, OPT_BOOL, false)
OPTION(mds_debug_scatterstat, OPT_BOOL, false)
OPTION(mds_debug_frag, OPT_BOOL, false)
OPTION(mds_debug_auth_pins, OPT_BOOL, false)
OPTION(mds_debug_subtrees, OPT_BOOL, false)
OPTION(mds_kill_mdstable_at, OPT_INT, 0)
OPTION(mds_kill_export_at, OPT_INT, 0)
OPTION(mds_kill_import_at, OPT_INT, 0)
OPTION(mds_kill_link_at, OPT_INT, 0)
OPTION(mds_kill_rename_at, OPT_INT, 0)
OPTION(mds_kill_openc_at, OPT_INT, 0)
OPTION(mds_kill_journal_at, OPT_INT, 0)
OPTION(mds_kill_journal_expire_at, OPT_INT, 0)
OPTION(mds_kill_journal_replay_at, OPT_INT, 0)
OPTION(mds_open_remote_link_mode, OPT_INT, 0)
OPTION(mds_inject_traceless_reply_probability, OPT_DOUBLE, 0) /* percentage
				of MDS modify replies to skip sending the
				client a trace on [0-1]*/
OPTION(mds_wipe_sessions, OPT_BOOL, 0)
OPTION(mds_wipe_ino_prealloc, OPT_BOOL, 0)
OPTION(mds_skip_ino, OPT_INT, 0)
OPTION(max_mds, OPT_INT, 1)
OPTION(mds_standby_for_name, OPT_STR, "")
OPTION(mds_standby_for_rank, OPT_INT, -1)
OPTION(mds_standby_replay, OPT_BOOL, false)

// If true, compact leveldb store on mount
OPTION(osd_compact_leveldb_on_mount, OPT_BOOL, false)

// Maximum number of backfills to or from a single osd
OPTION(osd_max_backfills, OPT_U64, 10)

// Refuse backfills when OSD full ratio is above this value
OPTION(osd_backfill_full_ratio, OPT_FLOAT, 0.85)

// Seconds to wait before retrying refused backfills
OPTION(osd_backfill_retry_interval, OPT_DOUBLE, 10.0)

OPTION(osd_uuid, OPT_UUID, uuid_d())
OPTION(osd_data, OPT_STR, "/var/lib/ceph/osd/$cluster-$id")
OPTION(osd_journal, OPT_STR, "/var/lib/ceph/osd/$cluster-$id/journal")
OPTION(osd_journal_size, OPT_INT, 5120)         // in mb
OPTION(osd_max_write_size, OPT_INT, 90)
OPTION(osd_max_pgls, OPT_U64, 1024) // max number of pgls entries to return
OPTION(osd_client_message_size_cap, OPT_U64, 500*1024L*1024L) // client data allowed in-memory (in bytes)
OPTION(osd_client_message_cap, OPT_U64, 100)              // num client messages allowed in-memory
OPTION(osd_pg_bits, OPT_INT, 6)  // bits per osd
OPTION(osd_pgp_bits, OPT_INT, 6)  // bits per osd
OPTION(osd_crush_chooseleaf_type, OPT_INT, 1) // 1 = host
OPTION(osd_min_rep, OPT_INT, 1)
OPTION(osd_max_rep, OPT_INT, 10)
OPTION(osd_pool_default_crush_rule, OPT_INT, 0)
OPTION(osd_pool_default_size, OPT_INT, 2)
OPTION(osd_pool_default_min_size, OPT_INT, 0)  // 0 means no specific default; ceph will use size-size/2
OPTION(osd_pool_default_pg_num, OPT_INT, 8) // number of PGs for new pools. Configure in global or mon section of ceph.conf
OPTION(osd_pool_default_pgp_num, OPT_INT, 8) // number of PGs for placement purposes. Should be equal to pg_num
OPTION(osd_pool_default_flags, OPT_INT, 0)   // default flags for new pools
OPTION(osd_pool_default_flag_hashpspool, OPT_BOOL, false)   // use new pg hashing to prevent pool/pg overlap
OPTION(osd_map_dedup, OPT_BOOL, true)
OPTION(osd_map_cache_size, OPT_INT, 500)
OPTION(osd_map_message_max, OPT_INT, 100)  // max maps per MOSDMap message
OPTION(osd_map_share_max_epochs, OPT_INT, 100)  // cap on # of inc maps we send to peers, clients
OPTION(osd_op_threads, OPT_INT, 2)    // 0 == no threading
OPTION(osd_peering_wq_batch_size, OPT_U64, 20)
OPTION(osd_op_pq_max_tokens_per_priority, OPT_U64, 4194304)
OPTION(osd_op_pq_min_cost, OPT_U64, 65536)
OPTION(osd_disk_threads, OPT_INT, 1)
OPTION(osd_recovery_threads, OPT_INT, 1)
OPTION(osd_recover_clone_overlap, OPT_BOOL, true)   // preserve clone_overlap during recovery/migration

// Only use clone_overlap for recovery if there are fewer than
// osd_recover_clone_overlap_limit entries in the overlap set
OPTION(osd_recover_clone_overlap_limit, OPT_INT, 10)

OPTION(osd_backfill_scan_min, OPT_INT, 64)
OPTION(osd_backfill_scan_max, OPT_INT, 512)
OPTION(osd_op_thread_timeout, OPT_INT, 15)
OPTION(osd_recovery_thread_timeout, OPT_INT, 30)
OPTION(osd_snap_trim_thread_timeout, OPT_INT, 60*60*1)
OPTION(osd_scrub_thread_timeout, OPT_INT, 60)
OPTION(osd_scrub_finalize_thread_timeout, OPT_INT, 60*10)
OPTION(osd_remove_thread_timeout, OPT_INT, 60*60)
OPTION(osd_command_thread_timeout, OPT_INT, 10*60)
OPTION(osd_age, OPT_FLOAT, .8)
OPTION(osd_age_time, OPT_INT, 0)
OPTION(osd_heartbeat_addr, OPT_ADDR, entity_addr_t())
OPTION(osd_heartbeat_interval, OPT_INT, 6)       // (seconds) how often we ping peers
OPTION(osd_heartbeat_grace, OPT_INT, 20)         // (seconds) how long before we decide a peer has failed
OPTION(osd_heartbeat_min_peers, OPT_INT, 10)     // minimum number of peers

// minimum number of peers tha tmust be reachable to mark ourselves
// back up after being wrongly marked down.
OPTION(osd_heartbeat_min_healthy_ratio, OPT_FLOAT, .33)

OPTION(osd_mon_heartbeat_interval, OPT_INT, 30)  // (seconds) how often to ping monitor if no peers
OPTION(osd_mon_report_interval_max, OPT_INT, 120)
OPTION(osd_mon_report_interval_min, OPT_INT, 5)  // pg stats, failures, up_thru, boot.
OPTION(osd_pg_stat_report_interval_max, OPT_INT, 500)  // report pg stats for any given pg at least this often
OPTION(osd_mon_ack_timeout, OPT_INT, 30) // time out a mon if it doesn't ack stats
OPTION(osd_default_data_pool_replay_window, OPT_INT, 45)
OPTION(osd_preserve_trimmed_log, OPT_BOOL, false)
OPTION(osd_auto_mark_unfound_lost, OPT_BOOL, false)
OPTION(osd_recovery_delay_start, OPT_FLOAT, 0)
OPTION(osd_recovery_max_active, OPT_INT, 15)
OPTION(osd_recovery_max_single_start, OPT_INT, 5)
OPTION(osd_recovery_max_chunk, OPT_U64, 8<<20)  // max size of push chunk
OPTION(osd_copyfrom_max_chunk, OPT_U64, 8<<20)   // max size of a COPYFROM chunk
OPTION(osd_push_per_object_cost, OPT_U64, 1000)  // push cost per object
OPTION(osd_max_push_cost, OPT_U64, 8<<20)  // max size of push message
OPTION(osd_max_push_objects, OPT_U64, 10)  // max objects in single push op
OPTION(osd_recovery_forget_lost_objects, OPT_BOOL, false)   // off for now
OPTION(osd_max_scrubs, OPT_INT, 1)
OPTION(osd_scrub_load_threshold, OPT_FLOAT, 0.5)
OPTION(osd_scrub_min_interval, OPT_FLOAT, 60*60*24)    // if load is low
OPTION(osd_scrub_max_interval, OPT_FLOAT, 7*60*60*24)  // regardless of load
OPTION(osd_scrub_chunk_min, OPT_INT, 5)
OPTION(osd_scrub_chunk_max, OPT_INT, 25)
OPTION(osd_deep_scrub_interval, OPT_FLOAT, 60*60*24*7) // once a week
OPTION(osd_deep_scrub_stride, OPT_INT, 524288)
OPTION(osd_scan_list_ping_tp_interval, OPT_U64, 100)
OPTION(osd_auto_weight, OPT_BOOL, false)
OPTION(osd_class_dir, OPT_STR, CEPH_LIBDIR "/rados-classes") // where rados plugins are stored
OPTION(osd_open_classes_on_start, OPT_BOOL, true)
OPTION(osd_check_for_log_corruption, OPT_BOOL, false)
OPTION(osd_use_stale_snap, OPT_BOOL, false)
OPTION(osd_rollback_to_cluster_snap, OPT_STR, "")
OPTION(osd_default_notify_timeout, OPT_U32, 30) // default notify timeout in seconds
OPTION(osd_kill_backfill_at, OPT_INT, 0)

// Bounds how infrequently a new map epoch will be persisted for a pg
OPTION(osd_pg_epoch_persisted_max_stale, OPT_U32, 200)

OPTION(osd_min_pg_log_entries, OPT_U32, 3000)  // number of entries to keep in the pg log when trimming it
OPTION(osd_max_pg_log_entries, OPT_U32, 10000) // max entries, say when degraded, before we trim
OPTION(osd_op_complaint_time, OPT_FLOAT, 30) // how many seconds old makes an op complaint-worthy
OPTION(osd_command_max_records, OPT_INT, 256)
OPTION(osd_op_log_threshold, OPT_INT, 5) // how many op log messages to show in one go
OPTION(osd_verify_sparse_read_holes, OPT_BOOL, false)  // read fiemap-reported holes and verify they are zeros
OPTION(osd_debug_drop_ping_probability, OPT_DOUBLE, 0)
OPTION(osd_debug_drop_ping_duration, OPT_INT, 0)
OPTION(osd_debug_drop_pg_create_probability, OPT_DOUBLE, 0)
OPTION(osd_debug_drop_pg_create_duration, OPT_INT, 1)
OPTION(osd_debug_drop_op_probability, OPT_DOUBLE, 0)   // probability of stalling/dropping a client op
OPTION(osd_debug_op_order, OPT_BOOL, false)
OPTION(osd_debug_verify_snaps_on_info, OPT_BOOL, false)
OPTION(osd_debug_verify_stray_on_activate, OPT_BOOL, false)
OPTION(osd_debug_skip_full_check_in_backfill_reservation, OPT_BOOL, false)
OPTION(osd_op_history_size, OPT_U32, 20)    // Max number of completed ops to track
OPTION(osd_op_history_duration, OPT_U32, 600) // Oldest completed op to track
OPTION(osd_target_transaction_size, OPT_INT, 30)     // to adjust various transactions that batch smaller items
OPTION(osd_failsafe_full_ratio, OPT_FLOAT, .97) // what % full makes an OSD "full" (failsafe)
OPTION(osd_failsafe_nearfull_ratio, OPT_FLOAT, .90) // what % full makes an OSD near full (failsafe)
OPTION(osd_leveldb_write_buffer_size, OPT_U64, 0) // OSD's leveldb write buffer size
OPTION(osd_leveldb_cache_size, OPT_U64, 0) // OSD's leveldb cache size
OPTION(osd_leveldb_block_size, OPT_U64, 0) // OSD's leveldb block size
OPTION(osd_leveldb_bloom_size, OPT_INT, 0) // OSD's leveldb bloom bits per entry
OPTION(osd_leveldb_max_open_files, OPT_INT, 0) // OSD's leveldb max open files
OPTION(osd_leveldb_compression, OPT_BOOL, true) // OSD's leveldb uses compression
OPTION(osd_leveldb_paranoid, OPT_BOOL, false) // OSD's leveldb paranoid flag
OPTION(osd_leveldb_log, OPT_STR, "/dev/null")  // enable OSD leveldb log file

// determines whether PGLog::check() compares written out log to stored log
OPTION(osd_debug_pg_log_writeout, OPT_BOOL, false)

/**
 * osd_client_op_priority and osd_recovery_op_priority adjust the relative
 * priority of client io vs recovery io.
 *
 * osd_client_op_priority/osd_recovery_op_priority determines the ratio of
 * available io between client and recovery.  Each option may be set between
 * 1..63.
 *
 * osd_recovery_op_warn_multiple scales the normal warning threshhold,
 * osd_op_complaint_time, so that slow recovery ops won't cause noise
 */
OPTION(osd_client_op_priority, OPT_U32, 63)
OPTION(osd_recovery_op_priority, OPT_U32, 10)
OPTION(osd_recovery_op_warn_multiple, OPT_U32, 16)

// Max time to wait between notifying mon of shutdown and shutting down
OPTION(osd_mon_shutdown_timeout, OPT_DOUBLE, 5)

OPTION(osd_max_object_size, OPT_U64, 100*1024L*1024L*1024L) // OSD's maximum object size
OPTION(osd_max_attr_size, OPT_U64, 0)

OPTION(filestore, OPT_BOOL, false)

/// filestore wb throttle limits
OPTION(filestore_wbthrottle_enable, OPT_BOOL, true)
OPTION(filestore_wbthrottle_btrfs_bytes_start_flusher, OPT_U64, 41943040)
OPTION(filestore_wbthrottle_btrfs_bytes_hard_limit, OPT_U64, 419430400)
OPTION(filestore_wbthrottle_btrfs_ios_start_flusher, OPT_U64, 500)
OPTION(filestore_wbthrottle_btrfs_ios_hard_limit, OPT_U64, 5000)
OPTION(filestore_wbthrottle_btrfs_inodes_start_flusher, OPT_U64, 500)
OPTION(filestore_wbthrottle_xfs_bytes_start_flusher, OPT_U64, 41943040)
OPTION(filestore_wbthrottle_xfs_bytes_hard_limit, OPT_U64, 419430400)
OPTION(filestore_wbthrottle_xfs_ios_start_flusher, OPT_U64, 500)
OPTION(filestore_wbthrottle_xfs_ios_hard_limit, OPT_U64, 5000)
OPTION(filestore_wbthrottle_xfs_inodes_start_flusher, OPT_U64, 500)

/// These must be less than the fd limit
OPTION(filestore_wbthrottle_btrfs_inodes_hard_limit, OPT_U64, 5000)
OPTION(filestore_wbthrottle_xfs_inodes_hard_limit, OPT_U64, 5000)

// Tests index failure paths
OPTION(filestore_index_retry_probability, OPT_DOUBLE, 0)

// Allow object read error injection
OPTION(filestore_debug_inject_read_err, OPT_BOOL, false)

OPTION(filestore_debug_omap_check, OPT_BOOL, 0) // Expensive debugging check on sync

// Use omap for xattrs for attrs over
// filestore_max_inline_xattr_size or
OPTION(filestore_max_inline_xattr_size, OPT_U32, 0)	//Override
OPTION(filestore_max_inline_xattr_size_xfs, OPT_U32, 65536)
OPTION(filestore_max_inline_xattr_size_btrfs, OPT_U32, 2048)
OPTION(filestore_max_inline_xattr_size_other, OPT_U32, 512)

// for more than filestore_max_inline_xattrs attrs
OPTION(filestore_max_inline_xattrs, OPT_U32, 0)	//Override
OPTION(filestore_max_inline_xattrs_xfs, OPT_U32, 10)
OPTION(filestore_max_inline_xattrs_btrfs, OPT_U32, 10)
OPTION(filestore_max_inline_xattrs_other, OPT_U32, 2)

OPTION(filestore_sloppy_crc, OPT_BOOL, false)         // track sloppy crcs
OPTION(filestore_sloppy_crc_block_size, OPT_INT, 65536)

OPTION(filestore_max_sync_interval, OPT_DOUBLE, 5)    // seconds
OPTION(filestore_min_sync_interval, OPT_DOUBLE, .01)  // seconds
OPTION(filestore_btrfs_snap, OPT_BOOL, true)
OPTION(filestore_btrfs_clone_range, OPT_BOOL, true)
OPTION(filestore_zfs_snap, OPT_BOOL, false) // zfsonlinux is still unstable
OPTION(filestore_fsync_flushes_journal_data, OPT_BOOL, false)
OPTION(filestore_fiemap, OPT_BOOL, false)     // (try to) use fiemap
OPTION(filestore_journal_parallel, OPT_BOOL, false)
OPTION(filestore_journal_writeahead, OPT_BOOL, false)
OPTION(filestore_journal_trailing, OPT_BOOL, false)
OPTION(filestore_queue_max_ops, OPT_INT, 50)
OPTION(filestore_queue_max_bytes, OPT_INT, 100 << 20)
OPTION(filestore_queue_committing_max_ops, OPT_INT, 500)        // this is ON TOP of filestore_queue_max_*
OPTION(filestore_queue_committing_max_bytes, OPT_INT, 100 << 20) //  "
OPTION(filestore_op_threads, OPT_INT, 2)
OPTION(filestore_op_thread_timeout, OPT_INT, 60)
OPTION(filestore_op_thread_suicide_timeout, OPT_INT, 180)
OPTION(filestore_commit_timeout, OPT_FLOAT, 600)
OPTION(filestore_fiemap_threshold, OPT_INT, 4096)
OPTION(filestore_merge_threshold, OPT_INT, 10)
OPTION(filestore_split_multiple, OPT_INT, 2)
OPTION(filestore_update_to, OPT_INT, 1000)
OPTION(filestore_blackhole, OPT_BOOL, false)     // drop any new transactions on the floor
OPTION(filestore_fd_cache_size, OPT_INT, 128)    // FD lru size
OPTION(filestore_dump_file, OPT_STR, "")         // file onto which store transaction dumps
OPTION(filestore_kill_at, OPT_INT, 0)            // inject a failure at the n'th opportunity
OPTION(filestore_inject_stall, OPT_INT, 0)       // artificially stall for N seconds in op queue thread
OPTION(filestore_fail_eio, OPT_BOOL, true)       // fail/crash on EIO
OPTION(filestore_replica_fadvise, OPT_BOOL, true)
OPTION(filestore_debug_verify_split, OPT_BOOL, false)
OPTION(journal_dio, OPT_BOOL, true)
OPTION(journal_aio, OPT_BOOL, true)
OPTION(journal_force_aio, OPT_BOOL, false)

// max bytes to search ahead in journal searching for corruption
OPTION(journal_max_corrupt_search, OPT_U64, 10<<20)
OPTION(journal_block_align, OPT_BOOL, true)
OPTION(journal_write_header_frequency, OPT_U64, 0)
OPTION(journal_max_write_bytes, OPT_INT, 10 << 20)
OPTION(journal_max_write_entries, OPT_INT, 100)
OPTION(journal_queue_max_ops, OPT_INT, 300)
OPTION(journal_queue_max_bytes, OPT_INT, 32 << 20)
OPTION(journal_align_min_size, OPT_INT, 64 << 10)  // align data payloads >= this.
OPTION(journal_replay_from, OPT_INT, 0)
OPTION(journal_zero_on_create, OPT_BOOL, false)
OPTION(journal_ignore_corruption, OPT_BOOL, false) // assume journal is not corrupt

OPTION(rbd_cache, OPT_BOOL, false) // whether to enable caching (writeback unless rbd_cache_max_dirty is 0)
OPTION(rbd_cache_writethrough_until_flush, OPT_BOOL, false) // whether to make writeback caching writethrough until flush is called, to be sure the user of librbd will send flushs so that writeback is safe
OPTION(rbd_cache_size, OPT_LONGLONG, 32<<20)         // cache size in bytes
OPTION(rbd_cache_max_dirty, OPT_LONGLONG, 24<<20)    // dirty limit in bytes - set to 0 for write-through caching
OPTION(rbd_cache_target_dirty, OPT_LONGLONG, 16<<20) // target dirty limit in bytes
OPTION(rbd_cache_max_dirty_age, OPT_FLOAT, 1.0)      // seconds in cache before writeback starts
OPTION(rbd_cache_block_writes_upfront, OPT_BOOL, false) // whether to block writes to the cache before the aio_write call completes (true), or block before the aio completion is called (false)
OPTION(rbd_concurrent_management_ops, OPT_INT, 10) // how many operations can be in flight for a management operation like deleting or resizing an image
OPTION(rbd_balance_snap_reads, OPT_BOOL, false)
OPTION(rbd_localize_snap_reads, OPT_BOOL, false)

/*
 * The following options change the behavior for librbd's image creation methods that
 * don't require all of the parameters. These are provided so that older programs
 * can take advantage of newer features without being rewritten to use new versions
 * of the image creation functions.
 *
 * rbd_create()/RBD::create() are affected by all of these options.
 *
 * rbd_create2()/RBD::create2() and rbd_clone()/RBD::clone() are affected by:
 * - rbd_default_order
 * - rbd_default_stripe_count
 * - rbd_default_stripe_size
 *
 * rbd_create3()/RBD::create3() and rbd_clone2/RBD::clone2() are only
 * affected by rbd_default_order.
 */
OPTION(rbd_default_format, OPT_INT, 1)
OPTION(rbd_default_order, OPT_INT, 22)
OPTION(rbd_default_stripe_count, OPT_U64, 1) // changing requires stripingv2 feature
OPTION(rbd_default_stripe_unit, OPT_U64, 4194304) // changing to non-object size requires stripingv2 feature
OPTION(rbd_default_features, OPT_INT, 3) // 1 for layering, 3 for layering+stripingv2. only applies to format 2 images

OPTION(nss_db_path, OPT_STR, "") // path to nss db

OPTION(rgw_data, OPT_STR, "/var/lib/ceph/radosgw/$cluster-$id")
OPTION(rgw_enable_apis, OPT_STR, "s3, swift, swift_auth, admin")
OPTION(rgw_cache_enabled, OPT_BOOL, true)   // rgw cache enabled
OPTION(rgw_cache_lru_size, OPT_INT, 10000)   // num of entries in rgw cache
OPTION(rgw_socket_path, OPT_STR, "")   // path to unix domain socket, if not specified, rgw will not run as external fcgi
OPTION(rgw_host, OPT_STR, "")  // host for radosgw, can be an IP, default is 0.0.0.0
OPTION(rgw_port, OPT_STR, "")  // port to listen, format as "8080" "5000", if not specified, rgw will not run external fcgi
OPTION(rgw_dns_name, OPT_STR, "")
OPTION(rgw_script_uri, OPT_STR, "") // alternative value for SCRIPT_URI if not set in request
OPTION(rgw_request_uri, OPT_STR,  "") // alternative value for REQUEST_URI if not set in request
OPTION(rgw_swift_url, OPT_STR, "")             // the swift url, being published by the internal swift auth
OPTION(rgw_swift_url_prefix, OPT_STR, "swift") // entry point for which a url is considered a swift url
OPTION(rgw_swift_auth_url, OPT_STR, "")        // default URL to go and verify tokens for v1 auth (if not using internal swift auth)
OPTION(rgw_swift_auth_entry, OPT_STR, "auth")  // entry point for which a url is considered a swift auth url
OPTION(rgw_keystone_url, OPT_STR, "")  // url for keystone server
OPTION(rgw_keystone_admin_token, OPT_STR, "")  // keystone admin token (shared secret)
OPTION(rgw_keystone_accepted_roles, OPT_STR, "Member, admin")  // roles required to serve requests
OPTION(rgw_keystone_token_cache_size, OPT_INT, 10000)  // max number of entries in keystone token cache
OPTION(rgw_keystone_revocation_interval, OPT_INT, 15 * 60)  // seconds between tokens revocation check
OPTION(rgw_s3_auth_use_rados, OPT_BOOL, true)  // should we try to use the internal credentials for s3?
OPTION(rgw_s3_auth_use_keystone, OPT_BOOL, false)  // should we try to use keystone for s3?
OPTION(rgw_admin_entry, OPT_STR, "admin")  // entry point for which a url is considered an admin request
OPTION(rgw_enforce_swift_acls, OPT_BOOL, true)
OPTION(rgw_swift_token_expiration, OPT_INT, 24 * 3600) // time in seconds for swift token expiration
OPTION(rgw_print_continue, OPT_BOOL, true)  // enable if 100-Continue works
OPTION(rgw_remote_addr_param, OPT_STR, "REMOTE_ADDR")  // e.g. X-Forwarded-For, if you have a reverse proxy
OPTION(rgw_op_thread_timeout, OPT_INT, 10*60)
OPTION(rgw_op_thread_suicide_timeout, OPT_INT, 0)
OPTION(rgw_thread_pool_size, OPT_INT, 100)
OPTION(rgw_num_control_oids, OPT_INT, 8)

OPTION(rgw_zone, OPT_STR, "") // zone name
OPTION(rgw_zone_root_pool, OPT_STR, ".rgw.root")    // pool where zone specific info is stored
OPTION(rgw_region, OPT_STR, "") // region name
OPTION(rgw_region_root_pool, OPT_STR, ".rgw.root")  // pool where all region info is stored
OPTION(rgw_default_region_info_oid, OPT_STR, "default.region")  // oid where default region info is stored
OPTION(rgw_log_nonexistent_bucket, OPT_BOOL, false)
OPTION(rgw_log_object_name, OPT_STR, "%Y-%m-%d-%H-%i-%n")      // man date to see codes (a subset are supported)
OPTION(rgw_log_object_name_utc, OPT_BOOL, false)
OPTION(rgw_usage_max_shards, OPT_INT, 32)
OPTION(rgw_usage_max_user_shards, OPT_INT, 1)
OPTION(rgw_enable_ops_log, OPT_BOOL, false) // enable logging every rgw operation
OPTION(rgw_enable_usage_log, OPT_BOOL, false) // enable logging bandwidth usage
OPTION(rgw_ops_log_rados, OPT_BOOL, true) // whether ops log should go to rados
OPTION(rgw_ops_log_socket_path, OPT_STR, "") // path to unix domain socket where ops log can go
OPTION(rgw_ops_log_data_backlog, OPT_INT, 5 << 20) // max data backlog for ops log
OPTION(rgw_usage_log_flush_threshold, OPT_INT, 1024) // threshold to flush pending log data
OPTION(rgw_usage_log_tick_interval, OPT_INT, 30) // flush pending log data every X seconds
OPTION(rgw_intent_log_object_name, OPT_STR, "%Y-%m-%d-%i-%n")  // man date to see codes (a subset are supported)
OPTION(rgw_intent_log_object_name_utc, OPT_BOOL, false)
OPTION(rgw_init_timeout, OPT_INT, 300) // time in seconds
OPTION(rgw_mime_types_file, OPT_STR, "/etc/mime.types")
OPTION(rgw_gc_max_objs, OPT_INT, 32)
OPTION(rgw_gc_obj_min_wait, OPT_INT, 2 * 3600)    // wait time before object may be handled by gc
OPTION(rgw_gc_processor_max_time, OPT_INT, 3600)  // total run time for a single gc processor work
OPTION(rgw_gc_processor_period, OPT_INT, 3600)  // gc processor cycle time
OPTION(rgw_s3_success_create_obj_status, OPT_INT, 0) // alternative success status response for create-obj (0 - default)
OPTION(rgw_resolve_cname, OPT_BOOL, false)  // should rgw try to resolve hostname as a dns cname record
OPTION(rgw_obj_stripe_size, OPT_INT, 4 << 20)
OPTION(rgw_extended_http_attrs, OPT_STR, "") // list of extended attrs that can be set on objects (beyond the default)
OPTION(rgw_exit_timeout_secs, OPT_INT, 120) // how many seconds to wait for process to go down before exiting unconditionally
OPTION(rgw_get_obj_window_size, OPT_INT, 16 << 20) // window size in bytes for single get obj request
OPTION(rgw_get_obj_max_req_size, OPT_INT, 4 << 20) // max length of a single get obj rados op
OPTION(rgw_relaxed_s3_bucket_names, OPT_BOOL, false) // enable relaxed bucket name rules for US region buckets
OPTION(rgw_list_buckets_max_chunk, OPT_INT, 1000) // max buckets to retrieve in a single op when listing user buckets
OPTION(rgw_md_log_max_shards, OPT_INT, 64) // max shards for metadata log
OPTION(rgw_num_zone_opstate_shards, OPT_INT, 128) // max shards for keeping inter-region copy progress info
OPTION(rgw_opstate_ratelimit_sec, OPT_INT, 30) // min time between opstate updates on a single upload (0 for disabling ratelimit)
OPTION(rgw_curl_wait_timeout_ms, OPT_INT, 1000) // timeout for certain curl calls
OPTION(rgw_copy_obj_progress, OPT_BOOL, true) // should dump progress during long copy operations?
OPTION(rgw_copy_obj_progress_every_bytes, OPT_INT, 1024 * 1024) // min bytes between copy progress output

OPTION(rgw_data_log_window, OPT_INT, 30) // data log entries window (in seconds)
OPTION(rgw_data_log_changes_size, OPT_INT, 1000) // number of in-memory entries to hold for data changes log
OPTION(rgw_data_log_num_shards, OPT_INT, 128) // number of objects to keep data changes log on
OPTION(rgw_data_log_obj_prefix, OPT_STR, "data_log") // 
OPTION(rgw_replica_log_obj_prefix, OPT_STR, "replica_log") // 

OPTION(rgw_bucket_quota_ttl, OPT_INT, 600) // time for cached bucket stats to be cached within rgw instance
OPTION(rgw_bucket_quota_soft_threshold, OPT_DOUBLE, 0.95) // threshold from which we don't rely on cached info for quota decisions
OPTION(rgw_bucket_quota_cache_size, OPT_INT, 10000) // number of entries in bucket quota cache

OPTION(rgw_standalone_server_port, OPT_INT, 0) // 0 means disabled

OPTION(mutex_perf_counter, OPT_BOOL, false) // enable/disable mutex perf counter

// This will be set to true when it is safe to start threads.
// Once it is true, it will never change.
OPTION(internal_safe_to_start_threads, OPT_BOOL, false)
