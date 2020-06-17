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
OPTION(host, OPT_STR) // "" means that ceph will use short hostname
OPTION(public_addr, OPT_ADDR)
OPTION(public_addrv, OPT_ADDRVEC)
OPTION(public_bind_addr, OPT_ADDR)
OPTION(cluster_addr, OPT_ADDR)
OPTION(public_network, OPT_STR)
OPTION(cluster_network, OPT_STR)
OPTION(lockdep, OPT_BOOL)
OPTION(lockdep_force_backtrace, OPT_BOOL) // always gather current backtrace at every lock
OPTION(run_dir, OPT_STR)       // the "/var/run/ceph" dir, created on daemon startup
OPTION(admin_socket, OPT_STR) // default changed by common_preinit()
OPTION(admin_socket_mode, OPT_STR) // permission bits to set for admin socket file, e.g., "0775", "0755"

OPTION(daemonize, OPT_BOOL) // default changed by common_preinit()
OPTION(setuser, OPT_STR)        // uid or user name
OPTION(setgroup, OPT_STR)        // gid or group name
OPTION(setuser_match_path, OPT_STR)  // make setuser/group conditional on this path matching ownership
OPTION(pid_file, OPT_STR) // default changed by common_preinit()
OPTION(chdir, OPT_STR)
OPTION(restapi_log_level, OPT_STR) 	// default set by Python code
OPTION(restapi_base_url, OPT_STR)	// "
OPTION(fatal_signal_handlers, OPT_BOOL)
OPTION(crash_dir, OPT_STR)
SAFE_OPTION(erasure_code_dir, OPT_STR) // default location for erasure-code plugins

OPTION(log_file, OPT_STR) // default changed by common_preinit()
OPTION(log_max_new, OPT_INT) // default changed by common_preinit()
OPTION(log_max_recent, OPT_INT) // default changed by common_preinit()
OPTION(log_to_file, OPT_BOOL)
OPTION(log_to_stderr, OPT_BOOL) // default changed by common_preinit()
OPTION(err_to_stderr, OPT_BOOL) // default changed by common_preinit()
OPTION(log_to_syslog, OPT_BOOL)
OPTION(err_to_syslog, OPT_BOOL)
OPTION(log_flush_on_exit, OPT_BOOL) // default changed by common_preinit()
OPTION(log_stop_at_utilization, OPT_FLOAT)  // stop logging at (near) full
OPTION(log_to_graylog, OPT_BOOL)
OPTION(err_to_graylog, OPT_BOOL)
OPTION(log_graylog_host, OPT_STR)
OPTION(log_graylog_port, OPT_INT)

// options will take k/v pairs, or single-item that will be assumed as general
// default for all, regardless of channel.
// e.g., "info" would be taken as the same as "default=info"
// also, "default=daemon audit=local0" would mean
//    "default all to 'daemon', override 'audit' with 'local0'
OPTION(clog_to_monitors, OPT_STR)
OPTION(clog_to_syslog, OPT_STR)
OPTION(clog_to_syslog_level, OPT_STR) // this level and above
OPTION(clog_to_syslog_facility, OPT_STR)
OPTION(clog_to_graylog, OPT_STR)
OPTION(clog_to_graylog_host, OPT_STR)
OPTION(clog_to_graylog_port, OPT_STR)

OPTION(mon_cluster_log_to_syslog, OPT_STR)
OPTION(mon_cluster_log_to_syslog_level, OPT_STR)   // this level and above
OPTION(mon_cluster_log_to_syslog_facility, OPT_STR)
OPTION(mon_cluster_log_to_file, OPT_BOOL)
OPTION(mon_cluster_log_file, OPT_STR)
OPTION(mon_cluster_log_file_level, OPT_STR)
OPTION(mon_cluster_log_to_graylog, OPT_STR)
OPTION(mon_cluster_log_to_graylog_host, OPT_STR)
OPTION(mon_cluster_log_to_graylog_port, OPT_STR)

OPTION(enable_experimental_unrecoverable_data_corrupting_features, OPT_STR)

SAFE_OPTION(plugin_dir, OPT_STR)

OPTION(compressor_zlib_isal, OPT_BOOL)
OPTION(compressor_zlib_level, OPT_INT) //regular zlib compression level, not applicable to isa-l optimized version
OPTION(compressor_zlib_winsize, OPT_INT) //regular zlib compression winsize, not applicable to isa-l optimized version
OPTION(compressor_zstd_level, OPT_INT) //regular zstd compression level

OPTION(qat_compressor_enabled, OPT_BOOL)

OPTION(plugin_crypto_accelerator, OPT_STR)

OPTION(mempool_debug, OPT_BOOL)

OPTION(openssl_engine_opts, OPT_STR)

OPTION(key, OPT_STR)
OPTION(keyfile, OPT_STR)
OPTION(keyring, OPT_STR)
OPTION(heartbeat_interval, OPT_INT)
OPTION(heartbeat_file, OPT_STR)
OPTION(heartbeat_inject_failure, OPT_INT)    // force an unhealthy heartbeat for N seconds
OPTION(perf, OPT_BOOL)       // enable internal perf counters

SAFE_OPTION(ms_type, OPT_STR)   // messenger backend. It will be modified in runtime, so use SAFE_OPTION
OPTION(ms_public_type, OPT_STR)   // messenger backend
OPTION(ms_cluster_type, OPT_STR)   // messenger backend
OPTION(ms_learn_addr_from_peer, OPT_BOOL)
OPTION(ms_tcp_nodelay, OPT_BOOL)
OPTION(ms_tcp_rcvbuf, OPT_INT)
OPTION(ms_tcp_prefetch_max_size, OPT_U32) // max prefetch size, we limit this to avoid extra memcpy
OPTION(ms_initial_backoff, OPT_DOUBLE)
OPTION(ms_max_backoff, OPT_DOUBLE)
OPTION(ms_crc_data, OPT_BOOL)
OPTION(ms_crc_header, OPT_BOOL)
OPTION(ms_die_on_bad_msg, OPT_BOOL)
OPTION(ms_die_on_unhandled_msg, OPT_BOOL)
OPTION(ms_die_on_old_message, OPT_BOOL)     // assert if we get a dup incoming message and shouldn't have (may be triggered by pre-541cd3c64be0dfa04e8a2df39422e0eb9541a428 code)
OPTION(ms_die_on_skipped_message, OPT_BOOL)  // assert if we skip a seq (kernel client does this intentionally)
OPTION(ms_die_on_bug, OPT_BOOL)
OPTION(ms_dispatch_throttle_bytes, OPT_U64)
OPTION(ms_bind_ipv6, OPT_BOOL)
OPTION(ms_bind_port_min, OPT_INT)
OPTION(ms_bind_port_max, OPT_INT)
OPTION(ms_bind_retry_count, OPT_INT) // If binding fails, how many times do we retry to bind
OPTION(ms_bind_retry_delay, OPT_INT) // Delay between attempts to bind
OPTION(ms_bind_before_connect, OPT_BOOL)
OPTION(ms_tcp_listen_backlog, OPT_INT)
OPTION(ms_connection_ready_timeout, OPT_U64)
OPTION(ms_connection_idle_timeout, OPT_U64)
OPTION(ms_pq_max_tokens_per_priority, OPT_U64)
OPTION(ms_pq_min_cost, OPT_U64)
OPTION(ms_inject_socket_failures, OPT_U64)
SAFE_OPTION(ms_inject_delay_type, OPT_STR)          // "osd mds mon client" allowed
OPTION(ms_inject_delay_msg_type, OPT_STR)      // the type of message to delay). This is an additional restriction on the general type filter ms_inject_delay_type.
OPTION(ms_inject_delay_max, OPT_DOUBLE)         // seconds
OPTION(ms_inject_delay_probability, OPT_DOUBLE) // range [0, 1]
OPTION(ms_inject_internal_delays, OPT_DOUBLE)   // seconds
OPTION(ms_blackhole_osd, OPT_BOOL)
OPTION(ms_blackhole_mon, OPT_BOOL)
OPTION(ms_blackhole_mds, OPT_BOOL)
OPTION(ms_blackhole_mgr, OPT_BOOL)
OPTION(ms_blackhole_client, OPT_BOOL)
OPTION(ms_dump_on_send, OPT_BOOL)           // hexdump msg to log on send
OPTION(ms_dump_corrupt_message_level, OPT_INT)  // debug level to hexdump undecodeable messages at
OPTION(ms_async_op_threads, OPT_U64)            // number of worker processing threads for async messenger created on init
OPTION(ms_async_max_op_threads, OPT_U64)        // max number of worker processing threads for async messenger
OPTION(ms_async_rdma_device_name, OPT_STR)
OPTION(ms_async_rdma_enable_hugepage, OPT_BOOL)
OPTION(ms_async_rdma_buffer_size, OPT_INT)
OPTION(ms_async_rdma_send_buffers, OPT_U32)
//size of the receive buffer pool, 0 is unlimited
OPTION(ms_async_rdma_receive_buffers, OPT_U32)
// max number of wr in srq
OPTION(ms_async_rdma_receive_queue_len, OPT_U32)
// support srq
OPTION(ms_async_rdma_support_srq, OPT_BOOL)
OPTION(ms_async_rdma_port_num, OPT_U32)
OPTION(ms_async_rdma_polling_us, OPT_U32)
OPTION(ms_async_rdma_local_gid, OPT_STR)       // GID format: "fe80:0000:0000:0000:7efe:90ff:fe72:6efe", no zero folding
OPTION(ms_async_rdma_roce_ver, OPT_INT)         // 0=RoCEv1, 1=RoCEv2, 2=RoCEv1.5
OPTION(ms_async_rdma_sl, OPT_INT)               // in RoCE, this means PCP
OPTION(ms_async_rdma_dscp, OPT_INT)            // in RoCE, this means DSCP

// rdma connection management
OPTION(ms_async_rdma_cm, OPT_BOOL)
OPTION(ms_async_rdma_type, OPT_STR)

// when there are enough accept failures, indicating there are unrecoverable failures,
// just do ceph_abort() . Here we make it configurable.
OPTION(ms_max_accept_failures, OPT_INT)

OPTION(ms_dpdk_port_id, OPT_INT)
SAFE_OPTION(ms_dpdk_coremask, OPT_STR)        // it is modified in unittest so that use SAFE_OPTION to declare
OPTION(ms_dpdk_memory_channel, OPT_STR)
OPTION(ms_dpdk_hugepages, OPT_STR)
OPTION(ms_dpdk_pmd, OPT_STR)
SAFE_OPTION(ms_dpdk_host_ipv4_addr, OPT_STR)
SAFE_OPTION(ms_dpdk_gateway_ipv4_addr, OPT_STR)
SAFE_OPTION(ms_dpdk_netmask_ipv4_addr, OPT_STR)
OPTION(ms_dpdk_lro, OPT_BOOL)
OPTION(ms_dpdk_hw_flow_control, OPT_BOOL)
// Weighing of a hardware network queue relative to a software queue (0=no work, 1=     equal share)")
OPTION(ms_dpdk_hw_queue_weight, OPT_FLOAT)
OPTION(ms_dpdk_debug_allow_loopback, OPT_BOOL)
OPTION(ms_dpdk_rx_buffer_count_per_core, OPT_INT)

OPTION(inject_early_sigterm, OPT_BOOL)

OPTION(mon_data, OPT_STR)
OPTION(mon_initial_members, OPT_STR)    // list of initial cluster mon ids; if specified, need majority to form initial quorum and create new cluster
OPTION(mon_compact_on_start, OPT_BOOL)  // compact leveldb on ceph-mon start
OPTION(mon_compact_on_bootstrap, OPT_BOOL)  // trigger leveldb compaction on bootstrap
OPTION(mon_compact_on_trim, OPT_BOOL)       // compact (a prefix) when we trim old states
OPTION(mon_osd_cache_size, OPT_INT)  // the size of osdmaps cache, not to rely on underlying store's cache

OPTION(mon_osd_cache_size_min, OPT_U64) // minimum amount of memory to cache osdmaps
OPTION(mon_memory_target, OPT_U64) // amount of mapped memory for osdmaps
OPTION(mon_memory_autotune, OPT_BOOL) // autotune cache memory for osdmap
OPTION(mon_cpu_threads, OPT_INT)
OPTION(mon_osd_mapping_pgs_per_chunk, OPT_INT)
OPTION(mon_clean_pg_upmaps_per_chunk, OPT_U64)
OPTION(mon_osd_max_creating_pgs, OPT_INT)
OPTION(mon_tick_interval, OPT_INT)
OPTION(mon_session_timeout, OPT_INT)    // must send keepalive or subscribe
OPTION(mon_subscribe_interval, OPT_DOUBLE)  // for legacy clients only
OPTION(mon_delta_reset_interval, OPT_DOUBLE)   // seconds of inactivity before we reset the pg delta to 0
OPTION(mon_osd_laggy_halflife, OPT_INT)        // (seconds) how quickly our laggy estimations decay
OPTION(mon_osd_laggy_weight, OPT_DOUBLE)          // weight for new 'samples's in laggy estimations
OPTION(mon_osd_laggy_max_interval, OPT_INT)      // maximum value of laggy_interval in laggy estimations
OPTION(mon_osd_adjust_heartbeat_grace, OPT_BOOL)    // true if we should scale based on laggy estimations
OPTION(mon_osd_adjust_down_out_interval, OPT_BOOL)  // true if we should scale based on laggy estimations
OPTION(mon_osd_auto_mark_in, OPT_BOOL)         // mark any booting osds 'in'
OPTION(mon_osd_auto_mark_auto_out_in, OPT_BOOL) // mark booting auto-marked-out osds 'in'
OPTION(mon_osd_auto_mark_new_in, OPT_BOOL)      // mark booting new osds 'in'
OPTION(mon_osd_destroyed_out_interval, OPT_INT) // seconds
OPTION(mon_osd_down_out_interval, OPT_INT) // seconds
OPTION(mon_osd_min_up_ratio, OPT_DOUBLE)    // min osds required to be up to mark things down
OPTION(mon_osd_min_in_ratio, OPT_DOUBLE)   // min osds required to be in to mark things out
OPTION(mon_osd_warn_op_age, OPT_DOUBLE)     // max op age before we generate a warning (make it a power of 2)
OPTION(mon_osd_err_op_age_ratio, OPT_DOUBLE)  // when to generate an error, as multiple of mon_osd_warn_op_age
OPTION(mon_osd_prime_pg_temp, OPT_BOOL)  // prime osdmap with pg mapping changes
OPTION(mon_osd_prime_pg_temp_max_time, OPT_FLOAT)  // max time to spend priming
OPTION(mon_osd_prime_pg_temp_max_estimate, OPT_FLOAT) // max estimate of pg total before we do all pgs in parallel
OPTION(mon_election_timeout, OPT_FLOAT)  // on election proposer, max waiting time for all ACKs
OPTION(mon_lease, OPT_FLOAT)       // lease interval
OPTION(mon_lease_renew_interval_factor, OPT_FLOAT) // on leader, to renew the lease
OPTION(mon_lease_ack_timeout_factor, OPT_FLOAT) // on leader, if lease isn't acked by all peons
OPTION(mon_accept_timeout_factor, OPT_FLOAT)    // on leader, if paxos update isn't accepted

OPTION(mon_clock_drift_allowed, OPT_FLOAT) // allowed clock drift between monitors
OPTION(mon_clock_drift_warn_backoff, OPT_FLOAT) // exponential backoff for clock drift warnings
OPTION(mon_timecheck_interval, OPT_FLOAT) // on leader, timecheck (clock drift check) interval (seconds)
OPTION(mon_timecheck_skew_interval, OPT_FLOAT) // on leader, timecheck (clock drift check) interval when in presence of a skew (seconds)
OPTION(mon_pg_check_down_all_threshold, OPT_FLOAT) // threshold of down osds after which we check all pgs
OPTION(mon_cache_target_full_warn_ratio, OPT_FLOAT) // position between pool cache_target_full and max where we start warning
OPTION(mon_osd_full_ratio, OPT_FLOAT) // what % full makes an OSD "full"
OPTION(mon_osd_backfillfull_ratio, OPT_FLOAT) // what % full makes an OSD backfill full (backfill halted)
OPTION(mon_osd_nearfull_ratio, OPT_FLOAT) // what % full makes an OSD near full
OPTION(mon_osd_initial_require_min_compat_client, OPT_STR)
OPTION(mon_allow_pool_delete, OPT_BOOL) // allow pool deletion
OPTION(mon_fake_pool_delete, OPT_BOOL)  // fake pool deletion (add _DELETED suffix)
OPTION(mon_globalid_prealloc, OPT_U32)   // how many globalids to prealloc
OPTION(mon_osd_report_timeout, OPT_INT)    // grace period before declaring unresponsive OSDs dead
OPTION(mon_warn_on_legacy_crush_tunables, OPT_BOOL) // warn if crush tunables are too old (older than mon_min_crush_required_version)
OPTION(mon_crush_min_required_version, OPT_STR)
OPTION(mon_warn_on_crush_straw_calc_version_zero, OPT_BOOL) // warn if crush straw_calc_version==0
OPTION(mon_warn_on_osd_down_out_interval_zero, OPT_BOOL) // warn if 'mon_osd_down_out_interval == 0'
OPTION(mon_warn_on_cache_pools_without_hit_sets, OPT_BOOL)
OPTION(mon_warn_on_misplaced, OPT_BOOL)
OPTION(mon_min_osdmap_epochs, OPT_INT)
OPTION(mon_max_log_epochs, OPT_INT)
OPTION(mon_max_mdsmap_epochs, OPT_INT)
OPTION(mon_max_osd, OPT_INT)
OPTION(mon_probe_timeout, OPT_DOUBLE)
OPTION(mon_client_bytes, OPT_U64)  // client msg data allowed in memory (in bytes)
OPTION(mon_log_max_summary, OPT_U64)
OPTION(mon_daemon_bytes, OPT_U64)  // mds, osd message memory cap (in bytes)
OPTION(mon_max_log_entries_per_event, OPT_INT)
OPTION(mon_reweight_min_pgs_per_osd, OPT_U64)   // min pgs per osd for reweight-by-pg command
OPTION(mon_reweight_min_bytes_per_osd, OPT_U64)   // min bytes per osd for reweight-by-utilization command
OPTION(mon_reweight_max_osds, OPT_INT)   // max osds to change per reweight-by-* command
OPTION(mon_reweight_max_change, OPT_DOUBLE)
OPTION(mon_health_to_clog, OPT_BOOL)
OPTION(mon_health_to_clog_interval, OPT_INT)
OPTION(mon_health_to_clog_tick_interval, OPT_DOUBLE)
OPTION(mon_data_avail_crit, OPT_INT)
OPTION(mon_data_avail_warn, OPT_INT)
OPTION(mon_data_size_warn, OPT_U64) // issue a warning when the monitor's data store goes over 15GB (in bytes)
OPTION(mon_warn_pg_not_scrubbed_ratio, OPT_FLOAT)
OPTION(mon_warn_pg_not_deep_scrubbed_ratio, OPT_FLOAT)
OPTION(mon_scrub_interval, OPT_INT) // once a day
OPTION(mon_scrub_timeout, OPT_INT) // let's give it 5 minutes; why not.
OPTION(mon_scrub_max_keys, OPT_INT) // max number of keys to scrub each time
OPTION(mon_scrub_inject_crc_mismatch, OPT_DOUBLE) // probability of injected crc mismatch [0.0, 1.0]
OPTION(mon_scrub_inject_missing_keys, OPT_DOUBLE) // probability of injected missing keys [0.0, 1.0]
OPTION(mon_config_key_max_entry_size, OPT_INT) // max num bytes per config-key entry
OPTION(mon_sync_timeout, OPT_DOUBLE)
OPTION(mon_sync_max_payload_size, OPT_SIZE)
OPTION(mon_sync_max_payload_keys, OPT_INT)
OPTION(mon_sync_debug, OPT_BOOL) // enable sync-specific debug
OPTION(mon_inject_sync_get_chunk_delay, OPT_DOUBLE)  // inject N second delay on each get_chunk request
OPTION(mon_osd_force_trim_to, OPT_INT)   // force mon to trim maps to this point, regardless of min_last_epoch_clean (dangerous)
OPTION(mon_mds_force_trim_to, OPT_INT)   // force mon to trim mdsmaps to this point (dangerous)
OPTION(mon_mds_skip_sanity, OPT_BOOL)  // skip safety assertions on FSMap (in case of bugs where we want to continue anyway)
OPTION(mon_osd_snap_trim_queue_warn_on, OPT_INT)

// monitor debug options
OPTION(mon_debug_deprecated_as_obsolete, OPT_BOOL) // consider deprecated commands as obsolete

// dump transactions
OPTION(mon_debug_dump_transactions, OPT_BOOL)
OPTION(mon_debug_dump_json, OPT_BOOL)
OPTION(mon_debug_dump_location, OPT_STR)
OPTION(mon_debug_no_require_bluestore_for_ec_overwrites, OPT_BOOL)
OPTION(mon_debug_no_initial_persistent_features, OPT_BOOL)
OPTION(mon_inject_transaction_delay_max, OPT_DOUBLE)      // seconds
OPTION(mon_inject_transaction_delay_probability, OPT_DOUBLE) // range [0, 1]

OPTION(mon_sync_provider_kill_at, OPT_INT)  // kill the sync provider at a specific point in the work flow
OPTION(mon_sync_requester_kill_at, OPT_INT) // kill the sync requester at a specific point in the work flow
OPTION(mon_force_quorum_join, OPT_BOOL) // force monitor to join quorum even if it has been previously removed from the map
OPTION(mon_keyvaluedb, OPT_STR)   // type of keyvaluedb backend

// UNSAFE -- TESTING ONLY! Allows addition of a cache tier with preexisting snaps
OPTION(mon_debug_unsafe_allow_tier_with_nonempty_snaps, OPT_BOOL)
OPTION(mon_osd_blacklist_default_expire, OPT_DOUBLE) // default one hour
OPTION(mon_osd_crush_smoke_test, OPT_BOOL)

OPTION(paxos_stash_full_interval, OPT_INT)   // how often (in commits) to stash a full copy of the PaxosService state
OPTION(paxos_max_join_drift, OPT_INT) // max paxos iterations before we must first sync the monitor stores
OPTION(paxos_propose_interval, OPT_DOUBLE)  // gather updates for this long before proposing a map update
OPTION(paxos_min_wait, OPT_DOUBLE)  // min time to gather updates for after period of inactivity
OPTION(paxos_min, OPT_INT)       // minimum number of paxos states to keep around
OPTION(paxos_trim_min, OPT_INT)  // number of extra proposals tolerated before trimming
OPTION(paxos_trim_max, OPT_INT) // max number of extra proposals to trim at a time
OPTION(paxos_service_trim_min, OPT_INT) // minimum amount of versions to trigger a trim (0 disables it)
OPTION(paxos_service_trim_max, OPT_INT) // maximum amount of versions to trim during a single proposal (0 disables it)
OPTION(paxos_kill_at, OPT_INT)
OPTION(auth_cluster_required, OPT_STR)   // required of mon, mds, osd daemons
OPTION(auth_service_required, OPT_STR)   // required by daemons of clients
OPTION(auth_client_required, OPT_STR)     // what clients require of daemons
OPTION(auth_supported, OPT_STR)               // deprecated; default value for above if they are not defined.
OPTION(max_rotating_auth_attempts, OPT_INT)
OPTION(cephx_require_signatures, OPT_BOOL)
OPTION(cephx_cluster_require_signatures, OPT_BOOL)
OPTION(cephx_service_require_signatures, OPT_BOOL)
OPTION(cephx_require_version, OPT_INT)
OPTION(cephx_cluster_require_version, OPT_INT)
OPTION(cephx_service_require_version, OPT_INT)
OPTION(cephx_sign_messages, OPT_BOOL)  // Default to signing session messages if supported
OPTION(auth_mon_ticket_ttl, OPT_DOUBLE)
OPTION(auth_service_ticket_ttl, OPT_DOUBLE)
OPTION(auth_debug, OPT_BOOL)          // if true, assert when weird things happen
OPTION(mon_client_hunt_parallel, OPT_U32)   // how many mons to try to connect to in parallel during hunt
OPTION(mon_client_hunt_interval, OPT_DOUBLE)   // try new mon every N seconds until we connect
OPTION(mon_client_log_interval, OPT_DOUBLE)  // send logs every N seconds
OPTION(mon_client_ping_interval, OPT_DOUBLE)  // ping every N seconds
OPTION(mon_client_ping_timeout, OPT_DOUBLE)   // fail if we don't hear back
OPTION(mon_client_hunt_interval_backoff, OPT_DOUBLE) // each time we reconnect to a monitor, double our timeout
OPTION(mon_client_hunt_interval_max_multiple, OPT_DOUBLE) // up to a max of 10*default (30 seconds)
OPTION(mon_client_max_log_entries_per_message, OPT_INT)
OPTION(mon_client_directed_command_retry, OPT_INT)
OPTION(client_cache_size, OPT_INT)
OPTION(client_cache_mid, OPT_FLOAT)
OPTION(client_use_random_mds, OPT_BOOL)
OPTION(client_mount_timeout, OPT_DOUBLE)
OPTION(client_tick_interval, OPT_DOUBLE)
OPTION(client_trace, OPT_STR)
OPTION(client_readahead_min, OPT_LONGLONG)  // readahead at _least_ this much.
OPTION(client_readahead_max_bytes, OPT_LONGLONG)  // default unlimited
OPTION(client_readahead_max_periods, OPT_LONGLONG)  // as multiple of file layout period (object size * num stripes)
OPTION(client_snapdir, OPT_STR)
OPTION(client_mount_uid, OPT_INT)
OPTION(client_mount_gid, OPT_INT)
OPTION(client_notify_timeout, OPT_INT) // in seconds
OPTION(osd_client_watch_timeout, OPT_INT) // in seconds
OPTION(client_caps_release_delay, OPT_INT) // in seconds
OPTION(client_quota_df, OPT_BOOL) // use quota for df on subdir mounts
OPTION(client_oc, OPT_BOOL)
OPTION(client_oc_size, OPT_INT)    // MB * n
OPTION(client_oc_max_dirty, OPT_INT)    // MB * n  (dirty OR tx.. bigish)
OPTION(client_oc_target_dirty, OPT_INT) // target dirty (keep this smallish)
OPTION(client_oc_max_dirty_age, OPT_DOUBLE)      // max age in cache before writeback
OPTION(client_oc_max_objects, OPT_INT)      // max objects in cache
OPTION(client_debug_getattr_caps, OPT_BOOL) // check if MDS reply contains wanted caps
OPTION(client_debug_force_sync_read, OPT_BOOL)     // always read synchronously (go to osds)
OPTION(client_debug_inject_tick_delay, OPT_INT) // delay the client tick for a number of seconds
OPTION(client_max_inline_size, OPT_U64)
OPTION(client_inject_release_failure, OPT_BOOL)  // synthetic client bug for testing
OPTION(client_inject_fixed_oldest_tid, OPT_BOOL)  // synthetic client bug for testing
OPTION(client_metadata, OPT_STR)
OPTION(client_acl_type, OPT_STR)
OPTION(client_permissions, OPT_BOOL)
OPTION(client_dirsize_rbytes, OPT_BOOL)

OPTION(client_try_dentry_invalidate, OPT_BOOL) // the client should try to use dentry invaldation instead of remounting, on kernels it believes that will work for
OPTION(client_check_pool_perm, OPT_BOOL)
OPTION(client_use_faked_inos, OPT_BOOL)

OPTION(crush_location, OPT_STR)       // whitespace-separated list of key=value pairs describing crush location
OPTION(crush_location_hook, OPT_STR)
OPTION(crush_location_hook_timeout, OPT_INT)

OPTION(objecter_tick_interval, OPT_DOUBLE)
OPTION(objecter_timeout, OPT_DOUBLE)    // before we ask for a map
OPTION(objecter_inflight_op_bytes, OPT_U64) // max in-flight data (both directions)
OPTION(objecter_inflight_ops, OPT_U64)               // max in-flight ios
OPTION(objecter_completion_locks_per_session, OPT_U64) // num of completion locks per each session, for serializing same object responses
OPTION(objecter_inject_no_watch_ping, OPT_BOOL)   // suppress watch pings
OPTION(objecter_retry_writes_after_first_reply, OPT_BOOL)   // ignore the first reply for each write, and resend the osd op instead
OPTION(objecter_debug_inject_relock_delay, OPT_BOOL)

// Max number of deletes at once in a single Filer::purge call
OPTION(filer_max_purge_ops, OPT_U32)
// Max number of truncate at once in a single Filer::truncate call
OPTION(filer_max_truncate_ops, OPT_U32)

OPTION(mds_data, OPT_STR)
// max xattr kv pairs size for each dir/file
OPTION(mds_max_xattr_pairs_size, OPT_U32)
OPTION(mds_max_file_recover, OPT_U32)
OPTION(mds_dir_max_commit_size, OPT_INT) // MB
OPTION(mds_dir_keys_per_op, OPT_INT)
OPTION(mds_decay_halflife, OPT_FLOAT)
OPTION(mds_beacon_interval, OPT_FLOAT)
OPTION(mds_beacon_grace, OPT_FLOAT)
OPTION(mds_enforce_unique_name, OPT_BOOL)

OPTION(mds_session_blacklist_on_timeout, OPT_BOOL)    // whether to blacklist clients whose sessions are dropped due to timeout
OPTION(mds_session_blacklist_on_evict, OPT_BOOL)  // whether to blacklist clients whose sessions are dropped via admin commands

OPTION(mds_sessionmap_keys_per_op, OPT_U32)    // how many sessions should I try to load/store in a single OMAP operation?
OPTION(mds_freeze_tree_timeout, OPT_FLOAT)    // detecting freeze tree deadlock
OPTION(mds_health_summarize_threshold, OPT_INT) // collapse N-client health metrics to a single 'many'
OPTION(mds_reconnect_timeout, OPT_FLOAT)  // seconds to wait for clients during mds restart
	      //  make it (mdsmap.session_timeout - mds_beacon_grace)
OPTION(mds_tick_interval, OPT_FLOAT)
OPTION(mds_dirstat_min_interval, OPT_FLOAT)    // try to avoid propagating more often than this
OPTION(mds_scatter_nudge_interval, OPT_FLOAT)  // how quickly dirstat changes propagate up the hierarchy
OPTION(mds_client_prealloc_inos, OPT_INT)
OPTION(mds_early_reply, OPT_BOOL)
OPTION(mds_default_dir_hash, OPT_INT)
OPTION(mds_log_pause, OPT_BOOL)
OPTION(mds_log_skip_corrupt_events, OPT_BOOL)
OPTION(mds_log_max_events, OPT_INT)
OPTION(mds_log_events_per_segment, OPT_INT)
OPTION(mds_log_segment_size, OPT_INT)  // segment size for mds log, default to default file_layout_t
OPTION(mds_log_max_segments, OPT_U32)
OPTION(mds_bal_export_pin, OPT_BOOL)  // allow clients to pin directory trees to ranks
OPTION(mds_bal_sample_interval, OPT_DOUBLE)  // every 3 seconds
OPTION(mds_bal_replicate_threshold, OPT_FLOAT)
OPTION(mds_bal_unreplicate_threshold, OPT_FLOAT)
OPTION(mds_bal_split_size, OPT_INT)
OPTION(mds_bal_split_rd, OPT_FLOAT)
OPTION(mds_bal_split_wr, OPT_FLOAT)
OPTION(mds_bal_split_bits, OPT_INT)
OPTION(mds_bal_merge_size, OPT_INT)
OPTION(mds_bal_fragment_size_max, OPT_INT) // order of magnitude higher than split size
OPTION(mds_bal_fragment_fast_factor, OPT_FLOAT) // multiple of size_max that triggers immediate split
OPTION(mds_bal_idle_threshold, OPT_FLOAT)
OPTION(mds_bal_max, OPT_INT)
OPTION(mds_bal_max_until, OPT_INT)
OPTION(mds_bal_mode, OPT_INT)
OPTION(mds_bal_min_rebalance, OPT_FLOAT)  // must be this much above average before we export anything
OPTION(mds_bal_min_start, OPT_FLOAT)      // if we need less than this, we don't do anything
OPTION(mds_bal_need_min, OPT_FLOAT)       // take within this range of what we need
OPTION(mds_bal_need_max, OPT_FLOAT)
OPTION(mds_bal_midchunk, OPT_FLOAT)       // any sub bigger than this taken in full
OPTION(mds_bal_minchunk, OPT_FLOAT)     // never take anything smaller than this
OPTION(mds_bal_target_decay, OPT_DOUBLE) // target decay half-life in MDSMap (2x larger is approx. 2x slower)
OPTION(mds_replay_interval, OPT_FLOAT) // time to wait before starting replay again
OPTION(mds_shutdown_check, OPT_INT)
OPTION(mds_thrash_exports, OPT_INT)
OPTION(mds_thrash_fragments, OPT_INT)
OPTION(mds_dump_cache_on_map, OPT_BOOL)
OPTION(mds_dump_cache_after_rejoin, OPT_BOOL)
OPTION(mds_verify_scatter, OPT_BOOL)
OPTION(mds_debug_scatterstat, OPT_BOOL)
OPTION(mds_debug_frag, OPT_BOOL)
OPTION(mds_debug_auth_pins, OPT_BOOL)
OPTION(mds_debug_subtrees, OPT_BOOL)
OPTION(mds_kill_mdstable_at, OPT_INT)
OPTION(mds_kill_export_at, OPT_INT)
OPTION(mds_kill_import_at, OPT_INT)
OPTION(mds_kill_link_at, OPT_INT)
OPTION(mds_kill_rename_at, OPT_INT)
OPTION(mds_kill_openc_at, OPT_INT)
OPTION(mds_kill_journal_expire_at, OPT_INT)
OPTION(mds_kill_journal_replay_at, OPT_INT)
OPTION(mds_journal_format, OPT_U32)  // Default to most recent JOURNAL_FORMAT_*
OPTION(mds_kill_create_at, OPT_INT)
OPTION(mds_inject_traceless_reply_probability, OPT_DOUBLE) /* percentage
				of MDS modify replies to skip sending the
				client a trace on [0-1]*/
OPTION(mds_wipe_sessions, OPT_BOOL)
OPTION(mds_wipe_ino_prealloc, OPT_BOOL)
OPTION(mds_skip_ino, OPT_INT)
OPTION(mds_enable_op_tracker, OPT_BOOL) // enable/disable MDS op tracking
OPTION(mds_op_history_size, OPT_U32)    // Max number of completed ops to track
OPTION(mds_op_history_duration, OPT_U32) // Oldest completed op to track
OPTION(mds_op_complaint_time, OPT_FLOAT) // how many seconds old makes an op complaint-worthy
OPTION(mds_op_log_threshold, OPT_INT) // how many op log messages to show in one go
OPTION(mds_snap_min_uid, OPT_U32) // The minimum UID required to create a snapshot
OPTION(mds_snap_max_uid, OPT_U32) // The maximum UID allowed to create a snapshot
OPTION(mds_snap_rstat, OPT_BOOL) // enable/disable nested stat for snapshot
OPTION(mds_verify_backtrace, OPT_U32)
// detect clients which aren't trimming completed requests
OPTION(mds_max_completed_flushes, OPT_U32)
OPTION(mds_max_completed_requests, OPT_U32)

OPTION(mds_action_on_write_error, OPT_U32) // 0: ignore; 1: force readonly; 2: crash
OPTION(mds_mon_shutdown_timeout, OPT_DOUBLE)

// Maximum number of concurrent stray files to purge
OPTION(mds_max_purge_files, OPT_U32)
// Maximum number of concurrent RADOS ops to issue in purging
OPTION(mds_max_purge_ops, OPT_U32)
// Maximum number of concurrent RADOS ops to issue in purging, scaled by PG count
OPTION(mds_max_purge_ops_per_pg, OPT_FLOAT)

OPTION(mds_purge_queue_busy_flush_period, OPT_FLOAT)

OPTION(mds_root_ino_uid, OPT_INT) // The UID of / on new filesystems
OPTION(mds_root_ino_gid, OPT_INT) // The GID of / on new filesystems

OPTION(mds_max_scrub_ops_in_progress, OPT_INT) // the number of simultaneous scrubs allowed

// Maximum number of damaged frags/dentries before whole MDS rank goes damaged
OPTION(mds_damage_table_max_entries, OPT_INT)

// Maximum increment for client writable range, counted by number of objects
OPTION(mds_client_writeable_range_max_inc_objs, OPT_U32)

// verify backend can support configured max object name length
OPTION(osd_check_max_object_name_len_on_startup, OPT_BOOL)

// Maximum number of backfills to or from a single osd
OPTION(osd_max_backfills, OPT_U64)

// Minimum recovery priority (255 = max, smaller = lower)
OPTION(osd_min_recovery_priority, OPT_INT)

// Seconds to wait before retrying refused backfills
OPTION(osd_backfill_retry_interval, OPT_DOUBLE)

// Seconds to wait before retrying refused recovery
OPTION(osd_recovery_retry_interval, OPT_DOUBLE)

// max agent flush ops
OPTION(osd_agent_max_ops, OPT_INT)
OPTION(osd_agent_max_low_ops, OPT_INT)
OPTION(osd_agent_min_evict_effort, OPT_FLOAT)
OPTION(osd_agent_quantize_effort, OPT_FLOAT)
OPTION(osd_agent_delay_time, OPT_FLOAT)

// osd ignore history.last_epoch_started in find_best_info
OPTION(osd_find_best_info_ignore_history_les, OPT_BOOL)

// decay atime and hist histograms after how many objects go by
OPTION(osd_agent_hist_halflife, OPT_INT)

// must be this amount over the threshold to enable,
// this amount below the threshold to disable.
OPTION(osd_agent_slop, OPT_FLOAT)

OPTION(osd_uuid, OPT_UUID)
OPTION(osd_data, OPT_STR)
OPTION(osd_journal, OPT_STR)
OPTION(osd_journal_size, OPT_INT)         // in mb
OPTION(osd_journal_flush_on_shutdown, OPT_BOOL) // Flush journal to data store on shutdown
// flags for specific control purpose during osd mount() process.
// e.g., can be 1 to skip over replaying journal
// or 2 to skip over mounting omap or 3 to skip over both.
// This might be helpful in case the journal is totally corrupted
// and we still want to bring the osd daemon back normally, etc.
OPTION(osd_os_flags, OPT_U32)
OPTION(osd_max_write_size, OPT_INT)
OPTION(osd_max_pgls, OPT_U64) // max number of pgls entries to return
OPTION(osd_client_message_size_cap, OPT_U64) // client data allowed in-memory (in bytes)
OPTION(osd_client_message_cap, OPT_U64)              // num client messages allowed in-memory
OPTION(osd_crush_update_weight_set, OPT_BOOL) // update weight set while updating weights
OPTION(osd_crush_chooseleaf_type, OPT_INT) // 1 = host
OPTION(osd_pool_use_gmt_hitset, OPT_BOOL) // try to use gmt for hitset archive names if all osds in cluster support it.
OPTION(osd_crush_update_on_start, OPT_BOOL)
OPTION(osd_class_update_on_start, OPT_BOOL) // automatically set device class on start
OPTION(osd_crush_initial_weight, OPT_DOUBLE) // if >=0, the initial weight is for newly added osds.
OPTION(osd_erasure_code_plugins, OPT_STR) // list of erasure code plugins

// Allows the "peered" state for recovery and backfill below min_size
OPTION(osd_allow_recovery_below_min_size, OPT_BOOL)

OPTION(osd_pool_default_ec_fast_read, OPT_BOOL) // whether turn on fast read on the pool or not
OPTION(osd_pool_default_flags, OPT_INT)   // default flags for new pools
OPTION(osd_pool_default_flag_hashpspool, OPT_BOOL)   // use new pg hashing to prevent pool/pg overlap
OPTION(osd_pool_default_flag_nodelete, OPT_BOOL) // pool can't be deleted
OPTION(osd_pool_default_flag_nopgchange, OPT_BOOL) // pool's pg and pgp num can't be changed
OPTION(osd_pool_default_flag_nosizechange, OPT_BOOL) // pool's size and min size can't be changed
OPTION(osd_pool_default_hit_set_bloom_fpp, OPT_FLOAT)
OPTION(osd_pool_default_cache_target_dirty_ratio, OPT_FLOAT)
OPTION(osd_pool_default_cache_target_dirty_high_ratio, OPT_FLOAT)
OPTION(osd_pool_default_cache_target_full_ratio, OPT_FLOAT)
OPTION(osd_pool_default_cache_min_flush_age, OPT_INT)  // seconds
OPTION(osd_pool_default_cache_min_evict_age, OPT_INT)  // seconds
OPTION(osd_pool_default_cache_max_evict_check_size, OPT_INT)  // max size to check for eviction
OPTION(osd_pool_default_read_lease_ratio, OPT_FLOAT)
OPTION(osd_hit_set_min_size, OPT_INT)  // min target size for a HitSet
OPTION(osd_hit_set_max_size, OPT_INT)  // max target size for a HitSet
OPTION(osd_hit_set_namespace, OPT_STR) // rados namespace for hit_set tracking

// conservative default throttling values
OPTION(osd_tier_promote_max_objects_sec, OPT_U64)
OPTION(osd_tier_promote_max_bytes_sec, OPT_U64)

OPTION(osd_objecter_finishers, OPT_INT)

OPTION(osd_map_dedup, OPT_BOOL)
OPTION(osd_map_cache_size, OPT_INT)
OPTION(osd_map_message_max, OPT_INT)  // max maps per MOSDMap message
OPTION(osd_map_message_max_bytes, OPT_SIZE)  // max maps per MOSDMap message
OPTION(osd_map_share_max_epochs, OPT_INT)  // cap on # of inc maps we send to peers, clients
OPTION(osd_inject_bad_map_crc_probability, OPT_FLOAT)
OPTION(osd_inject_failure_on_pg_removal, OPT_BOOL)
// shutdown the OSD if stuatus flipping more than max_markdown_count times in recent max_markdown_period seconds
OPTION(osd_max_markdown_period , OPT_INT)
OPTION(osd_max_markdown_count, OPT_INT)

OPTION(osd_op_pq_max_tokens_per_priority, OPT_U64)
OPTION(osd_op_pq_min_cost, OPT_U64)
OPTION(osd_recover_clone_overlap, OPT_BOOL)   // preserve clone_overlap during recovery/migration
OPTION(osd_op_num_threads_per_shard, OPT_INT)
OPTION(osd_op_num_threads_per_shard_hdd, OPT_INT)
OPTION(osd_op_num_threads_per_shard_ssd, OPT_INT)
OPTION(osd_op_num_shards, OPT_INT)
OPTION(osd_op_num_shards_hdd, OPT_INT)
OPTION(osd_op_num_shards_ssd, OPT_INT)

// PrioritzedQueue (prio), Weighted Priority Queue (wpq ; default),
// mclock_opclass, mclock_client, or debug_random. "mclock_opclass"
// and "mclock_client" are based on the mClock/dmClock algorithm
// (Gulati, et al. 2010). "mclock_opclass" prioritizes based on the
// class the operation belongs to. "mclock_client" does the same but
// also works to ienforce fairness between clients. "debug_random"
// chooses among all four with equal probability.
OPTION(osd_op_queue, OPT_STR)

OPTION(osd_op_queue_cut_off, OPT_STR) // Min priority to go to strict queue. (low, high)

OPTION(osd_ignore_stale_divergent_priors, OPT_BOOL) // do not assert on divergent_prior entries which aren't in the log and whose on-disk objects are newer

// Set to true for testing.  Users should NOT set this.
// If set to true even after reading enough shards to
// decode the object, any error will be reported.
OPTION(osd_read_ec_check_for_errors, OPT_BOOL) // return error if any ec shard has an error

OPTION(osd_debug_feed_pullee, OPT_INT)

OPTION(osd_backfill_scan_min, OPT_INT)
OPTION(osd_backfill_scan_max, OPT_INT)
OPTION(osd_op_thread_timeout, OPT_INT)
OPTION(osd_op_thread_suicide_timeout, OPT_INT)
OPTION(osd_recovery_sleep, OPT_FLOAT)         // seconds to sleep between recovery ops
OPTION(osd_recovery_sleep_hdd, OPT_FLOAT)
OPTION(osd_recovery_sleep_ssd, OPT_FLOAT)
OPTION(osd_snap_trim_sleep, OPT_DOUBLE)
OPTION(osd_scrub_invalid_stats, OPT_BOOL)
OPTION(osd_command_thread_timeout, OPT_INT)
OPTION(osd_command_thread_suicide_timeout, OPT_INT)
OPTION(osd_heartbeat_interval, OPT_INT)       // (seconds) how often we ping peers

// (seconds) how long before we decide a peer has failed
// This setting is read by the MONs and OSDs and has to be set to a equal value in both settings of the configuration
OPTION(osd_heartbeat_grace, OPT_INT)
OPTION(osd_heartbeat_min_peers, OPT_INT)     // minimum number of peers
OPTION(osd_heartbeat_use_min_delay_socket, OPT_BOOL) // prio the heartbeat tcp socket and set dscp as CS6 on it if true
OPTION(osd_heartbeat_min_size, OPT_INT) // the minimum size of OSD heartbeat messages to send

// max number of parallel snap trims/pg
OPTION(osd_pg_max_concurrent_snap_trims, OPT_U64)
// max number of trimming pgs
OPTION(osd_max_trimming_pgs, OPT_U64)

// minimum number of peers that must be reachable to mark ourselves
// back up after being wrongly marked down.
OPTION(osd_heartbeat_min_healthy_ratio, OPT_FLOAT)

OPTION(osd_mon_heartbeat_interval, OPT_INT)  // (seconds) how often to ping monitor if no peers
OPTION(osd_mon_report_interval, OPT_INT)  // failures, up_thru, boot.
OPTION(osd_mon_report_max_in_flight, OPT_INT)  // max updates in flight
OPTION(osd_beacon_report_interval, OPT_INT)       // (second) how often to send beacon message to monitor
OPTION(osd_pg_stat_report_interval_max, OPT_INT)  // report pg stats for any given pg at least this often
OPTION(osd_mon_ack_timeout, OPT_DOUBLE) // time out a mon if it doesn't ack stats
OPTION(osd_stats_ack_timeout_factor, OPT_DOUBLE) // multiples of mon_ack_timeout
OPTION(osd_stats_ack_timeout_decay, OPT_DOUBLE)
OPTION(osd_default_data_pool_replay_window, OPT_INT)
OPTION(osd_auto_mark_unfound_lost, OPT_BOOL)
OPTION(osd_recovery_delay_start, OPT_FLOAT)
OPTION(osd_recovery_max_active, OPT_U64)
OPTION(osd_recovery_max_active_hdd, OPT_U64)
OPTION(osd_recovery_max_active_ssd, OPT_U64)
OPTION(osd_recovery_max_single_start, OPT_U64)
OPTION(osd_recovery_max_chunk, OPT_U64)  // max size of push chunk
OPTION(osd_recovery_max_omap_entries_per_chunk, OPT_U64) // max number of omap entries per chunk; 0 to disable limit
OPTION(osd_copyfrom_max_chunk, OPT_U64)   // max size of a COPYFROM chunk
OPTION(osd_push_per_object_cost, OPT_U64)  // push cost per object
OPTION(osd_max_push_cost, OPT_U64)  // max size of push message
OPTION(osd_max_push_objects, OPT_U64)  // max objects in single push op
OPTION(osd_max_scrubs, OPT_INT)
OPTION(osd_scrub_during_recovery, OPT_BOOL) // Allow new scrubs to start while recovery is active on the OSD
OPTION(osd_repair_during_recovery, OPT_BOOL) // Allow new requested repairs to start while recovery is active on the OSD
OPTION(osd_scrub_begin_hour, OPT_INT)
OPTION(osd_scrub_end_hour, OPT_INT)
OPTION(osd_scrub_begin_week_day, OPT_INT)
OPTION(osd_scrub_end_week_day, OPT_INT)
OPTION(osd_scrub_load_threshold, OPT_FLOAT)
OPTION(osd_scrub_min_interval, OPT_FLOAT)    // if load is low
OPTION(osd_scrub_max_interval, OPT_FLOAT)  // regardless of load
OPTION(osd_scrub_interval_randomize_ratio, OPT_FLOAT) // randomize the scheduled scrub in the span of [min,min*(1+randomize_ratio))
OPTION(osd_scrub_backoff_ratio, OPT_DOUBLE)   // the probability to back off the scheduled scrub
OPTION(osd_scrub_chunk_min, OPT_INT)
OPTION(osd_scrub_chunk_max, OPT_INT)
OPTION(osd_scrub_sleep, OPT_FLOAT)   // sleep between [deep]scrub ops
OPTION(osd_scrub_extended_sleep, OPT_FLOAT)   // more sleep between [deep]scrub ops
OPTION(osd_scrub_auto_repair, OPT_BOOL)   // whether auto-repair inconsistencies upon deep-scrubbing
OPTION(osd_scrub_auto_repair_num_errors, OPT_U32)   // only auto-repair when number of errors is below this threshold
OPTION(osd_deep_scrub_interval, OPT_FLOAT) // once a week
OPTION(osd_deep_scrub_randomize_ratio, OPT_FLOAT) // scrubs will randomly become deep scrubs at this rate (0.15 -> 15% of scrubs are deep)
OPTION(osd_deep_scrub_stride, OPT_INT)
OPTION(osd_deep_scrub_keys, OPT_INT)
OPTION(osd_deep_scrub_update_digest_min_age, OPT_INT)   // objects must be this old (seconds) before we update the whole-object digest on scrub
OPTION(osd_skip_data_digest, OPT_BOOL)
OPTION(osd_deep_scrub_large_omap_object_key_threshold, OPT_U64)
OPTION(osd_deep_scrub_large_omap_object_value_sum_threshold, OPT_U64)
OPTION(osd_class_dir, OPT_STR) // where rados plugins are stored
OPTION(osd_open_classes_on_start, OPT_BOOL)
OPTION(osd_class_load_list, OPT_STR) // list of object classes allowed to be loaded (allow all: *)
OPTION(osd_class_default_list, OPT_STR) // list of object classes with default execute perm (allow all: *)
OPTION(osd_check_for_log_corruption, OPT_BOOL)
OPTION(osd_use_stale_snap, OPT_BOOL)
OPTION(osd_rollback_to_cluster_snap, OPT_STR)
OPTION(osd_default_notify_timeout, OPT_U32) // default notify timeout in seconds
OPTION(osd_kill_backfill_at, OPT_INT)

// Bounds how infrequently a new map epoch will be persisted for a pg
OPTION(osd_pg_epoch_persisted_max_stale, OPT_U32) // make this < map_cache_size!

OPTION(osd_target_pg_log_entries_per_osd, OPT_U32)
OPTION(osd_min_pg_log_entries, OPT_U32)  // number of entries to keep in the pg log when trimming it
OPTION(osd_max_pg_log_entries, OPT_U32) // max entries, say when degraded, before we trim
OPTION(osd_pg_log_dups_tracked, OPT_U32) // how many versions back to track combined in both pglog's regular + dup logs
OPTION(osd_object_clean_region_max_num_intervals, OPT_INT) // number of intervals in clean_offsets
OPTION(osd_force_recovery_pg_log_entries_factor, OPT_FLOAT) // max entries factor before force recovery
OPTION(osd_pg_log_trim_min, OPT_U32)
OPTION(osd_pg_log_trim_max, OPT_U32)
OPTION(osd_op_complaint_time, OPT_FLOAT) // how many seconds old makes an op complaint-worthy
OPTION(osd_command_max_records, OPT_INT)
OPTION(osd_max_pg_blocked_by, OPT_U32)    // max peer osds to report that are blocking our progress
OPTION(osd_op_log_threshold, OPT_INT) // how many op log messages to show in one go
OPTION(osd_backoff_on_unfound, OPT_BOOL)   // object unfound
OPTION(osd_backoff_on_degraded, OPT_BOOL) // [mainly for debug?] object unreadable/writeable
OPTION(osd_backoff_on_peering, OPT_BOOL)  // [debug] pg peering
OPTION(osd_debug_crash_on_ignored_backoff, OPT_BOOL) // crash osd if client ignores a backoff; useful for debugging
OPTION(osd_debug_inject_dispatch_delay_probability, OPT_DOUBLE)
OPTION(osd_debug_inject_dispatch_delay_duration, OPT_DOUBLE)
OPTION(osd_debug_drop_ping_probability, OPT_DOUBLE)
OPTION(osd_debug_drop_ping_duration, OPT_INT)
OPTION(osd_debug_op_order, OPT_BOOL)
OPTION(osd_debug_verify_missing_on_start, OPT_BOOL)
OPTION(osd_debug_verify_snaps, OPT_BOOL)
OPTION(osd_debug_verify_stray_on_activate, OPT_BOOL)
OPTION(osd_debug_skip_full_check_in_backfill_reservation, OPT_BOOL)
OPTION(osd_debug_reject_backfill_probability, OPT_DOUBLE)
OPTION(osd_debug_inject_copyfrom_error, OPT_BOOL)  // inject failure during copyfrom completion
OPTION(osd_debug_misdirected_ops, OPT_BOOL)
OPTION(osd_debug_skip_full_check_in_recovery, OPT_BOOL)
OPTION(osd_debug_random_push_read_error, OPT_DOUBLE)
OPTION(osd_debug_verify_cached_snaps, OPT_BOOL)
OPTION(osd_debug_deep_scrub_sleep, OPT_FLOAT)
OPTION(osd_debug_no_acting_change, OPT_BOOL)
OPTION(osd_debug_pretend_recovery_active, OPT_BOOL)
OPTION(osd_enable_op_tracker, OPT_BOOL) // enable/disable OSD op tracking
OPTION(osd_num_op_tracker_shard, OPT_U32) // The number of shards for holding the ops
OPTION(osd_op_history_size, OPT_U32)    // Max number of completed ops to track
OPTION(osd_op_history_duration, OPT_U32) // Oldest completed op to track
OPTION(osd_op_history_slow_op_size, OPT_U32)           // Max number of slow ops to track
OPTION(osd_op_history_slow_op_threshold, OPT_DOUBLE) // track the op if over this threshold
OPTION(osd_target_transaction_size, OPT_INT)     // to adjust various transactions that batch smaller items
OPTION(osd_failsafe_full_ratio, OPT_FLOAT) // what % full makes an OSD "full" (failsafe)
OPTION(osd_fast_shutdown, OPT_BOOL)
OPTION(osd_fast_fail_on_connection_refused, OPT_BOOL) // immediately mark OSDs as down once they refuse to accept connections

OPTION(osd_pg_object_context_cache_count, OPT_INT)
OPTION(osd_tracing, OPT_BOOL) // true if LTTng-UST tracepoints should be enabled
OPTION(osd_function_tracing, OPT_BOOL) // true if function instrumentation should use LTTng

OPTION(osd_fast_info, OPT_BOOL) // use fast info attr, if we can

// determines whether PGLog::check() compares written out log to stored log
OPTION(osd_debug_pg_log_writeout, OPT_BOOL)
OPTION(osd_loop_before_reset_tphandle, OPT_U32) // Max number of loop before we reset thread-pool's handle
OPTION(osd_max_snap_prune_intervals_per_epoch, OPT_U64) // Max number of snap intervals to report to mgr in pg_stat_t

// default timeout while caling WaitInterval on an empty queue
OPTION(threadpool_default_timeout, OPT_INT)
// default wait time for an empty queue before pinging the hb timeout
OPTION(threadpool_empty_queue_max_wait, OPT_INT)

OPTION(leveldb_log_to_ceph_log, OPT_BOOL)
OPTION(leveldb_write_buffer_size, OPT_U64) // leveldb write buffer size
OPTION(leveldb_cache_size, OPT_U64) // leveldb cache size
OPTION(leveldb_block_size, OPT_U64) // leveldb block size
OPTION(leveldb_bloom_size, OPT_INT) // leveldb bloom bits per entry
OPTION(leveldb_max_open_files, OPT_INT) // leveldb max open files
OPTION(leveldb_compression, OPT_BOOL) // leveldb uses compression
OPTION(leveldb_paranoid, OPT_BOOL) // leveldb paranoid flag
OPTION(leveldb_log, OPT_STR)  // enable leveldb log file
OPTION(leveldb_compact_on_mount, OPT_BOOL)

OPTION(rocksdb_log_to_ceph_log, OPT_BOOL)  // log to ceph log
OPTION(rocksdb_cache_size, OPT_U64)  // rocksdb cache size (unless set by bluestore/etc)
OPTION(rocksdb_cache_row_ratio, OPT_FLOAT)   // ratio of cache for row (vs block)
OPTION(rocksdb_cache_shard_bits, OPT_INT)  // rocksdb block cache shard bits, 4 bit -> 16 shards
OPTION(rocksdb_cache_type, OPT_STR) // 'lru' or 'clock'
OPTION(rocksdb_block_size, OPT_INT)  // default rocksdb block size
OPTION(rocksdb_perf, OPT_BOOL) // Enabling this will have 5-10% impact on performance for the stats collection
OPTION(rocksdb_collect_compaction_stats, OPT_BOOL) //For rocksdb, this behavior will be an overhead of 5%~10%, collected only rocksdb_perf is enabled.
OPTION(rocksdb_collect_extended_stats, OPT_BOOL) //For rocksdb, this behavior will be an overhead of 5%~10%, collected only rocksdb_perf is enabled.
OPTION(rocksdb_collect_memory_stats, OPT_BOOL) //For rocksdb, this behavior will be an overhead of 5%~10%, collected only rocksdb_perf is enabled.

// rocksdb options that will be used for omap(if omap_backend is rocksdb)
OPTION(filestore_rocksdb_options, OPT_STR)
// rocksdb options that will be used in monstore
OPTION(mon_rocksdb_options, OPT_STR)

/**
 * osd_*_priority adjust the relative priority of client io, recovery io,
 * snaptrim io, etc
 *
 * osd_*_priority determines the ratio of available io between client and
 * recovery.  Each option may be set between
 * 1..63.
 */
OPTION(osd_client_op_priority, OPT_U32)
OPTION(osd_recovery_op_priority, OPT_U32)
OPTION(osd_peering_op_priority, OPT_U32)

OPTION(osd_snap_trim_priority, OPT_U32)
OPTION(osd_snap_trim_cost, OPT_U32) // set default cost equal to 1MB io

OPTION(osd_scrub_priority, OPT_U32)
// set default cost equal to 50MB io
OPTION(osd_scrub_cost, OPT_U32)
// set requested scrub priority higher than scrub priority to make the
// requested scrubs jump the queue of scheduled scrubs
OPTION(osd_requested_scrub_priority, OPT_U32)

OPTION(osd_pg_delete_priority, OPT_U32)
OPTION(osd_pg_delete_cost, OPT_U32) // set default cost equal to 1MB io

OPTION(osd_recovery_priority, OPT_U32)
// set default cost equal to 20MB io
OPTION(osd_recovery_cost, OPT_U32)

/**
 * osd_recovery_op_warn_multiple scales the normal warning threshold,
 * osd_op_complaint_time, so that slow recovery ops won't cause noise
 */
OPTION(osd_recovery_op_warn_multiple, OPT_U32)

// Max time to wait between notifying mon of shutdown and shutting down
OPTION(osd_mon_shutdown_timeout, OPT_DOUBLE)
OPTION(osd_shutdown_pgref_assert, OPT_BOOL) // crash if the OSD has stray PG refs on shutdown

OPTION(osd_max_object_size, OPT_U64) // OSD's maximum object size
OPTION(osd_max_object_name_len, OPT_U32) // max rados object name len
OPTION(osd_max_object_namespace_len, OPT_U32) // max rados object namespace len
OPTION(osd_max_attr_name_len, OPT_U32)    // max rados attr name len; cannot go higher than 100 chars for file system backends
OPTION(osd_max_attr_size, OPT_U64)

OPTION(osd_max_omap_entries_per_request, OPT_U64)
OPTION(osd_max_omap_bytes_per_request, OPT_U64)
OPTION(osd_max_write_op_reply_len, OPT_U64)

OPTION(osd_objectstore, OPT_STR)  // ObjectStore backend type
OPTION(osd_objectstore_tracing, OPT_BOOL) // true if LTTng-UST tracepoints should be enabled
OPTION(osd_objectstore_fuse, OPT_BOOL)

OPTION(osd_bench_small_size_max_iops, OPT_U32) // 100 IOPS
OPTION(osd_bench_large_size_max_throughput, OPT_U64) // 100 MB/s
OPTION(osd_bench_max_block_size, OPT_U64) // cap the block size at 64MB
OPTION(osd_bench_duration, OPT_U32) // duration of 'osd bench', capped at 30s to avoid triggering timeouts

OPTION(osd_blkin_trace_all, OPT_BOOL) // create a blkin trace for all osd requests
OPTION(osdc_blkin_trace_all, OPT_BOOL) // create a blkin trace for all objecter requests

OPTION(osd_discard_disconnected_ops, OPT_BOOL)

OPTION(memstore_device_bytes, OPT_U64)
OPTION(memstore_page_set, OPT_BOOL)
OPTION(memstore_page_size, OPT_U64)
OPTION(memstore_debug_omit_block_device_write, OPT_BOOL)

OPTION(bdev_debug_inflight_ios, OPT_BOOL)
OPTION(bdev_inject_crash, OPT_INT)  // if N>0, then ~ 1/N IOs will complete before we crash on flush.
OPTION(bdev_inject_crash_flush_delay, OPT_INT) // wait N more seconds on flush
OPTION(bdev_aio, OPT_BOOL)
OPTION(bdev_aio_poll_ms, OPT_INT)  // milliseconds
OPTION(bdev_aio_max_queue_depth, OPT_INT)
OPTION(bdev_aio_reap_max, OPT_INT)
OPTION(bdev_block_size, OPT_INT)
OPTION(bdev_debug_aio, OPT_BOOL)
OPTION(bdev_debug_aio_suicide_timeout, OPT_FLOAT)
OPTION(bdev_debug_aio_log_age, OPT_DOUBLE)

// if yes, osd will unbind all NVMe devices from kernel driver and bind them
// to the uio_pci_generic driver. The purpose is to prevent the case where
// NVMe driver is loaded while osd is running.
OPTION(bdev_nvme_unbind_from_kernel, OPT_BOOL)
OPTION(bdev_nvme_retry_count, OPT_INT) // -1 means by default which is 4
OPTION(bdev_enable_discard, OPT_BOOL)
OPTION(bdev_async_discard, OPT_BOOL)

OPTION(objectstore_blackhole, OPT_BOOL)

OPTION(bluefs_alloc_size, OPT_U64)
OPTION(bluefs_shared_alloc_size, OPT_U64)
OPTION(bluefs_max_prefetch, OPT_U64)
OPTION(bluefs_min_log_runway, OPT_U64)  // alloc when we get this low
OPTION(bluefs_max_log_runway, OPT_U64)  // alloc this much at a time
OPTION(bluefs_log_compact_min_ratio, OPT_FLOAT)      // before we consider
OPTION(bluefs_log_compact_min_size, OPT_U64)  // before we consider
OPTION(bluefs_min_flush_size, OPT_U64)  // ignore flush until its this big
OPTION(bluefs_compact_log_sync, OPT_BOOL)  // sync or async log compaction?
OPTION(bluefs_buffered_io, OPT_BOOL)
OPTION(bluefs_sync_write, OPT_BOOL)
OPTION(bluefs_allocator, OPT_STR)     // stupid | bitmap
OPTION(bluefs_log_replay_check_allocations, OPT_BOOL)

OPTION(bluestore_bluefs, OPT_BOOL)
OPTION(bluestore_bluefs_env_mirror, OPT_BOOL) // mirror to normal Env for debug
OPTION(bluestore_bluefs_min, OPT_U64) // 1gb
OPTION(bluestore_bluefs_min_ratio, OPT_FLOAT)  // min fs free / total free
OPTION(bluestore_bluefs_max_ratio, OPT_FLOAT)  // max fs free / total free
OPTION(bluestore_bluefs_gift_ratio, OPT_FLOAT) // how much to add at a time
OPTION(bluestore_bluefs_reclaim_ratio, OPT_FLOAT) // how much to reclaim at a time
OPTION(bluestore_bluefs_balance_interval, OPT_FLOAT) // how often (sec) to balance free space between bluefs and bluestore
// how often (sec) to dump allocator on allocation failure
OPTION(bluestore_bluefs_alloc_failure_dump_interval, OPT_FLOAT)

// Enforces db sync with legacy bluefs extents information on close.
// Enables downgrades to pre-nautilus releases
OPTION(bluestore_bluefs_db_compatibility, OPT_BOOL)

// If you want to use spdk driver, you need to specify NVMe serial number here
// with "spdk:" prefix.
// Users can use 'lspci -vvv -d 8086:0953 | grep "Device Serial Number"' to
// get the serial number of Intel(R) Fultondale NVMe controllers.
// Example:
// bluestore_block_path = spdk:55cd2e404bd73932
OPTION(bluestore_block_path, OPT_STR)
OPTION(bluestore_block_size, OPT_U64)  // 10gb for testing
OPTION(bluestore_block_create, OPT_BOOL)
OPTION(bluestore_block_db_path, OPT_STR)
OPTION(bluestore_block_db_size, OPT_U64)   // rocksdb ssts (hot/warm)
OPTION(bluestore_block_db_create, OPT_BOOL)
OPTION(bluestore_block_wal_path, OPT_STR)
OPTION(bluestore_block_wal_size, OPT_U64) // rocksdb wal
OPTION(bluestore_block_wal_create, OPT_BOOL)
OPTION(bluestore_block_preallocate_file, OPT_BOOL) //whether preallocate space if block/db_path/wal_path is file rather that block device.
OPTION(bluestore_ignore_data_csum, OPT_BOOL)
OPTION(bluestore_csum_type, OPT_STR) // none|xxhash32|xxhash64|crc32c|crc32c_16|crc32c_8
OPTION(bluestore_retry_disk_reads, OPT_U64)
OPTION(bluestore_min_alloc_size, OPT_U32)
OPTION(bluestore_min_alloc_size_hdd, OPT_U32)
OPTION(bluestore_min_alloc_size_ssd, OPT_U32)
OPTION(bluestore_max_alloc_size, OPT_U32)
OPTION(bluestore_prefer_deferred_size, OPT_U32)
OPTION(bluestore_prefer_deferred_size_hdd, OPT_U32)
OPTION(bluestore_prefer_deferred_size_ssd, OPT_U32)
OPTION(bluestore_compression_mode, OPT_STR)  // force|aggressive|passive|none
OPTION(bluestore_compression_algorithm, OPT_STR)
OPTION(bluestore_compression_min_blob_size, OPT_U32)
OPTION(bluestore_compression_min_blob_size_hdd, OPT_U32)
OPTION(bluestore_compression_min_blob_size_ssd, OPT_U32)
OPTION(bluestore_compression_max_blob_size, OPT_U32)
OPTION(bluestore_compression_max_blob_size_hdd, OPT_U32)
OPTION(bluestore_compression_max_blob_size_ssd, OPT_U32)
/*
 * Specifies minimum expected amount of saved allocation units
 * per single blob to enable compressed blobs garbage collection
 *
 */
OPTION(bluestore_gc_enable_blob_threshold, OPT_INT)
/*
 * Specifies minimum expected amount of saved allocation units
 * per all blobsb to enable compressed blobs garbage collection
 *
 */
OPTION(bluestore_gc_enable_total_threshold, OPT_INT)

OPTION(bluestore_max_blob_size, OPT_U32)
OPTION(bluestore_max_blob_size_hdd, OPT_U32)
OPTION(bluestore_max_blob_size_ssd, OPT_U32)
/*
 * Require the net gain of compression at least to be at this ratio,
 * otherwise we don't compress.
 * And ask for compressing at least 12.5%(1/8) off, by default.
 */
OPTION(bluestore_compression_required_ratio, OPT_DOUBLE)
OPTION(bluestore_extent_map_shard_max_size, OPT_U32)
OPTION(bluestore_extent_map_shard_target_size, OPT_U32)
OPTION(bluestore_extent_map_shard_min_size, OPT_U32)
OPTION(bluestore_extent_map_shard_target_size_slop, OPT_DOUBLE)
OPTION(bluestore_extent_map_inline_shard_prealloc_size, OPT_U32)
OPTION(bluestore_cache_trim_interval, OPT_DOUBLE)
OPTION(bluestore_cache_trim_max_skip_pinned, OPT_U32) // skip this many onodes pinned in cache before we give up
OPTION(bluestore_cache_type, OPT_STR)   // lru, 2q
OPTION(bluestore_2q_cache_kin_ratio, OPT_DOUBLE)    // kin page slot size / max page slot size
OPTION(bluestore_2q_cache_kout_ratio, OPT_DOUBLE)   // number of kout page slot / total number of page slot
OPTION(bluestore_cache_size, OPT_U64)
OPTION(bluestore_cache_size_hdd, OPT_U64)
OPTION(bluestore_cache_size_ssd, OPT_U64)
OPTION(bluestore_cache_meta_ratio, OPT_DOUBLE)
OPTION(bluestore_cache_kv_ratio, OPT_DOUBLE)
OPTION(bluestore_alloc_stats_dump_interval, OPT_DOUBLE)
OPTION(bluestore_kvbackend, OPT_STR)
OPTION(bluestore_allocator, OPT_STR)     // stupid | bitmap
OPTION(bluestore_freelist_blocks_per_key, OPT_INT)
OPTION(bluestore_bitmapallocator_blocks_per_zone, OPT_INT) // must be power of 2 aligned, e.g., 512, 1024, 2048...
OPTION(bluestore_bitmapallocator_span_size, OPT_INT) // must be power of 2 aligned, e.g., 512, 1024, 2048...
OPTION(bluestore_max_deferred_txc, OPT_U64)
OPTION(bluestore_max_defer_interval, OPT_U64)
OPTION(bluestore_rocksdb_options, OPT_STR)
OPTION(bluestore_fsck_on_mount, OPT_BOOL)
OPTION(bluestore_fsck_on_mount_deep, OPT_BOOL)
OPTION(bluestore_fsck_quick_fix_on_mount, OPT_BOOL)
OPTION(bluestore_fsck_on_umount, OPT_BOOL)
OPTION(bluestore_fsck_on_umount_deep, OPT_BOOL)
OPTION(bluestore_fsck_on_mkfs, OPT_BOOL)
OPTION(bluestore_fsck_on_mkfs_deep, OPT_BOOL)
OPTION(bluestore_sync_submit_transaction, OPT_BOOL) // submit kv txn in queueing thread (not kv_sync_thread)
OPTION(bluestore_fsck_read_bytes_cap, OPT_U64)
OPTION(bluestore_fsck_quick_fix_threads, OPT_INT)
OPTION(bluestore_throttle_bytes, OPT_U64)
OPTION(bluestore_throttle_deferred_bytes, OPT_U64)
OPTION(bluestore_throttle_cost_per_io_hdd, OPT_U64)
OPTION(bluestore_throttle_cost_per_io_ssd, OPT_U64)
OPTION(bluestore_throttle_cost_per_io, OPT_U64)
OPTION(bluestore_deferred_batch_ops, OPT_U64)
OPTION(bluestore_deferred_batch_ops_hdd, OPT_U64)
OPTION(bluestore_deferred_batch_ops_ssd, OPT_U64)
OPTION(bluestore_nid_prealloc, OPT_INT)
OPTION(bluestore_blobid_prealloc, OPT_U64)
OPTION(bluestore_clone_cow, OPT_BOOL)  // do copy-on-write for clones
OPTION(bluestore_default_buffered_read, OPT_BOOL)
OPTION(bluestore_default_buffered_write, OPT_BOOL)
OPTION(bluestore_debug_misc, OPT_BOOL)
OPTION(bluestore_debug_no_reuse_blocks, OPT_BOOL)
OPTION(bluestore_debug_small_allocations, OPT_INT)
OPTION(bluestore_debug_too_many_blobs_threshold, OPT_INT)
OPTION(bluestore_debug_freelist, OPT_BOOL)
OPTION(bluestore_debug_prefill, OPT_FLOAT)
OPTION(bluestore_debug_prefragment_max, OPT_INT)
OPTION(bluestore_debug_inject_read_err, OPT_BOOL)
OPTION(bluestore_debug_randomize_serial_transaction, OPT_INT)
OPTION(bluestore_debug_omit_block_device_write, OPT_BOOL)
OPTION(bluestore_debug_fsck_abort, OPT_BOOL)
OPTION(bluestore_debug_omit_kv_commit, OPT_BOOL)
OPTION(bluestore_debug_permit_any_bdev_label, OPT_BOOL)
OPTION(bluestore_debug_random_read_err, OPT_DOUBLE)
OPTION(bluestore_debug_inject_bug21040, OPT_BOOL)
OPTION(bluestore_debug_inject_csum_err_probability, OPT_FLOAT)
OPTION(bluestore_fsck_error_on_no_per_pool_stats, OPT_BOOL)
OPTION(bluestore_warn_on_bluefs_spillover, OPT_BOOL)
OPTION(bluestore_warn_on_legacy_statfs, OPT_BOOL)
OPTION(bluestore_warn_on_spurious_read_errors, OPT_BOOL)
OPTION(bluestore_fsck_error_on_no_per_pool_omap, OPT_BOOL)
OPTION(bluestore_warn_on_no_per_pool_omap, OPT_BOOL)
OPTION(bluestore_log_op_age, OPT_DOUBLE)
OPTION(bluestore_log_omap_iterator_age, OPT_DOUBLE)
OPTION(bluestore_log_collection_list_age, OPT_DOUBLE)
OPTION(bluestore_debug_enforce_settings, OPT_STR)
OPTION(bluestore_volume_selection_policy, OPT_STR)
OPTION(bluestore_volume_selection_reserved_factor, OPT_DOUBLE)
OPTION(bluestore_volume_selection_reserved, OPT_INT)

OPTION(kstore_max_ops, OPT_U64)
OPTION(kstore_max_bytes, OPT_U64)
OPTION(kstore_backend, OPT_STR)
OPTION(kstore_rocksdb_options, OPT_STR)
OPTION(kstore_fsck_on_mount, OPT_BOOL)
OPTION(kstore_fsck_on_mount_deep, OPT_BOOL)
OPTION(kstore_nid_prealloc, OPT_U64)
OPTION(kstore_sync_transaction, OPT_BOOL)
OPTION(kstore_sync_submit_transaction, OPT_BOOL)
OPTION(kstore_onode_map_size, OPT_U64)
OPTION(kstore_default_stripe_size, OPT_INT)

OPTION(filestore_omap_backend, OPT_STR)
OPTION(filestore_omap_backend_path, OPT_STR)

/// filestore wb throttle limits
OPTION(filestore_wbthrottle_enable, OPT_BOOL)
OPTION(filestore_wbthrottle_btrfs_bytes_start_flusher, OPT_U64)
OPTION(filestore_wbthrottle_btrfs_bytes_hard_limit, OPT_U64)
OPTION(filestore_wbthrottle_btrfs_ios_start_flusher, OPT_U64)
OPTION(filestore_wbthrottle_btrfs_ios_hard_limit, OPT_U64)
OPTION(filestore_wbthrottle_btrfs_inodes_start_flusher, OPT_U64)
OPTION(filestore_wbthrottle_xfs_bytes_start_flusher, OPT_U64)
OPTION(filestore_wbthrottle_xfs_bytes_hard_limit, OPT_U64)
OPTION(filestore_wbthrottle_xfs_ios_start_flusher, OPT_U64)
OPTION(filestore_wbthrottle_xfs_ios_hard_limit, OPT_U64)
OPTION(filestore_wbthrottle_xfs_inodes_start_flusher, OPT_U64)

/// These must be less than the fd limit
OPTION(filestore_wbthrottle_btrfs_inodes_hard_limit, OPT_U64)
OPTION(filestore_wbthrottle_xfs_inodes_hard_limit, OPT_U64)

//Introduce a O_DSYNC write in the filestore
OPTION(filestore_odsync_write, OPT_BOOL)

// Tests index failure paths
OPTION(filestore_index_retry_probability, OPT_DOUBLE)

// Allow object read error injection
OPTION(filestore_debug_inject_read_err, OPT_BOOL)
OPTION(filestore_debug_random_read_err, OPT_DOUBLE)

OPTION(filestore_debug_omap_check, OPT_BOOL) // Expensive debugging check on sync
OPTION(filestore_omap_header_cache_size, OPT_INT)

// Use omap for xattrs for attrs over
// filestore_max_inline_xattr_size or
OPTION(filestore_max_inline_xattr_size, OPT_U32)	//Override
OPTION(filestore_max_inline_xattr_size_xfs, OPT_U32)
OPTION(filestore_max_inline_xattr_size_btrfs, OPT_U32)
OPTION(filestore_max_inline_xattr_size_other, OPT_U32)

// for more than filestore_max_inline_xattrs attrs
OPTION(filestore_max_inline_xattrs, OPT_U32)	//Override
OPTION(filestore_max_inline_xattrs_xfs, OPT_U32)
OPTION(filestore_max_inline_xattrs_btrfs, OPT_U32)
OPTION(filestore_max_inline_xattrs_other, OPT_U32)

// max xattr value size
OPTION(filestore_max_xattr_value_size, OPT_U32)	//Override
OPTION(filestore_max_xattr_value_size_xfs, OPT_U32)
OPTION(filestore_max_xattr_value_size_btrfs, OPT_U32)
// ext4 allows 4k xattrs total including some smallish extra fields and the
// keys.  We're allowing 2 512 inline attrs in addition some some filestore
// replay attrs.  After accounting for those, we still need to fit up to
// two attrs of this value.  That means we need this value to be around 1k
// to be safe.  This is hacky, but it's not worth complicating the code
// to work around ext4's total xattr limit.
OPTION(filestore_max_xattr_value_size_other, OPT_U32)

OPTION(filestore_sloppy_crc, OPT_BOOL)         // track sloppy crcs
OPTION(filestore_sloppy_crc_block_size, OPT_INT)

OPTION(filestore_max_alloc_hint_size, OPT_U64) // bytes

OPTION(filestore_max_sync_interval, OPT_DOUBLE)    // seconds
OPTION(filestore_min_sync_interval, OPT_DOUBLE)  // seconds
OPTION(filestore_btrfs_snap, OPT_BOOL)
OPTION(filestore_btrfs_clone_range, OPT_BOOL)
OPTION(filestore_zfs_snap, OPT_BOOL) // zfsonlinux is still unstable
OPTION(filestore_fsync_flushes_journal_data, OPT_BOOL)
OPTION(filestore_fiemap, OPT_BOOL)     // (try to) use fiemap
OPTION(filestore_punch_hole, OPT_BOOL)
OPTION(filestore_seek_data_hole, OPT_BOOL)     // (try to) use seek_data/hole
OPTION(filestore_splice, OPT_BOOL)
OPTION(filestore_fadvise, OPT_BOOL)
//collect device partition information for management application to use
OPTION(filestore_collect_device_partition_information, OPT_BOOL)

// (try to) use extsize for alloc hint NOTE: extsize seems to trigger
// data corruption in xfs prior to kernel 3.5.  filestore will
// implicitly disable this if it cannot confirm the kernel is newer
// than that.
// NOTE: This option involves a tradeoff: When disabled, fragmentation is
// worse, but large sequential writes are faster. When enabled, large
// sequential writes are slower, but fragmentation is reduced.
OPTION(filestore_xfs_extsize, OPT_BOOL)

OPTION(filestore_journal_parallel, OPT_BOOL)
OPTION(filestore_journal_writeahead, OPT_BOOL)
OPTION(filestore_journal_trailing, OPT_BOOL)
OPTION(filestore_queue_max_ops, OPT_U64)
OPTION(filestore_queue_max_bytes, OPT_U64)

OPTION(filestore_caller_concurrency, OPT_INT)

/// Expected filestore throughput in B/s
OPTION(filestore_expected_throughput_bytes, OPT_DOUBLE)
/// Expected filestore throughput in ops/s
OPTION(filestore_expected_throughput_ops, OPT_DOUBLE)

/// Filestore max delay multiple.  Defaults to 0 (disabled)
OPTION(filestore_queue_max_delay_multiple, OPT_DOUBLE)
/// Filestore high delay multiple.  Defaults to 0 (disabled)
OPTION(filestore_queue_high_delay_multiple, OPT_DOUBLE)

/// Filestore max delay multiple bytes.  Defaults to 0 (disabled)
OPTION(filestore_queue_max_delay_multiple_bytes, OPT_DOUBLE)
/// Filestore high delay multiple bytes.  Defaults to 0 (disabled)
OPTION(filestore_queue_high_delay_multiple_bytes, OPT_DOUBLE)

/// Filestore max delay multiple ops.  Defaults to 0 (disabled)
OPTION(filestore_queue_max_delay_multiple_ops, OPT_DOUBLE)
/// Filestore high delay multiple ops.  Defaults to 0 (disabled)
OPTION(filestore_queue_high_delay_multiple_ops, OPT_DOUBLE)

/// Use above to inject delays intended to keep the op queue between low and high
OPTION(filestore_queue_low_threshhold, OPT_DOUBLE)
OPTION(filestore_queue_high_threshhold, OPT_DOUBLE)

OPTION(filestore_op_threads, OPT_INT)
OPTION(filestore_op_thread_timeout, OPT_INT)
OPTION(filestore_op_thread_suicide_timeout, OPT_INT)
OPTION(filestore_commit_timeout, OPT_FLOAT)
OPTION(filestore_fiemap_threshold, OPT_INT)
OPTION(filestore_merge_threshold, OPT_INT)
OPTION(filestore_split_multiple, OPT_INT)
OPTION(filestore_split_rand_factor, OPT_U32) // randomize the split threshold by adding 16 * [0)
OPTION(filestore_update_to, OPT_INT)
OPTION(filestore_blackhole, OPT_BOOL)     // drop any new transactions on the floor
OPTION(filestore_fd_cache_size, OPT_INT)    // FD lru size
OPTION(filestore_fd_cache_shards, OPT_INT)   // FD number of shards
OPTION(filestore_ondisk_finisher_threads, OPT_INT)
OPTION(filestore_apply_finisher_threads, OPT_INT)
OPTION(filestore_dump_file, OPT_STR)         // file onto which store transaction dumps
OPTION(filestore_kill_at, OPT_INT)            // inject a failure at the n'th opportunity
OPTION(filestore_inject_stall, OPT_INT)       // artificially stall for N seconds in op queue thread
OPTION(filestore_fail_eio, OPT_BOOL)       // fail/crash on EIO
OPTION(filestore_debug_verify_split, OPT_BOOL)
OPTION(journal_dio, OPT_BOOL)
OPTION(journal_aio, OPT_BOOL)
OPTION(journal_force_aio, OPT_BOOL)
OPTION(journal_block_size, OPT_INT)

OPTION(journal_block_align, OPT_BOOL)
OPTION(journal_write_header_frequency, OPT_U64)
OPTION(journal_max_write_bytes, OPT_INT)
OPTION(journal_max_write_entries, OPT_INT)

/// Target range for journal fullness
OPTION(journal_throttle_low_threshhold, OPT_DOUBLE)
OPTION(journal_throttle_high_threshhold, OPT_DOUBLE)

/// Multiple over expected at high_threshhold. Defaults to 0 (disabled).
OPTION(journal_throttle_high_multiple, OPT_DOUBLE)
/// Multiple over expected at max.  Defaults to 0 (disabled).
OPTION(journal_throttle_max_multiple, OPT_DOUBLE)

OPTION(journal_align_min_size, OPT_INT)  // align data payloads >= this.
OPTION(journal_replay_from, OPT_INT)
OPTION(journal_zero_on_create, OPT_BOOL)
OPTION(journal_ignore_corruption, OPT_BOOL) // assume journal is not corrupt
OPTION(journal_discard, OPT_BOOL) //using ssd disk as journal, whether support discard nouse journal-data.

OPTION(fio_dir, OPT_STR) // fio data directory for fio-objectstore

OPTION(rados_mon_op_timeout, OPT_DOUBLE) // how many seconds to wait for a response from the monitor before returning an error from a rados operation. 0 means no limit.
OPTION(rados_osd_op_timeout, OPT_DOUBLE) // how many seconds to wait for a response from osds before returning an error from a rados operation. 0 means no limit.
OPTION(rados_tracing, OPT_BOOL) // true if LTTng-UST tracepoints should be enabled


OPTION(rgw_max_attr_name_len, OPT_SIZE)
OPTION(rgw_max_attr_size, OPT_SIZE)
OPTION(rgw_max_attrs_num_in_req, OPT_U64)

OPTION(rgw_max_chunk_size, OPT_INT)
OPTION(rgw_put_obj_min_window_size, OPT_INT)
OPTION(rgw_put_obj_max_window_size, OPT_INT)
OPTION(rgw_max_put_size, OPT_U64)
OPTION(rgw_max_put_param_size, OPT_U64) // max input size for PUT requests accepting json/xml params

/**
 * override max bucket index shards in zone configuration (if not zero)
 *
 * Represents the number of shards for the bucket index object, a value of zero
 * indicates there is no sharding. By default (no sharding, the name of the object
 * is '.dir.{marker}', with sharding, the name is '.dir.{markder}.{sharding_id}',
 * sharding_id is zero-based value. It is not recommended to set a too large value
 * (e.g. thousand) as it increases the cost for bucket listing.
 */
OPTION(rgw_override_bucket_index_max_shards, OPT_U32)

/**
 * Represents the maximum AIO pending requests for the bucket index object shards.
 */
OPTION(rgw_bucket_index_max_aio, OPT_U32)

/**
 * whether or not the quota/gc threads should be started
 */
OPTION(rgw_enable_quota_threads, OPT_BOOL)
OPTION(rgw_enable_gc_threads, OPT_BOOL)
OPTION(rgw_enable_lc_threads, OPT_BOOL)

/* overrides for librgw/nfs */
OPTION(rgw_nfs_run_gc_threads, OPT_BOOL)
OPTION(rgw_nfs_run_lc_threads, OPT_BOOL)
OPTION(rgw_nfs_run_quota_threads, OPT_BOOL)
OPTION(rgw_nfs_run_sync_thread, OPT_BOOL)

OPTION(rgw_data, OPT_STR)
OPTION(rgw_enable_apis, OPT_STR)
OPTION(rgw_cache_enabled, OPT_BOOL)   // rgw cache enabled
OPTION(rgw_cache_lru_size, OPT_INT)   // num of entries in rgw cache
OPTION(rgw_socket_path, OPT_STR)   // path to unix domain socket, if not specified, rgw will not run as external fcgi
OPTION(rgw_host, OPT_STR)  // host for radosgw, can be an IP, default is 0.0.0.0
OPTION(rgw_port, OPT_STR)  // port to listen, format as "8080" "5000", if not specified, rgw will not run external fcgi
OPTION(rgw_dns_name, OPT_STR) // hostname suffix on buckets
OPTION(rgw_dns_s3website_name, OPT_STR) // hostname suffix on buckets for s3-website endpoint
OPTION(rgw_service_provider_name, OPT_STR) //service provider name which is contained in http response headers
OPTION(rgw_content_length_compat, OPT_BOOL) // Check both HTTP_CONTENT_LENGTH and CONTENT_LENGTH in fcgi env
OPTION(rgw_lifecycle_work_time, OPT_STR) //job process lc  at 00:00-06:00s
OPTION(rgw_lc_lock_max_time, OPT_INT)  // total run time for a single lc processor work
OPTION(rgw_lc_max_worker, OPT_INT)// number of (parellized) LCWorker threads
OPTION(rgw_lc_max_wp_worker, OPT_INT)// number of per-LCWorker pool threads
OPTION(rgw_lc_max_objs, OPT_INT)
OPTION(rgw_lc_max_rules, OPT_U32)  // Max rules set on one bucket
OPTION(rgw_lc_debug_interval, OPT_INT)  // Debug run interval, in seconds
OPTION(rgw_script_uri, OPT_STR) // alternative value for SCRIPT_URI if not set in request
OPTION(rgw_request_uri, OPT_STR) // alternative value for REQUEST_URI if not set in request
OPTION(rgw_ignore_get_invalid_range, OPT_BOOL) // treat invalid (e.g., negative) range requests as full
OPTION(rgw_swift_url, OPT_STR)             // the swift url, being published by the internal swift auth
OPTION(rgw_swift_url_prefix, OPT_STR) // entry point for which a url is considered a swift url
OPTION(rgw_swift_auth_url, OPT_STR)        // default URL to go and verify tokens for v1 auth (if not using internal swift auth)
OPTION(rgw_swift_auth_entry, OPT_STR)  // entry point for which a url is considered a swift auth url
OPTION(rgw_swift_tenant_name, OPT_STR)  // tenant name to use for swift access
OPTION(rgw_swift_account_in_url, OPT_BOOL)  // assume that URL always contain the account (aka tenant) part
OPTION(rgw_swift_enforce_content_length, OPT_BOOL)  // enforce generation of Content-Length even in cost of performance or scalability
OPTION(rgw_keystone_url, OPT_STR)  // url for keystone server
OPTION(rgw_keystone_admin_token, OPT_STR)  // keystone admin token (shared secret)
OPTION(rgw_keystone_admin_token_path, OPT_STR)  // path to keystone admin token (shared secret)
OPTION(rgw_keystone_admin_user, OPT_STR)  // keystone admin user name
OPTION(rgw_keystone_admin_password, OPT_STR)  // keystone admin user password
OPTION(rgw_keystone_admin_password_path, OPT_STR)  // path to keystone admin user password
OPTION(rgw_keystone_admin_tenant, OPT_STR)  // keystone admin user tenant (for keystone v2.0)
OPTION(rgw_keystone_admin_project, OPT_STR)  // keystone admin user project (for keystone v3)
OPTION(rgw_keystone_admin_domain, OPT_STR)  // keystone admin user domain
OPTION(rgw_keystone_barbican_user, OPT_STR)  // keystone user to access barbican secrets
OPTION(rgw_keystone_barbican_password, OPT_STR)  // keystone password for barbican user
OPTION(rgw_keystone_barbican_tenant, OPT_STR)  // keystone barbican user tenant (for keystone v2.0)
OPTION(rgw_keystone_barbican_project, OPT_STR)  // keystone barbican user project (for keystone v3)
OPTION(rgw_keystone_barbican_domain, OPT_STR)  // keystone barbican user domain
OPTION(rgw_keystone_api_version, OPT_INT) // Version of Keystone API to use (2 or 3)
OPTION(rgw_keystone_accepted_roles, OPT_STR)  // roles required to serve requests
OPTION(rgw_keystone_accepted_admin_roles, OPT_STR) // list of roles allowing an user to gain admin privileges
OPTION(rgw_keystone_token_cache_size, OPT_INT)  // max number of entries in keystone token cache
OPTION(rgw_keystone_verify_ssl, OPT_BOOL) // should we try to verify keystone's ssl
OPTION(rgw_cross_domain_policy, OPT_STR)
OPTION(rgw_healthcheck_disabling_path, OPT_STR) // path that existence causes the healthcheck to respond 503
OPTION(rgw_s3_auth_use_rados, OPT_BOOL)  // should we try to use the internal credentials for s3?
OPTION(rgw_s3_auth_use_keystone, OPT_BOOL)  // should we try to use keystone for s3?
OPTION(rgw_s3_auth_order, OPT_STR) // s3 authentication order to try
OPTION(rgw_barbican_url, OPT_STR)  // url for barbican server
OPTION(rgw_opa_url, OPT_STR)  // url for OPA server
OPTION(rgw_opa_token, OPT_STR)  // Bearer token OPA uses to authenticate client requests
OPTION(rgw_opa_verify_ssl, OPT_BOOL) // should we try to verify OPA's ssl
OPTION(rgw_use_opa_authz, OPT_BOOL) // should we use OPA to authorize client requests?

/* OpenLDAP-style LDAP parameter strings */
/* rgw_ldap_uri  space-separated list of LDAP servers in URI format */
OPTION(rgw_ldap_uri, OPT_STR)
/* rgw_ldap_binddn  LDAP entry RGW will bind with (user match) */
OPTION(rgw_ldap_binddn, OPT_STR)
/* rgw_ldap_searchdn  LDAP search base (basedn) */
OPTION(rgw_ldap_searchdn, OPT_STR)
/* rgw_ldap_dnattr  LDAP attribute containing RGW user names (to form binddns)*/
OPTION(rgw_ldap_dnattr, OPT_STR)
/* rgw_ldap_secret  file containing credentials for rgw_ldap_binddn */
OPTION(rgw_ldap_secret, OPT_STR)
/* rgw_s3_auth_use_ldap  use LDAP for RGW auth? */
OPTION(rgw_s3_auth_use_ldap, OPT_BOOL)
/* rgw_ldap_searchfilter  LDAP search filter */
OPTION(rgw_ldap_searchfilter, OPT_STR)

OPTION(rgw_admin_entry, OPT_STR)  // entry point for which a url is considered an admin request
OPTION(rgw_enforce_swift_acls, OPT_BOOL)
OPTION(rgw_swift_token_expiration, OPT_INT) // time in seconds for swift token expiration
OPTION(rgw_print_continue, OPT_BOOL)  // enable if 100-Continue works
OPTION(rgw_print_prohibited_content_length, OPT_BOOL) // violate RFC 7230 and send Content-Length in 204 and 304
OPTION(rgw_remote_addr_param, OPT_STR)  // e.g. X-Forwarded-For, if you have a reverse proxy
OPTION(rgw_op_thread_timeout, OPT_INT)
OPTION(rgw_op_thread_suicide_timeout, OPT_INT)
OPTION(rgw_thread_pool_size, OPT_INT)
OPTION(rgw_num_control_oids, OPT_INT)
OPTION(rgw_verify_ssl, OPT_BOOL) // should http_client try to verify ssl when sent https request

/* The following are tunables for caches of RGW NFS (and other file
 * client) objects.
 *
 * The file handle cache is a partitioned hash table
 * (fhcache_partitions), each with a closed hash part and backing
 * b-tree mapping.  The number of partions is expected to be a small
 * prime, the cache size something larger but less than 5K, the total
 * size of the cache is n_part * cache_size.
 */
OPTION(rgw_nfs_lru_lanes, OPT_INT)
OPTION(rgw_nfs_lru_lane_hiwat, OPT_INT)
OPTION(rgw_nfs_fhcache_partitions, OPT_INT)
OPTION(rgw_nfs_fhcache_size, OPT_INT) /* 3*2017=6051 */
OPTION(rgw_nfs_namespace_expire_secs, OPT_INT) /* namespace invalidate
						     * timer */
OPTION(rgw_nfs_max_gc, OPT_INT) /* max gc events per cycle */
OPTION(rgw_nfs_write_completion_interval_s, OPT_INT) /* stateless (V3)
							  * commit
							  * delay */
OPTION(rgw_nfs_s3_fast_attrs, OPT_BOOL) /* use fast S3 attrs from
					 * bucket index--currently
					 * assumes NFS mounts are
					 * immutable */

OPTION(rgw_zone, OPT_STR) // zone name
OPTION(rgw_zone_root_pool, OPT_STR)    // pool where zone specific info is stored
OPTION(rgw_default_zone_info_oid, OPT_STR)  // oid where default zone info is stored
OPTION(rgw_region, OPT_STR) // region name
OPTION(rgw_region_root_pool, OPT_STR)  // pool where all region info is stored
OPTION(rgw_default_region_info_oid, OPT_STR)  // oid where default region info is stored
OPTION(rgw_zonegroup, OPT_STR) // zone group name
OPTION(rgw_zonegroup_root_pool, OPT_STR)  // pool where all zone group info is stored
OPTION(rgw_default_zonegroup_info_oid, OPT_STR)  // oid where default zone group info is stored
OPTION(rgw_realm, OPT_STR) // realm name
OPTION(rgw_realm_root_pool, OPT_STR)  // pool where all realm info is stored
OPTION(rgw_default_realm_info_oid, OPT_STR)  // oid where default realm info is stored
OPTION(rgw_period_root_pool, OPT_STR)  // pool where all period info is stored
OPTION(rgw_period_latest_epoch_info_oid, OPT_STR) // oid where current period info is stored
OPTION(rgw_log_nonexistent_bucket, OPT_BOOL)
OPTION(rgw_log_object_name, OPT_STR)      // man date to see codes (a subset are supported)
OPTION(rgw_log_object_name_utc, OPT_BOOL)
OPTION(rgw_usage_max_shards, OPT_INT)
OPTION(rgw_usage_max_user_shards, OPT_INT)
OPTION(rgw_enable_ops_log, OPT_BOOL) // enable logging every rgw operation
OPTION(rgw_enable_usage_log, OPT_BOOL) // enable logging bandwidth usage
OPTION(rgw_ops_log_rados, OPT_BOOL) // whether ops log should go to rados
OPTION(rgw_ops_log_socket_path, OPT_STR) // path to unix domain socket where ops log can go
OPTION(rgw_ops_log_data_backlog, OPT_INT) // max data backlog for ops log
OPTION(rgw_fcgi_socket_backlog, OPT_INT) // socket  backlog for fcgi
OPTION(rgw_usage_log_flush_threshold, OPT_INT) // threshold to flush pending log data
OPTION(rgw_usage_log_tick_interval, OPT_INT) // flush pending log data every X seconds
OPTION(rgw_init_timeout, OPT_INT) // time in seconds
OPTION(rgw_mime_types_file, OPT_STR)
OPTION(rgw_gc_max_objs, OPT_INT)
OPTION(rgw_gc_obj_min_wait, OPT_INT)    // wait time before object may be handled by gc, recommended lower limit is 30 mins
OPTION(rgw_gc_processor_max_time, OPT_INT)  // total run time for a single gc processor work
OPTION(rgw_gc_processor_period, OPT_INT)  // gc processor cycle time
OPTION(rgw_gc_max_concurrent_io, OPT_INT)  // gc processor cycle time
OPTION(rgw_gc_max_trim_chunk, OPT_INT)  // gc trim chunk size
OPTION(rgw_s3_success_create_obj_status, OPT_INT) // alternative success status response for create-obj (0 - default)
OPTION(rgw_resolve_cname, OPT_BOOL)  // should rgw try to resolve hostname as a dns cname record
OPTION(rgw_obj_stripe_size, OPT_INT)
OPTION(rgw_extended_http_attrs, OPT_STR) // list of extended attrs that can be set on objects (beyond the default)
OPTION(rgw_exit_timeout_secs, OPT_INT) // how many seconds to wait for process to go down before exiting unconditionally
OPTION(rgw_get_obj_window_size, OPT_INT) // window size in bytes for single get obj request
OPTION(rgw_get_obj_max_req_size, OPT_INT) // max length of a single get obj rados op
OPTION(rgw_relaxed_s3_bucket_names, OPT_BOOL) // enable relaxed bucket name rules for US region buckets
OPTION(rgw_defer_to_bucket_acls, OPT_STR) // if the user has bucket perms)
OPTION(rgw_list_buckets_max_chunk, OPT_INT) // max buckets to retrieve in a single op when listing user buckets
OPTION(rgw_md_log_max_shards, OPT_INT) // max shards for metadata log
OPTION(rgw_curl_wait_timeout_ms, OPT_INT) // timeout for certain curl calls
OPTION(rgw_curl_low_speed_limit, OPT_INT) // low speed limit for certain curl calls
OPTION(rgw_curl_low_speed_time, OPT_INT) // low speed time for certain curl calls
OPTION(rgw_copy_obj_progress, OPT_BOOL) // should dump progress during long copy operations?
OPTION(rgw_copy_obj_progress_every_bytes, OPT_INT) // min bytes between copy progress output
OPTION(rgw_sync_obj_etag_verify, OPT_BOOL) // verify if the copied object from remote is identical to source
OPTION(rgw_obj_tombstone_cache_size, OPT_INT) // how many objects in tombstone cache, which is used in multi-zone sync to keep
                                                    // track of removed objects' mtime

OPTION(rgw_data_log_window, OPT_INT) // data log entries window (in seconds)
OPTION(rgw_data_log_changes_size, OPT_INT) // number of in-memory entries to hold for data changes log
OPTION(rgw_data_log_num_shards, OPT_INT) // number of objects to keep data changes log on
OPTION(rgw_data_log_obj_prefix, OPT_STR) //

OPTION(rgw_bucket_quota_ttl, OPT_INT) // time for cached bucket stats to be cached within rgw instance
OPTION(rgw_bucket_quota_soft_threshold, OPT_DOUBLE) // threshold from which we don't rely on cached info for quota decisions
OPTION(rgw_bucket_quota_cache_size, OPT_INT) // number of entries in bucket quota cache
OPTION(rgw_bucket_default_quota_max_objects, OPT_INT) // number of objects allowed
OPTION(rgw_bucket_default_quota_max_size, OPT_LONGLONG) // Max size of object in bytes

OPTION(rgw_expose_bucket, OPT_BOOL) // Return the bucket name in the 'Bucket' response header

OPTION(rgw_frontends, OPT_STR) // rgw front ends

OPTION(rgw_user_quota_bucket_sync_interval, OPT_INT) // time period for accumulating modified buckets before syncing stats
OPTION(rgw_user_quota_sync_interval, OPT_INT) // time period for accumulating modified buckets before syncing entire user stats
OPTION(rgw_user_quota_sync_idle_users, OPT_BOOL) // whether stats for idle users be fully synced
OPTION(rgw_user_quota_sync_wait_time, OPT_INT) // min time between two full stats sync for non-idle users
OPTION(rgw_user_default_quota_max_objects, OPT_INT) // number of objects allowed
OPTION(rgw_user_default_quota_max_size, OPT_LONGLONG) // Max size of object in bytes

OPTION(rgw_multipart_min_part_size, OPT_INT) // min size for each part (except for last one) in multipart upload
OPTION(rgw_multipart_part_upload_limit, OPT_INT) // parts limit in multipart upload

OPTION(rgw_max_slo_entries, OPT_INT) // default number of max entries in slo

OPTION(rgw_olh_pending_timeout_sec, OPT_INT) // time until we retire a pending olh change
OPTION(rgw_user_max_buckets, OPT_INT) // global option to set max buckets count for all user

OPTION(rgw_objexp_gc_interval, OPT_U32) // maximum time between round of expired objects garbage collecting
OPTION(rgw_objexp_hints_num_shards, OPT_U32) // maximum number of parts in which the hint index is stored in
OPTION(rgw_objexp_chunk_size, OPT_U32) // maximum number of entries in a single operation when processing objexp data

OPTION(rgw_enable_static_website, OPT_BOOL) // enable static website feature
OPTION(rgw_log_http_headers, OPT_STR) // list of HTTP headers to log when seen, ignores case (e.g., http_x_forwarded_for

OPTION(rgw_num_async_rados_threads, OPT_INT) // num of threads to use for async rados operations
OPTION(rgw_md_notify_interval_msec, OPT_INT) // metadata changes notification interval to followers
OPTION(rgw_run_sync_thread, OPT_BOOL) // whether radosgw (not radosgw-admin) spawns the sync thread
OPTION(rgw_sync_lease_period, OPT_INT) // time in second for lease that rgw takes on a specific log (or log shard)
OPTION(rgw_sync_log_trim_interval, OPT_INT) // time in seconds between attempts to trim sync logs

OPTION(rgw_sync_data_inject_err_probability, OPT_DOUBLE) // range [0, 1]
OPTION(rgw_sync_meta_inject_err_probability, OPT_DOUBLE) // range [0, 1]
OPTION(rgw_sync_trace_history_size, OPT_INT) // max number of complete sync trace entries to keep
OPTION(rgw_sync_trace_per_node_log_size, OPT_INT) // how many log entries to keep per node
OPTION(rgw_sync_trace_servicemap_update_interval, OPT_INT) // interval in seconds between sync trace servicemap update


OPTION(rgw_period_push_interval, OPT_DOUBLE) // seconds to wait before retrying "period push"
OPTION(rgw_period_push_interval_max, OPT_DOUBLE) // maximum interval after exponential backoff

OPTION(rgw_safe_max_objects_per_shard, OPT_INT) // safe max loading
OPTION(rgw_shard_warning_threshold, OPT_DOUBLE) // pct of safe max
						    // at which to warn

OPTION(rgw_swift_versioning_enabled, OPT_BOOL) // whether swift object versioning feature is enabled

OPTION(rgw_trust_forwarded_https, OPT_BOOL) // trust Forwarded and X-Forwarded-Proto headers for ssl termination
OPTION(rgw_crypt_require_ssl, OPT_BOOL) // requests including encryption key headers must be sent over ssl
OPTION(rgw_crypt_default_encryption_key, OPT_STR) // base64 encoded key for encryption of rgw objects

OPTION(rgw_crypt_s3_kms_backend, OPT_STR) // Where SSE-KMS encryption keys are stored
OPTION(rgw_crypt_vault_auth, OPT_STR) // Type of authentication method to be used with Vault
OPTION(rgw_crypt_vault_token_file, OPT_STR) // Path to the token file for Vault authentication
OPTION(rgw_crypt_vault_addr, OPT_STR) // Vault server base address
OPTION(rgw_crypt_vault_prefix, OPT_STR) // Optional URL prefix to Vault secret path
OPTION(rgw_crypt_vault_secret_engine, OPT_STR) // kv, transit or other supported secret engines
OPTION(rgw_crypt_vault_namespace, OPT_STR) // Vault Namespace (only availabe in Vault Enterprise Version)

OPTION(rgw_crypt_s3_kms_encryption_keys, OPT_STR) // extra keys that may be used for aws:kms
                                                      // defined as map "key1=YmluCmJvb3N0CmJvb3N0LQ== key2=b3V0CnNyYwpUZXN0aW5nCg=="
OPTION(rgw_crypt_suppress_logs, OPT_BOOL)   // suppress logs that might print customer key
OPTION(rgw_list_bucket_min_readahead, OPT_INT) // minimum number of entries to read from rados for bucket listing

OPTION(rgw_rest_getusage_op_compat, OPT_BOOL) // dump description of total stats for s3 GetUsage API

OPTION(throttler_perf_counter, OPT_BOOL) // enable/disable throttler perf counter

/* The following are tunables for torrent data */
OPTION(rgw_torrent_flag, OPT_BOOL)    // produce torrent function flag
OPTION(rgw_torrent_tracker, OPT_STR)    // torrent field announce and announce list
OPTION(rgw_torrent_createby, OPT_STR)    // torrent field created by
OPTION(rgw_torrent_comment, OPT_STR)    // torrent field comment
OPTION(rgw_torrent_encoding, OPT_STR)    // torrent field encoding
OPTION(rgw_torrent_origin, OPT_STR)    // torrent origin
OPTION(rgw_torrent_sha_unit, OPT_INT)    // torrent field piece length 512K

OPTION(event_tracing, OPT_BOOL) // true if LTTng-UST tracepoints should be enabled

OPTION(debug_deliberately_leak_memory, OPT_BOOL)
OPTION(debug_asok_assert_abort, OPT_BOOL)

OPTION(rgw_swift_custom_header, OPT_STR) // option to enable swift custom headers

OPTION(rgw_swift_need_stats, OPT_BOOL) // option to enable stats on bucket listing for swift

OPTION(rgw_acl_grants_max_num, OPT_INT) // According to AWS S3(http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html), An ACL can have up to 100 grants.
OPTION(rgw_cors_rules_max_num, OPT_INT) // According to AWS S3(http://docs.aws.amazon.com/AmazonS3/latest/dev/cors.html), An cors can have up to 100 rules.
OPTION(rgw_delete_multi_obj_max_num, OPT_INT) // According to AWS S3(https://docs.aws.amazon.com/AmazonS3/latest/dev/DeletingObjects.html), Amazon S3 also provides the Multi-Object Delete API that you can use to delete up to 1000 objects in a single HTTP request.
OPTION(rgw_website_routing_rules_max_num, OPT_INT) // According to AWS S3, An website routing config can have up to 50 rules.
OPTION(rgw_sts_entry, OPT_STR)
OPTION(rgw_sts_key, OPT_STR)
OPTION(rgw_s3_auth_use_sts, OPT_BOOL)  // should we try to use sts for s3?
OPTION(rgw_sts_max_session_duration, OPT_U64) // Max duration in seconds for which the session token is valid.
OPTION(fake_statfs_for_testing, OPT_INT) // Set a value for kb and compute kb_used from total of num_bytes
OPTION(rgw_sts_token_introspection_url, OPT_STR)  // url for introspecting web tokens
OPTION(rgw_sts_client_id, OPT_STR) // Client Id
OPTION(rgw_sts_client_secret, OPT_STR) // Client Secret
OPTION(debug_allow_any_pool_priority, OPT_BOOL)
OPTION(rgw_gc_max_deferred_entries_size, OPT_U64) // GC deferred entries size in queue head
OPTION(rgw_gc_max_queue_size, OPT_U64) // GC max queue size
OPTION(rgw_gc_max_deferred, OPT_U64) // GC max number of deferred entries
