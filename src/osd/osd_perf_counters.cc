// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd_perf_counters.h"
#include "include/common_fwd.h"


PerfCounters *build_osd_logger(CephContext *cct) {
  PerfCountersBuilder osd_plb(cct, "osd", l_osd_first, l_osd_last);

  // Latency axis configuration for op histograms, values are in nanoseconds
  PerfHistogramCommon::axis_config_d op_hist_x_axis_config{
    "Latency (usec)",
    PerfHistogramCommon::SCALE_LOG2, ///< Latency in logarithmic scale
    0,                               ///< Start at 0
    100000,                          ///< Quantization unit is 100usec
    32,                              ///< Enough to cover much longer than slow requests
  };

  // Op size axis configuration for op histograms, values are in bytes
  PerfHistogramCommon::axis_config_d op_hist_y_axis_config{
    "Request size (bytes)",
    PerfHistogramCommon::SCALE_LOG2, ///< Request size in logarithmic scale
    0,                               ///< Start at 0
    512,                             ///< Quantization unit is 512 bytes
    32,                              ///< Enough to cover requests larger than GB
  };


  // All the basic OSD operation stats are to be considered useful
  osd_plb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  osd_plb.add_u64(
    l_osd_op_wip, "op_wip",
    "Replication operations currently being processed (primary)");
  osd_plb.add_u64_counter(
    l_osd_op, "op",
    "Client operations",
    "ops", PerfCountersBuilder::PRIO_CRITICAL);
  osd_plb.add_u64_counter(
    l_osd_op_inb,   "op_in_bytes",
    "Client operations total write size",
    "wr", PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));
  osd_plb.add_u64_counter(
    l_osd_op_outb,  "op_out_bytes",
    "Client operations total read size",
    "rd", PerfCountersBuilder::PRIO_INTERESTING, unit_t(UNIT_BYTES));
  osd_plb.add_time_avg(
    l_osd_op_lat,   "op_latency",
    "Latency of client operations (including queue time)",
    "l", 9);
  osd_plb.add_time_avg(
    l_osd_op_process_lat, "op_process_latency",
    "Latency of client operations (excluding queue time)");
  osd_plb.add_time_avg(
    l_osd_op_prepare_lat, "op_prepare_latency",
    "Latency of client operations (excluding queue time and wait for finished)");

  osd_plb.add_u64_counter(
    l_osd_op_delayed_unreadable, "op_delayed_unreadable",
    "Count of ops delayed due to target object being unreadable");
  osd_plb.add_u64_counter(
    l_osd_op_delayed_degraded, "op_delayed_degraded",
    "Count of ops delayed due to target object being degraded");

  osd_plb.add_u64_counter(
    l_osd_op_r, "op_r", "Client read operations");
  osd_plb.add_u64_counter(
    l_osd_op_r_outb, "op_r_out_bytes", "Client data read", NULL, PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  osd_plb.add_time_avg(
    l_osd_op_r_lat, "op_r_latency",
    "Latency of read operation (including queue time)");
  osd_plb.add_u64_counter_histogram(
    l_osd_op_r_lat_outb_hist, "op_r_latency_out_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of operation latency (including queue time) + data read");
  osd_plb.add_time_avg(
    l_osd_op_r_process_lat, "op_r_process_latency",
    "Latency of read operation (excluding queue time)");
  osd_plb.add_time_avg(
    l_osd_op_r_prepare_lat, "op_r_prepare_latency",
    "Latency of read operations (excluding queue time and wait for finished)");
  osd_plb.add_u64_counter(
    l_osd_op_w, "op_w", "Client write operations");
  osd_plb.add_u64_counter(
    l_osd_op_w_inb, "op_w_in_bytes", "Client data written");
  osd_plb.add_time_avg(
    l_osd_op_w_lat,  "op_w_latency",
    "Latency of write operation (including queue time)");
  osd_plb.add_u64_counter_histogram(
    l_osd_op_w_lat_inb_hist, "op_w_latency_in_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of operation latency (including queue time) + data written");
  osd_plb.add_time_avg(
    l_osd_op_w_process_lat, "op_w_process_latency",
    "Latency of write operation (excluding queue time)");
  osd_plb.add_time_avg(
    l_osd_op_w_prepare_lat, "op_w_prepare_latency",
    "Latency of write operations (excluding queue time and wait for finished)");
  osd_plb.add_u64_counter(
    l_osd_op_rw, "op_rw",
    "Client read-modify-write operations");
  osd_plb.add_u64_counter(
    l_osd_op_rw_inb, "op_rw_in_bytes",
    "Client read-modify-write operations write in", NULL, PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  osd_plb.add_u64_counter(
    l_osd_op_rw_outb,"op_rw_out_bytes",
    "Client read-modify-write operations read out ", NULL, PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  osd_plb.add_time_avg(
    l_osd_op_rw_lat, "op_rw_latency",
    "Latency of read-modify-write operation (including queue time)");
  osd_plb.add_u64_counter_histogram(
    l_osd_op_rw_lat_inb_hist, "op_rw_latency_in_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of rw operation latency (including queue time) + data written");
  osd_plb.add_u64_counter_histogram(
    l_osd_op_rw_lat_outb_hist, "op_rw_latency_out_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of rw operation latency (including queue time) + data read");
  osd_plb.add_time_avg(
    l_osd_op_rw_process_lat, "op_rw_process_latency",
    "Latency of read-modify-write operation (excluding queue time)");
  osd_plb.add_time_avg(
    l_osd_op_rw_prepare_lat, "op_rw_prepare_latency",
    "Latency of read-modify-write operations (excluding queue time and wait for finished)");
  osd_plb.add_time_avg(l_osd_op_before_queue_op_lat, "op_before_queue_op_lat",
    "Latency of IO before calling queue(before really queue into ShardedOpWq)"); // client io before queue op_wq latency

  // Now we move on to some more obscure stats, revert to assuming things
  // are low priority unless otherwise specified.
  osd_plb.set_prio_default(PerfCountersBuilder::PRIO_DEBUGONLY);

  osd_plb.add_time_avg(l_osd_op_before_dequeue_op_lat, "op_before_dequeue_op_lat",
    "Latency of IO before calling dequeue_op(already dequeued and get PG lock)"); // client io before dequeue_op latency

  osd_plb.add_u64_counter(
    l_osd_sop, "subop", "Suboperations");
  osd_plb.add_u64_counter(
    l_osd_sop_inb, "subop_in_bytes", "Suboperations total size", NULL, 0, unit_t(UNIT_BYTES));
  osd_plb.add_time_avg(l_osd_sop_lat, "subop_latency", "Suboperations latency");

  osd_plb.add_u64_counter(l_osd_sop_w, "subop_w", "Replicated writes");
  osd_plb.add_u64_counter(
    l_osd_sop_w_inb, "subop_w_in_bytes", "Replicated written data size", NULL, 0, unit_t(UNIT_BYTES));
  osd_plb.add_time_avg(
    l_osd_sop_w_lat, "subop_w_latency", "Replicated writes latency");
  osd_plb.add_u64_counter(
    l_osd_sop_pull, "subop_pull", "Suboperations pull requests");
  osd_plb.add_time_avg(
    l_osd_sop_pull_lat, "subop_pull_latency", "Suboperations pull latency");
  osd_plb.add_u64_counter(
    l_osd_sop_push, "subop_push", "Suboperations push messages");
  osd_plb.add_u64_counter(
    l_osd_sop_push_inb, "subop_push_in_bytes", "Suboperations pushed size", NULL, 0, unit_t(UNIT_BYTES));
  osd_plb.add_time_avg(
    l_osd_sop_push_lat, "subop_push_latency", "Suboperations push latency");

  osd_plb.add_u64_counter(l_osd_pull, "pull", "Pull requests sent");
  osd_plb.add_u64_counter(l_osd_push, "push", "Push messages sent");
  osd_plb.add_u64_counter(l_osd_push_outb, "push_out_bytes", "Pushed size", NULL, 0, unit_t(UNIT_BYTES));

  osd_plb.add_u64_counter(
    l_osd_rop, "recovery_ops",
    "Started recovery operations",
    "rop", PerfCountersBuilder::PRIO_INTERESTING);

  osd_plb.add_u64_counter(
   l_osd_rbytes, "recovery_bytes",
   "recovery bytes",
   "rbt", PerfCountersBuilder::PRIO_INTERESTING);

  osd_plb.add_time_avg(
    l_osd_recovery_push_queue_lat,
    "l_osd_recovery_push_queue_latency",
    "MOSDPGPush queue latency");
  osd_plb.add_time_avg(
    l_osd_recovery_push_reply_queue_lat,
    "l_osd_recovery_push_reply_queue_latency",
    "MOSDPGPushReply queue latency");
  osd_plb.add_time_avg(
    l_osd_recovery_pull_queue_lat,
    "l_osd_recovery_pull_queue_latency",
    "MOSDPGPull queue latency");
  osd_plb.add_time_avg(
    l_osd_recovery_backfill_queue_lat,
    "l_osd_recovery_backfill_queue_latency",
    "MOSDPGBackfill queue latency");
  osd_plb.add_time_avg(
    l_osd_recovery_backfill_remove_queue_lat,
    "l_osd_recovery_backfill_remove_queue_latency",
    "MOSDPGBackfillDelete queue latency");
  osd_plb.add_time_avg(
    l_osd_recovery_scan_queue_lat,
    "l_osd_recovery_scan_queue_latency",
    "MOSDPGScan queue latency");

  osd_plb.add_time_avg(
    l_osd_recovery_queue_lat,
    "l_osd_recovery_queue_latency",
    "PGRecovery queue latency");
  osd_plb.add_time_avg(
    l_osd_recovery_context_queue_lat,
    "l_osd_recovery_context_queue_latency",
    "PGRecoveryContext queue latency");

  osd_plb.add_u64(l_osd_loadavg, "loadavg", "CPU load");
  osd_plb.add_u64(
    l_osd_cached_crc, "cached_crc", "Total number getting crc from crc_cache");
  osd_plb.add_u64(
    l_osd_cached_crc_adjusted, "cached_crc_adjusted",
    "Total number getting crc from crc_cache with adjusting");
  osd_plb.add_u64(l_osd_missed_crc, "missed_crc", 
    "Total number of crc cache misses");

  osd_plb.add_u64(l_osd_pg, "numpg", "Placement groups",
		  "pgs", PerfCountersBuilder::PRIO_USEFUL);
  osd_plb.add_u64(
    l_osd_pg_primary, "numpg_primary",
    "Placement groups for which this osd is primary");
  osd_plb.add_u64(
    l_osd_pg_replica, "numpg_replica",
    "Placement groups for which this osd is replica");
  osd_plb.add_u64(
    l_osd_pg_stray, "numpg_stray",
    "Placement groups ready to be deleted from this osd");
  osd_plb.add_u64(
    l_osd_pg_removing, "numpg_removing",
    "Placement groups queued for local deletion", "pgsr",
    PerfCountersBuilder::PRIO_USEFUL);
  osd_plb.add_u64(
    l_osd_hb_to, "heartbeat_to_peers", "Heartbeat (ping) peers we send to");
  osd_plb.add_u64_counter(l_osd_map, "map_messages", "OSD map messages");
  osd_plb.add_u64_counter(l_osd_mape, "map_message_epochs", "OSD map epochs");
  osd_plb.add_u64_counter(
    l_osd_mape_dup, "map_message_epoch_dups", "OSD map duplicates");
  osd_plb.add_u64_counter(
    l_osd_waiting_for_map, "messages_delayed_for_map",
    "Operations waiting for OSD map");

  osd_plb.add_u64_counter(
    l_osd_map_cache_hit, "osd_map_cache_hit", "osdmap cache hit");
  osd_plb.add_u64_counter(
    l_osd_map_cache_miss, "osd_map_cache_miss", "osdmap cache miss");
  osd_plb.add_u64_counter(
    l_osd_map_cache_miss_low, "osd_map_cache_miss_low",
    "osdmap cache miss below cache lower bound");
  osd_plb.add_u64_avg(
    l_osd_map_cache_miss_low_avg, "osd_map_cache_miss_low_avg",
    "osdmap cache miss, avg distance below cache lower bound");
  osd_plb.add_u64_counter(
    l_osd_map_bl_cache_hit, "osd_map_bl_cache_hit",
    "OSDMap buffer cache hits");
  osd_plb.add_u64_counter(
    l_osd_map_bl_cache_miss, "osd_map_bl_cache_miss",
    "OSDMap buffer cache misses");

  osd_plb.add_u64(
    l_osd_stat_bytes, "stat_bytes", "OSD size", "size",
    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  osd_plb.add_u64(
    l_osd_stat_bytes_used, "stat_bytes_used", "Used space", "used",
    PerfCountersBuilder::PRIO_USEFUL, unit_t(UNIT_BYTES));
  osd_plb.add_u64(l_osd_stat_bytes_avail, "stat_bytes_avail", "Available space", NULL, 0, unit_t(UNIT_BYTES));

  osd_plb.add_u64_counter(
    l_osd_copyfrom, "copyfrom", "Rados \"copy-from\" operations");

  osd_plb.add_u64_counter(l_osd_tier_promote, "tier_promote", "Tier promotions");
  osd_plb.add_u64_counter(l_osd_tier_flush, "tier_flush", "Tier flushes");
  osd_plb.add_u64_counter(
    l_osd_tier_flush_fail, "tier_flush_fail", "Failed tier flushes");
  osd_plb.add_u64_counter(
    l_osd_tier_try_flush, "tier_try_flush", "Tier flush attempts");
  osd_plb.add_u64_counter(
    l_osd_tier_try_flush_fail, "tier_try_flush_fail",
    "Failed tier flush attempts");
  osd_plb.add_u64_counter(
    l_osd_tier_evict, "tier_evict", "Tier evictions");
  osd_plb.add_u64_counter(
    l_osd_tier_whiteout, "tier_whiteout", "Tier whiteouts");
  osd_plb.add_u64_counter(
    l_osd_tier_dirty, "tier_dirty", "Dirty tier flag set");
  osd_plb.add_u64_counter(
    l_osd_tier_clean, "tier_clean", "Dirty tier flag cleaned");
  osd_plb.add_u64_counter(
    l_osd_tier_delay, "tier_delay", "Tier delays (agent waiting)");
  osd_plb.add_u64_counter(
    l_osd_tier_proxy_read, "tier_proxy_read", "Tier proxy reads");
  osd_plb.add_u64_counter(
    l_osd_tier_proxy_write, "tier_proxy_write", "Tier proxy writes");

  osd_plb.add_u64_counter(
    l_osd_agent_wake, "agent_wake", "Tiering agent wake up");
  osd_plb.add_u64_counter(
    l_osd_agent_skip, "agent_skip", "Objects skipped by agent");
  osd_plb.add_u64_counter(
    l_osd_agent_flush, "agent_flush", "Tiering agent flushes");
  osd_plb.add_u64_counter(
    l_osd_agent_evict, "agent_evict", "Tiering agent evictions");

  osd_plb.add_u64_counter(
    l_osd_object_ctx_cache_hit, "object_ctx_cache_hit", "Object context cache hits");
  osd_plb.add_u64_counter(
    l_osd_object_ctx_cache_total, "object_ctx_cache_total", "Object context cache lookups");

  osd_plb.add_u64_counter(l_osd_op_cache_hit, "op_cache_hit");
  osd_plb.add_time_avg(
    l_osd_tier_flush_lat, "osd_tier_flush_lat", "Object flush latency");
  osd_plb.add_time_avg(
    l_osd_tier_promote_lat, "osd_tier_promote_lat", "Object promote latency");
  osd_plb.add_time_avg(
    l_osd_tier_r_lat, "osd_tier_r_lat", "Object proxy read latency");

  osd_plb.add_u64_counter(
    l_osd_pg_info, "osd_pg_info", "PG updated its info (using any method)");
  osd_plb.add_u64_counter(
    l_osd_pg_fastinfo, "osd_pg_fastinfo",
    "PG updated its info using fastinfo attr");
  osd_plb.add_u64_counter(
    l_osd_pg_biginfo, "osd_pg_biginfo", "PG updated its biginfo attr");

  /// scrub's replicas reservation time/#replicas histogram
  PerfHistogramCommon::axis_config_d rsrv_hist_x_axis_config{
      "number of replicas",
      PerfHistogramCommon::SCALE_LINEAR,
      0,   ///< Start at 0
      1,   ///< Quantization unit is 1
      8,   ///< 9 OSDs in the active set
  };
  PerfHistogramCommon::axis_config_d rsrv_hist_y_axis_config{
      "duration",
      PerfHistogramCommon::SCALE_LOG2,	///< Request size in logarithmic scale
      0,				///< Start at 0
      250'000,				///< 250us granularity
      10,				///< should be enough
  };
  osd_plb.add_u64_counter_histogram(
      l_osd_scrub_reservation_dur_hist, "scrub_resrv_repnum_vs_duration",
      rsrv_hist_x_axis_config, rsrv_hist_y_axis_config, "Histogram of scrub replicas reservation duration");

  // mclock QoS queue op counter
  osd_plb.add_u64_counter(l_osd_mclock_immediate_op, "mclock_immediate_op",
    "osd op_scheduler_class::immediate type op count");
  osd_plb.add_u64_counter(l_osd_mclock_client_op, "mclock_client_op",
    "osd op_scheduler_class::client type op count");
  osd_plb.add_u64_counter(l_osd_mclock_recovery_op, "mclock_recovery_op",
    "osd op_scheduler_class::background_recovery type op count");
  osd_plb.add_u64_counter(l_osd_mclock_best_effort_op, "mclock_best_effort_op",
    "osd op_scheduler_class::background_best_effort type op count");

  return osd_plb.create_perf_counters();
}


PerfCounters *build_recoverystate_perf(CephContext *cct) {
  PerfCountersBuilder rs_perf(cct, "recoverystate_perf", rs_first, rs_last);

  rs_perf.add_time_avg(rs_initial_latency, "initial_latency", "Initial recovery state latency");
  rs_perf.add_time_avg(rs_started_latency, "started_latency", "Started recovery state latency");
  rs_perf.add_time_avg(rs_reset_latency, "reset_latency", "Reset recovery state latency");
  rs_perf.add_time_avg(rs_start_latency, "start_latency", "Start recovery state latency");
  rs_perf.add_time_avg(rs_primary_latency, "primary_latency", "Primary recovery state latency");
  rs_perf.add_time_avg(rs_peering_latency, "peering_latency", "Peering recovery state latency");
  rs_perf.add_time_avg(rs_backfilling_latency, "backfilling_latency", "Backfilling recovery state latency");
  rs_perf.add_time_avg(rs_waitremotebackfillreserved_latency, "waitremotebackfillreserved_latency", "Wait remote backfill reserved recovery state latency");
  rs_perf.add_time_avg(rs_waitlocalbackfillreserved_latency, "waitlocalbackfillreserved_latency", "Wait local backfill reserved recovery state latency");
  rs_perf.add_time_avg(rs_notbackfilling_latency, "notbackfilling_latency", "Notbackfilling recovery state latency");
  rs_perf.add_time_avg(rs_repnotrecovering_latency, "repnotrecovering_latency", "Repnotrecovering recovery state latency");
  rs_perf.add_time_avg(rs_repwaitrecoveryreserved_latency, "repwaitrecoveryreserved_latency", "Rep wait recovery reserved recovery state latency");
  rs_perf.add_time_avg(rs_repwaitbackfillreserved_latency, "repwaitbackfillreserved_latency", "Rep wait backfill reserved recovery state latency");
  rs_perf.add_time_avg(rs_reprecovering_latency, "reprecovering_latency", "RepRecovering recovery state latency");
  rs_perf.add_time_avg(rs_activating_latency, "activating_latency", "Activating recovery state latency");
  rs_perf.add_time_avg(rs_waitlocalrecoveryreserved_latency, "waitlocalrecoveryreserved_latency", "Wait local recovery reserved recovery state latency");
  rs_perf.add_time_avg(rs_waitremoterecoveryreserved_latency, "waitremoterecoveryreserved_latency", "Wait remote recovery reserved recovery state latency");
  rs_perf.add_time_avg(rs_recovering_latency, "recovering_latency", "Recovering recovery state latency");
  rs_perf.add_time_avg(rs_recovered_latency, "recovered_latency", "Recovered recovery state latency");
  rs_perf.add_time_avg(rs_clean_latency, "clean_latency", "Clean recovery state latency");
  rs_perf.add_time_avg(rs_active_latency, "active_latency", "Active recovery state latency");
  rs_perf.add_time_avg(rs_replicaactive_latency, "replicaactive_latency", "Replicaactive recovery state latency");
  rs_perf.add_time_avg(rs_stray_latency, "stray_latency", "Stray recovery state latency");
  rs_perf.add_time_avg(rs_getinfo_latency, "getinfo_latency", "Getinfo recovery state latency");
  rs_perf.add_time_avg(rs_getlog_latency, "getlog_latency", "Getlog recovery state latency");
  rs_perf.add_time_avg(rs_waitactingchange_latency, "waitactingchange_latency", "Waitactingchange recovery state latency");
  rs_perf.add_time_avg(rs_incomplete_latency, "incomplete_latency", "Incomplete recovery state latency");
  rs_perf.add_time_avg(rs_down_latency, "down_latency", "Down recovery state latency");
  rs_perf.add_time_avg(rs_getmissing_latency, "getmissing_latency", "Getmissing recovery state latency");
  rs_perf.add_time_avg(rs_waitupthru_latency, "waitupthru_latency", "Waitupthru recovery state latency");
  rs_perf.add_time_avg(rs_notrecovering_latency, "notrecovering_latency", "Notrecovering recovery state latency");

  return rs_perf.create_perf_counters();
}


PerfCounters *build_scrub_labeled_perf(CephContext *cct, std::string label)
{
  // the labels matrix is:
  //   <shallow/deep>  X  <replicated/EC>  // maybe later we'll add <periodic/operator>
  PerfCountersBuilder scrub_perf(cct, label, scrbcnt_first, scrbcnt_last);

  scrub_perf.set_prio_default(PerfCountersBuilder::PRIO_INTERESTING);

  scrub_perf.add_u64_counter(scrbcnt_started, "num_scrubs_started", "scrubs attempted count");
  scrub_perf.add_u64_counter(scrbcnt_active_started, "num_scrubs_past_reservation", "scrubs count");
  scrub_perf.add_u64_counter(scrbcnt_failed, "failed_scrubs", "failed scrubs count");
  scrub_perf.add_u64_counter(scrbcnt_successful, "successful_scrubs", "successful scrubs count");
  scrub_perf.add_time_avg(scrbcnt_failed_elapsed, "failed_scrubs_elapsed", "time to scrub failure");
  scrub_perf.add_time_avg(scrbcnt_successful_elapsed, "successful_scrubs_elapsed", "time to scrub completion");

  scrub_perf.add_u64_counter(scrbcnt_preempted, "preemptions", "preemptions on scrubs");
  scrub_perf.add_u64_counter(scrbcnt_chunks_selected, "chunk_selected", "chunk selection during scrubs");
  scrub_perf.add_u64_counter(scrbcnt_chunks_busy, "chunk_busy", "chunk busy during scrubs");
  scrub_perf.add_u64_counter(scrbcnt_blocked, "locked_object", "waiting on locked object events");
  scrub_perf.add_u64_counter(scrbcnt_write_blocked, "write_blocked_by_scrub", "write blocked by scrub");

  // the replica reservation process
  scrub_perf.add_u64_counter(scrbcnt_resrv_success, "scrub_reservations_completed", "successfully completed reservation processes");
  scrub_perf.add_time_avg(scrbcnt_resrv_successful_elapsed, "successful_reservations_elapsed", "time to scrub reservation completion");
  scrub_perf.add_u64_counter(scrbcnt_resrv_aborted, "reservation_process_aborted", "scrub reservation was aborted");
  scrub_perf.add_u64_counter(scrbcnt_resrv_timed_out, "reservation_process_timed_out", "scrub reservation timed out");
  scrub_perf.add_u64_counter(scrbcnt_resrv_rejected, "reservation_process_failure", "scrub reservation failed due to replica denial");
  scrub_perf.add_u64_counter(scrbcnt_resrv_skipped, "reservation_process_skipped", "scrub reservation skipped for high priority scrub");
  scrub_perf.add_time_avg(scrbcnt_resrv_failed_elapsed, "failed_reservations_elapsed", "time for scrub reservation to fail");
  scrub_perf.add_u64(scrbcnt_resrv_replicas_num, "replicas_in_reservation", "number of replicas in reservation");

  return scrub_perf.create_perf_counters();
}
