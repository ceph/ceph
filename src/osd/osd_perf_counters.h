// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/common_fwd.h"
#include "common/perf_counters.h"
#include "common/perf_counters_key.h"

enum osd_counter_idx_t {
  l_osd_first = 10000,
  l_osd_op_wip,
  l_osd_op,
  l_osd_op_inb,
  l_osd_op_outb,
  l_osd_op_lat,
  l_osd_op_process_lat,
  l_osd_op_prepare_lat,
  l_osd_op_r,
  l_osd_op_r_outb,
  l_osd_op_r_lat,
  l_osd_op_r_lat_outb_hist,
  l_osd_op_r_process_lat,
  l_osd_op_r_prepare_lat,
  l_osd_op_w,
  l_osd_op_w_inb,
  l_osd_op_w_lat,
  l_osd_op_w_lat_inb_hist,
  l_osd_op_w_process_lat,
  l_osd_op_w_prepare_lat,
  l_osd_op_rw,
  l_osd_op_rw_inb,
  l_osd_op_rw_outb,
  l_osd_op_rw_lat,
  l_osd_op_rw_lat_inb_hist,
  l_osd_op_rw_lat_outb_hist,
  l_osd_op_rw_process_lat,
  l_osd_op_rw_prepare_lat,

  l_osd_op_delayed_unreadable,
  l_osd_op_delayed_degraded,

  l_osd_op_before_queue_op_lat,
  l_osd_op_before_dequeue_op_lat,

  l_osd_replica_read,
  l_osd_replica_read_redirect_missing,
  l_osd_replica_read_redirect_conflict,
  l_osd_replica_read_served,

  l_osd_sop,
  l_osd_sop_inb,
  l_osd_sop_lat,
  l_osd_sop_w,
  l_osd_sop_w_inb,
  l_osd_sop_w_lat,
  l_osd_sop_pull,
  l_osd_sop_pull_lat,
  l_osd_sop_push,
  l_osd_sop_push_inb,
  l_osd_sop_push_lat,

  l_osd_pull,
  l_osd_push,
  l_osd_push_outb,

  l_osd_rop,
  l_osd_rbytes,

  l_osd_recovery_push_queue_lat,
  l_osd_recovery_push_reply_queue_lat,
  l_osd_recovery_pull_queue_lat,
  l_osd_recovery_backfill_queue_lat,
  l_osd_recovery_backfill_remove_queue_lat,
  l_osd_recovery_scan_queue_lat,

  l_osd_recovery_queue_lat,
  l_osd_recovery_context_queue_lat,

  l_osd_loadavg,
  l_osd_cached_crc,
  l_osd_cached_crc_adjusted,
  l_osd_missed_crc,

  l_osd_pg,
  l_osd_pg_primary,
  l_osd_pg_replica,
  l_osd_pg_stray,
  l_osd_pg_removing,
  l_osd_hb_to,
  l_osd_map,
  l_osd_mape,
  l_osd_mape_dup,

  l_osd_waiting_for_map,

  l_osd_map_cache_hit,
  l_osd_map_cache_miss,
  l_osd_map_cache_miss_low,
  l_osd_map_cache_miss_low_avg,
  l_osd_map_bl_cache_hit,
  l_osd_map_bl_cache_miss,

  l_osd_stat_bytes,
  l_osd_stat_bytes_used,
  l_osd_stat_bytes_avail,

  l_osd_copyfrom,

  l_osd_tier_promote,
  l_osd_tier_flush,
  l_osd_tier_flush_fail,
  l_osd_tier_try_flush,
  l_osd_tier_try_flush_fail,
  l_osd_tier_evict,
  l_osd_tier_whiteout,
  l_osd_tier_dirty,
  l_osd_tier_clean,
  l_osd_tier_delay,
  l_osd_tier_proxy_read,
  l_osd_tier_proxy_write,

  l_osd_agent_wake,
  l_osd_agent_skip,
  l_osd_agent_flush,
  l_osd_agent_evict,

  l_osd_object_ctx_cache_hit,
  l_osd_object_ctx_cache_total,

  l_osd_op_cache_hit,
  l_osd_tier_flush_lat,
  l_osd_tier_promote_lat,
  l_osd_tier_r_lat,

  l_osd_pg_info,
  l_osd_pg_fastinfo,
  l_osd_pg_biginfo,

  // scrubber related. Here, as the rest of the scrub counters
  // are labeled, and histograms do not fully support labels.
  l_osd_scrub_reservation_dur_hist,

  l_osd_watch_timeouts,

  // scrub I/O (no EC vs. replicated differentiation)
  l_osd_scrub_omapgetheader_cnt,  ///< omap get header calls count
  l_osd_scrub_omapgetheader_bytes,  ///< bytes read by omap get header
  l_osd_scrub_omapget_cnt,      ///< omap get calls count
  l_osd_scrub_omapget_bytes,    ///< total bytes read by omap get

  // ----   scrub I/O - replicated pools
  l_osd_scrub_rppool_getattr_cnt, ///< get_attr calls count
  l_osd_scrub_rppool_stats_cnt, ///< stats calls count
  l_osd_scrub_rppool_read_cnt, ///< read calls count
  l_osd_scrub_rppool_read_bytes, ///< total bytes read

  // ----   scrub I/O - EC
  l_osd_scrub_ec_getattr_cnt, ///< get_attr calls count
  l_osd_scrub_ec_stats_cnt, ///< stats calls count
  l_osd_scrub_ec_read_cnt, ///< read calls count
  l_osd_scrub_ec_read_bytes, ///< total bytes read

  // ----   scrub - replicated pools
  l_osd_scrub_rppool_started, ///< scrubs that got started
  l_osd_scrub_rppool_active_started, ///< scrubs that got past replicas reservation
  l_osd_scrub_rppool_successful, ///< successful scrubs count
  l_osd_scrub_rppool_successful_elapsed, ///< time to complete a successful scrub
  l_osd_scrub_rppool_failed, ///< failed scrubs count
  l_osd_scrub_rppool_failed_elapsed, ///< time from start to failure

  // ----   scrub reservation process - replicated pools

  /// successful replicas reservation count
  l_osd_scrub_rppool_reserv_success,
  /// time to complete a successful replicas reservation
  l_osd_scrub_rppool_reserv_successful_elapsed,
  /// failed attempt to reserve replicas due to an abort
  l_osd_scrub_rppool_reserv_aborted,
  /// reservation failed due to a 'rejected' response
  l_osd_scrub_rppool_reserv_rejected,
  /// reservation skipped for high-priority scrubs
  l_osd_scrub_rppool_reserv_skipped,
  /// time for a replicas reservation process to fail
  l_osd_scrub_rppool_reserv_failed_elapsed,
  /// number of replicas
  l_osd_scrub_rppool_reserv_secondaries_num,


  // ----   scrub - EC
  l_osd_scrub_ec_started, ///< scrubs that got started
  l_osd_scrub_ec_active_started, /// scrubs that got past secondaries reservation
  l_osd_scrub_ec_successful, ///< successful scrubs count
  l_osd_scrub_ec_successful_elapsed, ///< time to complete a successful scrub
  l_osd_scrub_ec_failed, ///< failed scrubs count
  l_osd_scrub_ec_failed_elapsed, ///< time from start to failure

  // ----   scrub reservation process - EC

  /// successful replicas reservation count
  l_osd_scrub_ec_reserv_success,
  /// time to complete a successful replicas reservation
  l_osd_scrub_ec_reserv_successful_elapsed,
  /// failed attempt to reserve replicas due to an abort
  l_osd_scrub_ec_reserv_aborted,
  /// reservation failed due to a 'rejected' response
  l_osd_scrub_ec_reserv_rejected,
  /// reservation skipped for high-priority scrubs
  l_osd_scrub_ec_reserv_skipped,
  /// time for a replicas reservation process to fail
  l_osd_scrub_ec_reserv_failed_elapsed,
  /// number of replicas
  l_osd_scrub_ec_reserv_secondaries_num,

  l_osd_last,
};

PerfCounters *build_osd_logger(CephContext *cct);

// PeeringState perf counters
enum {
  rs_first = 20000,
  rs_initial_latency,
  rs_started_latency,
  rs_reset_latency,
  rs_start_latency,
  rs_primary_latency,
  rs_peering_latency,
  rs_backfilling_latency,
  rs_waitremotebackfillreserved_latency,
  rs_waitlocalbackfillreserved_latency,
  rs_notbackfilling_latency,
  rs_repnotrecovering_latency,
  rs_repwaitrecoveryreserved_latency,
  rs_repwaitbackfillreserved_latency,
  rs_reprecovering_latency,
  rs_activating_latency,
  rs_waitlocalrecoveryreserved_latency,
  rs_waitremoterecoveryreserved_latency,
  rs_recovering_latency,
  rs_recovered_latency,
  rs_clean_latency,
  rs_active_latency,
  rs_replicaactive_latency,
  rs_stray_latency,
  rs_getinfo_latency,
  rs_getlog_latency,
  rs_waitactingchange_latency,
  rs_incomplete_latency,
  rs_down_latency,
  rs_getmissing_latency,
  rs_waitupthru_latency,
  rs_notrecovering_latency,
  rs_last,
};

PerfCounters *build_recoverystate_perf(CephContext *cct);

// Scrubber perf counters. There are four sets (shallow vs. deep,
// EC vs. replicated) of these counters:
enum {
  scrbcnt_first = 20500,

  // -- interruptions of various types
  /// # preemptions
  scrbcnt_preempted,
  /// # chunks selection performed
  scrbcnt_chunks_selected,
  /// # busy chunks
  scrbcnt_chunks_busy,
  /// # waiting on object events
  scrbcnt_blocked,
  /// # write blocked by the scrub
  scrbcnt_write_blocked,

  scrbcnt_last,
};

PerfCounters *build_scrub_labeled_perf(CephContext *cct, std::string label);
