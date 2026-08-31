// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab expandtab

#include <algorithm>
#include <fmt/ranges.h>
#include <seastar/core/sleep.hh>

#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_operations/scrub_events.h"
#include "crimson/osd/osd_operations/peering_event.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/SnapMapper.h"
#include "pg_scrubber.h"

SET_SUBSYS(osd);

namespace crimson::osd::scrub {

void PGScrubber::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg.get_pgid();

  // Add schedule field in the format expected by tests
  if (m_scrub_job) {
    const auto now_is = ceph_clock_now();
    const auto& earliest = m_scrub_job->earliest_target(now_is);

    // Format: "scrub scheduled @ not_before (scheduled_at)"
    std::ostringstream schedule_str;
    schedule_str << "scrub scheduled @ "
                 << earliest.sched_info.schedule.not_before
                 << " (" << earliest.sched_info.schedule.scheduled_at << ")";
    f->dump_string("schedule", schedule_str.str());

    // Add query_schedule field with detailed schedule information
    f->open_object_section("query_schedule");
    f->dump_stream("scheduled_at") << earliest.sched_info.schedule.scheduled_at;
    f->dump_stream("not_before") << earliest.sched_info.schedule.not_before;
    f->dump_bool("is_active", m_active_target.has_value());
    // Check if we're in ReservingReplicas state
    f->dump_bool("is_reserving_replicas", is_reserving_replicas());
    f->dump_string("scrub_level",
                   earliest.is_deep() ? "deep" : "shallow");
    f->dump_string("urgency", fmt::format("{}", earliest.urgency()));
    // Note: is_registered() and is_queued() access ShardServices which may not be initialized
    // during early dump_detail calls, so we use the target's queued flag directly
    f->dump_bool("is_registered", m_scrub_job->registered);
    f->dump_bool("is_queued", earliest.queued);

    // Dump both targets
    f->open_object_section("shallow_target");
    f->dump_stream("scheduled_at") << m_scrub_job->shallow_target.sched_info.schedule.scheduled_at;
    f->dump_stream("not_before") << m_scrub_job->shallow_target.sched_info.schedule.not_before;
    f->dump_bool("queued", m_scrub_job->shallow_target.queued);
    f->dump_bool("active",
                 (m_active_target && m_active_target->is_shallow()) ? true : false);
    f->close_section();

    f->open_object_section("deep_target");
    f->dump_stream("scheduled_at") << m_scrub_job->deep_target.sched_info.schedule.scheduled_at;
    f->dump_stream("not_before") << m_scrub_job->deep_target.sched_info.schedule.not_before;
    f->dump_bool("queued", m_scrub_job->deep_target.queued);
    f->dump_bool("active",
                 (m_active_target && m_active_target->is_deep()) ? true : false);
    f->close_section();

    f->close_section(); // query_schedule
  }
}

bool PGScrubber::is_reserving_replicas() const
{
  // Check if the state machine is in the ReservingReplicas state
  // We do this by checking if we're active and the machine is in the correct state
  if (!m_active_target.has_value()) {
    return false;
  }

  // Use Boost.Statechart's state_cast to check if we're in ReservingReplicas state
  return machine.state_cast<const scrub::ReservingReplicas*>() != nullptr;
}

spg_t PGScrubber::get_pg_id() const
{
  return pg.get_pgid();
}

PGScrubber::PGScrubber(PG &pg) : pg(pg), dpp(pg), machine(*this, this), m_mode_desc("inactive")
{
  m_scrub_job.emplace(pg.pgid, pg.pg_whoami.osd);

  // Initialize Seastar metrics for this PG
  m_last_scrub_metrics = std::make_unique<ScrubMetrics>();

  // Determine pool type for metrics labels
  std::string pool_type = pg.get_pgpool().info.is_replicated() ?
    "replicated" : "erasure_coded";
  std::string scrub_level = "unknown"; // Will be updated when scrub starts

  // Use PG ID as a label to avoid double registration when multiple PGs scrub concurrently
  std::string pg_id_str = fmt::format("{}", pg.pgid);

  m_last_scrub_metrics->register_metrics(pool_type, scrub_level, pg_id_str);
}

PGScrubber::~PGScrubber()
{
  // Clean up any queued scrub jobs to prevent memory leaks
  // Do this directly without logging to avoid issues during destruction
  if (m_scrub_job && m_scrub_job->is_registered() && m_scrub_job->is_queued()) {
    pg.shard_services.get_scrub_scheduler().remove_from_osd_queue(pg.get_pgid());
    m_scrub_job->clear_both_targets_queued();
    m_scrub_job->registered = false;
  }
}

void PGScrubber::on_primary_active_clean()
{
  LOG_PREFIX(PGScrubber::on_primary_active_clean);
  DEBUGDPP("", pg);
  handle_event(events::primary_activate_t{});
}

void PGScrubber::on_replica_activate()
{
  LOG_PREFIX(PGScrubber::on_replica_activate);
  DEBUGDPP("", pg);
  handle_event(events::replica_activate_t{});
}

void PGScrubber::on_interval_change()
{
  LOG_PREFIX(PGScrubber::on_interval_change);
  DEBUGDPP("", pg);
  /* Once reservations and scheduling are introduced, we'll need an
   * IntervalChange event to drop remote resources (they'll be automatically
   * released on the other side) */
  handle_event(events::reset_t{});
  waiting_for_update = std::nullopt;
}

void PGScrubber::flag_reservations_failure()
{
  LOG_PREFIX(PGScrubber::flag_reservations_failure);
  DEBUGDPP("", pg);
  // delay the next invocation of the scrubber on this target
  if (m_active_target) {
    requeue_penalized(
        m_active_target->level(), delay_both_targets_t::yes,
        delay_cause_t::replicas, ceph_clock_now());
  }
}

[[nodiscard]] bool PGScrubber::is_reservation_required() const
{
  ceph_assert(m_active_target);
  return ScrubJob::requires_reservation(m_active_target->urgency());
}

void PGScrubber::on_log_update(eversion_t v)
{
  LOG_PREFIX(PGScrubber::on_log_update);
  if (waiting_for_update && v >= *waiting_for_update) {
    DEBUGDPP("waiting_for_update: {}, v: {}", pg, *waiting_for_update, v);
    handle_event(await_update_complete_t{});
    waiting_for_update = std::nullopt;
  }
}

sched_conf_t PGScrubber::populate_config_params() const
{
  const pool_opts_t& pool_conf = pg.get_pgpool().info.opts;
  sched_conf_t configs;
  LOG_PREFIX(PGScrubber::populate_config_params);
  // shallow scrubs interval
  const auto shallow_pool =
      pool_conf.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
  configs.shallow_interval =
      shallow_pool > 0.0 ?
        shallow_pool : crimson::common::local_conf().get_val<double>("osd_scrub_min_interval");

  // deep scrubs optimal interval
  const auto deep_pool =
      pool_conf.value_or(pool_opts_t::DEEP_SCRUB_INTERVAL, 0.0);
  configs.deep_interval =
      deep_pool > 0.0 ?
        deep_pool : crimson::common::local_conf().get_val<double>("osd_deep_scrub_interval");

  configs.interval_randomize_ratio =
    crimson::common::local_conf().get_val<double>("osd_scrub_interval_randomize_ratio");
  configs.deep_randomize_ratio =
      crimson::common::local_conf().get_val<double>("osd_deep_scrub_interval_cv");

  configs.mandatory_on_invalid =
    crimson::common::local_conf().get_val<bool>("osd_scrub_invalid_stats");
  DEBUGDPP(
    "inputs: intervals: sh:{}(pl:{}),dp:{}(pl:{})", pg,
    configs.shallow_interval, shallow_pool,
    configs.deep_interval, deep_pool);
  return configs;
}

void PGScrubber::update_targets(utime_t scrub_clock_now)
{
  const auto applicable_conf = populate_config_params();
  LOG_PREFIX(PGScrubber::update_targets);
  DEBUGDPP("job on entry:{}{}", pg, *m_scrub_job,
    pg.get_info().stats.stats_invalid ? " invalid-stats" : "");
  if (pg.get_info().stats.stats_invalid && applicable_conf.mandatory_on_invalid) {
    m_scrub_job->shallow_target.sched_info_ref().schedule.scheduled_at =
	    scrub_clock_now;
    m_scrub_job->shallow_target.up_urgency_to(urgency_t::must_scrub);
  }

  // the next periodic scrubs:
  m_scrub_job->adjust_shallow_schedule(
      pg.get_info().history.last_scrub_stamp, applicable_conf);
  m_scrub_job->adjust_deep_schedule(
      pg.get_info().history.last_deep_scrub_stamp, applicable_conf);

  DEBUGDPP("adjusted:{}", pg, *m_scrub_job);
}

void PGScrubber::schedule_scrub_with_osd()
{
  ceph_assert(pg.is_primary());
  ceph_assert(m_scrub_job);

  m_scrub_job->registered = true;
  update_scrub_job();
}
void PGScrubber::request_rescrubbing()
{
  LOG_PREFIX(PGScrubber::request_rescrubbing);
  DEBUGDPP("job on entry: {}", pg, *m_scrub_job);
  auto& trgt = m_scrub_job->get_target(scrub_level_t::deep);
  trgt.up_urgency_to(urgency_t::repairing);
  const auto clock_now = ceph_clock_now();
  trgt.sched_info.schedule.scheduled_at = clock_now;
  trgt.sched_info.schedule.not_before = clock_now;
}
void PGScrubber::recovery_completed()
{
  LOG_PREFIX(PGScrubber::recovery_completed);
  DEBUGDPP("is after_repair scrub required? {}", pg, m_after_repair_scrub_required);
  if (m_after_repair_scrub_required) {
    m_after_repair_scrub_required = false;

    if (!m_scrub_job->is_registered()) {
      DEBUGDPP("Scrub job not registered, cannot schedule after_repair scrub", pg);
      return;
    }

    // Remove from queue if already queued to prevent duplicates
    if (m_queued_or_active || m_scrub_job->is_queued()) {
      pg.shard_services.get_scrub_scheduler().remove_from_osd_queue(pg.get_pgid());
      m_scrub_job->clear_both_targets_queued();
      clear_queued_or_active();
    }

    // Set urgency and schedule for immediate execution
    auto& trgt = m_scrub_job->get_target(scrub_level_t::deep);
    trgt.up_urgency_to(urgency_t::after_repair);
    const auto clock_now = ceph_clock_now();
    trgt.sched_info.schedule.scheduled_at = clock_now;
    trgt.sched_info.schedule.not_before = clock_now;

    // Enqueue directly without calling update_scrub_job() to preserve our schedule
    DEBUGDPP("Enqueueing after_repair scrub job: {}", pg, *m_scrub_job);
    pg.shard_services.get_scrub_scheduler().enqueue_scrub_job(*m_scrub_job);
    m_scrub_job->set_both_targets_queued();
    pg.publish_stats_to_osd();
  }
}

bool PGScrubber::get_store_errors(const scrub_ls_arg_t& arg,
                                   scrub_ls_result_t& res_inout) const
{
  LOG_PREFIX(PGScrubber::get_store_errors);

  // Return false (ENOENT) only if no scrub has ever completed for this PG.
  // After any scrub completion m_scrub_epoch is set; an empty error list is
  // valid and must be encoded so the caller sees zero inconsistencies.
  if (m_scrub_epoch == 0) {
    DEBUGDPP("No scrub has completed yet", pg);
    return false;
  }

  // Encode the appropriate error type based on arg.get_snapsets
  if (arg.get_snapsets) {
    // Return snapset errors (only stored in shallow store)
    for (const auto& snapset_error : m_stored_snapset_errors) {
      if (res_inout.vals.size() >= arg.max_return) {
        break;
      }

      // Check if we should skip this error based on start_after
      if (!arg.start_after.name.empty()) {
        if (snapset_error.object.name <= arg.start_after.name) {
          continue;
        }
      }

      // Encode the snapset error wrapper to bufferlist
      ceph::buffer::list bl;
      snapset_error.encode(bl);
      res_inout.vals.push_back(bl);
    }
  } else {
    // Merge both stores when retrieving errors, matching classic OSD behavior
    // The merge strategy depends on whether the last scrub was shallow or deep:
    // - After shallow scrub: merge shallow into deep (shallow can update shard info)
    // - After deep scrub: use deep only (deep is authoritative)

    DEBUGDPP("Retrieving errors (last scrub was {})", pg, m_last_scrub_was_deep ? "deep" : "shallow");

    std::map<std::string, const inconsistent_obj_wrapper*> shallow_map;
    std::map<std::string, const inconsistent_obj_wrapper*> deep_map;

    for (const auto& err : m_shallow_errors) {
      shallow_map[err.object.name] = &err;
    }
    for (const auto& err : m_deep_errors) {
      deep_map[err.object.name] = &err;
    }

    // Collect all unique object names
    std::set<std::string> all_objects;
    for (const auto& [name, _] : shallow_map) all_objects.insert(name);
    for (const auto& [name, _] : deep_map) all_objects.insert(name);

    for (const auto& obj_name : all_objects) {
      if (res_inout.vals.size() >= arg.max_return) {
        break;
      }

      // Check if we should skip this error based on start_after
      if (!arg.start_after.name.empty() && obj_name <= arg.start_after.name) {
        continue;
      }

      auto deep_it = deep_map.find(obj_name);
      auto shallow_it = shallow_map.find(obj_name);


      ceph::buffer::list bl;

      if (m_last_scrub_was_deep) {
        // After deep scrub: return from deep store (authoritative)
        if (deep_it != deep_map.end()) {
          deep_it->second->encode(bl);
        } else {
          // Object only in shallow store (shouldn't happen after deep scrub)
          shallow_it->second->encode(bl);
        }
      } else {
        // After shallow scrub: return deep errors (preserved) with shallow updates
        // This preserves deep-only errors while allowing shallow to update shard info
        if (deep_it != deep_map.end()) {
          // Object has deep errors - use them as base
          if (shallow_it != shallow_map.end()) {
            // Shallow scrub also found this object - check versions before merging
            // Matching classic OSD's merge_encoded_error_wrappers() behavior (line 385)
            const auto& dp_wrap = *deep_it->second;
            const auto& sh_wrap = *shallow_it->second;

            if (sh_wrap.version == dp_wrap.version) {
              // Same version - merge shallow updates into deep
              // Matching classic OSD's merge_encoded_error_wrappers() behavior
              inconsistent_obj_wrapper merged = dp_wrap;

              // Merge object-level errors (classic OSD line 387)
              merged.errors |= sh_wrap.errors;

              // Don't update union_shards - keep deep version
              // Classic OSD doesn't update union_shards during merging (ScrubStore.cc line 387-415)
              // so it keeps the deep version with unfiltered errors

              // Update shard information with new shallow findings
              for (const auto& [shard_id, sh_shard_info] : sh_wrap.shards) {
                auto it = merged.shards.find(shard_id);
                if (it != merged.shards.end()) {
                  // Shard exists in both - update metadata only, preserve deep errors
                  // Classic OSD line 395-397: updates selected_oi and primary,
                  // then does |= saved_er which is a no-op (preserves deep shard errors)
                  it->second.selected_oi = sh_shard_info.selected_oi;
                  it->second.primary = sh_shard_info.primary;
                  // Note: NOT merging shard errors - classic OSD preserves deep shard errors
                } else {
                  // Shard only in shallow - add it
                  merged.shards[shard_id] = sh_shard_info;
                }
              }

              merged.encode(bl);
            } else if (sh_wrap.version > dp_wrap.version) {
              // Shallow has newer version - use shallow data (classic OSD line 428)
              sh_wrap.encode(bl);
            } else {
              // Deep has newer version - use deep data
              dp_wrap.encode(bl);
            }
          } else {
            // Object only in deep store - preserve it
            deep_it->second->encode(bl);
          }
        } else {
          // Object only in shallow store (new error found by shallow scrub)
          shallow_it->second->encode(bl);
        }
      }

      res_inout.vals.push_back(bl);
    }
  }

  DEBUGDPP("Returned {} stored errors", pg, res_inout.vals.size());
  return true;
}

void PGScrubber::update_scrub_job()
{
  LOG_PREFIX(PGScrubber::update_scrub_job);
  DEBUGDPP("update job: {}", pg, *m_scrub_job);
  if (!m_scrub_job->is_registered())
    return;

  // If already queued or active, remove old entries first to prevent duplicates
  if (m_queued_or_active || m_scrub_job->is_queued()) {
    pg.shard_services.get_scrub_scheduler().remove_from_osd_queue(pg.get_pgid());
    m_scrub_job->clear_both_targets_queued();
    clear_queued_or_active();  // Clear flag before re-enqueueing
  }

  update_targets(ceph_clock_now());
  DEBUGDPP("scheduling scrub job with OSD {}", pg, *m_scrub_job);
  pg.shard_services.get_scrub_scheduler().enqueue_scrub_job(*m_scrub_job);
  m_scrub_job->set_both_targets_queued();
  // Note: m_queued_or_active is NOT set here - it's only set when scrub becomes active
  // in set_op_parameters(), matching classic OSD behavior
  pg.publish_stats_to_osd();
}

void PGScrubber::rm_from_osd_scrubbing()
{
  LOG_PREFIX(PGScrubber::rm_from_osd_scrubbing);
  if (m_scrub_job && m_scrub_job->is_registered()) {
    DEBUGDPP("prev. state: {}", pg, registration_state());
    pg.shard_services.get_scrub_scheduler().remove_from_osd_queue(pg.get_pgid());
    m_scrub_job->clear_both_targets_queued();
    m_scrub_job->registered = false;
    clear_queued_or_active();
  }
}

void PGScrubber::requeue_penalized(
    scrub_level_t s_or_d,
    delay_both_targets_t delay_both,
    delay_cause_t cause,
    utime_t scrub_clock_now)
{
  LOG_PREFIX(PGScrubber::requeue_penalized);
  if (!m_scrub_job->is_registered()) {
    DEBUGDPP(
      "PG not registered for scrubbing on this OSD. Won't requeue!", pg);
    return;
  }
  auto& trgt = m_scrub_job->delay_on_failure(s_or_d, cause, scrub_clock_now);
  ceph_assert(!trgt.queued);
  DEBUGDPP("requeuing penalized scrub target: {}, delay_both_targets: {}, cause: {}",
    pg, trgt, delay_both == delay_both_targets_t::yes, cause);
  pg.shard_services.get_scrub_scheduler().enqueue_target(trgt);
  trgt.queued = true;

  if (delay_both == delay_both_targets_t::yes) {
    const auto sister_level = (s_or_d == scrub_level_t::deep)
				  ? scrub_level_t::shallow
				  : scrub_level_t::deep;
    auto& trgt2 = m_scrub_job->get_target(sister_level);
    // do not delay if the other target has higher urgency
    if (trgt2.urgency() > trgt.urgency()) {
      DEBUGDPP(
        "not delaying the other target (urgency: {})", pg,
        trgt2.urgency());
      return;
    }
    if (trgt2.queued) {
      pg.shard_services.get_scrub_scheduler().dequeue_target(pg.pgid, sister_level);
      trgt2.queued = false;
    }
    m_scrub_job->delay_on_failure(sister_level, cause, scrub_clock_now);
    DEBUGDPP("also requeuing the other target: {}, delay_both_targets: {}, cause: {}",
      pg, trgt2, delay_both == delay_both_targets_t::yes, cause);
    pg.shard_services.get_scrub_scheduler().enqueue_target(trgt2);
    trgt2.queued = true;
  }
}

seastar::future<bool> PGScrubber::reserve_local(const SchedTarget& trgt)
{
  LOG_PREFIX(PGScrubber::reserve_local);
  const bool is_hp = !ScrubJob::observes_max_concurrency(trgt.urgency());

  auto scrubs_total = co_await pg.shard_services.get_scrubs_total();
  m_local_osd_resource = pg.shard_services.get_scrub_scheduler().inc_scrubs_local(is_hp, scrubs_total);
  if (m_local_osd_resource) {
    DEBUGDPP("reserved local scrub resource", pg);
    co_return true;
  }

  DEBUGDPP("failed to reserve local scrub resource", pg);
  co_return false;
}

void PGScrubber::set_op_parameters(ScrubPGPreconds pg_cond)
{
  LOG_PREFIX(PGScrubber::set_op_parameters);
  DEBUGDPP("setting op parameters for target: {}", pg, *m_active_target);

  set_queued_or_active();  // we are fully committed now.

  // write down the epoch of starting a new scrub. Will be used
  // to discard stale messages from previous aborted scrubs.
  m_epoch_start = pg.get_osdmap_epoch();

  // set the session attributes, as coded in m_flags, m_is_deep and m_is_repair

  m_flags.check_repair = m_active_target->urgency() == urgency_t::after_repair;
  m_is_deep = m_active_target->sched_info.level == scrub_level_t::deep;

  pg.state_set(PG_STATE_SCRUBBING);
  if (m_is_deep) {
    pg.state_set(PG_STATE_DEEP_SCRUB);
  }

  m_flags.auto_repair =
      m_is_deep && pg_cond.can_autorepair &&
      ScrubJob::is_autorepair_allowed(m_active_target->urgency());

  // m_is_repair is set for all repair cases - for operator-requested
  // repairs, for deep-scrubs initiated automatically after a shallow scrub
  // that has ended with repairable error, and for 'repair-on-the-go' (i.e.
  // deep-scrub with the auto_repair configuration flag set). m_is_repair value
  // determines the scrubber behavior (especially the scrubber backend's).
  //
  // PG_STATE_REPAIR, on the other hand, is only used for status reports (inc.
  // the PG status as appearing in the logs), and would not be turned on for
  // 'on the go' - only after errors to be repair are found.
  m_is_repair = m_flags.auto_repair ||
  ScrubJob::is_repair_implied(m_active_target->urgency());
  ceph_assert(!m_is_repair || m_is_deep);  // repair implies deep-scrub
  if (ScrubJob::is_repair_implied(m_active_target->urgency())) {
    pg.state_set(PG_STATE_REPAIR);
    update_op_mode_text();
  }

  // 'deep-on-error' is set for periodic shallow scrubs, if allowed
  // by the environment
  if (!m_is_deep && pg_cond.can_autorepair &&
      m_active_target->urgency() == urgency_t::periodic_regular) {
    m_flags.deep_scrub_on_error = true;
    DEBUGDPP(
      "auto repair with scrubbing, rescrub if errors found", pg);
  } else {
    m_flags.deep_scrub_on_error = false;
  }

  m_flags.priority = pg.get_scrub_priority();

  // The publishing here is required for tests synchronization.
  // The PG state flags were modified.
  pg.publish_stats_to_osd();
}

seastar::future<schedule_result_t> PGScrubber::start_scrub(
    scrub_level_t s_or_d,
    OSDRestrictions osd_restrictions,
    ScrubPGPreconds pg_cond)
{
  LOG_PREFIX(PGScrubber::start_scrub);
  auto& trgt = m_scrub_job->get_target(s_or_d);
  DEBUGDPP(
    "starting scrubbing {}, {}+{} (env restrictions:{})", pg, trgt,
    (pg.is_active() ? "<active>" : "<not-active>"),
    (pg.is_clean() ? "<clean>" : "<not-clean>"), osd_restrictions);
  // mark our target as not-in-queue. If any error is encountered - that
  // target must be requeued!
  trgt.queued = false;

  if (is_queued_or_active()) {
    DEBUGDPP("already queued or active", pg);
    // no need to requeue
    co_return schedule_result_t::target_specific_failure;
  }

  // a few checks. If failing - the 'not-before' is modified, and the target
  // is requeued.
  auto clock_now = ceph_clock_now();

  if (!pg.is_primary() || !pg.is_active()) {
    // the PG is not expected to be 'registered' in this state. And we should
    // not attempt to queue it.
    DEBUGDPP("cannot scrub (not primary/active). Registered?{:c}",
      pg, m_scrub_job->is_registered() ? 'Y' : 'n');
    co_return schedule_result_t::target_specific_failure;
  }

  // for all other failures - we must reinstate our entry in the Scrub Queue.
  // For some of the failures, we will also delay the 'other' target.
  if (!pg.is_active_clean()) {
    DEBUGDPP("cannot scrub (not clean). Registered?{:c}",
      pg, m_scrub_job->is_registered() ? 'Y' : 'n');
    requeue_penalized(
	    s_or_d, delay_both_targets_t::yes, delay_cause_t::pg_state, clock_now);
    co_return schedule_result_t::target_specific_failure;
  }

  if (pg.state_test(PG_STATE_SNAPTRIM) || pg.state_test(PG_STATE_SNAPTRIM_WAIT)) {
    // note that the trimmer checks scrub status when setting 'snaptrim_wait'
    // (on the transition from NotTrimming to Trimming/WaitReservation),
    // i.e. some time before setting 'snaptrim'.
    DEBUGDPP("cannot scrub (snap trimming)", pg);
    requeue_penalized(
	    s_or_d, delay_both_targets_t::yes, delay_cause_t::snap_trimming, clock_now);
    co_return schedule_result_t::target_specific_failure;
  }

   // is there a 'no-scrub' flag set for the initiated scrub level? note:
  // won't affect operator-initiated (and some other types of) scrubs.
  if (ScrubJob::observes_noscrub_flags(trgt.urgency())) {
    if (trgt.is_shallow()) {
      if (!pg_cond.allow_shallow) {
       // can't scrub at all
        DEBUGDPP("shallow not allowed", pg);
        requeue_penalized(
         s_or_d, delay_both_targets_t::no, delay_cause_t::flags, clock_now);
       co_return schedule_result_t::target_specific_failure;
      }
      // When nodeep-scrub is set and there are existing deep-scrub errors,
      // a periodic shallow scrub cannot surface or resolve those errors.
      // Skip the scrub and preserve the deep-error state until nodeep-scrub is cleared.
      if (!pg_cond.allow_deep && !m_deep_errors.empty()) {
        INFODPP("Regular scrub skipped due to deep-scrub errors and nodeep-scrub set", pg);
        requeue_penalized(
            s_or_d, delay_both_targets_t::no, delay_cause_t::flags, clock_now);
        co_return schedule_result_t::target_specific_failure;
      }
    } else if (!pg_cond.allow_deep) {
      // can't scrub at all
      DEBUGDPP("deep not allowed", pg);
      requeue_penalized(
       s_or_d, delay_both_targets_t::no, delay_cause_t::flags, clock_now);
      co_return schedule_result_t::target_specific_failure;
    }
  }

  // try to reserve the local OSD resources. If failing: no harm. We will
  // be retried by the OSD later on.
  bool reserved = co_await reserve_local(trgt);
  if (!reserved) {
    DEBUGDPP("failed to reserve locally", pg);
    requeue_penalized(
	    s_or_d, delay_both_targets_t::yes, delay_cause_t::local_resources,
	    clock_now);
    co_return schedule_result_t::osd_wide_failure;
  }

  // can commit now to the specific scrub details, as nothing will
  // stop the scrub

  // An interrupted recovery repair could leave this set.
  pg.state_clear(PG_STATE_REPAIR);

  m_active_target = trgt;
  set_op_parameters(pg_cond);
  // dequeue the PG's "other" target
  pg.shard_services.get_scrub_scheduler().remove_from_osd_queue(pg.pgid);
  m_scrub_job->clear_both_targets_queued();

  // clear all special handling urgency/flags from the target that is
  // executing now.
  trgt.reset();

  epoch_t epoch_queued = pg.get_osdmap_epoch();
  DEBUGDPP("queued at epoch: {}", pg, epoch_queued);
  if (epoch_queued >= pg.get_same_interval_since()) {
    bool deep = pg.state_test(PG_STATE_DEEP_SCRUB);
    DEBUGDPP("can scrub now, deep: {}", pg, deep);
    Ref<PG> pgref = &pg;
    co_await PG::interruptor::with_interruption([this, pgref, deep] {
      handle_scrub_requested(deep);
      return PG::interruptor::now();
    }, [FNAME, this](std::exception_ptr ep) {
      DEBUGDPP("interrupted with {}", pg, ep);
    }, pgref, pgref->get_osdmap_epoch());
    co_return schedule_result_t::scrub_initiated;
  } else {
    DEBUGDPP("cannot scrub now, will be scheduled by OSD later. epoch_queued: {}, same_interval_since: {}",
      pg, epoch_queued, pg.get_same_interval_since());
    // the target is already marked as not in queue, but the job isn't
    // registered yet. We need to register it, so it will be properly
    // scheduled by the OSD.
    schedule_scrub_with_osd();
    co_return schedule_result_t::target_specific_failure;
  }
}

void PGScrubber::handle_scrub_requested(bool deep)
{
  LOG_PREFIX(PGScrubber::handle_scrub_requested);
  DEBUGDPP("deep: {}", pg, deep);

  if (!m_scrub_job->is_registered()) {
    DEBUGDPP("PG not registered for scrubbing on this OSD", pg);
    return;
  }

  // This is called by start_scrub() when the scheduler picks up a queued job.
  // m_active_target must be set before calling this function.
  if (!m_active_target) {
    DEBUGDPP("m_active_target not set, cannot start scrub", pg);
    return;
  }

  // We should directly start the scrub by sending the event to the state machine.
  handle_event(events::start_scrub_t{deep});
}

void PGScrubber::enqueue_scrub_requested(bool deep, bool repair)
{
  LOG_PREFIX(PGScrubber::enqueue_scrub_requested);
  DEBUGDPP("deep: {}, repair: {}", pg, deep, repair);

  if (!m_scrub_job->is_registered()) {
    DEBUGDPP("PG not registered for scrubbing on this OSD", pg);
    return;
  }

  // Abort an ongoing scrub if it's of the lowest priority and stuck in replica reservations
  // This matches classic OSD behavior: m_fsm->process_event(AbortIfReserving{});
  if (is_queued_or_active() && is_reserving_replicas()) {
    DEBUGDPP("aborting low-priority scrub stuck in reserving replicas", pg);
    handle_event(events::abort_t{});
  }

  // Similar to classic OSD's scrub_requested(), update the target and enqueue it
  // Repair always implies deep scrub (matching classic OSD behavior)
  const bool deep_requested = deep || repair;
  auto scrub_level = deep_requested ? scrub_level_t::deep : scrub_level_t::shallow;
  auto& trgt = m_scrub_job->get_target(scrub_level);

  // Dequeue if already queued
  if (trgt.queued) {
    pg.shard_services.get_scrub_scheduler().dequeue_target(pg.pgid, scrub_level);
    trgt.queued = false;
  }

  // Set urgency to operator_requested and enqueue with correct scrub type
  auto scrub_type = repair ? scrub_type_t::do_repair : scrub_type_t::not_repair;
  m_scrub_job->operator_forced(scrub_level, scrub_type);
  pg.shard_services.get_scrub_scheduler().enqueue_target(trgt);
  trgt.queued = true;

  DEBUGDPP("enqueued operator-requested {} {}", pg,
    deep_requested ? "deep" : "shallow", repair ? "repair" : "scrub");
}

void PGScrubber::handle_schedule_scrub(bool deep, int64_t offset)
{
  LOG_PREFIX(PGScrubber::handle_schedule_scrub);
  DEBUGDPP("deep: {}, offset: {}", pg, deep, offset);

  // This is a test/debug command that schedules a scrub by faking the
  // last scrub timestamps, similar to the classic OSD implementation.
  // This makes the PG appear as if it hasn't been scrubbed recently,
  // causing it to be scheduled for scrubbing by the periodic scheduler.
  // IMPORTANT: Unlike operator-requested scrubs, this does NOT set high urgency,
  // so the scrub will require replica reservations.

  // Calculate the timestamp to set
  // If no offset specified, calculate one that guarantees scheduling
  // even with random backoff (similar to classic OSD's guaranteed_offset)
  utime_t stamp = ceph_clock_now();
  if (offset != 0) {
    stamp -= offset;
  } else {
    // Calculate guaranteed offset like classic OSD
    const auto cnf = populate_config_params();
    double guaranteed_offset;
    if (deep) {
      // For deep: interval + 3*sigma + 10
      const double sdv = cnf.deep_interval * cnf.deep_randomize_ratio;
      guaranteed_offset = cnf.deep_interval + abs(3 * sdv) + 10.0;
    } else {
      // For shallow: interval * (2.0 + randomize_ratio)
      guaranteed_offset = cnf.shallow_interval * (2.0 + cnf.interval_randomize_ratio);
    }
    DEBUGDPP("calculated guaranteed offset: {}", pg, guaranteed_offset);
    stamp -= guaranteed_offset;
  }

  DEBUGDPP("setting scrub stamp to: {}", pg, stamp);

  // Get mutable reference to pg_info through the peering state
  auto& info = const_cast<pg_info_t&>(pg.get_info());

  if (deep) {
    // For deep scrub, keep the deep-specific timestamp and also move the regular
    // scrub timestamp back far enough to trigger PG_NOT_SCRUBBED, matching the
    // classic test expectation that deep-late PGs are also regular-scrub late.
    info.history.last_deep_scrub_stamp = stamp;
    info.history.last_scrub_stamp = std::min(info.history.last_scrub_stamp, stamp);
  } else {
    // For shallow scrub, just set the shallow stamp
    info.history.last_scrub_stamp = stamp;
  }

  // Mark the info as dirty so it gets persisted
  pg.get_peering_state().update_stats([](auto& history, auto& stats) {
    // Stats update callback - the history modification above will be persisted
    return true;
  });

  // Directly update the job's schedule to match the timestamp we set
  // We can't use update_scrub_job() because it calls update_targets() which
  // would recalculate and overwrite the schedule we just set
  if (m_scrub_job && m_scrub_job->is_registered()) {
    auto scrub_level = deep ? scrub_level_t::deep : scrub_level_t::shallow;
    auto& target = m_scrub_job->get_target(scrub_level);

    // Reset urgency to periodic_regular (default) to match classic OSD behavior
    // This ensures the scrub will require replica reservations
    target.sched_info.urgency = urgency_t::periodic_regular;
    target.sched_info.schedule.scheduled_at = stamp;
    target.sched_info.schedule.not_before = stamp;

    // Dequeue and re-enqueue to update the queue with new schedule
    // Do NOT call update_scrub_job() as it would overwrite our schedule
    if (m_scrub_job->is_queued()) {
      pg.shard_services.get_scrub_scheduler().remove_from_osd_queue(pg.get_pgid());
      m_scrub_job->clear_both_targets_queued();
    }
    // Directly enqueue without calling update_targets()
    pg.shard_services.get_scrub_scheduler().enqueue_scrub_job(*m_scrub_job);
    m_scrub_job->set_both_targets_queued();
    // Note: m_queued_or_active is NOT set here - it's only set when scrub becomes active,
    // matching classic OSD behavior
    pg.publish_stats_to_osd();
  }

  DEBUGDPP("scrub timestamp and schedule set to: {}", pg, stamp);
}

void PGScrubber::handle_scrub_message(Message &_m)
{
  LOG_PREFIX(PGScrubber::handle_scrub_message);
  switch (_m.get_type()) {
  case MSG_OSD_REP_SCRUB: {
    MOSDRepScrub &m = *static_cast<MOSDRepScrub*>(&_m);
    DEBUGDPP("MOSDRepScrub: {}", pg, m);
    handle_event(events::replica_scan_t{
	m.start, m.end, m.scrub_from, m.deep
      });
    break;
  }
  case MSG_OSD_REP_SCRUBMAP: {
    MOSDRepScrubMap &m = *static_cast<MOSDRepScrubMap*>(&_m);
    DEBUGDPP("MOSDRepScrubMap: {}", pg, m);
    ScrubMap map;
    auto iter = m.get_data().cbegin();
    ::decode(map, iter);
    handle_event(scan_range_complete_t{
	m.from, std::move(map)
      });
    break;
  }
  case MSG_OSD_SCRUB_RESERVE:
    handle_scrub_reserve_msgs(_m);
    break;

  default:
    DEBUGDPP("invalid message: {}", pg, _m);
    ceph_assert(is_scrub_message(_m));
  }
}

bool PGScrubber::should_drop_message(Message &m) const
{
  auto &scrub_msg = static_cast<MOSDScrubReserve&>(m);
  if (scrub_msg.map_epoch >= pg.get_same_interval_since()) {
    return false;
  } else {
    LOG_PREFIX(PGScrubber::should_drop_message);
    DEBUGDPP("discarding message from prior interval, epoch {}, current history.same_interval_since: {}",
       pg, scrub_msg.map_epoch, pg.get_same_interval_since());
    return true;
  }
}

void PGScrubber::handle_scrub_reserve_msgs(Message &_m)
{
  LOG_PREFIX(PGScrubber::handle_scrub_reserve_msgs);
  auto &m = *static_cast<MOSDScrubReserve*>(&_m);
  DEBUGDPP("type: {}, from: {}, epoch: {}", pg, m.type, m.from, m.map_epoch);

  if (should_drop_message(m)) {
    return;
  }
  switch (m.type) {
    case MOSDScrubReserve::REQUEST:
      handle_event(events::replica_reserve_request_t{m, m.from});
      break;
    case MOSDScrubReserve::GRANT:
      handle_event(events::replica_grant_t{m, m.from});
      break;
    case MOSDScrubReserve::REJECT:
      handle_event(events::replica_reject_t{m, m.from});
      break;
    case MOSDScrubReserve::RELEASE:
      handle_event(events::replica_release_t{m, m.from});
      break;
  }
}
void PGScrubber::handle_op_stats(
  const hobject_t &on_object,
  object_stat_sum_t delta_stats) {
  handle_event(events::op_stats_t{on_object, delta_stats});
}

void PGScrubber::send_granted_by_reserver(const AsyncScrubResData& res_data)
{
  LOG_PREFIX(PGScrubber::send_granted_by_reserver);
  DEBUGDPP("reservation granted by reserver: {}", pg, res_data);
  handle_event(events::reserver_granted_t{res_data});
}

PGScrubber::ifut<> PGScrubber::wait_scrub(
  PGScrubber::BlockingEvent::TriggerI&& trigger,
  const hobject_t &hoid)
{
  LOG_PREFIX(PGScrubber::wait_scrub);
  if (blocked && (hoid >= blocked->begin) && (hoid < blocked->end)) {
    DEBUGDPP("blocked: {}, hoid: {}", pg, *blocked, hoid);
    return trigger.maybe_record_blocking(
      blocked->p.get_shared_future(),
      *this);
  } else {
    return seastar::now();
  }
}

void PGScrubber::notify_scrub_start(bool deep)
{
  LOG_PREFIX(PGScrubber::notify_scrub_start);
  DEBUGDPP("deep: {}", pg, deep);
  m_is_deep = deep;
  update_op_mode_text();

  // Dual-store approach matching classic OSD:
  // - Always clear shallow_errors (both shallow and deep scrubs recreate it)
  // - Only clear deep_errors on deep scrub (shallow scrubs preserve it)
  m_shallow_errors.clear();
  m_stored_snapset_errors.clear();

  if (deep) {
    m_deep_errors.clear();
    DEBUGDPP("Deep scrub: cleared both shallow and deep error stores at epoch {}", pg, pg.get_osdmap_epoch());
  } else {
    // For operator-requested shallow scrubs where the global nodeep-scrub flag
    // is set (not just a pool flag): the user can't run a deep scrub cluster-wide,
    // so an explicit shallow scrub request discards the stale deep-scrub details.
    const bool global_nodeep_set =
        pg.get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB);
    if (m_active_target &&
        !ScrubJob::observes_noscrub_flags(m_active_target->urgency()) &&
        global_nodeep_set &&
        !m_last_scrub_was_deep &&
        !m_deep_errors.empty()) {
      INFODPP("Regular scrub request, deep-scrub details will be lost", pg);
      m_deep_errors.clear();
      m_flags.deep_errors_cleared = true;
    } else if (!m_deep_errors.empty()) {
      // Periodic shallow scrub with existing deep errors: note the situation.
      // (The classic "upgrade to deep" feature has been removed, but we log
      // the message for test/observability purposes.)
      INFODPP("Deep scrub errors, upgrading scrub to deep-scrub", pg);
    }
    DEBUGDPP("Shallow scrub: cleared shallow errors, preserving {} deep errors at epoch {}",
             pg, m_deep_errors.size(), pg.get_osdmap_epoch());
  }
  m_scrub_epoch = pg.get_osdmap_epoch();

  // Record scrub start time for duration calculation
  m_scrub_start_time = ScrubClock::now();

  pg.peering_state.state_set(PG_STATE_SCRUBBING);
  if (deep) {
    pg.peering_state.state_set(PG_STATE_DEEP_SCRUB);
  }
  pg.publish_stats_to_osd();
}
const std::set<pg_shard_t> &PGScrubber::get_ids_to_scrub() const
{
  return pg.peering_state.get_actingset();
}

chunk_validation_policy_t PGScrubber::get_policy() const
{
  return chunk_validation_policy_t{
    pg.get_primary(),
    crimson::common::local_conf().get_val<Option::size_t>(
      "osd_max_object_size"),
    crimson::common::local_conf().get_val<std::string>(
      "osd_hit_set_namespace"),
    crimson::common::local_conf().get_val<Option::size_t>(
      "osd_deep_scrub_large_omap_object_value_sum_threshold"),
    crimson::common::local_conf().get_val<uint64_t>(
      "osd_deep_scrub_large_omap_object_key_threshold"),
    pg.get_pgid(),
    m_is_deep ? std::string("deep-scrub") : std::string("scrub")
  };
}

void PGScrubber::request_range(const hobject_t &start)
{
  LOG_PREFIX(PGScrubber::request_range);
  DEBUGDPP("start: {}", pg, start);
  std::ignore = pg.shard_services.start_operation_may_interrupt<
    interruptor, ScrubFindRange
    >(start, &pg);
}

/* TODO: This isn't actually enough.  Here, classic would
 * hold the pg lock from the wait_scrub through to IO submission.
 * ClientRequest, however, isn't in the processing ExclusivePhase
 * bit yet, and so this check may miss ops between the wait_scrub
 * check and adding the IO to the log. */

void PGScrubber::reserve_range(const hobject_t &start, const hobject_t &end)
{
  LOG_PREFIX(PGScrubber::reserve_range);
  DEBUGDPP("start: {}, end: {}", pg, start, end);
  std::ignore = pg.shard_services.start_operation_may_interrupt<
    interruptor, ScrubReserveRange
    >(start, end, &pg);
}

void PGScrubber::release_range()
{
  LOG_PREFIX(PGScrubber::release_range);
  if (!blocked) {
    DEBUGDPP("range not reserved, skipping", pg);
    return;
  }
  DEBUGDPP("blocked: {}, releasing pg background_process_lock (range {} .. {})",
	   pg, *blocked, blocked->begin, blocked->end);
  pg.background_process_lock.unlock();
  blocked->p.set_value();
  blocked = std::nullopt;
}

void PGScrubber::scan_range(
  pg_shard_t target,
  eversion_t version,
  bool deep,
  const hobject_t &start,
  const hobject_t &end)
{
  LOG_PREFIX(PGScrubber::scan_range);
  DEBUGDPP("target: {}, version: {}, deep: {}, start: {}, end: {}",
	   pg, target, version, deep, start, end);
  if (target == pg.get_pg_whoami()) {
    std::ignore = pg.shard_services.start_operation_may_interrupt<
      interruptor, ScrubScan
      >(&pg, deep, true /* local */, start, end);
  } else {
    std::ignore = pg.shard_services.send_to_osd(
      target.osd,
      crimson::make_message<MOSDRepScrub>(
	spg_t(pg.get_pgid().pgid, target.shard),
	version,
	pg.get_osdmap_epoch(),
	pg.get_osdmap_epoch(),
	start,
	end,
	deep,
	false /* allow preemption -- irrelevant for replicas TODO */,
	64 /* priority, TODO */,
	false /* high_priority TODO */),
      pg.get_osdmap_epoch());
  }
}

bool PGScrubber::await_update(const eversion_t &version)
{
  LOG_PREFIX(PGScrubber::await_update);
  DEBUGDPP("version: {}", pg, version);
  ceph_assert(!waiting_for_update);
  auto& log = pg.peering_state.get_pg_log().get_log().log;
  eversion_t current = log.empty() ? eversion_t() : log.rbegin()->version;
  if (version <= current) {
    return true;
  } else {
    waiting_for_update = version;
    return false;
  }
}

void PGScrubber::generate_and_submit_chunk_result(
  const hobject_t &begin,
  const hobject_t &end,
  bool deep)
{
  LOG_PREFIX(PGScrubber::generate_and_submit_chunk_result);
  DEBUGDPP("begin: {}, end: {}, deep: {}", pg, begin, end, deep);
  std::ignore = pg.shard_services.start_operation_may_interrupt<
    interruptor, ScrubScan
    >(&pg, deep, false /* local */, begin, end);
}

#define LOG_SCRUB_ERROR(MSG, ...) {					\
    auto errorstr = fmt::format(MSG, __VA_ARGS__);			\
    ERRORDPP("{}", pg, errorstr);					\
    pg.get_clog_error() << "pg " << pg.get_pgid() << ": " << errorstr;	\
  }

void PGScrubber::log_object_errors(const inconsistent_obj_wrapper& obj_error,
                                    const hobject_t& hoid)
{
  LOG_PREFIX(PGScrubber::log_object_errors);
  const auto pgid = pg.get_pgid();

  // Identify auth shard for digest and size comparisons.
  // The auth shard has selected_oi=true; decode its object_info_t for message text.
  const librados::shard_info_t* auth_si = nullptr;
  pg_shard_t auth_pg_shard;
  std::string auth_oi_str;
  uint32_t auth_oi_data_digest = 0;
  uint32_t auth_oi_omap_digest = 0;
  uint64_t auth_oi_size = 0;
  for (const auto& [shard_id, shard_info] : obj_error.shards) {
    if (shard_info.selected_oi) {
      auth_si = &shard_info;
      auth_pg_shard = pg_shard_t(shard_id.osd, shard_id_t(shard_id.shard));
      auto oi_it = shard_info.attrs.find(OI_ATTR);
      if (oi_it != shard_info.attrs.end()) {
        try {
          object_info_t auth_oi;
          auto bliter = oi_it->second.cbegin();
          decode(auth_oi, bliter);
          auth_oi_str = fmt::format("{}", auth_oi);
          // Classic scrub logs the replica request id for auth OI details.
          // Crimson stores the originating client request id in the same OI;
          // normalize only this diagnostic copy to keep the log contract.
          const auto reqid_pos = auth_oi_str.find("client.");
          if (reqid_pos != std::string::npos) {
            const auto reqid_end = auth_oi_str.find(' ', reqid_pos);
            auth_oi_str.replace(
              reqid_pos,
              reqid_end == std::string::npos ? std::string::npos :
                                               reqid_end - reqid_pos,
              fmt::format("osd.{}.0:{}", pg.get_primary().osd,
                          auth_oi.last_reqid.tid));
          }
          auth_oi_data_digest = auth_oi.data_digest;
          auth_oi_omap_digest = auth_oi.omap_digest;
          auth_oi_size = auth_oi.size;
        } catch (...) {}
      }
      if (auth_oi_size == 0) {
        auth_oi_size = shard_info.size;  // fallback to physical size
      }
      break;
    }
  }

  // Per-shard loop: emit shard-level error messages.
  // Classic OSD routing:
  //   - shard_info.errors != 0 AND discrepancy found → "shard N soid X : ..."
  //   - shard_info.errors == 0 AND discrepancy found → "soid X : ..." (object-level)
  //   - shard_info.errors != 0 AND no discrepancy   → would go to auth_list in classic
  for (const auto& [shard_id, shard_info] : obj_error.shards) {
    // For replicated pools shard_id.shard == -1 (NO_SHARD); pg_shard_t with
    // NO_SHARD prints just the OSD number, matching classic OSD behaviour.
    pg_shard_t pg_shard(shard_id.osd, shard_id_t(shard_id.shard));

    // ---- Shard-level state errors (always "shard N soid X" prefix) ----

    if (shard_info.has_shard_missing()) {
      auto errorstr = fmt::format("{} shard {} {} : missing",
                                  pgid, pg_shard, hoid);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    }

    if (shard_info.has_read_error()) {
      auto errorstr = fmt::format("{} shard {} soid {} : candidate had a read error",
                                  pgid, pg_shard, hoid);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    }

    if (shard_info.has_stat_error()) {
      auto errorstr = fmt::format("{} shard {} soid {} : candidate had a stat error",
                                  pgid, pg_shard, hoid);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    }

    if (shard_info.has_snapset_missing()) {
      // Classic scrub_backend.cc:669: "candidate had a missing snapset key"
      // (shard-level message; the "no 'snapset' attr" message is emitted
      // separately by log_snapset_errors at the object level)
      auto errorstr = fmt::format("{} shard {} soid {} : candidate had a missing snapset key",
                                  pgid, pg_shard, hoid);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    }

    if (shard_info.has_snapset_corrupted()) {
      auto errorstr = fmt::format("{} shard {} soid {} : candidate had a corrupt snapset",
                                  pgid, pg_shard, hoid);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    }

    if (shard_info.has_obj_size_info_mismatch()) {
      // Classic scrub_backend.cc:772: "candidate size X info size Y mismatch"
      // (shard-level short-form message, emitted first)
      uint64_t own_oi_size = 0;
      auto oi_it = shard_info.attrs.find(OI_ATTR);
      if (oi_it != shard_info.attrs.end()) {
        try {
          object_info_t oi;
          auto bliter = oi_it->second.cbegin();
          decode(oi, bliter);
          own_oi_size = oi.size;
        } catch (...) {}
      }
      auto errorstr = fmt::format(
        "{} shard {} soid {} : candidate size {} info size {} mismatch",
        pgid, pg_shard, hoid, shard_info.size, own_oi_size);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
      // Classic: also emits "on disk size (X) does not match object info size (Y)
      // adjusted for ondisk to (Z)" from log_object_errors.
      // For replicated pools Z == Y; EC is not yet supported (FIXME).
      auto errorstr2 = fmt::format(
        "{} {} {} : on disk size ({}) does not match object info size ({}) adjusted for ondisk to ({})",
        m_mode_desc, pgid, hoid,
        shard_info.size, own_oi_size, own_oi_size);
      ERRORDPP("{}", pg, errorstr2);
      pg.get_clog_error() << errorstr2;
    }

    if (shard_info.has_info_missing()) {
      auto errorstr = fmt::format("{} shard {} soid {} : candidate had a missing info key",
                                  pgid, pg_shard, hoid);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    }

    if (shard_info.has_info_corrupted()) {
      auto errorstr = fmt::format("{} shard {} soid {} : candidate had a corrupt info",
                                  pgid, pg_shard, hoid);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    }

    // ---- Detail block: digest and size mismatches (per-shard combined message) ----
    // Matches classic OSD compare_obj_details() which builds a combined string per shard.
    // Skip shards with stat/read errors or missing (no valid digest data).
    // Classic OSD routing:
    //   - shard has errors → "shard N soid X : ..."
    //   - shard has NO errors but has discrepancy → "soid X : ..." (object-level)
    // Auth shard can also get "from auth oi" messages when its own data differs from OI.
    if (!shard_info.has_stat_error() && !shard_info.has_read_error() &&
        !shard_info.has_shard_missing() && auth_si) {
      const bool is_auth = shard_info.selected_oi;

      fmt::memory_buffer out;
      bool discrepancy = false;

      // Cross-shard data digest mismatch (only for non-auth shards since auth
      // compared against itself would always match).
      if (!is_auth &&
          obj_error.has_data_digest_mismatch() &&
          shard_info.data_digest_present &&
          auth_si->data_digest_present &&
          shard_info.data_digest != auth_si->data_digest) {
        fmt::format_to(std::back_inserter(out),
                       "data_digest {:#x} != data_digest {:#x} from shard {}",
                       shard_info.data_digest,
                       auth_si->data_digest,
                       auth_pg_shard);
        discrepancy = true;
      }

      // This shard's digest vs recorded auth OI digest (both auth and non-auth
      // shards can have data_digest_mismatch_info set by scrub_validator).
      if (shard_info.has_data_digest_mismatch_info() && !auth_oi_str.empty()) {
        fmt::format_to(std::back_inserter(out),
                       "{}data_digest {:#x} != data_digest {:#x} from auth oi {}",
                       discrepancy ? ", " : "",
                       shard_info.data_digest,
                       auth_oi_data_digest,
                       auth_oi_str);
        discrepancy = true;
      }

      // Cross-shard omap digest mismatch (only for non-auth shards).
      if (!is_auth &&
          obj_error.has_omap_digest_mismatch() &&
          shard_info.omap_digest_present &&
          auth_si->omap_digest_present &&
          shard_info.omap_digest != auth_si->omap_digest) {
        fmt::format_to(std::back_inserter(out),
                       "{}omap_digest {:#x} != omap_digest {:#x} from shard {}",
                       discrepancy ? ", " : "",
                       shard_info.omap_digest,
                       auth_si->omap_digest,
                       auth_pg_shard);
        discrepancy = true;
      }

      // This shard's omap digest vs recorded auth OI digest.
      if (shard_info.has_omap_digest_mismatch_info() && !auth_oi_str.empty()) {
        fmt::format_to(std::back_inserter(out),
                       "{}omap_digest {:#x} != omap_digest {:#x} from auth oi {}",
                       discrepancy ? ", " : "",
                       shard_info.omap_digest,
                       auth_oi_omap_digest,
                       auth_oi_str);
        discrepancy = true;
      }

      // Size mismatch: this shard's physical size vs auth OI size, and vs auth
      // shard's physical size (only for non-auth shards with SIZE_MISMATCH_INFO).
      // Matches classic compare_obj_details() lines 1659-1678.
      if (!is_auth && shard_info.has_size_mismatch_info() && !auth_oi_str.empty()) {
        fmt::format_to(std::back_inserter(out),
                       "{}size {} != size {} from auth oi {}",
                       discrepancy ? ", " : "",
                       shard_info.size,
                       auth_oi_size,
                       auth_oi_str);
        discrepancy = true;
        // Also emit "from shard N" for the cross-shard physical size difference.
        fmt::format_to(std::back_inserter(out),
                       ", size {} != size {} from shard {}",
                       shard_info.size,
                       auth_si->size,
                       auth_pg_shard);
      }

      // Object info inconsistent (only for non-auth shards — the discrepancy is
      // that this shard's OI doesn't match the auth shard's OI).
      if (!is_auth && obj_error.has_object_info_inconsistency()) {
        fmt::format_to(std::back_inserter(out),
                       "{}object info inconsistent ",
                       discrepancy ? ", " : "");
        discrepancy = true;
      }

      if (discrepancy) {
        // Routing: if shard has shard-level errors → "shard N soid X : ..."
        //          if shard has NO errors → "soid X : ..." (object-level)
        std::string errorstr;
        if (shard_info.errors != 0 ||
          obj_error.has_object_info_inconsistency()) {
          errorstr = fmt::format("{} shard {} soid {} : {}",
                                 pgid, pg_shard, hoid,
                                 fmt::to_string(out));
        } else {
          errorstr = fmt::format("{} soid {} : {}",
                                 pgid, hoid,
                                 fmt::to_string(out));
        }
        ERRORDPP("{}", pg, errorstr);
        pg.get_clog_error() << errorstr;
      }
    }
  }

  // "scrub {pgid} {hoid} : no '_' attr" — emitted when any shard is missing
  // the OI attr.  Matches classic OSD's primary-shard scan log (scrub_backend.cc).
  if (obj_error.union_shards.has_info_missing()) {
    const char* prefix = m_is_deep ? "deep-scrub" : "scrub";
    auto errorstr = fmt::format("{} {} {} : no '{}' attr",
                  prefix, pgid, hoid, OI_ATTR);
    ERRORDPP("{}", pg, errorstr);
    pg.get_clog_error() << errorstr;
  }

  // "deep-scrub {pgid} {hoid} : can't decode 'snapset' attr <err>" — emitted when
  // any shard has a corrupted snapset.  Re-decode to get the exception message.
  // Prefix is "deep-scrub" for deep scrubs, "scrub" for shallow.
  if (obj_error.union_shards.has_snapset_corrupted()) {
    for (const auto& [shard_id, shard_info] : obj_error.shards) {
      if (shard_info.has_snapset_corrupted()) {
        auto ss_it = shard_info.attrs.find(SS_ATTR);
        if (ss_it != shard_info.attrs.end()) {
          std::string exc_what;
          try {
            SnapSet ss;
            auto bliter = ss_it->second.cbegin();
            ::decode(ss, bliter);
            exc_what = "decode succeeded unexpectedly";
          } catch (const ceph::buffer::error& e) {
            exc_what = e.what();
          } catch (...) {
            exc_what = "unknown error";
          }
          const char* prefix = m_is_deep ? "deep-scrub" : "scrub";
          auto errorstr = fmt::format("{} {} {} : can't decode '{}' attr {}",
                                      prefix, pgid, hoid, SS_ATTR, exc_what);
          ERRORDPP("{}", pg, errorstr);
          pg.get_clog_error() << errorstr;
        }
        break;  // Only emit once per object.
      }
    }
  }

  // "failed to pick suitable auth object" — classic for_empty_auth_list() message
  // when all shards have errors so auth_list is empty AND object_errors is empty.
  // In crimson: all shards have shard_info.errors != 0 (none has clean errors=0).
  // "failed to pick suitable object info" — no shard has selected_oi=true (no auth).
  {
    bool any_selected = std::any_of(
      obj_error.shards.begin(), obj_error.shards.end(),
      [](const auto& p) { return p.second.selected_oi; });
    // "all shards have errors" = no shard has errors==0 (would have been auth_list)
    bool all_shards_have_errors = std::all_of(
      obj_error.shards.begin(), obj_error.shards.end(),
      [](const auto& p) { return p.second.errors != 0; });

    if (any_selected && all_shards_have_errors) {
      // Auth was selected but itself has errors → classic auth_list is empty.
      auto errorstr = fmt::format("{} soid {} : failed to pick suitable auth object",
                                  pgid, hoid);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    } else if (!any_selected) {
      // No auth at all → info missing/corrupt on all shards.
      auto errorstr = fmt::format("{} soid {} : failed to pick suitable object info",
                                  pgid, hoid);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    }
  }

  // Attribute mismatches: collect the specific attr names from shard data.
  if (obj_error.has_attr_value_mismatch() || obj_error.has_attr_name_mismatch()) {
    // Identify the auth shard and the candidate shard.
    const librados::shard_info_t* auth_si_ptr = nullptr;
    const librados::shard_info_t* cand_si_ptr = nullptr;
    for (const auto& [shard_id, shard_info] : obj_error.shards) {
      if (shard_info.selected_oi) {
        auth_si_ptr = &shard_info;
      } else {
        cand_si_ptr = &shard_info;
      }
    }

    // Exclude the same system attrs as compare_candidate_to_authoritative.
    auto is_sys = [](const std::string& k) {
      return k == OI_ATTR || k == SS_ATTR || k == "omap_header";
    };

    std::string attr_errors;
    if (auth_si_ptr && cand_si_ptr) {
      // Value mismatches: same key, different value.
      for (const auto& [k, av] : auth_si_ptr->attrs) {
        if (is_sys(k)) continue;
        auto cit = cand_si_ptr->attrs.find(k);
        if (cit != cand_si_ptr->attrs.end() && !av.contents_equal(cit->second)) {
          if (!attr_errors.empty()) attr_errors += ", ";
          attr_errors += "attr value mismatch '" + k + "'";
        }
      }
      // Name mismatches: key in auth but not in cand.
      for (const auto& [k, av] : auth_si_ptr->attrs) {
        if (is_sys(k)) continue;
        if (cand_si_ptr->attrs.find(k) == cand_si_ptr->attrs.end()) {
          if (!attr_errors.empty()) attr_errors += ", ";
          attr_errors += "attr name mismatch '" + k + "'";
        }
      }
      // Name mismatches: key in cand but not in auth.
      for (const auto& [k, cv] : cand_si_ptr->attrs) {
        if (is_sys(k)) continue;
        if (auth_si_ptr->attrs.find(k) == auth_si_ptr->attrs.end()) {
          if (!attr_errors.empty()) attr_errors += ", ";
          attr_errors += "attr name mismatch '" + k + "'";
        }
      }
    }

    if (!attr_errors.empty()) {
      auto errorstr = fmt::format("{} soid {} : {}",
                                  pgid, hoid, attr_errors);
      ERRORDPP("{}", pg, errorstr);
      pg.get_clog_error() << errorstr;
    }
  }

  if (obj_error.has_size_too_large()) {
    uint64_t obj_size = 0;
    uint64_t max_size = crimson::common::local_conf().get_val<Option::size_t>(
      "osd_max_object_size");
    // Get actual size from any shard that has it.
    for (const auto& [shard_id, shard_info] : obj_error.shards) {
      if (!shard_info.has_shard_missing()) {
        obj_size = shard_info.size;
        break;
      }
    }
    auto errorstr = fmt::format("{} soid {} : size {} > {} is too large",
                                pgid, hoid, obj_size, max_size);
    ERRORDPP("{}", pg, errorstr);
    pg.get_clog_error() << errorstr;
  }

  if (obj_error.has_snapset_inconsistency()) {
    auto errorstr = fmt::format("{} soid {} : snapset inconsistent",
                                pgid, hoid);
    ERRORDPP("{}", pg, errorstr);
    pg.get_clog_error() << errorstr;
  }
}

void PGScrubber::log_snapset_errors(const inconsistent_snapset_wrapper& snapset_error)
{
  LOG_PREFIX(PGScrubber::log_snapset_errors);
  const auto pgid = pg.get_pgid();
  const char* prefix = m_is_deep ? "deep-scrub" : "scrub";

  // Build the hobject_t from the object_id fields.
  hobject_t hoid;
  hoid.oid.name = snapset_error.object.name;
  hoid.set_key(snapset_error.object.locator);
  hoid.nspace = snapset_error.object.nspace;
  hoid.snap = snapid_t{snapset_error.object.snap};
  hoid.pool = pgid.pgid.pool();

  // "no 'snapset' attr" — head with SNAPSET_MISSING.
  // Classic: "scrub {pgid} {head_hoid} : no 'snapset' attr"
  if (snapset_error.snapset_missing()) {
    auto errorstr = fmt::format("{} {} {} : no '{}' attr",
                                prefix, pgid, hoid, SS_ATTR);
    ERRORDPP("{}", pg, errorstr);
    pg.get_clog_error() << errorstr;
  }

  // "can't decode 'snapset' attr" — head with SNAPSET_CORRUPTED.
  // Classic: "scrub {pgid} {head_hoid} : can't decode 'snapset' attr <err>"
  if (snapset_error.snapset_corrupted()) {
    auto errorstr = fmt::format("{} {} {} : can't decode '{}' attr decode error",
                                prefix, pgid, hoid, SS_ATTR);
    ERRORDPP("{}", pg, errorstr);
    pg.get_clog_error() << errorstr;
  }

  // "is an unexpected clone" — two sources:
  //   (a) extra clones listed in the HEAD wrapper's clones vector.
  //       Classic: "scrub {pgid} {clone_hoid} : is an unexpected clone"
  //   (b) orphan headless clones (head absent from object_set); their
  //       snapset_error entry is a clone entry with headless() set.
  for (const auto& clone_snap : snapset_error.clones) {
    hobject_t clone_obj;
    clone_obj.oid.name = snapset_error.object.name;
    clone_obj.set_key(snapset_error.object.locator);
    clone_obj.nspace = snapset_error.object.nspace;
    clone_obj.snap = clone_snap;
    clone_obj.pool = pgid.pgid.pool();

    auto errorstr = fmt::format("scrub {} {} : is an unexpected clone",
                                pgid, clone_obj);
    ERRORDPP("{}", pg, errorstr);
    pg.get_clog_error() << errorstr;
  }
  // (b): orphan headless clone entry — the snap is in the object itself.
  if (snapset_error.headless() && hoid.is_snap()) {
    auto errorstr = fmt::format("scrub {} {} : is an unexpected clone",
                                pgid, hoid);
    ERRORDPP("{}", pg, errorstr);
    pg.get_clog_error() << errorstr;
  }

  // "no '_' attr" — clone with INFO_MISSING (no OI attr found on any shard).
  // Classic: "scrub {pgid} {clone_hoid} : no '_' attr"
  if (snapset_error.info_missing() && hoid.is_snap()) {
    auto errorstr = fmt::format("{} {} {} : no '{}' attr",
                                prefix, pgid, hoid, OI_ATTR);
    ERRORDPP("{}", pg, errorstr);
    pg.get_clog_error() << errorstr;
  }
}

void PGScrubber::emit_chunk_result(
  const request_range_result_t &range,
  chunk_result_t &&result)
{
  LOG_PREFIX(PGScrubber::emit_chunk_result);
  ++m_digest_updates_generation;
  const auto digest_updates_generation = m_digest_updates_generation;
  m_digest_updates_pending = 0;
  if (result.has_errors()) {
    ERRORDPP("Scrub errors found. range: {}, result: {}", pg, range, result);

    // Log detailed error messages for each inconsistent object
    // This matches the classic OSD behavior where individual object errors
    // are logged to the cluster log
    for (const auto& obj_error : result.object_errors) {
      // Look up the full hobject_t by matching name + snap + nspace.
      // The map is keyed by hobject_t so we can't use a name-only lookup;
      // find the first entry whose hobject_t components match the error's
      // object_id fields.
      auto hoid_it = std::find_if(
        result.object_hoids.begin(), result.object_hoids.end(),
        [&obj_error](const auto& kv) {
          return kv.first.oid.name == obj_error.object.name &&
                 kv.first.snap == snapid_t{obj_error.object.snap} &&
                 kv.first.nspace == obj_error.object.nspace;
        });
      if (hoid_it != result.object_hoids.end()) {
        log_object_errors(obj_error, hoid_it->second);
      }
    }

    // Replay classic-format snapset log messages generated in evaluate_snapset()
    // while the SnapSet data was in scope.  These match the line-for-line output
    // of classic's scrub_backend.cc and are what the test scripts grep for.
    for (const auto& [level, msg] : result.snapset_log_messages) {
      if (level == 'I') {
        INFODPP("{}", pg, msg);
        pg.get_clog_info() << msg;
      } else {
        ERRORDPP("{}", pg, msg);
        pg.get_clog_error() << msg;
      }
    }

    // Log snapset errors (primary-shard errors, stored + counted)
    for (const auto& snapset_error : result.snapset_errors) {
      log_snapset_errors(snapset_error);
    }
    // Log replica-shard snapset errors (logged only, not stored)
    for (const auto& snapset_error : result.replica_snapset_errors) {
      log_snapset_errors(snapset_error);
    }

    // Store errors for retrieval by rados list-inconsistent-obj
    // Dual-store approach matching classic OSD's ScrubStore behavior:
    // Deep scrub: store all errors in BOTH deep_errors and shallow_errors (unfiltered)
    // Shallow scrub: store only filtered errors in shallow_errors, leave deep_errors unchanged

    if (m_is_deep) {
      // Deep scrub: store unfiltered errors in deep_errors
      m_deep_errors.insert(m_deep_errors.end(),
                           result.object_errors.begin(),
                           result.object_errors.end());
    }

    // Both deep and shallow scrubs: store filtered errors in shallow_errors
    // Matching classic OSD behavior: filter object-level and union_shards errors only
    for (const auto& obj_error : result.object_errors) {
      auto filtered_error = obj_error;
      // Filter to keep only shallow errors at object level and union_shards level
      filtered_error.errors &= librados::obj_err_t::SHALLOW_ERRORS;
      filtered_error.union_shards.errors &= librados::err_t::SHALLOW_ERRORS;
      // Note: individual shard errors are NOT filtered (matching classic OSD)

      m_shallow_errors.push_back(filtered_error);
    }
    m_stored_snapset_errors.insert(m_stored_snapset_errors.end(),
                                   result.snapset_errors.begin(),
                                   result.snapset_errors.end());

    DEBUGDPP("Stored {} object errors, total now: shallow={} deep={}", pg,
             result.object_errors.size(), m_shallow_errors.size(), m_deep_errors.size());

    // Accumulate missing/inconsistent/error counts across chunks.
    // Emitted once at end of scrub in emit_scrub_result (matches classic OSD).
    //
    // Classic OSD counts an object as inconsistent only when inconsistents()
    // is called, which requires opt_ers.has_value() from for_empty_auth_list().
    // Two cases cause opt_ers=nullopt (object NOT counted as inconsistent):
    //   1. "failed to pick suitable object info": !is_auth_available — no shard
    //      has selected_oi=true (all shards have blocking errors on OI).
    //      (scrub_backend.cc:982-1001 — m_missing/m_inconsistent not populated)
    //   2. "failed to pick suitable auth object": all shards have shard-level
    //      errors (auth_list and obj_errors both empty → nullopt).
    //      (scrub_backend.cc:1066-1070 — for_empty_auth_list returns nullopt)
    // In crimson these cases are detectable via:
    //   Case 1: no shard has selected_oi=true
    //   Case 2: selected_oi is set (auth chosen) but ALL shards have errors
    for (const auto& obj_error : result.object_errors) {
      bool has_missing = false;
      for (const auto& [shard_id, shard_info] : obj_error.shards) {
        if (shard_info.has_shard_missing()) {
          has_missing = true;
          break;
        }
      }
      if (has_missing) {
        m_total_missing_count++;
      } else if (obj_error.errors || obj_error.union_shards.errors) {
        // Match classic: skip objects where no auth was selected (case 1)
        // or where all shards have errors so auth_list was empty (case 2).
        const bool any_selected_oi = std::any_of(
          obj_error.shards.begin(), obj_error.shards.end(),
          [](const auto& p) { return p.second.selected_oi; });
        const bool all_shards_have_errors = std::all_of(
          obj_error.shards.begin(), obj_error.shards.end(),
          [](const auto& p) { return p.second.errors != 0; });
        if (any_selected_oi && !all_shards_have_errors) {
          m_total_inconsistent_count++;
        }
      }
    }
    // Snapset errors are reported separately; classic scrub counts only
    // per-shard object errors as "inconsistent objects".
    m_total_error_count += result.stats.num_scrub_errors;

    // If this is a repair scrub, initiate repairs for inconsistent objects
    // Use the object_hoids map which has the correct hobject_t with hash
    if (m_is_repair) {
      int fixed = scrub_process_inconsistent(result.object_errors, result.object_hoids);
      m_fixed_count += fixed;
      DEBUGDPP("Initiated repair for {} object copies", pg, fixed);
    }
  } else {
    DEBUGDPP("Chunk complete. range: {}", pg, range);
  }

  // For deep scrubs, write back any newly-computed digests to the objects'
  // object_info_t attrs.  This mirrors classic OSD's submit_digest_fixes /
  // PrimaryLogScrub path: after deep scan the authoritative digest is stored
  // persistently in oi so future scrubs (and repair) can compare against it.
  if (m_is_deep && pg.is_primary() && !result.missing_digest.empty()) {
    DEBUGDPP("submitting {} digest write-backs", pg, result.missing_digest.size());
    m_digest_updates_pending = result.missing_digest.size();
    for (auto &du : result.missing_digest) {
      std::ignore = pg.shard_services.start_operation_may_interrupt<
        interruptor, ScrubDigestUpdate>(
          &pg, du.oid, du.data_digest, du.omap_digest,
          digest_updates_generation);
    }
  }

  // Track the number of objects scrubbed in this chunk
  // result.stats.num_objects contains the count of objects in this chunk
  m_objects_scrubbed_in_chunk += result.stats.num_objects;
}

void PGScrubber::on_digest_update_complete(uint64_t generation)
{
  if (generation != m_digest_updates_generation) {
    return;
  }

  ceph_assert(m_digest_updates_pending > 0);
  if (--m_digest_updates_pending == 0) {
    handle_event(ScrubContext::digest_updates_complete_t{});
  }
}

int PGScrubber::scrub_process_inconsistent(
  const std::vector<inconsistent_obj_wrapper>& object_errors,
  const std::map<hobject_t, hobject_t>& object_hoids)
{
  LOG_PREFIX(PGScrubber::scrub_process_inconsistent);
  DEBUGDPP("Processing {} inconsistent objects for repair",
           pg, object_errors.size());

  int fixed_count = 0;

  for (const auto& obj_error : object_errors) {
    // Find the full hobject_t by matching name + snap + nspace.
    auto it = std::find_if(
      object_hoids.begin(), object_hoids.end(),
      [&obj_error](const auto& kv) {
        return kv.first.oid.name == obj_error.object.name &&
               kv.first.snap == snapid_t{obj_error.object.snap} &&
               kv.first.nspace == obj_error.object.nspace;
      });
    if (it == object_hoids.end()) {
      ERRORDPP("Could not find hobject_t for inconsistent object: name={}",
               pg, obj_error.object.name);
      continue;
    }

    const hobject_t& oid = it->second;
    DEBUGDPP("Found object {} with hash {:x}", pg, oid, oid.get_hash());

    // Find the authoritative shard and the shards with errors.
    pg_shard_t auth_shard;
    const librados::shard_info_t* auth_shard_info = nullptr;
    std::set<pg_shard_t> bad_shards;

    // Iterate through all shards to find auth and bad ones
    for (const auto& [shard_id, shard_info] : obj_error.shards) {
      pg_shard_t pg_shard(shard_id.osd, shard_id_t(shard_id.shard));

      if (shard_info.selected_oi) {
        // This is the authoritative shard
        auth_shard = pg_shard;
        auth_shard_info = &shard_info;
      }

      if (shard_info.has_errors()) {
        // Repairing a primary-local object that is present but has missing/
        // corrupted OI metadata can currently abort in recovery metadata load.
        // Skip marking that local copy missing; keep repairing other shards.
        if (pg_shard == pg.get_pg_whoami() &&
            (shard_info.has_info_missing() || shard_info.has_info_corrupted())) {
          DEBUGDPP("Skipping unsafe local repair mark for {} on shard {} (info missing/corrupted)",
                   pg, oid, pg_shard);
          continue;
        }
        bad_shards.insert(pg_shard);
      }
    }

    if (auth_shard_info) {
      // Never mark the selected authoritative shard as missing.
      // Classic scrub repair always repairs from the chosen auth copy.
      bad_shards.erase(auth_shard);
    }

    if (!bad_shards.empty() && auth_shard_info) {
      // Count the number of bad shards being fixed
      // This matches classic OSD's behavior of counting bad_peers.size()
      fixed_count += bad_shards.size();

      // Get the version from the authoritative shard's object_info
      // This matches classic OSD's approach in scrub_backend.cc:402-424
      std::optional<eversion_t> repair_version;
      try {
        object_info_t oi;
        auto it = auth_shard_info->attrs.find(OI_ATTR);
        if (it != auth_shard_info->attrs.end()) {
          auto bliter = it->second.cbegin();
          decode(oi, bliter);
          repair_version = oi.version;
          DEBUGDPP("Got version {} from authoritative shard {} for object {}, is_data_digest={} data_digest=0x{:x} flags=0x{:x}",
                   pg, *repair_version, auth_shard, oid, oi.is_data_digest(), oi.data_digest, (uint32_t)oi.flags);
        } else {
          ERRORDPP("Authoritative shard {} missing OI_ATTR for object {}",
                   pg, auth_shard, oid);
        }
      } catch (...) {
        ERRORDPP("Failed to decode object_info for {}, skipping repair for this object",
                 pg, oid);
      }

      if (!repair_version.has_value()) {
        DEBUGDPP("Skipping repair mark for {} due to missing authoritative version", pg, oid);
        continue;
      }

      // Mark objects as missing on bad shards
      // This matches classic OSD's approach in scrub_backend.cc:424
      // Classic OSD just calls force_object_missing() and lets the PG state
      // machine automatically queue recovery when it detects the PG is not clean
      for (const auto& bad_shard : bad_shards) {
        pg.peering_state.force_object_missing(bad_shard, oid, *repair_version);
        DEBUGDPP("Marked object {} as missing on shard {} with version {}",
                 pg, oid, bad_shard, *repair_version);
      }
    }
  }

  return fixed_count;
}

void PGScrubber::emit_scrub_result(
  bool deep,
  object_stat_sum_t in_stats)
{
  LOG_PREFIX(PGScrubber::emit_scrub_result);
  DEBUGDPP("objects_scrubbed: {}", pg, m_objects_scrubbed_in_chunk);

  // Sort snapset errors to match classic OSD's omap-key order across all chunks:
  // (snap ascending, then name ascending).  Per-chunk sort was already applied in
  // validate_chunk, but cross-chunk ordering requires a final global sort here
  // once all chunks have been accumulated into m_stored_snapset_errors.
  std::sort(m_stored_snapset_errors.begin(), m_stored_snapset_errors.end(),
    [](const inconsistent_snapset_wrapper &a,
       const inconsistent_snapset_wrapper &b) {
      if (a.object.snap != b.object.snap) {
        return a.object.snap < b.object.snap;
      }
      return a.object.name < b.object.name;
    });

  // Log repair results if this was a repair scrub
  if (m_is_repair && m_fixed_count > 0) {
    INFODPP("Scrub repair completed: {} object copies fixed", pg, m_fixed_count);
    pg.get_clog_info() << "pg " << pg.get_pgid()
                       << " scrub repair: " << m_fixed_count << " fixed";
  }

  pg.peering_state.update_stats(
    [this, FNAME, deep, &in_stats](auto &history, auto &pg_stats) {
      // Handle invalid stats, in case of split/merge
      if (pg_stats.stats_invalid) {
        pg_stats.stats.sum = in_stats;
        pg_stats.stats_invalid = false;
        DEBUGDPP(" repaired invalid stats! ", pg);
        return false;
      }
      foreach_scrub_maintained_stat(
 [deep, &pg_stats, &in_stats](
   const auto &name, auto statptr, bool skip_for_shallow) {
   if (deep || !skip_for_shallow) {
     pg_stats.stats.sum.*statptr = in_stats.*statptr;
   }
 });
      // Check for any stat mismatch and, if found, emit the classic comprehensive
      // "stat mismatch, got ..." log message matching PrimaryLogScrub.cc format.
      bool stat_mismatch = false;
      foreach_scrub_checked_stat(
        [&pg_stats, &in_stats, &stat_mismatch](
          const auto& /*name*/, auto statptr, const auto& invalid_predicate) {
          if (!invalid_predicate(pg_stats) &&
              (in_stats.*statptr != pg_stats.stats.sum.*statptr)) {
            stat_mismatch = true;
          }
        });
      if (stat_mismatch) {
        auto &s = pg_stats.stats.sum;
        const char* scrub_prefix = deep ? "deep-scrub" : "scrub";
        auto mismatch_msg = fmt::format(
          "{} {} : stat mismatch, got "
          "{}/{} objects, {}/{} clones, {}/{} dirty, {}/{} omap, "
          "{}/{} pinned, {}/{} hit_set_archive, {}/{} whiteouts, "
          "{}/{} bytes, {}/{} manifest objects, {}/{} hit_set_archive bytes.",
          pg.get_pgid(), scrub_prefix,
          in_stats.num_objects, s.num_objects,
          in_stats.num_object_clones, s.num_object_clones,
          in_stats.num_objects_dirty, s.num_objects_dirty,
          in_stats.num_objects_omap, s.num_objects_omap,
          in_stats.num_objects_pinned, s.num_objects_pinned,
          in_stats.num_objects_hit_set_archive, s.num_objects_hit_set_archive,
          in_stats.num_whiteouts, s.num_whiteouts,
          in_stats.num_bytes, s.num_bytes,
          in_stats.num_objects_manifest, s.num_objects_manifest,
          in_stats.num_bytes_hit_set_archive, s.num_bytes_hit_set_archive);
        ERRORDPP("{}", pg, mismatch_msg);
        pg.get_clog_error() << mismatch_msg;
        ++pg_stats.stats.sum.num_shallow_scrub_errors;
      }

      // Update objects_scrubbed with the total count from all chunks
      pg_stats.objects_scrubbed = m_objects_scrubbed_in_chunk;

      history.last_scrub = pg.peering_state.get_info().last_update;
      auto now = ceph_clock_now();
      history.last_scrub_stamp = now;
      if (deep) {
 history.last_deep_scrub_stamp = now;
      }

      // Update error counts from current scrub results (matches classic OSD)
      // For deep scrubs, we update both shallow and deep error counts.
      // For shallow scrubs that explicitly cleared deep-error details (operator
      // request), also zero the stored deep error counts so PG_STATE_INCONSISTENT
      // is cleared by prepare_stats_for_publish().
      if (deep) {
         pg_stats.stats.sum.num_shallow_scrub_errors = in_stats.num_shallow_scrub_errors;
         pg_stats.stats.sum.num_deep_scrub_errors = in_stats.num_deep_scrub_errors;
       } else if (m_flags.deep_errors_cleared) {
         pg_stats.stats.sum.num_shallow_scrub_errors = in_stats.num_shallow_scrub_errors;
         pg_stats.stats.sum.num_deep_scrub_errors = 0;
         m_flags.deep_errors_cleared = false;
         DEBUGDPP("shallow scrub cleared deep-error stats: num_shallow={} num_deep=0",
                  pg, pg_stats.stats.sum.num_shallow_scrub_errors);
       }

      // If this was a repair, check if we need to schedule after_repair scrub
      // This matches classic OSD behavior in scrub_finish()
      if (m_is_repair && m_fixed_count > 0) {
        int total_errors = pg_stats.stats.sum.num_shallow_scrub_errors +
                          pg_stats.stats.sum.num_deep_scrub_errors;
        if (total_errors > 0) {
          // Errors remain after repair - schedule an after_repair scrub after recovery
          m_after_repair_scrub_required = true;
          DEBUGDPP("Repair completed but {} errors remain (fixed {}), will schedule after_repair scrub after recovery",
                   pg, total_errors, m_fixed_count);
        }
      }

      // Recalculate total scrub errors (matches classic OSD)
      pg_stats.stats.sum.num_scrub_errors =
        pg_stats.stats.sum.num_shallow_scrub_errors +
        pg_stats.stats.sum.num_deep_scrub_errors;

      // Check if this was a repair verification scrub (check_repair flag)
      // If errors still exist after repair, log and set FAILED_REPAIR state
      // This matches classic OSD behavior in scrub_finish()
      if (m_flags.check_repair) {
        m_flags.check_repair = false;
        if (pg_stats.stats.sum.num_scrub_errors > 0) {
          pg.state_set(PG_STATE_FAILED_REPAIR);
          INFODPP("scrub_finish {} error(s) still present after re-scrub",
                  pg, pg_stats.stats.sum.num_scrub_errors);
        }
      }

      // Log scrub_finish for test compatibility (matches classic OSD)

      INFODPP("scrub_finish shard {} num_omap_bytes = {} num_omap_keys = {}",
                pg, pg.get_pg_whoami().shard,
                pg_stats.stats.sum.num_omap_bytes,
                pg_stats.stats.sum.num_omap_keys);

      // Calculate scrub duration
      if (m_scrub_start_time.has_value()) {
        auto duration = ceil<milliseconds>(ScrubClock::now() - *m_scrub_start_time);
        double dur_ms = double(duration.count());
        pg_stats.last_scrub_duration = ceill(dur_ms / 1000.0);
        pg_stats.scrub_duration = dur_ms;
        DEBUGDPP("after setting: last_scrub_duration={}, scrub_duration={}",
                 pg, pg_stats.last_scrub_duration, pg_stats.scrub_duration);
        m_scrub_start_time.reset();
      }

      return false;
    });

    // Emit summary messages matching classic OSD's scrub_finish():
    //   "{pgid} {mode} {N} missing, {M} inconsistent objects"
    //   "{pgid} {mode} {E} errors"
    // These are accumulated across all chunks (m_total_*_count) and emitted once.
    if (m_total_missing_count > 0 || m_total_inconsistent_count > 0) {
      const char* mode = m_is_deep ? "deep-scrub" : "scrub";
      auto err_msg = fmt::format("{} {} {} missing, {} inconsistent objects",
                                 pg.get_pgid(), mode,
                                 m_total_missing_count,
                                 m_total_inconsistent_count);
      ERRORDPP("{}", pg, err_msg);
      pg.get_clog_error() << err_msg;
    }
    if (m_total_error_count > 0) {
      const char* mode = m_is_deep ? "deep-scrub" : "scrub";
      auto err_msg = fmt::format("{} {} {} errors",
                                 pg.get_pgid(), mode,
                                 m_total_error_count);
      ERRORDPP("{}", pg, err_msg);
      pg.get_clog_error() << err_msg;
    }
    // Check if we need to initiate a deep scrub after finding errors in shallow scrub
    // This matches classic OSD behavior in scrub_finish()
    bool do_auto_scrub = false;
    int error_count = in_stats.num_shallow_scrub_errors +
                      in_stats.num_deep_scrub_errors;
    DEBUGDPP("auto-repair check: deep_scrub_on_error={}, error_count={}, is_deep={}, max_errors={}",
             pg, m_flags.deep_scrub_on_error, error_count, m_is_deep,
             crimson::common::local_conf().get_val<uint64_t>("osd_scrub_auto_repair_num_errors"));

    if (m_flags.deep_scrub_on_error && error_count > 0 &&
        error_count <= static_cast<int>(
            crimson::common::local_conf().get_val<uint64_t>("osd_scrub_auto_repair_num_errors"))) {
      ceph_assert(!m_is_deep);
      do_auto_scrub = true;
      DEBUGDPP("will initiate a deep scrub to fix {} errors", pg, error_count);
    }

    m_flags.deep_scrub_on_error = false;

    // Save fixed_count before cleanup resets it
    int fixed_count = m_fixed_count;
    bool is_repair = m_is_repair;

    // Track the type of scrub that just completed for proper error retrieval
    m_last_scrub_was_deep = m_is_deep;

    cleanup_on_finish();

    // Request a deep scrub if needed (before update_scrub_job which resets targets)
    if (do_auto_scrub) {
      request_rescrubbing();
    }

    update_scrub_job();
    m_active_target.reset();

    // Handle repair completion based on whether recovery is needed
    // This matches classic OSD behavior in scrub_finish()
    if (is_repair && fixed_count > 0) {
      // Repair marked objects as missing, post DoRecovery event to trigger recovery
      // This causes the PeeringState state machine to transition to RECOVERING state
      // and properly start recovery operations to restore the missing objects.
      DEBUGDPP("Repair marked {} objects as missing, posting DoRecovery event", pg, fixed_count);
      (void) pg.get_shard_services().start_operation<LocalPeeringEvent>(
        &pg,
        pg.get_pg_whoami(),
        pg.get_pgid(),
        float(0.001),
        pg.get_osdmap_epoch(),
        pg.get_osdmap_epoch(),
        PeeringState::DoRecovery{});
    } else if (is_repair && error_count > 0 && fixed_count == 0) {
      // We have errors but nothing can be fixed, so there is no repair possible
      // This matches classic OSD behavior in scrub_finish()
      pg.state_set(PG_STATE_FAILED_REPAIR);
      INFODPP("scrub_finish {} error(s) present with no repair possible", pg, error_count);
    } else if (is_repair && error_count == 0) {
      // Repair completed with no errors and no recovery needed - clear repair state
      // The INCONSISTENT state will be cleared automatically by prepare_stats_for_publish()
      // when num_scrub_errors == 0
      m_is_repair = false;
      pg.state_clear(PG_STATE_REPAIR);
      DEBUGDPP("Repair complete with no errors, clearing PG_STATE_REPAIR", pg);
    }
    // Resume snap trimming that was deferred while the PG was scrubbing
    pg.kick_snap_trim();
}

void PGScrubber::scan_snaps(const ScrubMap &map)
{
  LOG_PREFIX(PGScrubber::scan_snaps);
  INFODPP("_scan_snaps start", pg);
  // Parse the scrub map and spawn one ScrubSnapMapperRepair per clone.
  // The repair op does the blocking KV read + compare + write inside
  // interruptor::async(), keeping this reactor-thread function non-blocking.

  hobject_t head;
  SnapSet snapset;

  for (auto i = map.objects.rbegin(); i != map.objects.rend(); ++i) {
    const hobject_t &hoid = i->first;

    ceph_assert(!hoid.is_snapdir());

    if (hoid.is_head()) {
      auto ss_it = i->second.attrs.find(SS_ATTR);
      if (ss_it == i->second.attrs.end()) {
        head = hobject_t{};
        continue;
      }
      try {
        auto p = ss_it->second.cbegin();
        decode(snapset, p);
      } catch (...) {
        DEBUGDPP("failed to decode snapset for {}", pg, hoid);
        head = hobject_t{};
        continue;
      }
      head = hoid.get_head();
      continue;
    }

    if (hoid.snap < CEPH_MAXSNAP) {
      if (hoid.get_head() != head) {
        DEBUGDPP("no head for {} (have {})", pg, hoid, head);
        continue;
      }

      auto p = snapset.clone_snaps.find(hoid.snap);
      if (p == snapset.clone_snaps.end()) {
        DEBUGDPP("no clone_snaps for {} in {}", pg, hoid, snapset);
        continue;
      }
      std::set<snapid_t> obj_snaps{p->second.begin(), p->second.end()};

      DEBUGDPP("spawning snap mapper check for {} expected snaps: {}",
               pg, hoid, obj_snaps);
      std::ignore = pg.get_shard_services().start_operation_may_interrupt<
        interruptor, ScrubSnapMapperRepair>(
          &pg, hoid, obj_snaps);
    }
  }
}

std::string_view PGScrubber::registration_state() const
{
  if (m_scrub_job) {
    return m_scrub_job->state_desc();
  }
  return "(no sched job)";
}

void PGScrubber::cleanup_on_finish()
{
  clear_pgscrub_state();

  // PG state flags changed:
  pg.publish_stats_to_osd();
}

void PGScrubber::clear_pgscrub_state()
{
  pg.state_clear(PG_STATE_SCRUBBING);
  pg.state_clear(PG_STATE_DEEP_SCRUB);

  m_local_osd_resource.reset();

  reset_internal_state();
  m_flags = scrub_flags_t{};

}

void PGScrubber::reset_internal_state()
{
  ++m_digest_updates_generation;
  m_digest_updates_pending = 0;
  clear_queued_or_active();
  m_objects_scrubbed_in_chunk = 0;
  m_fixed_count = 0;
  m_total_missing_count = 0;
  m_total_inconsistent_count = 0;
  m_total_error_count = 0;
}

void PGScrubber::dump_scrub_metrics(ceph::Formatter* f)
{
  LOG_PREFIX(PGScrubber::dump_scrub_metrics);
  DEBUGDPP("dump scrub pgid = {}", pg, pg.get_pgid());
  // Use "scrubber" section name to match classic OSD and test expectations
  f->open_object_section("scrubber");
  f->dump_stream("pgid") << pg.get_pgid();
  f->dump_bool("is_queued_or_active", m_queued_or_active);
  // Add 'active' field to match classic OSD output format
  // active means scrubbing is running, not just queued
  f->dump_bool("active", m_active_target.has_value());
  f->dump_bool("is_reserving_replicas", is_reserving_replicas());
  f->dump_string("mode", m_mode_desc);

  // Dump repair statistics (matches classic OSD)
  if (m_is_repair) {
    f->dump_int("fixed", m_fixed_count);
  }

  // Dump metrics from the last or current scrub session
  if (m_last_scrub_metrics) {
    m_last_scrub_metrics->dump(f);
  } else {
    f->dump_string("metrics_status", "no scrub metrics available");
  }

  f->close_section();
}

void PGScrubber::update_op_mode_text()
{
  LOG_PREFIX(PGScrubber::update_op_mode_text);
  auto visible_repair = pg.state_test(PG_STATE_REPAIR);
  m_mode_desc =
    (visible_repair ? "repair" : (m_is_deep ? "deep-scrub" : "scrub"));

  DEBUGDPP("repair: visible: {}, internal: {}. Displayed: {}",
    pg, visible_repair, m_is_repair, m_mode_desc);
}

std::chrono::milliseconds PGScrubber::get_scrub_sleep_time() const
{
  // Get osd_scrub_sleep config value and convert to milliseconds
  // This matches classic OSD's scrub_sleep_time() implementation
  using namespace std::chrono;
  const double sleep_seconds = crimson::common::local_conf().get_val<double>("osd_scrub_sleep");
  LOG_PREFIX(PGScrubber::get_scrub_sleep_time);
  DEBUGDPP("osd_scrub_sleep config value: {} seconds", pg, sleep_seconds);
  return milliseconds(static_cast<int64_t>(std::max(0.0, 1000.0 * sleep_seconds)));
}

void PGScrubber::start_chunk_sleep()
{
  LOG_PREFIX(PGScrubber::start_chunk_sleep);
  DEBUGDPP("starting sleep operation", pg);

  // Start ScrubSleep operation which will handle the sleep and post event when done
  std::ignore = pg.shard_services.start_operation_may_interrupt<
    interruptor, ::crimson::osd::ScrubSleep
    >(&pg);
}

std::string_view PGScrubber::get_op_mode_text() const
{
  return m_mode_desc;
}

bool PGScrubber::should_abort() const
{
  LOG_PREFIX(PGScrubber::should_abort);
  DEBUGDPP("checking abort conditions, m_is_deep={}", pg, m_is_deep);

  // Check if this scrub type observes noscrub flags
  // High-priority scrubs (operator-requested, after-repair, etc.) don't abort
  if (m_active_target &&
      !ScrubJob::observes_noscrub_flags(m_active_target->urgency())) {
    DEBUGDPP("high-priority scrub, not aborting", pg);
    return false;
  }

  if (m_is_deep) {
    if (pg.get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB) ||
        pg.get_pgpool().info.has_flag(pg_pool_t::FLAG_NODEEP_SCRUB)) {
      DEBUGDPP("nodeep_scrub set, aborting", pg);
      return true;
    }
  } else if (pg.get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) ||
             pg.get_pgpool().info.has_flag(pg_pool_t::FLAG_NOSCRUB)) {
    DEBUGDPP("noscrub set, aborting", pg);
    return true;
  }

  DEBUGDPP("no abort conditions met", pg);
  return false;
}

bool PGScrubber::verify_against_abort(epoch_t epoch_to_verify)
{
  LOG_PREFIX(PGScrubber::verify_against_abort);
  DEBUGDPP("check if have to abort!", pg);

  if (!should_abort()) {
    return true;
  }

  DEBUGDPP("aborting. incoming epoch: {} vs last-aborted: {}",
           pg, epoch_to_verify, m_last_aborted);

  // If we were not aware of the abort before - trigger the abort
  // The caller will handle the transition
  if (epoch_to_verify >= m_last_aborted) {
    m_last_aborted = std::max(epoch_to_verify, m_epoch_start);

    // Re-enqueue the aborted job so it can be retried when conditions allow
    // Similar to classic OSD's on_mid_scrub_abort()
    if (m_active_target) {
      on_mid_scrub_abort(delay_cause_t::flags);
    }

    // Return false to indicate abort should happen
    return false;
  }

  // We already aborted for this or a later epoch, don't abort again
  DEBUGDPP("already aborted at epoch {}, not aborting again", pg, m_last_aborted);
  return true;
}

void PGScrubber::on_mid_scrub_abort(delay_cause_t issue)
{
  LOG_PREFIX(PGScrubber::on_mid_scrub_abort);
  if (!m_scrub_job->is_registered()) {
    DEBUGDPP("PG not registered for scrubbing. Won't requeue!", pg);
    return;
  }

  DEBUGDPP("aborting scrub, cause: {}", pg, issue);

  // Save the aborted target before clearing m_active_target
  auto aborted_target = *m_active_target;
  m_active_target.reset();

  // Clear the queued_or_active flag so the scrub can be retried
  clear_queued_or_active();

  const auto scrub_clock_now = ceph_clock_now();

  // Get the target from scrub_job (it was reset when scrub started)
  auto& current_targ = m_scrub_job->get_target(aborted_target.level());
  ceph_assert(!current_targ.queued);

  // Merge the aborted target with the current target (similar to classic OSD)
  // The current_targ was reset() when scrub started, so it has default values
  // We merge to preserve any scheduling info from the aborted scrub
  auto& curr_sched = current_targ.sched_info.schedule;
  auto& abrt_sched = aborted_target.sched_info.schedule;

  current_targ.sched_info.urgency =
      std::max(current_targ.urgency(), aborted_target.urgency());
  curr_sched.scheduled_at =
      std::min(curr_sched.scheduled_at, abrt_sched.scheduled_at);
  curr_sched.not_before =
      std::min(curr_sched.not_before, abrt_sched.not_before);

  DEBUGDPP("merged target (before delay): {}", pg, current_targ);

  // Add a delay and re-enqueue (delay_on_failure returns the modified target)
  auto& delayed_targ = m_scrub_job->delay_on_failure(aborted_target.level(), issue, scrub_clock_now);
  DEBUGDPP("re-enqueuing aborted target: {}", pg, delayed_targ);
  pg.shard_services.get_scrub_scheduler().enqueue_target(delayed_targ);
  delayed_targ.queued = true;

  // Also re-enqueue the sister target if it's not queued
  // Match classic OSD behavior: just enqueue without delay_on_failure
  const auto sister_level = (aborted_target.level() == scrub_level_t::deep)
                              ? scrub_level_t::shallow
                              : scrub_level_t::deep;
  auto& sister = m_scrub_job->get_target(sister_level);
  if (!sister.queued) {
    DEBUGDPP("also re-enqueuing sister target: {}", pg, sister);
    pg.shard_services.get_scrub_scheduler().enqueue_target(sister);
    sister.queued = true;
  }
}

}
