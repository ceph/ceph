// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab expandtab

#include <fmt/ranges.h>

#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_operations/scrub_events.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "pg_scrubber.h"

SET_SUBSYS(osd);

namespace crimson::osd::scrub {

void PGScrubber::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pg.get_pgid();
}

PGScrubber::PGScrubber(PG &pg) : pg(pg), dpp(pg), machine(*this)
{
  m_scrub_job.emplace(pg.pgid, pg.pg_whoami.osd);
}

PGScrubber::~PGScrubber()
{
  m_scrub_job.reset();
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
  ceph_assert(!blocked);
}

void PGScrubber::on_log_update(eversion_t v)
{
  LOG_PREFIX(PGScrubber::on_interval_change);
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
      pg.get_info().history.last_scrub_stamp, applicable_conf, scrub_clock_now);
  m_scrub_job->adjust_deep_schedule(
      pg.get_info().history.last_deep_scrub_stamp, applicable_conf,
      scrub_clock_now);

  DEBUGDPP("adjusted:{}", pg, *m_scrub_job);
}

void PGScrubber::schedule_scrub_with_osd()
{
  ceph_assert(pg.is_primary());
  ceph_assert(m_scrub_job);

  m_scrub_job->registered = true;
  update_scrub_job();
}

void PGScrubber::update_scrub_job()
{
  LOG_PREFIX(PGScrubber::update_scrub_job);
   DEBUGDPP("update job: {}", pg, *m_scrub_job);
  if (!m_scrub_job->is_registered())
    return;
  if (m_scrub_job->is_queued()) {
    pg.shard_services.get_scrub_scheduler().remove_from_osd_queue(pg.get_pgid());
    m_scrub_job->clear_both_targets_queued();
  }
  update_targets(ceph_clock_now());
  DEBUGDPP("scheduling scrub job with OSD {}", pg, *m_scrub_job);
  pg.shard_services.get_scrub_scheduler().enqueue_scrub_job(*m_scrub_job);
  m_scrub_job->set_both_targets_queued();
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

bool PGScrubber::reserve_local(const SchedTarget& trgt)
{
  LOG_PREFIX(PGScrubber::reserve_local);
  const bool is_hp = !ScrubJob::observes_max_concurrency(trgt.urgency());

  m_local_osd_resource = pg.shard_services.get_scrub_scheduler().inc_scrubs_local(is_hp);
  if (m_local_osd_resource) {
    DEBUGDPP("reserved local scrub resource", pg);
    return true;
  }

  DEBUGDPP("failed to reserve local scrub resource", pg);
  return false;
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

schedule_result_t PGScrubber::start_scrub(
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
    return schedule_result_t::target_specific_failure;
  }

  // a few checks. If failing - the 'not-before' is modified, and the target
  // is requeued.
  auto clock_now = ceph_clock_now();

  if (!pg.is_primary() || !pg.is_active()) {
    // the PG is not expected to be 'registered' in this state. And we should
    // not attempt to queue it.
    DEBUGDPP("{}: cannot scrub (not primary/active). Registered?{:c}", pg,
      m_scrub_job->is_registered() ? 'Y' : 'n');
    return schedule_result_t::target_specific_failure;
  }

  // for all other failures - we must reinstate our entry in the Scrub Queue.
  // For some of the failures, we will also delay the 'other' target.
  if (!pg.is_active_clean()) {
    DEBUGDPP("{}: cannot scrub (not clean). Registered?{:c}", pg,
      m_scrub_job->is_registered() ? 'Y' : 'n');
    requeue_penalized(
	    s_or_d, delay_both_targets_t::yes, delay_cause_t::pg_state, clock_now);
    return schedule_result_t::target_specific_failure;
  }

  if (pg.state_test(PG_STATE_SNAPTRIM) || pg.state_test(PG_STATE_SNAPTRIM_WAIT)) {
    // note that the trimmer checks scrub status when setting 'snaptrim_wait'
    // (on the transition from NotTrimming to Trimming/WaitReservation),
    // i.e. some time before setting 'snaptrim'.
    DEBUGDPP("{}: cannot scrub (snap trimming)", pg);
    requeue_penalized(
	    s_or_d, delay_both_targets_t::yes, delay_cause_t::snap_trimming, clock_now);
    return schedule_result_t::target_specific_failure;
  }

   // is there a 'no-scrub' flag set for the initiated scrub level? note:
  // won't affect operator-initiated (and some other types of) scrubs.
  if (ScrubJob::observes_noscrub_flags(trgt.urgency())) {
    if (trgt.is_shallow()) {
      if (!pg_cond.allow_shallow) {
	      // can't scrub at all
        DEBUGDPP("{}: shallow not allowed", pg);
        requeue_penalized(
	        s_or_d, delay_both_targets_t::no, delay_cause_t::flags, clock_now);
	      return schedule_result_t::target_specific_failure;
      }
    } else if (!pg_cond.allow_deep) {
      // can't scrub at all
      DEBUGDPP("{}: deep not allowed", pg);
      requeue_penalized(
	      s_or_d, delay_both_targets_t::no, delay_cause_t::flags, clock_now);
      return schedule_result_t::target_specific_failure;
    }
  }

  // try to reserve the local OSD resources. If failing: no harm. We will
  // be retried by the OSD later on.
  if (!reserve_local(trgt)) {
    DEBUGDPP("{}: failed to reserve locally", pg);
    requeue_penalized(
	    s_or_d, delay_both_targets_t::yes, delay_cause_t::local_resources,
	    clock_now);
    return schedule_result_t::osd_wide_failure;
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
    std::ignore = PG::interruptor::with_interruption([this, pgref, deep] {
      handle_scrub_requested(deep);
      return PG::interruptor::now();
    }, [FNAME, this](std::exception_ptr ep) {
      DEBUGDPP("interrupted with {}", pg, ep);
    }, pgref, pgref->get_osdmap_epoch());
    return schedule_result_t::scrub_initiated;
  } else {
    DEBUGDPP("cannot scrub now, will be scheduled by OSD later. epoch_queued: {}, same_interval_since: {}",
      pg, epoch_queued, pg.get_same_interval_since());
    // the target is already marked as not in queue, but the job isn't
    // registered yet. We need to register it, so it will be properly
    // scheduled by the OSD.
    schedule_scrub_with_osd();
    return schedule_result_t::target_specific_failure;
  }
}

void PGScrubber::handle_scrub_requested(bool deep)
{
  LOG_PREFIX(PGScrubber::handle_scrub_requested);
  DEBUGDPP("deep: {}", pg, deep);
  handle_event(events::start_scrub_t{deep});
}

void PGScrubber::handle_scrub_message(Message &_m)
{
  LOG_PREFIX(PGScrubber::handle_scrub_requested);
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
  default:
    DEBUGDPP("invalid message: {}", pg, _m);
    ceph_assert(is_scrub_message(_m));
  }
}

void PGScrubber::handle_op_stats(
  const hobject_t &on_object,
  object_stat_sum_t delta_stats) {
  handle_event(events::op_stats_t{on_object, delta_stats});
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
  pg.peering_state.state_set(PG_STATE_SCRUBBING);
  if (deep) {
    pg.peering_state.state_set(PG_STATE_DEEP_SCRUB);
  }
  pg.publish_stats_to_osd();
}

void PGScrubber::notify_scrub_end(bool deep)
{
  LOG_PREFIX(PGScrubber::notify_scrub_end);
  DEBUGDPP("deep: {}", pg, deep);
  pg.state_clear(PG_STATE_SCRUBBING);
  if (deep) {
    pg.state_clear(PG_STATE_DEEP_SCRUB);
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
      "osd_deep_scrub_large_omap_object_key_threshold")
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
  ceph_assert(blocked);
  DEBUGDPP("blocked: {}", pg, *blocked);
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

void PGScrubber::emit_chunk_result(
  const request_range_result_t &range,
  chunk_result_t &&result)
{
  LOG_PREFIX(PGScrubber::emit_chunk_result);
  if (result.has_errors()) {
    LOG_SCRUB_ERROR(
      "Scrub errors found. range: {}, result: {}",
      range, result);
  } else {
    DEBUGDPP("Chunk complete. range: {}", pg, range);
  }
}

void PGScrubber::emit_scrub_result(
  bool deep,
  object_stat_sum_t in_stats)
{
  LOG_PREFIX(PGScrubber::emit_scrub_result);
  DEBUGDPP("", pg);
  pg.peering_state.update_stats(
    [this, FNAME, deep, &in_stats](auto &history, auto &pg_stats) {
      // Handle invalid stats, in case of split/merge
      if (pg_stats.stats_invalid) {
        pg_stats.stats.sum = in_stats;
        pg_stats.stats_invalid = false;
        DEBUGDPP(" repaired invalid stats! ", pg);
      }
      foreach_scrub_maintained_stat(
	[deep, &pg_stats, &in_stats](
	  const auto &name, auto statptr, bool skip_for_shallow) {
	  if (deep && !skip_for_shallow) {
	    pg_stats.stats.sum.*statptr = in_stats.*statptr;
	  }
	});
      foreach_scrub_checked_stat(
	[this, FNAME, &pg_stats, &in_stats](
	  const auto &name, auto statptr, const auto &invalid_predicate) {
	  if (!invalid_predicate(pg_stats) &&
	      (in_stats.*statptr != pg_stats.stats.sum.*statptr)) {
	    LOG_SCRUB_ERROR(
	      "stat mismatch for {}: scrubbed value: {}, stored pg value: {}",
	      name, in_stats.*statptr, pg_stats.stats.sum.*statptr);
	    ++pg_stats.stats.sum.num_shallow_scrub_errors;
	  }
	});
      history.last_scrub = pg.peering_state.get_info().last_update;
      auto now = ceph_clock_now();
      history.last_scrub_stamp = now;
      if (deep) {
	history.last_deep_scrub_stamp = now;
      }
      cleanup_on_finish();
      update_scrub_job();
      m_active_target.reset();
      return false; // notify_scrub_end will flush stats to osd
    });
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
  //requeue_waiting();  //implement later

  reset_internal_state();
  m_flags = scrub_flags_t{};

  // type-specific state clear
  //_scrub_clear_state();
}

void PGScrubber::reset_internal_state()
{
  clear_queued_or_active();
}
}
