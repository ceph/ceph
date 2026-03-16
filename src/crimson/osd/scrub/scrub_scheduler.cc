// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
#include "include/random.h" // for ceph::util::generate_random_number()

#include "crimson/osd/scrub/scrub_scheduler.h"
#include "crimson/osd/shard_services.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
SET_SUBSYS(osd);
namespace crimson::osd {

void ScrubScheduler::enqueue_scrub_job(const scrub::ScrubJob& sjob)
{
  m_queue.enqueue_scrub_job(sjob);
}

void ScrubScheduler::enqueue_target(const scrub::SchedTarget& trgt)
{
  m_queue.enqueue_target(trgt);
}

void ScrubScheduler::dequeue_target(spg_t pgid, scrub_level_t s_or_d)
{
  m_queue.dequeue_target(pgid, s_or_d);
}

void ScrubScheduler::remove_from_osd_queue(spg_t pgid)
{
  m_queue.remove_from_osd_queue(pgid);
}

static inline bool isbetween_modulo(int64_t from, int64_t till, int p)
{
  // the 1st condition is because we have defined from==till as "always true"
  return (till == from) || ((till >= from) ^ (p >= from) ^ (p < till));
}

seastar::future<bool> ScrubScheduler::is_recovery_active()
{
  return shard_services.is_recovery_active();
}

bool ScrubScheduler::scrub_time_permit(utime_t now) const
{
  LOG_PREFIX(ScrubScheduler::scrub_time_permit);
  const time_t tt = now.sec();
  tm bdt;
  localtime_r(&tt, &bdt);

  bool day_permits = isbetween_modulo(
      crimson::common::local_conf().get_val<int64_t>("osd_scrub_begin_week_day"),
      crimson::common::local_conf().get_val<int64_t>("osd_scrub_end_week_day"),
      bdt.tm_wday);
  if (!day_permits) {
    DEBUG("week day {} outside permitted scrub days [{},{}]",
          bdt.tm_wday,
          crimson::common::local_conf().get_val<int64_t>("osd_scrub_begin_week_day"),
          crimson::common::local_conf().get_val<int64_t>("osd_scrub_end_week_day"));
    return false;
  }
  bool time_permits = isbetween_modulo(
      crimson::common::local_conf().get_val<int64_t>("osd_scrub_begin_hour"),
      crimson::common::local_conf().get_val<int64_t>("osd_scrub_end_hour"),
      bdt.tm_hour);
  DEBUG("hour {} {} permitted scrub hours [{},{}]",
        bdt.tm_hour,
        time_permits ? "within" : "outside",
        crimson::common::local_conf().get_val<int64_t>("osd_scrub_begin_hour"),
        crimson::common::local_conf().get_val<int64_t>("osd_scrub_end_hour"));
  return time_permits;
}
bool ScrubScheduler::scrub_load_below_threshold() const
{
  auto stats = shard_services.report_stats();

  // 1. reactor pressure
  if (stats.reactor_utilization >
      crimson::common::local_conf().get_val<double>("scrub_reactor_load_threshold")){  //0.8 in yaml
    return false;
  }

  return true;
}

bool ScrubScheduler::scrub_random_backoff() const
{
  LOG_PREFIX(ScrubScheduler::scrub_random_backoff);
  DEBUG("evaluating random backoff for scrub scheduling");
  auto ratio = crimson::common::local_conf().get_val<double>("osd_scrub_backoff_ratio");
  if (ceph::util::generate_random_number<double>(0.0, 1.0) < ratio) {
    DEBUG("random backoff ratio {} triggered, delaying scrub", ratio);
    return true;
  }
  return false;
}
//adjust this part
scrub::OSDRestrictions ScrubScheduler::restrictions_on_scrubbing(
    bool is_recovery_active,
    utime_t scrub_clock_now) const
{
  LOG_PREFIX(ScrubScheduler::restrictions_on_scrubbing);
  DEBUG("evaluating restrictions on scrubbing at time {}", scrub_clock_now);
  scrub::OSDRestrictions env_conditions;

  // some "environmental conditions" prevent all but specific types
  // (urgency levels) of scrubs

  if (!m_resource_bookkeeper.can_inc_scrubs()) {
    // our local shard is already running too many scrubs
    DEBUG("max concurrency reached, cannot start new scrubs");
    env_conditions.max_concurrency_reached = true;

  } else if (scrub_random_backoff()) {
    // dice-roll says we should not scrub now
    DEBUG("random backoff active, delaying scrub");
    env_conditions.random_backoff_active = true;
  }

  if (is_recovery_active && 
    ! crimson::common::local_conf().get_val<bool>("osd_scrub_during_recovery")) {
    DEBUG("recovery active and osd_scrub_during_recovery is false, "
          "delaying scrub");
    env_conditions.recovery_in_progress = true;
  }

  env_conditions.restricted_time = !scrub_time_permit(scrub_clock_now);
  env_conditions.cpu_overloaded = !scrub_load_below_threshold();

  return env_conditions;
}

scrub::schedule_result_t ScrubScheduler::initiate_a_scrub(
    const scrub::SchedEntry& candidate,
    scrub::OSDRestrictions restrictions)
{
  // TODO check if pg can do scrub, classic use try lock pg
  auto pg = shard_services.get_pg(candidate.pgid);
  return pg->start_scrubbing(candidate, restrictions);
}
bool ScrubScheduler::is_sched_target_eligible(
    const scrub::SchedEntry& e,
    const scrub::OSDRestrictions& r,
    utime_t time_now)
{
  LOG_PREFIX(ScrubScheduler::is_sched_target_eligible);
  using ScrubJob = scrub::ScrubJob;
  if (e.schedule.not_before > time_now) {
    DEBUG("target not eligible yet, scheduled for {}, current time is {}",
          e.schedule.not_before, time_now);
    return false;
  }
  if (r.max_concurrency_reached &&
      ScrubJob::observes_max_concurrency(e.urgency)) {
    DEBUG("max concurrency reached, target urgency {}, not eligible to start scrub",
          e.urgency);
    return false;
  }
  if (r.random_backoff_active &&
      ScrubJob::observes_random_backoff(e.urgency)) {
    DEBUG("random backoff active, target urgency {}, not eligible to start scrub",
          e.urgency);
    return false;
  }
  if (r.restricted_time && ScrubJob::observes_allowed_hours(e.urgency)) {
    DEBUG("current time outside allowed scrub hours, target urgency {}, not eligible to start scrub",
          e.urgency);
    return false;
  }
  if (r.cpu_overloaded && ScrubJob::observes_load_limit(e.urgency)) {
    DEBUG("CPU overloaded, target urgency {}, not eligible to start scrub",
          e.urgency);
    return false;
  }
  if (r.recovery_in_progress && ScrubJob::observes_recovery(e.urgency)) {
    DEBUG("recovery in progress, target urgency {}, not eligible to start scrub",
          e.urgency);
    return false;
  }
  return true;
}
void ScrubScheduler::initiate_scrub(bool is_recovery_active)
{
  LOG_PREFIX(ScrubScheduler::initiate_scrub);
  // to do check pg blocked ref: Chunkisbusy ScrubFindRange 
  DEBUG("checking for scrub candidates...");
  const utime_t scrub_time = ceph_clock_now();
  const auto env_restrictions =
    restrictions_on_scrubbing(is_recovery_active, scrub_time);

  auto candidate = m_queue.pop_ready_entry(
      is_sched_target_eligible, env_restrictions, scrub_time);
  if (!candidate) {
    DEBUG("no eligible scrub candidate found");
    return;
  }
  DEBUG("found scrub candidate: pg {}, urgency {}, scheduled at {}, not before {}",
        candidate->pgid, candidate->urgency, candidate->schedule.scheduled_at,
        candidate->schedule.not_before);
  switch (initiate_a_scrub(*candidate, env_restrictions)) {
    case scrub::schedule_result_t::target_specific_failure:
      DEBUG("scrub initiation failed for {}, reason is target_specific_failure", *candidate);
      break;
    case scrub::schedule_result_t::osd_wide_failure:
      DEBUG("scrub initiation failed for {}, reason is osd_wide_failure", *candidate);
      break;

    case scrub::schedule_result_t::scrub_initiated:
      DEBUG("scrub initiated for {}", *candidate);
      break;
  }
}

std::unique_ptr<scrub::LocalResourceWrapper> ScrubScheduler::inc_scrubs_local(
    bool is_high_priority)
{
  return m_resource_bookkeeper.inc_scrubs_local(is_high_priority);
}

void ScrubScheduler::dec_scrubs_local()
{
  m_resource_bookkeeper.dec_scrubs_local();
}
} // namespace crimson::osd