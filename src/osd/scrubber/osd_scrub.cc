// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./osd_scrub.h"

#include "osd/OSD.h"
#include "osd/osd_perf_counters.h"
#include "osdc/Objecter.h"

#include "pg_scrubber.h"
#include "common/debug.h"

using namespace ::std::chrono;
using namespace ::std::chrono_literals;
using namespace ::std::literals;
using schedule_result_t = Scrub::schedule_result_t;


#define dout_subsys ceph_subsys_osd
#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)

template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}

OsdScrub::OsdScrub(
    CephContext* cct,
    Scrub::ScrubSchedListener& osd_svc,
    const ceph::common::ConfigProxy& config)
    : cct{cct}
    , m_osd_svc{osd_svc}
    , conf{config}
    , m_resource_bookkeeper{[this](std::string msg) { log_fwd(msg); }, conf}
    , m_queue{cct, m_osd_svc}
    , m_log_prefix{fmt::format("osd.{} osd-scrub:", m_osd_svc.get_nodeid())}
{
  create_scrub_perf_counters();
}

OsdScrub::~OsdScrub()
{
  destroy_scrub_perf_counters();
}

std::ostream& OsdScrub::gen_prefix(std::ostream& out, std::string_view fn) const
{
  if (fn.starts_with("operator")) {
    // it's a lambda, and __func__ is not available
    return out << m_log_prefix;
  } else {
    return out << m_log_prefix << fn << ": ";
  }
}

void OsdScrub::dump_scrubs(ceph::Formatter* f) const
{
  m_queue.dump_scrubs(f);
}

void OsdScrub::dump_scrub_reservations(ceph::Formatter* f) const
{
  m_resource_bookkeeper.dump_scrub_reservations(f);
  Formatter::ObjectSection rmt_section{*f, "remote_scrub_reservations"sv};
  m_osd_svc.get_scrub_reserver().dump(f);
}

void OsdScrub::log_fwd(std::string_view text)
{
  dout(20) << text << dendl;
}

bool OsdScrub::scrub_random_backoff() const
{
  if (random_bool_with_probability(conf->osd_scrub_backoff_ratio)) {
    dout(20) << fmt::format(
		    "lost coin flip, randomly backing off (ratio: {:.3f})",
		    conf->osd_scrub_backoff_ratio)
	     << dendl;
    return true;  // backing off
  }
  return false;
}

void OsdScrub::debug_log_all_jobs() const
{
  m_queue.for_each_job([this](const Scrub::SchedEntry& sj) {
    dout(20) << fmt::format("\tscrub-queue jobs: {}", sj) << dendl;
  }, 20);
}


void OsdScrub::initiate_scrub(bool is_recovery_active)
{
  if (auto blocked_pgs = get_blocked_pgs_count(); blocked_pgs > 0) {
    // some PGs managed by this OSD were blocked by a locked object during
    // scrub. This means we might not have the resources needed to scrub now.
    dout(10)
	<< fmt::format(
	       "PGs are blocked while scrubbing due to locked objects ({} PGs)",
	       blocked_pgs)
	<< dendl;
  }

  const utime_t scrub_time = ceph_clock_now();

  // check the OSD-wide environment conditions (scrub resources, time, etc.).
  // These may restrict the type of scrubs we are allowed to start, or just
  // prevent us from starting any non-operator-initiated scrub at all.
  const auto env_restrictions =
      restrictions_on_scrubbing(is_recovery_active, scrub_time);

  dout(10) << fmt::format(
		  "scrub scheduling (@tick) starts. "
		  "time now:{:s}, recovery is active?:{} restrictions:{}",
		  scrub_time, is_recovery_active, env_restrictions)
	   << dendl;

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>() &&
      !env_restrictions.max_concurrency_reached &&
      !env_restrictions.random_backoff_active) {
    debug_log_all_jobs();
  }

  auto candidate = m_queue.pop_ready_entry(
      is_sched_target_eligible, env_restrictions, scrub_time);
  if (!candidate) {
    dout(20) << "no PGs are ready for scrubbing" << dendl;
    return;
  }

  switch (initiate_a_scrub(*candidate, env_restrictions)) {
    case schedule_result_t::target_specific_failure:
    case schedule_result_t::osd_wide_failure:
      // No scrub this tick.
      // someone else will requeue the target, if needed.
      break;

    case schedule_result_t::scrub_initiated:
      dout(20) << fmt::format("scrub initiated for pg[{}]", candidate->pgid)
	       << dendl;
      break;
  }
}


/*
 * Note: only checking those conditions that are frequent, and should not cause
 * a queue reshuffle.
 */
bool OsdScrub::is_sched_target_eligible(
    const Scrub::SchedEntry& e,
    const Scrub::OSDRestrictions& r,
    utime_t time_now)
{
  using ScrubJob = Scrub::ScrubJob;
  if (e.schedule.not_before > time_now) {
    return false;
  }
  if (r.max_concurrency_reached &&
      ScrubJob::observes_max_concurrency(e.urgency)) {
    return false;
  }
  if (r.random_backoff_active &&
      ScrubJob::observes_random_backoff(e.urgency)) {
    return false;
  }
  if (r.restricted_time && ScrubJob::observes_allowed_hours(e.urgency)) {
    return false;
  }
  if (r.cpu_overloaded && ScrubJob::observes_load_limit(e.urgency)) {
    return false;
  }
  if (r.recovery_in_progress && ScrubJob::observes_recovery(e.urgency)) {
    return false;
  }
  return true;
}


Scrub::OSDRestrictions OsdScrub::restrictions_on_scrubbing(
    bool is_recovery_active,
    utime_t scrub_clock_now) const
{
  Scrub::OSDRestrictions env_conditions;

  // some "environmental conditions" prevent all but specific types
  // (urgency levels) of scrubs

  if (!m_resource_bookkeeper.can_inc_scrubs()) {
    // our local OSD is already running too many scrubs
    dout(15) << "OSD cannot inc scrubs" << dendl;
    env_conditions.max_concurrency_reached = true;

  } else if (scrub_random_backoff()) {
    // dice-roll says we should not scrub now
    dout(15) << "Lost on the dice. Regular scheduled scrubs are not permitted."
	     << dendl;
    env_conditions.random_backoff_active = true;
  }

  if (is_recovery_active && !conf->osd_scrub_during_recovery) {
    dout(15) << "recovery in progress. Operator-initiated scrubs only."
	     << dendl;
    env_conditions.recovery_in_progress = true;
  }

  env_conditions.restricted_time = !scrub_time_permit(scrub_clock_now);
  env_conditions.cpu_overloaded = !scrub_load_below_threshold();

  return env_conditions;
}


Scrub::schedule_result_t OsdScrub::initiate_a_scrub(
    const Scrub::SchedEntry& candidate,
    Scrub::OSDRestrictions restrictions)
{
  dout(20) << fmt::format(
		  "trying pg[{}] (target:{})", candidate.pgid, candidate)
	   << dendl;

  // we have a candidate to scrub. We need some PG information to
  // know if scrubbing is allowed

  auto locked_pg = m_osd_svc.get_locked_pg(candidate.pgid);
  if (!locked_pg) {
    // the PG was dequeued in the short timespan between querying the
    // scrub queue - and now.
    dout(5) << fmt::format("pg[{}] not found", candidate.pgid) << dendl;
    return Scrub::schedule_result_t::target_specific_failure;
  }

  // note: the 'candidate' (a SchedEntry, identifying PG & level)
  // was already dequeued. The "original" scrub job cannot be accessed from
  // here directly. Thus - we leave it to start_scrubbing() (via a call
  // to PgScrubber::start_scrub_session()) to mark it as dequeued.
  return locked_pg->pg()->start_scrubbing(candidate, restrictions);
}


void OsdScrub::on_config_change()
{
  auto to_notify = m_queue.get_pgs(
      [](const Scrub::SchedEntry& sj, bool) -> bool { return true; });

  for (const auto& p : to_notify) {
    dout(30) << fmt::format("rescheduling pg[{}] scrubs", p) << dendl;
    auto locked_pg = m_osd_svc.get_locked_pg(p);
    if (!locked_pg)
      continue;

    dout(15) << fmt::format(
		    "updating scrub schedule on {}",
		    (locked_pg->pg())->get_pgid())
	     << dendl;
    locked_pg->pg()->on_scrub_schedule_input_change();
  }
}


// ////////////////////////////////////////////////////////////////////////// //
// CPU load tracking and related

std::optional<double> OsdScrub::update_load_average()
{
  // cache the number of CPUs
  loadavg_cpu_count = std::max(sysconf(_SC_NPROCESSORS_ONLN), 1L);

  double loadavg;
  if (getloadavg(&loadavg, 1) != 1) {
    return std::nullopt;
  }
  return loadavg;
}


bool OsdScrub::scrub_load_below_threshold() const
{
  // fetch an up-to-date load average.
  // For the number of CPUs - rely on the last known value, fetched in the
  // 'heartbeat' thread.
  double loadavg;
  if (getloadavg(&loadavg, 1) != 1) {
    loadavg = 0;
  }

  const double loadavg_per_cpu = loadavg / loadavg_cpu_count;
  if (loadavg_per_cpu < conf->osd_scrub_load_threshold) {
    dout(20) << fmt::format(
		    "loadavg per cpu {:.3f} < max {:.3f} (#CPUs:{}) = yes",
		    loadavg_per_cpu, conf->osd_scrub_load_threshold,
		    loadavg_cpu_count)
	     << dendl;
    return true;
  }

  dout(5) << fmt::format(
		  "loadavg {:.3f} >= max {:.3f} (#CPUs:{}) = no",
		  loadavg_per_cpu, conf->osd_scrub_load_threshold,
		  loadavg_cpu_count)
	   << dendl;
  return false;
}


// ////////////////////////////////////////////////////////////////////////// //

// checks for half-closed ranges. Modify the (p<till)to '<=' to check for
// closed.
static inline bool isbetween_modulo(int64_t from, int64_t till, int p)
{
  // the 1st condition is because we have defined from==till as "always true"
  return (till == from) || ((till >= from) ^ (p >= from) ^ (p < till));
}

bool OsdScrub::scrub_time_permit(utime_t now) const
{
  const time_t tt = now.sec();
  tm bdt;
  localtime_r(&tt, &bdt);

  bool day_permits = isbetween_modulo(
      conf->osd_scrub_begin_week_day, conf->osd_scrub_end_week_day,
      bdt.tm_wday);
  if (!day_permits) {
    dout(20) << fmt::format(
		    "should run between week day {} - {} now {} - no",
		    conf->osd_scrub_begin_week_day,
		    conf->osd_scrub_end_week_day, bdt.tm_wday)
	     << dendl;
    return false;
  }

  bool time_permits = isbetween_modulo(
      conf->osd_scrub_begin_hour, conf->osd_scrub_end_hour, bdt.tm_hour);
  dout(20) << fmt::format(
		  "should run between {} - {} now {} = {}",
		  conf->osd_scrub_begin_hour, conf->osd_scrub_end_hour,
		  bdt.tm_hour, (time_permits ? "yes" : "no"))
	   << dendl;
  return time_permits;
}


std::chrono::milliseconds OsdScrub::scrub_sleep_time(
    utime_t t_now,
    bool scrub_respects_ext_sleep) const
{
  const milliseconds regular_sleep_period =
      milliseconds{int64_t(std::max(0.0, 1'000 * conf->osd_scrub_sleep))};

  if (!scrub_respects_ext_sleep || scrub_time_permit(t_now)) {
    return regular_sleep_period;
  }

  // relevant if scrubbing started during allowed time, but continued into
  // forbidden hours
  const milliseconds extended_sleep =
      milliseconds{int64_t(1'000 * conf->osd_scrub_extended_sleep)};
  dout(20) << fmt::format(
		  "scrubbing started during allowed time, but continued into "
		  "forbidden hours. regular_sleep_period {} extended_sleep {}",
		  regular_sleep_period, extended_sleep)
	   << dendl;
  return std::max(extended_sleep, regular_sleep_period);
}


// ////////////////////////////////////////////////////////////////////////// //
// scrub-related performance counters

void OsdScrub::create_scrub_perf_counters()
{
  auto idx = perf_counters_indices.begin();
  // create a separate set for each pool type & scrub level
  for (const auto& label : perf_labels) {
    PerfCounters* counters = build_scrub_labeled_perf(cct, label);
    ceph_assert(counters);
    cct->get_perfcounters_collection()->add(counters);
    m_perf_counters[*(idx++)] = counters;
  }
}

void OsdScrub::destroy_scrub_perf_counters()
{
  for (const auto& [label, counters] : m_perf_counters) {
    std::ignore = label;
    cct->get_perfcounters_collection()->remove(counters);
    delete counters;
  }
  m_perf_counters.clear();
}

PerfCounters* OsdScrub::get_perf_counters(int pool_type, scrub_level_t level)
{
  return m_perf_counters[pc_index_t{level, pool_type}];
}

// ////////////////////////////////////////////////////////////////////////// //
// forwarders to the queue


void OsdScrub::enqueue_scrub_job(const Scrub::ScrubJob& sjob)
{
  m_queue.enqueue_scrub_job(sjob);
}

void OsdScrub::enqueue_target(const Scrub::SchedTarget& trgt)
{
  m_queue.enqueue_target(trgt);
}

void OsdScrub::dequeue_target(spg_t pgid, scrub_level_t s_or_d)
{
  m_queue.dequeue_target(pgid, s_or_d);
}

void OsdScrub::remove_from_osd_queue(spg_t pgid)
{
  m_queue.remove_from_osd_queue(pgid);
}

std::unique_ptr<Scrub::LocalResourceWrapper> OsdScrub::inc_scrubs_local(
    bool is_high_priority)
{
  return m_resource_bookkeeper.inc_scrubs_local(is_high_priority);
}

void OsdScrub::dec_scrubs_local()
{
  m_resource_bookkeeper.dec_scrubs_local();
}

void OsdScrub::mark_pg_scrub_blocked(spg_t blocked_pg)
{
  m_queue.mark_pg_scrub_blocked(blocked_pg);
}

void OsdScrub::clear_pg_scrub_blocked(spg_t blocked_pg)
{
  m_queue.clear_pg_scrub_blocked(blocked_pg);
}

int OsdScrub::get_blocked_pgs_count() const
{
  return m_queue.get_blocked_pgs_count();
}
