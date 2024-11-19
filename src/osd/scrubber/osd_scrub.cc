// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./osd_scrub.h"

#include "osd/OSD.h"
#include "osd/osd_perf_counters.h"
#include "osdc/Objecter.h"

#include "pg_scrubber.h"

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
    , m_load_tracker{cct, conf, m_osd_svc.get_nodeid()}
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
  f->open_array_section("remote_scrub_reservations");
  m_osd_svc.get_scrub_reserver().dump(f);
  f->close_section();
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

  // some environmental conditions prevent all but high priority scrubs

  if (!m_resource_bookkeeper.can_inc_scrubs()) {
    // our local OSD is already running too many scrubs
    dout(15) << "OSD cannot inc scrubs" << dendl;
    env_conditions.max_concurrency_reached = true;

  } else if (scrub_random_backoff()) {
    // dice-roll says we should not scrub now
    dout(15) << "Lost in dice. Only high priority scrubs allowed." << dendl;
    env_conditions.random_backoff_active = true;

  } else if (is_recovery_active && !conf->osd_scrub_during_recovery) {
    if (conf->osd_repair_during_recovery) {
      dout(15)
	  << "will only schedule explicitly requested repair due to active "
	     "recovery"
	  << dendl;
      env_conditions.allow_requested_repair_only = true;

    } else {
      dout(15) << "recovery in progress. Operator-initiated scrubs only."
	       << dendl;
      env_conditions.recovery_in_progress = true;
    }
  } else {

    // regular, i.e. non-high-priority scrubs are allowed
    env_conditions.restricted_time = !scrub_time_permit(scrub_clock_now);
    env_conditions.cpu_overloaded =
	!m_load_tracker.scrub_load_below_threshold();
  }

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
    locked_pg->pg()->on_scrub_schedule_input_change(
	Scrub::delay_ready_t::no_delay);
  }
}

// ////////////////////////////////////////////////////////////////////////// //
// CPU load tracking and related

OsdScrub::LoadTracker::LoadTracker(
    CephContext* cct,
    const ceph::common::ConfigProxy& config,
    int node_id)
    : cct{cct}
    , conf{config}
    , log_prefix{fmt::format("osd.{} scrub-queue::load-tracker::", node_id)}
{
  // initialize the daily loadavg with current 15min loadavg
  if (double loadavgs[3]; getloadavg(loadavgs, 3) == 3) {
    daily_loadavg = loadavgs[2];
  } else {
    derr << "OSD::init() : couldn't read loadavgs\n" << dendl;
    daily_loadavg = 1.0;
  }
}

///\todo replace with Knuth's algo (to reduce the numerical error)
std::optional<double> OsdScrub::LoadTracker::update_load_average()
{
  auto hb_interval = conf->osd_heartbeat_interval;
  int n_samples = std::chrono::duration_cast<seconds>(24h).count();
  if (hb_interval > 1) {
    n_samples = std::max(n_samples / hb_interval, 1L);
  }

  double loadavg;
  if (getloadavg(&loadavg, 1) == 1) {
    daily_loadavg = (daily_loadavg * (n_samples - 1) + loadavg) / n_samples;
    return 100 * loadavg;
  }

  return std::nullopt;	// getloadavg() failed
}

bool OsdScrub::LoadTracker::scrub_load_below_threshold() const
{
  double loadavgs[3];
  if (getloadavg(loadavgs, 3) != 3) {
    dout(10) << "couldn't read loadavgs" << dendl;
    return false;
  }

  // allow scrub if below configured threshold
  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
  double loadavg_per_cpu = cpus > 0 ? loadavgs[0] / cpus : loadavgs[0];
  if (loadavg_per_cpu < conf->osd_scrub_load_threshold) {
    dout(20) << fmt::format(
		    "loadavg per cpu {:.3f} < max {:.3f} = yes",
		    loadavg_per_cpu, conf->osd_scrub_load_threshold)
	     << dendl;
    return true;
  }

  // allow scrub if below daily avg and currently decreasing
  if (loadavgs[0] < daily_loadavg && loadavgs[0] < loadavgs[2]) {
    dout(20) << fmt::format(
		    "loadavg {:.3f} < daily_loadavg {:.3f} and < 15m avg "
		    "{:.3f} = yes",
		    loadavgs[0], daily_loadavg, loadavgs[2])
	     << dendl;
    return true;
  }

  dout(10) << fmt::format(
		  "loadavg {:.3f} >= max {:.3f} and ( >= daily_loadavg {:.3f} "
		  "or >= 15m avg {:.3f} ) = no",
		  loadavgs[0], conf->osd_scrub_load_threshold, daily_loadavg,
		  loadavgs[2])
	   << dendl;
  return false;
}

std::ostream& OsdScrub::LoadTracker::gen_prefix(
    std::ostream& out,
    std::string_view fn) const
{
  return out << log_prefix << fn << ": ";
}

std::optional<double> OsdScrub::update_load_average()
{
  return m_load_tracker.update_load_average();
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
    utime_t t,
    bool high_priority_scrub) const
{
  const milliseconds regular_sleep_period =
      milliseconds{int64_t(std::max(0.0, 1'000 * conf->osd_scrub_sleep))};

  if (high_priority_scrub || scrub_time_permit(t)) {
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
