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
  return out << m_log_prefix << fn << ": ";
}

void OsdScrub::dump_scrubs(ceph::Formatter* f) const
{
  m_queue.dump_scrubs(f);
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


void OsdScrub::initiate_scrub(bool is_recovery_active)
{
  const utime_t scrub_time = ceph_clock_now();
  dout(10) << fmt::format(
		  "time now:{:s}, recovery is active?:{}", scrub_time,
		  is_recovery_active)
	   << dendl;

  if (auto blocked_pgs = get_blocked_pgs_count(); blocked_pgs > 0) {
    // some PGs managed by this OSD were blocked by a locked object during
    // scrub. This means we might not have the resources needed to scrub now.
    dout(10)
	<< fmt::format(
	       "PGs are blocked while scrubbing due to locked objects ({} PGs)",
	       blocked_pgs)
	<< dendl;
  }

  // check the OSD-wide environment conditions (scrub resources, time, etc.).
  // These may restrict the type of scrubs we are allowed to start, or just
  // prevent us from starting any non-operator-initiated scrub at all.
  auto env_restrictions =
      restrictions_on_scrubbing(is_recovery_active, scrub_time);

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>() &&
      !env_restrictions.high_priority_only) {
    dout(20) << "scrub scheduling (@tick) starts" << dendl;
    auto all_jobs = m_queue.list_registered_jobs();
    for (const auto& sj : all_jobs) {
      dout(20) << fmt::format("\tscrub-queue jobs: {}", *sj) << dendl;
    }
  }

  // at this phase of the refactoring: minimal changes to the
  // queue interface used here: we ask for a list of
  // eligible targets (based on the known restrictions).
  // We try all elements of this list until a (possibly temporary) success.
  auto candidates = m_queue.ready_to_scrub(env_restrictions, scrub_time);
  if (candidates.empty()) {
    dout(20) << "no PGs are ready for scrubbing" << dendl;
    return;
  }

  for (const auto& candidate : candidates) {
    dout(20) << fmt::format("initiating scrub on pg[{}]", candidate) << dendl;

    // we have a candidate to scrub. But we may fail when trying to initiate that
    // scrub. For some failures - we can continue with the next candidate. For
    // others - we should stop trying to scrub at this tick.
    auto res = initiate_a_scrub(candidate, env_restrictions);

    if (res == schedule_result_t::target_specific_failure) {
      // continue with the next job.
      // \todo: consider separate handling of "no such PG", as - later on -
      // we should be removing both related targets.
      continue;
    } else if (res == schedule_result_t::osd_wide_failure) {
      // no point in trying the other candidates at this time
      break;
    } else {
      // the happy path. We are done
      dout(20) << fmt::format("scrub initiated for pg[{}]", candidate.pgid)
               << dendl;
      break;
    }
  }
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
    env_conditions.high_priority_only = true;

  } else if (scrub_random_backoff()) {
    // dice-roll says we should not scrub now
      dout(15) << "Lost in dice. Only high priority scrubs allowed."
	       << dendl;
      env_conditions.high_priority_only = true;

  } else if (m_queue.is_reserving_now()) {
    // if there is a PG that is just now trying to reserve scrub replica
    // resources - we should wait and not initiate a new scrub
    dout(10) << "scrub resources reservation in progress" << dendl;
    env_conditions.high_priority_only = true;

  } else if (is_recovery_active && !conf->osd_scrub_during_recovery) {
    if (conf->osd_repair_during_recovery) {
      dout(15)
	  << "will only schedule explicitly requested repair due to active "
	     "recovery"
	  << dendl;
      env_conditions.allow_requested_repair_only = true;

    } else {
      dout(15) << "recovery in progress. Only high priority scrubs allowed."
	       << dendl;
      env_conditions.high_priority_only = true;
    }
  } else {

    // regular, i.e. non-high-priority scrubs are allowed
    env_conditions.time_permit = scrub_time_permit(scrub_clock_now);
    env_conditions.load_is_low = m_load_tracker.scrub_load_below_threshold();
    env_conditions.only_deadlined =
	!env_conditions.time_permit || !env_conditions.load_is_low;
  }

  return env_conditions;
}


Scrub::schedule_result_t OsdScrub::initiate_a_scrub(
    spg_t pgid,
    Scrub::OSDRestrictions restrictions)
{
  dout(20) << fmt::format("trying pg[{}]", pgid) << dendl;

  // we have a candidate to scrub. We need some PG information to
  // know if scrubbing is allowed

  auto locked_pg = m_osd_svc.get_locked_pg(pgid);
  if (!locked_pg) {
    // the PG was dequeued in the short timespan between creating the
    // candidates list (ready_to_scrub()) and here
    dout(5) << fmt::format("pg[{}] not found", pgid) << dendl;
    return Scrub::schedule_result_t::target_specific_failure;
  }

  // later on, here is where the scrub target would be dequeued
  return locked_pg->pg()->start_scrubbing(restrictions);
}

void OsdScrub::on_config_change()
{
  auto to_notify = m_queue.list_registered_jobs();

  for (const auto& p : to_notify) {
    dout(30) << fmt::format("rescheduling pg[{}] scrubs", *p) << dendl;
    auto locked_pg = m_osd_svc.get_locked_pg(p->pgid);
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
  int hb_interval = conf->osd_heartbeat_interval;
  int n_samples = std::chrono::duration_cast<seconds>(24h).count();
  if (hb_interval > 1) {
    n_samples = std::max(n_samples / hb_interval, 1);
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

Scrub::sched_params_t OsdScrub::determine_scrub_time(
    const requested_scrub_t& request_flags,
    const pg_info_t& pg_info,
    const pool_opts_t& pool_conf) const
{
  return m_queue.determine_scrub_time(request_flags, pg_info, pool_conf);
}

void OsdScrub::update_job(
    Scrub::ScrubJobRef sjob,
    const Scrub::sched_params_t& suggested,
    bool reset_notbefore)
{
  m_queue.update_job(sjob, suggested, reset_notbefore);
}

void OsdScrub::register_with_osd(
    Scrub::ScrubJobRef sjob,
    const Scrub::sched_params_t& suggested)
{
  m_queue.register_with_osd(sjob, suggested);
}

void OsdScrub::remove_from_osd_queue(Scrub::ScrubJobRef sjob)
{
  m_queue.remove_from_osd_queue(sjob);
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

bool OsdScrub::inc_scrubs_remote(pg_t pgid)
{
  return m_resource_bookkeeper.inc_scrubs_remote(pgid);
}

void OsdScrub::dec_scrubs_remote(pg_t pgid)
{
  m_resource_bookkeeper.dec_scrubs_remote(pgid);
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

bool OsdScrub::set_reserving_now(spg_t reserving_id, utime_t now_is)
{
  return m_queue.set_reserving_now(reserving_id, now_is);
}

void OsdScrub::clear_reserving_now(spg_t reserving_id)
{
  m_queue.clear_reserving_now(reserving_id);
}
