// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./osd_scrub_sched.h"

#include "osdc/Objecter.h"
#include "osd/OSD.h"

#include "pg_scrubber.h"

using namespace ::std::chrono;
using namespace ::std::chrono_literals;
using namespace ::std::literals;

#define dout_subsys ceph_subsys_osd
#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix                                                            \
  *_dout << "osd." << osd_service.get_nodeid() << " scrub-queue::" << __func__ \
	 << " "



void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
{
  ceph_assert(f != nullptr);
  std::lock_guard lck(jobs_lock);

  f->open_array_section("scrubs");

  std::for_each(to_scrub.cbegin(), to_scrub.cend(), [&f](const ScrubJobRef& j) {
    j->dump(f);
  });

  std::for_each(penalized.cbegin(),
		penalized.cend(),
		[&f](const ScrubJobRef& j) { j->dump(f); });

  f->close_section();
}

// ////////////////////////////////////////////////////////////////////////// //
// scrub initiation - OSD code temporarily moved here from OSD.cc

#undef dout_context
#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, get_osdmap_epoch())

static ostream& _prefix(std::ostream* _dout, int whoami, epoch_t epoch) {
  return *_dout << "osd." << whoami << " " << epoch << " ";
}

void OSD::sched_scrub()
{
  auto& scrub_scheduler = service.get_scrub_services();

  if (auto blocked_pgs = scrub_scheduler.get_blocked_pgs_count();
      blocked_pgs > 0) {
    // some PGs managed by this OSD were blocked by a locked object during
    // scrub. This means we might not have the resources needed to scrub now.
    dout(10)
      << fmt::format(
	   "{}: PGs are blocked while scrubbing due to locked objects ({} PGs)",
	   __func__,
	   blocked_pgs)
      << dendl;
  }

  // fail fast if no resources are available
  if (!scrub_scheduler.can_inc_scrubs()) {
    dout(20) << __func__ << ": OSD cannot inc scrubs" << dendl;
    return;
  }

  // if there is a PG that is just now trying to reserve scrub replica resources -
  // we should wait and not initiate a new scrub
  if (scrub_scheduler.is_reserving_now()) {
    dout(20) << __func__ << ": scrub resources reservation in progress" << dendl;
    return;
  }

  Scrub::OSDRestrictions env_conditions;

  if (service.is_recovery_active() && !cct->_conf->osd_scrub_during_recovery) {
    if (!cct->_conf->osd_repair_during_recovery) {
      dout(15) << __func__ << ": not scheduling scrubs due to active recovery"
	       << dendl;
      return;
    }
    dout(10) << __func__
      << " will only schedule explicitly requested repair due to active recovery"
      << dendl;
    env_conditions.allow_requested_repair_only = true;
  }

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    dout(20) << __func__ << " sched_scrub starts" << dendl;
    auto all_jobs = scrub_scheduler.list_registered_jobs();
    for (const auto& sj : all_jobs) {
      dout(20) << "sched_scrub scrub-queue jobs: " << *sj << dendl;
    }
  }

  auto was_started = scrub_scheduler.select_pg_and_scrub(env_conditions);
  dout(20) << "sched_scrub done (" << ScrubQueue::attempt_res_text(was_started)
	   << ")" << dendl;
}

Scrub::schedule_result_t OSDService::initiate_a_scrub(spg_t pgid,
						      bool allow_requested_repair_only)
{
  dout(20) << __func__ << " trying " << pgid << dendl;

  // we have a candidate to scrub. We need some PG information to know if scrubbing is
  // allowed

  PGRef pg = osd->lookup_lock_pg(pgid);
  if (!pg) {
    // the PG was dequeued in the short timespan between creating the candidates list
    // (collect_ripe_jobs()) and here
    dout(5) << __func__ << " pg  " << pgid << " not found" << dendl;
    return Scrub::schedule_result_t::no_such_pg;
  }

  // This has already started, so go on to the next scrub job
  if (pg->is_scrub_queued_or_active()) {
    pg->unlock();
    dout(20) << __func__ << ": already in progress pgid " << pgid << dendl;
    return Scrub::schedule_result_t::already_started;
  }
  // Skip other kinds of scrubbing if only explicitly requested repairing is allowed
  if (allow_requested_repair_only && !pg->get_planned_scrub().must_repair) {
    pg->unlock();
    dout(10) << __func__ << " skip " << pgid
	     << " because repairing is not explicitly requested on it" << dendl;
    return Scrub::schedule_result_t::preconditions;
  }

  auto scrub_attempt = pg->sched_scrub();
  pg->unlock();
  return scrub_attempt;
}

void OSD::resched_all_scrubs()
{
  dout(10) << __func__ << ": start" << dendl;
  auto all_jobs = service.get_scrub_services().list_registered_jobs();
  for (auto& e : all_jobs) {

    auto& job = *e;
    dout(20) << __func__ << ": examine " << job.pgid << dendl;

    PGRef pg = _lookup_lock_pg(job.pgid);
    if (!pg)
      continue;

    dout(15) << __func__ << ": updating scrub schedule on " << job.pgid << dendl;
    pg->on_scrub_schedule_input_change();

    pg->unlock();
  }
  dout(10) << __func__ << ": done" << dendl;
}




#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix                                                            \
  *_dout << "osd." << osd_service.get_nodeid() << " scrub-queue::" << __func__ \
	 << " "



// ////////////////////////////////////////////////////////////////////////// //
// CPU load tracking and related

std::optional<double> ScrubQueue::update_load_average()
{
  int hb_interval = conf()->osd_heartbeat_interval;
  int n_samples = std::chrono::duration_cast<seconds>(24h).count();
  if (hb_interval > 1) {
    n_samples /= hb_interval;
    if (n_samples < 1)
      n_samples = 1;
  }

  // get CPU load avg
  double loadavg;
  if (getloadavg(&loadavg, 1) == 1) {
    daily_loadavg = (daily_loadavg * (n_samples - 1) + loadavg) / n_samples;
    dout(17) << "heartbeat: daily_loadavg " << daily_loadavg << dendl;
    return 100 * loadavg;
  }

  return std::nullopt;
}

bool ScrubQueue::scrub_load_below_threshold() const
{
  double loadavgs[3];
  if (getloadavg(loadavgs, 3) != 3) {
    dout(10) << __func__ << " couldn't read loadavgs\n" << dendl;
    return false;
  }

  // allow scrub if below configured threshold
  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
  double loadavg_per_cpu = cpus > 0 ? loadavgs[0] / cpus : loadavgs[0];
  if (loadavg_per_cpu < conf()->osd_scrub_load_threshold) {
    dout(20) << "loadavg per cpu " << loadavg_per_cpu << " < max "
	     << conf()->osd_scrub_load_threshold << " = yes" << dendl;
    return true;
  }

  // allow scrub if below daily avg and currently decreasing
  if (loadavgs[0] < daily_loadavg && loadavgs[0] < loadavgs[2]) {
    dout(20) << "loadavg " << loadavgs[0] << " < daily_loadavg "
	     << daily_loadavg << " and < 15m avg " << loadavgs[2] << " = yes"
	     << dendl;
    return true;
  }

  dout(20) << "loadavg " << loadavgs[0] << " >= max "
	   << conf()->osd_scrub_load_threshold << " and ( >= daily_loadavg "
	   << daily_loadavg << " or >= 15m avg " << loadavgs[2] << ") = no"
	   << dendl;
  return false;
}

// checks for half-closed ranges. Modify the (p<till)to '<=' to check for
// closed.
static inline bool isbetween_modulo(int64_t from, int64_t till, int p)
{
  // the 1st condition is because we have defined from==till as "always true"
  return (till == from) || ((till >= from) ^ (p >= from) ^ (p < till));
}

bool ScrubQueue::scrub_time_permit(utime_t now) const
{
  tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);

  bool day_permit = isbetween_modulo(conf()->osd_scrub_begin_week_day,
				     conf()->osd_scrub_end_week_day,
				     bdt.tm_wday);
  if (!day_permit) {
    dout(20) << "should run between week day "
	     << conf()->osd_scrub_begin_week_day << " - "
	     << conf()->osd_scrub_end_week_day << " now " << bdt.tm_wday
	     << " - no" << dendl;
    return false;
  }

  bool time_permit = isbetween_modulo(conf()->osd_scrub_begin_hour,
				      conf()->osd_scrub_end_hour,
				      bdt.tm_hour);
  dout(20) << "should run between " << conf()->osd_scrub_begin_hour << " - "
	   << conf()->osd_scrub_end_hour << " now (" << bdt.tm_hour
	   << ") = " << (time_permit ? "yes" : "no") << dendl;
  return time_permit;
}

std::chrono::milliseconds ScrubQueue::scrub_sleep_time(bool must_scrub) const
{
  std::chrono::milliseconds regular_sleep_period{
    uint64_t(std::max(0.0, conf()->osd_scrub_sleep) * 1000)};

  if (must_scrub || scrub_time_permit(time_now())) {
    return regular_sleep_period;
  }

  // relevant if scrubbing started during allowed time, but continued into
  // forbidden hours
  std::chrono::milliseconds extended_sleep{
    uint64_t(std::max(0.0, conf()->osd_scrub_extended_sleep) * 1000)};
  dout(20) << "w/ extended sleep (" << extended_sleep << ")" << dendl;

  return std::max(extended_sleep, regular_sleep_period);
}


