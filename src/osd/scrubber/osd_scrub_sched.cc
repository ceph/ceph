// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./osd_scrub_sched.h"

#include <string_view>
#include "osd/OSD.h"

#include "pg_scrubber.h"

using namespace ::std::chrono;
using namespace ::std::chrono_literals;
using namespace ::std::literals;
using qu_state_t = Scrub::qu_state_t;
using must_scrub_t = Scrub::must_scrub_t;
using ScrubQContainer = Scrub::ScrubQContainer;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
using ScrubJob = Scrub::ScrubJob;



// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue

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

ScrubQueue::ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds)
    : cct{cct}
    , osd_service{osds}
{}

std::ostream& ScrubQueue::gen_prefix(std::ostream& out, std::string_view fn)
    const
{
  return out << fmt::format(
	     "osd.{} scrub-queue:{}: ", osd_service.get_nodeid(), fn);
}

/*
 * Modify the scrub job state:
 * - if 'registered' (as expected): mark as 'unregistering'. The job will be
 *   dequeued the next time sched_scrub() is called.
 * - if already 'not_registered': shouldn't really happen, but not a problem.
 *   The state will not be modified.
 * - same for 'unregistering'.
 *
 * Note: not holding the jobs lock
 */
void ScrubQueue::remove_from_osd_queue(Scrub::ScrubJobRef scrub_job)
{
  dout(15) << "removing pg[" << scrub_job->pgid << "] from OSD scrub queue"
	   << dendl;

  qu_state_t expected_state{qu_state_t::registered};
  auto ret =
    scrub_job->state.compare_exchange_strong(expected_state,
					     qu_state_t::unregistering);

  if (ret) {

    dout(10) << "pg[" << scrub_job->pgid << "] sched-state changed from "
	     << ScrubJob::qu_state_text(expected_state) << " to "
	     << ScrubJob::qu_state_text(scrub_job->state) << dendl;

  } else {

    // job wasn't in state 'registered' coming in
    dout(5) << "removing pg[" << scrub_job->pgid
	    << "] failed. State was: " << ScrubJob::qu_state_text(expected_state)
	    << dendl;
  }
}

void ScrubQueue::register_with_osd(
  Scrub::ScrubJobRef scrub_job,
  const sched_params_t& suggested)
{
  qu_state_t state_at_entry = scrub_job->state.load();
  dout(20) << fmt::format(
		"pg[{}] state at entry: <{:.14}>", scrub_job->pgid,
		state_at_entry)
	   << dendl;

  switch (state_at_entry) {
    case qu_state_t::registered:
      // just updating the schedule?
      update_job(scrub_job, suggested, false /* keep n.b. delay */);
      break;

    case qu_state_t::not_registered:
      // insertion under lock
      {
	std::unique_lock lck{jobs_lock};

	if (state_at_entry != scrub_job->state) {
	  lck.unlock();
	  dout(5) << " scrub job state changed. Retrying." << dendl;
	  // retry
	  register_with_osd(scrub_job, suggested);
	  break;
	}

	update_job(scrub_job, suggested, true /* resets not_before */);
	to_scrub.push_back(scrub_job);
	scrub_job->in_queues = true;
	scrub_job->state = qu_state_t::registered;
      }
      break;

    case qu_state_t::unregistering:
      // restore to the to_sched queue
      {
	// must be under lock, as the job might be removed from the queue
	// at any minute
	std::lock_guard lck{jobs_lock};

	update_job(scrub_job, suggested, true /* resets not_before */);
	if (scrub_job->state == qu_state_t::not_registered) {
	  dout(5) << " scrub job state changed to 'not registered'" << dendl;
	  to_scrub.push_back(scrub_job);
	}
	scrub_job->in_queues = true;
	scrub_job->state = qu_state_t::registered;
      }
      break;
  }

  dout(10) << fmt::format(
		"pg[{}] sched-state changed from <{:.14}> to <{:.14}> (@{:s})",
		scrub_job->pgid, state_at_entry, scrub_job->state.load(),
		scrub_job->schedule.not_before)
	   << dendl;
}


void ScrubQueue::update_job(Scrub::ScrubJobRef scrub_job,
			    const sched_params_t& suggested,
                            bool reset_nb)
{
  // adjust the suggested scrub time according to OSD-wide status
  auto adjusted = adjust_target_time(suggested);
  scrub_job->high_priority = suggested.is_must == must_scrub_t::mandatory;
  scrub_job->update_schedule(adjusted, reset_nb);
}


void ScrubQueue::delay_on_failure(
    Scrub::ScrubJobRef sjob,
    std::chrono::seconds delay,
    Scrub::delay_cause_t delay_cause,
    utime_t now_is)
{
  dout(10) << fmt::format(
		  "pg[{}] delay_on_failure: delay:{} now:{:s}",
		  sjob->pgid, delay, now_is)
	   << dendl;
  sjob->delay_on_failure(delay, delay_cause, now_is);
}


std::vector<ScrubTargetId> ScrubQueue::ready_to_scrub(
    OSDRestrictions restrictions,  // note: 4B in size! (copy)
    utime_t scrub_tick)
{
  dout(10) << fmt::format(
		  " @{:s}: registered: {} ({})", scrub_tick,
		  to_scrub.size(), restrictions)
	   << dendl;

  //  create a list of candidates (copying, as otherwise creating a deadlock):
  //  - (if we didn't handle directly) remove invalid jobs
  //  - create a copy of the to_scrub (possibly up to first not-ripe)
  //  unlock, then try the lists
  std::unique_lock lck{jobs_lock};

  // remove the 'updated' flag from all entries
  std::for_each(
      to_scrub.begin(), to_scrub.end(),
      [](const auto& jobref) -> void { jobref->updated = false; });

  // collect all valid & ripe jobs. Note that we must copy,
  // as when we use the lists we will not be holding jobs_lock (see
  // explanation above)

  // and in this step 1 of the refactoring (Aug 2023): the set returned must be
  // transformed into a vector of targets (which, in this phase, are
  // the PG id-s).
  auto to_scrub_copy = collect_ripe_jobs(to_scrub, restrictions, scrub_tick);
  lck.unlock();

  std::vector<ScrubTargetId> all_ready;
  std::transform(
      to_scrub_copy.cbegin(), to_scrub_copy.cend(),
      std::back_inserter(all_ready),
      [](const auto& jobref) -> ScrubTargetId { return jobref->pgid; });
  return all_ready;
}


// must be called under lock
void ScrubQueue::rm_unregistered_jobs(ScrubQContainer& group)
{
  std::for_each(group.begin(), group.end(), [](auto& job) {
    if (job->state == qu_state_t::unregistering) {
      job->in_queues = false;
      job->state = qu_state_t::not_registered;
    } else if (job->state == qu_state_t::not_registered) {
      job->in_queues = false;
    }
  });

  group.erase(std::remove_if(group.begin(), group.end(), invalid_state),
	      group.end());
}

namespace {
struct cmp_time_n_priority_t {
  bool operator()(const Scrub::ScrubJobRef& lhs, const Scrub::ScrubJobRef& rhs)
      const
  {
    return lhs->is_high_priority() > rhs->is_high_priority() ||
	   (lhs->is_high_priority() == rhs->is_high_priority() &&
	    lhs->schedule.scheduled_at < rhs->schedule.scheduled_at);
  }
};
}  // namespace

// called under lock
ScrubQContainer ScrubQueue::collect_ripe_jobs(
    ScrubQContainer& group,
    OSDRestrictions restrictions,
    utime_t time_now)
{
  auto filtr = [time_now, rst = restrictions](const auto& jobref) -> bool {
    return jobref->schedule.not_before <= time_now &&
	   (!rst.high_priority_only || jobref->high_priority) &&
	   (!rst.only_deadlined || (!jobref->schedule.deadline.is_zero() &&
				    jobref->schedule.deadline <= time_now));
  };

  rm_unregistered_jobs(group);
  // copy ripe jobs (unless prohibited by 'restrictions')
  ScrubQContainer ripes;
  ripes.reserve(group.size());

  std::copy_if(group.begin(), group.end(), std::back_inserter(ripes), filtr);
  std::sort(ripes.begin(), ripes.end(), cmp_time_n_priority_t{});

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (const auto& jobref : group) {
      if (!filtr(jobref)) {
	dout(20) << fmt::format(
			" not eligible: {} @ {:s} ({:s},{:s})", jobref->pgid,
			jobref->schedule.not_before,
			jobref->schedule.scheduled_at, jobref->last_issue)
		 << dendl;
      }
    }
  }

  return ripes;
}


Scrub::scrub_schedule_t ScrubQueue::adjust_target_time(
  const sched_params_t& times) const
{
  Scrub::scrub_schedule_t sched_n_dead{
    times.proposed_time, times.proposed_time, times.proposed_time};

  if (times.is_must == Scrub::must_scrub_t::not_mandatory) {
    // unless explicitly requested, postpone the scrub with a random delay
    double scrub_min_interval = times.min_interval > 0
				  ? times.min_interval
				  : conf()->osd_scrub_min_interval;
    double scrub_max_interval = times.max_interval > 0
				  ? times.max_interval
				  : conf()->osd_scrub_max_interval;

    sched_n_dead.scheduled_at += scrub_min_interval;
    double r = rand() / (double)RAND_MAX;
    sched_n_dead.scheduled_at +=
      scrub_min_interval * conf()->osd_scrub_interval_randomize_ratio * r;

    if (scrub_max_interval <= 0) {
      sched_n_dead.deadline = utime_t{};
    } else {
      sched_n_dead.deadline += scrub_max_interval;
    }
    // note: no specific job can be named in the log message
    dout(20) << fmt::format(
		  "not-must. Was:{:s} {{min:{}/{} max:{}/{} ratio:{}}} "
		  "Adjusted:{:s} ({:s})",
		  times.proposed_time, fmt::group_digits(times.min_interval),
		  fmt::group_digits(conf()->osd_scrub_min_interval),
		  fmt::group_digits(times.max_interval),
		  fmt::group_digits(conf()->osd_scrub_max_interval),
		  conf()->osd_scrub_interval_randomize_ratio,
		  sched_n_dead.scheduled_at, sched_n_dead.deadline)
	     << dendl;
  }
  // else - no log needed. All relevant data will be logged by the caller
  return sched_n_dead;
}


void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
{
  ceph_assert(f != nullptr);
  std::lock_guard lck(jobs_lock);

  f->open_array_section("scrubs");
  std::for_each(
      to_scrub.cbegin(), to_scrub.cend(),
      [&f](const Scrub::ScrubJobRef& j) { j->dump(f); });
  f->close_section();
}

ScrubQContainer ScrubQueue::list_registered_jobs() const
{
  ScrubQContainer all_jobs;
  all_jobs.reserve(to_scrub.size());
  dout(20) << " size: " << all_jobs.capacity() << dendl;

  std::lock_guard lck{jobs_lock};
  std::copy_if(to_scrub.begin(),
	       to_scrub.end(),
	       std::back_inserter(all_jobs),
	       registered_job);
  return all_jobs;
}

// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue - maintaining the 'blocked on a locked object' count

void ScrubQueue::clear_pg_scrub_blocked(spg_t blocked_pg)
{
  dout(5) << fmt::format(": pg {} is unblocked", blocked_pg) << dendl;
  --blocked_scrubs_cnt;
  ceph_assert(blocked_scrubs_cnt >= 0);
}

void ScrubQueue::mark_pg_scrub_blocked(spg_t blocked_pg)
{
  dout(5) << fmt::format(": pg {} is blocked on an object", blocked_pg)
	  << dendl;
  ++blocked_scrubs_cnt;
}

int ScrubQueue::get_blocked_pgs_count() const
{
  return blocked_scrubs_cnt;
}
