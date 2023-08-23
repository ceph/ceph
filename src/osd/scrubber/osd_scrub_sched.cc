// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./osd_scrub_sched.h"

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

// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd." << whoami << " "

Scrub::ScrubJob::ScrubJob(CephContext* cct, const spg_t& pg, int node_id)
    : RefCountedObject{cct}
    , pgid{pg}
    , whoami{node_id}
    , cct{cct}
{}

// debug usage only
ostream& operator<<(ostream& out, const Scrub::ScrubJob& sjob)
{
  out << sjob.pgid << ",  " << sjob.schedule.scheduled_at
      << " dead: " << sjob.schedule.deadline << " - "
      << sjob.registration_state() << " / failure: " << sjob.resources_failure
      << " / pen. t.o.: " << sjob.penalty_timeout
      << " / queue state: " << Scrub::ScrubJob::qu_state_text(sjob.state);

  return out;
}

void Scrub::ScrubJob::update_schedule(
  const Scrub::scrub_schedule_t& adjusted)
{
  schedule = adjusted;
  penalty_timeout = utime_t(0, 0);  // helps with debugging

  // 'updated' is changed here while not holding jobs_lock. That's OK, as
  // the (atomic) flag will only be cleared by select_pg_and_scrub() after
  // scan_penalized() is called and the job was moved to the to_scrub queue.
  updated = true;
  dout(10) << fmt::format("{}: pg[{}] adjusted: {:s} ({})", __func__, pgid,
                          schedule.scheduled_at, registration_state()) << dendl;
}

std::string Scrub::ScrubJob::scheduling_state(utime_t now_is,
						   bool is_deep_expected) const
{
  // if not in the OSD scheduling queues, not a candidate for scrubbing
  if (state != qu_state_t::registered) {
    return "no scrub is scheduled";
  }

  // if the time has passed, we are surely in the queue
  // (note that for now we do not tell client if 'penalized')
  if (now_is > schedule.scheduled_at) {
    // we are never sure that the next scrub will indeed be shallow:
    return fmt::format("queued for {}scrub", (is_deep_expected ? "deep " : ""));
  }

  return fmt::format("{}scrub scheduled @ {:s}",
		     (is_deep_expected ? "deep " : ""),
		     schedule.scheduled_at);
}


// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue

#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix                                                            \
  *_dout << "osd." << osd_service.get_nodeid() << " scrub-queue::" << __func__ \
	 << " "


ScrubQueue::ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds)
    : cct{cct}
    , osd_service{osds}
{
  // initialize the daily loadavg with current 15min loadavg
  if (double loadavgs[3]; getloadavg(loadavgs, 3) == 3) {
    daily_loadavg = loadavgs[2];
  } else {
    derr << "OSD::init() : couldn't read loadavgs\n" << dendl;
    daily_loadavg = 1.0;
  }
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
	     << Scrub::ScrubJob::qu_state_text(expected_state) << " to "
	     << Scrub::ScrubJob::qu_state_text(scrub_job->state) << dendl;

  } else {

    // job wasn't in state 'registered' coming in
    dout(5) << "removing pg[" << scrub_job->pgid
	    << "] failed. State was: " << Scrub::ScrubJob::qu_state_text(expected_state)
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
      update_job(scrub_job, suggested);
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

	update_job(scrub_job, suggested);
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

	update_job(scrub_job, suggested);
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
		scrub_job->schedule.scheduled_at)
	   << dendl;
}

// look mommy - no locks!
void ScrubQueue::update_job(Scrub::ScrubJobRef scrub_job,
			    const sched_params_t& suggested)
{
  // adjust the suggested scrub time according to OSD-wide status
  auto adjusted = adjust_target_time(suggested);
  scrub_job->update_schedule(adjusted);
}

sched_params_t ScrubQueue::determine_scrub_time(
  const requested_scrub_t& request_flags,
  const pg_info_t& pg_info,
  const pool_opts_t& pool_conf) const
{
  sched_params_t res;

  if (request_flags.must_scrub || request_flags.need_auto) {

    // Set the smallest time that isn't utime_t()
    res.proposed_time = PgScrubber::scrub_must_stamp();
    res.is_must = Scrub::must_scrub_t::mandatory;
    // we do not need the interval data in this case

  } else if (pg_info.stats.stats_invalid && conf()->osd_scrub_invalid_stats) {
    res.proposed_time = time_now();
    res.is_must = Scrub::must_scrub_t::mandatory;

  } else {
    res.proposed_time = pg_info.history.last_scrub_stamp;
    res.min_interval = pool_conf.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
    res.max_interval = pool_conf.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  }

  dout(15) << fmt::format(
		"suggested: {:s} hist: {:s} v:{}/{} must:{} pool-min:{} {}",
		res.proposed_time, pg_info.history.last_scrub_stamp,
		(bool)pg_info.stats.stats_invalid,
		conf()->osd_scrub_invalid_stats,
		(res.is_must == must_scrub_t::mandatory ? "y" : "n"),
		res.min_interval, request_flags)
	   << dendl;
  return res;
}


// used under jobs_lock
void ScrubQueue::move_failed_pgs(utime_t now_is)
{
  int punished_cnt{0};	// for log/debug only

  for (auto job = to_scrub.begin(); job != to_scrub.end();) {
    if ((*job)->resources_failure) {
      auto sjob = *job;

      // last time it was scheduled for a scrub, this PG failed in securing
      // remote resources. Move it to the secondary scrub queue.

      dout(15) << "moving " << sjob->pgid
	       << " state: " << Scrub::ScrubJob::qu_state_text(sjob->state) << dendl;

      // determine the penalty time, after which the job should be reinstated
      utime_t after = now_is;
      after += conf()->osd_scrub_sleep * 2 + utime_t{300'000ms};

      // note: currently - not taking 'deadline' into account when determining
      // 'penalty_timeout'.
      sjob->penalty_timeout = after;
      sjob->resources_failure = false;
      sjob->updated = false;  // as otherwise will be pardoned immediately

      // place in the penalty list, and remove from the to-scrub group
      penalized.push_back(sjob);
      job = to_scrub.erase(job);
      punished_cnt++;
    } else {
      job++;
    }
  }

  if (punished_cnt) {
    dout(15) << "# of jobs penalized: " << punished_cnt << dendl;
  }
}

// clang-format off
/*
 * Implementation note:
 * Clang (10 & 11) produces here efficient table-based code, comparable to using
 * a direct index into an array of strings.
 * Gcc (11, trunk) is almost as efficient.
 */
std::string_view ScrubQueue::attempt_res_text(Scrub::schedule_result_t v)
{
  switch (v) {
    case Scrub::schedule_result_t::scrub_initiated: return "scrubbing"sv;
    case Scrub::schedule_result_t::none_ready: return "no ready job"sv;
    case Scrub::schedule_result_t::no_local_resources: return "local resources shortage"sv;
    case Scrub::schedule_result_t::already_started: return "denied as already started"sv;
    case Scrub::schedule_result_t::no_such_pg: return "pg not found"sv;
    case Scrub::schedule_result_t::bad_pg_state: return "prevented by pg state"sv;
    case Scrub::schedule_result_t::preconditions: return "preconditions not met"sv;
  }
  // g++ (unlike CLANG), requires an extra 'return' here
  return "(unknown)"sv;
}

std::string_view Scrub::ScrubJob::qu_state_text(qu_state_t st)
{
  switch (st) {
    case qu_state_t::not_registered: return "not registered w/ OSD"sv;
    case qu_state_t::registered: return "registered"sv;
    case qu_state_t::unregistering: return "unregistering"sv;
  }
  // g++ (unlike CLANG), requires an extra 'return' here
  return "(unknown)"sv;
}
// clang-format on

std::vector<ScrubTargetId> ScrubQueue::ready_to_scrub(
    OSDRestrictions restrictions, // note: 4B! (copy)
    utime_t scrub_tick)
{
  dout(10) << fmt::format(
		  " @{}: reg./pen. sizes: {} / {} ({})", scrub_tick, to_scrub.size(),
		  penalized.size(), restrictions)
	   << dendl;
  //  create a list of candidates (copying, as otherwise creating a deadlock):
  //  - possibly restore penalized
  //  - (if we didn't handle directly) remove invalid jobs
  //  - create a copy of the to_scrub (possibly up to first not-ripe)
  //  - same for the penalized (although that usually be a waste)
  //  unlock, then try the lists

  std::unique_lock lck{jobs_lock};

  // pardon all penalized jobs that have deadlined (or were updated)
  scan_penalized(restore_penalized, scrub_tick);
  restore_penalized = false;

  // remove the 'updated' flag from all entries
  std::for_each(
      to_scrub.begin(), to_scrub.end(),
      [](const auto& jobref) -> void { jobref->updated = false; });

  // add failed scrub attempts to the penalized list
  move_failed_pgs(scrub_tick);

  // collect all valid & ripe jobs from the two lists. Note that we must copy,
  // as when we use the lists we will not be holding jobs_lock (see
  // explanation above)

  // and in this step 1 of the refactoring (Aug 2023): the set returned must be
  // transformed into a vector of targets (which, in this phase, are
  // the PG id-s).
  auto to_scrub_copy = collect_ripe_jobs(to_scrub, restrictions, scrub_tick);
  auto penalized_copy = collect_ripe_jobs(penalized, restrictions, scrub_tick);
  lck.unlock();
  std::vector<ScrubTargetId> all_ready;
  std::transform(
      to_scrub_copy.begin(), to_scrub_copy.end(), std::back_inserter(all_ready),
      [](const auto& jobref) -> ScrubTargetId { return jobref->pgid; });
  // not bothering to handle the "reached the penalized - so all should be
  // forgiven" case, as the penalty queue is destined to be removed in the
  // followup PR.
  std::transform(
      penalized_copy.begin(), penalized_copy.end(),
      std::back_inserter(all_ready),
      [](const auto& jobref) -> ScrubTargetId { return jobref->pgid; });
  return all_ready;
}


void ScrubQueue::scrub_initiated(spg_t)
{
  // dequeue the target - not needed at this stage
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
struct cmp_sched_time_t {
  bool operator()(const Scrub::ScrubJobRef& lhs,
		  const Scrub::ScrubJobRef& rhs) const
  {
    return lhs->schedule.scheduled_at < rhs->schedule.scheduled_at;
  }
};
}  // namespace

// called under lock
ScrubQContainer ScrubQueue::collect_ripe_jobs(
    ScrubQContainer& group,
    OSDRestrictions restrictions,
    utime_t time_now)
{
  auto filtr = [time_now, restrictions](const auto& jobref) -> bool {
    return jobref->schedule.scheduled_at <= time_now &&
	   (!restrictions.only_deadlined ||
	    (!jobref->schedule.deadline.is_zero() &&
	     jobref->schedule.deadline <= time_now));
  };

  rm_unregistered_jobs(group);
  // copy ripe jobs (unless prohibited by 'restrictions')
  ScrubQContainer ripes;
  ripes.reserve(group.size());

  std::copy_if(group.begin(), group.end(), std::back_inserter(ripes), filtr);
  std::sort(ripes.begin(), ripes.end(), cmp_sched_time_t{});

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (const auto& jobref : group) {
      if (!filtr(jobref)) {
	dout(20) << fmt::format(
			" not ripe: {} @ {:s}", jobref->pgid,
			jobref->schedule.scheduled_at)
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
    times.proposed_time, times.proposed_time};

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


// note: called with jobs_lock held
void ScrubQueue::scan_penalized(bool forgive_all, utime_t time_now)
{
  dout(20) << time_now << (forgive_all ? " all " : " - ") << penalized.size()
	   << dendl;

  // clear dead entries (deleted PGs, or those PGs we are no longer their
  // primary)
  rm_unregistered_jobs(penalized);

  if (forgive_all) {

    std::copy(penalized.begin(), penalized.end(), std::back_inserter(to_scrub));
    penalized.clear();

  } else {

    auto forgiven_last = std::partition(
      penalized.begin(),
      penalized.end(),
      [time_now](const auto& e) {
	return (*e).updated || ((*e).penalty_timeout <= time_now);
      });

    std::copy(penalized.begin(), forgiven_last, std::back_inserter(to_scrub));
    penalized.erase(penalized.begin(), forgiven_last);
    dout(20) << "penalized after screening: " << penalized.size() << dendl;
  }
}

void Scrub::ScrubJob::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << schedule.scheduled_at;
  f->dump_stream("deadline") << schedule.deadline;
  f->dump_bool("forced",
	       schedule.scheduled_at == PgScrubber::scrub_must_stamp());
  f->close_section();
}

void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
{
  ceph_assert(f != nullptr);
  std::lock_guard lck(jobs_lock);

  f->open_array_section("scrubs");

  std::for_each(to_scrub.cbegin(), to_scrub.cend(), [&f](const Scrub::ScrubJobRef& j) {
    j->dump(f);
  });

  std::for_each(penalized.cbegin(),
		penalized.cend(),
		[&f](const Scrub::ScrubJobRef& j) { j->dump(f); });

  f->close_section();
}

ScrubQContainer ScrubQueue::list_registered_jobs() const
{
  ScrubQContainer all_jobs;
  all_jobs.reserve(to_scrub.size() + penalized.size());
  dout(20) << " size: " << all_jobs.capacity() << dendl;

  std::lock_guard lck{jobs_lock};

  std::copy_if(to_scrub.begin(),
	       to_scrub.end(),
	       std::back_inserter(all_jobs),
	       registered_job);
  std::copy_if(penalized.begin(),
	       penalized.end(),
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

// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue - maintaining the 'some PG is reserving' flag

bool ScrubQueue::set_reserving_now()
{
  auto was_set = a_pg_is_reserving.exchange(true);
  return !was_set;
}

void ScrubQueue::clear_reserving_now()
{
  a_pg_is_reserving = false;
}

bool ScrubQueue::is_reserving_now() const
{
  return a_pg_is_reserving;
}
