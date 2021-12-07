// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "osd_scrub_sched.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scollectd_api.hh>
#include <seastar/core/thread.hh>

#include "common/dout.h"
#include "crimson/common/log.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/scrubber/pg_scrubber.h"
#include "global/global_context.h"
#include "include/utime.h"
#include "include/utime_fmt.h"

using namespace ::std::literals;
using std::ostream;

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd." << whoami << "  "

ScrubQueue::ScrubJob::ScrubJob(CephContext* cct, const spg_t& pg, int node_id)
    : RefCountedObject{cct}
    , pgid{pg}
    , whoami{node_id}
    , cct{cct}
{}

// debug usage only
ostream& operator<<(ostream& out, const ScrubQueue::ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}

void ScrubQueue::ScrubJob::update_schedule(
  const ScrubQueue::scrub_schedule_t& adjusted)
{
  schedule = adjusted;
  penalty_timeout = utime_t(0, 0);  // helps with debugging

  // 'updated' is changed here while not holding jobs_lock. That's OK, as
  // the (atomic) flag will only be cleared by select_pg_and_scrub() after
  // scan_penalized() is called and the job was moved to the to_scrub queue.
  updated = true;

  logger().info("{}: pg[{}] adjusted: {} {}",
		__func__,
		pgid,
		adjusted.scheduled_at,
		registration_state());
  dout(10) << " pg[" << pgid << "] adjusted: " << schedule.scheduled_at << "  "
	   << registration_state() << dendl;
}

std::string ScrubQueue::ScrubJob::scheduling_state(utime_t now_is,
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

  return fmt::format("{}scrub scheduled @ {}",
		     (is_deep_expected ? "deep " : ""),
		     schedule.scheduled_at);
}


// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue

// note - can't have 'func' as part of the general prefix constructed here,
// as most of our Crimson code runs as part of a lambda.
#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix \
  *_dout << "osd." << osd_service.get_nodeid() << " scrub-queue::"


ScrubQueue::ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds)
    : cct{cct}
    , osd_service{osds}
{
  // the cct handed over to us in crimson is null
  auto temp_cct = std::make_unique<CephContext>();
  cct = temp_cct.release();
  daily_loadavg = 1.0;
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
void ScrubQueue::remove_from_osd_queue(ScrubJobRef scrub_job)
{
  dout(15) << __func__ << ": removing pg[" << scrub_job->pgid
	   << "] from OSD scrub queue" << dendl;

  qu_state_t expected_state{qu_state_t::registered};
  auto ret =
    scrub_job->state.compare_exchange_strong(expected_state,
					     qu_state_t::unregistering);

  if (ret) {

    dout(10) << __func__ << ": pg[" << scrub_job->pgid
	     << "] sched-state changed from " << qu_state_text(expected_state)
	     << " to " << qu_state_text(scrub_job->state) << dendl;

  } else {

    // job wasn't in state 'registered' coming in
    dout(5) << __func__ << ": removing pg[" << scrub_job->pgid
	    << "] failed. State was: " << qu_state_text(expected_state)
	    << dendl;
  }
}

void ScrubQueue::register_with_osd(ScrubJobRef scrub_job,
				   const ScrubQueue::sched_params_t& suggested)
{
  qu_state_t state_at_entry = scrub_job->state.load();

  dout(15) << __func__ << ": pg[" << scrub_job->pgid << "] was "
	   << qu_state_text(state_at_entry) << dendl;

  switch (state_at_entry) {
    case qu_state_t::registered:
      // just updating the schedule?
      update_job(scrub_job, suggested);
      break;

    case qu_state_t::not_registered:
      // insertion under lock
      {
	// std::unique_lock lck{jobs_lock};

	if (state_at_entry != scrub_job->state) {
	  // lck.unlock();
	  dout(5) << " scrub job state changed" << dendl;
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
	// lock_guard lck{jobs_lock};

	update_job(scrub_job, suggested);
	if (scrub_job->state == qu_state_t::not_registered) {
	  dout(5) << __func__ << ": scrub job state changed to 'not registered'"
		  << dendl;
	  to_scrub.push_back(scrub_job);
	}
	scrub_job->in_queues = true;
	scrub_job->state = qu_state_t::registered;
      }
      break;
  }

  dout(10) << __func__ << ": pg(" << scrub_job->pgid
	   << ") sched-state changed from " << qu_state_text(state_at_entry)
	   << " to " << qu_state_text(scrub_job->state)
	   << " at: " << scrub_job->schedule.scheduled_at << dendl;
}

// look mommy - no locks!
void ScrubQueue::update_job(ScrubJobRef scrub_job,
			    const ScrubQueue::sched_params_t& suggested)
{
  // adjust the suggested scrub time according to OSD-wide status
  auto adjusted = adjust_target_time(suggested);
  scrub_job->update_schedule(adjusted);
}

ScrubQueue::sched_params_t ScrubQueue::determine_scrub_time(
  const requested_scrub_t& request_flags,
  const pg_info_t& pg_info,
  const pool_opts_t pool_conf) const
{
  ScrubQueue::sched_params_t res;
  dout(15) << __func__ << ": requested_scrub_t: {}" << request_flags << dendl;

  if (request_flags.must_scrub || request_flags.need_auto) {

    // Set the smallest time that isn't utime_t()
    res.proposed_time = PgScrubber::scrub_must_stamp();
    res.is_must = ScrubQueue::must_scrub_t::mandatory;
    // we do not need the interval data in this case

  } else if (pg_info.stats.stats_invalid && conf()->osd_scrub_invalid_stats) {
    res.proposed_time = time_now();
    res.is_must = ScrubQueue::must_scrub_t::mandatory;

  } else {
    res.proposed_time = pg_info.history.last_scrub_stamp;
    res.min_interval = pool_conf.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
    res.max_interval = pool_conf.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  }

  dout(15) << fmt::format(
		"{}: suggested: {} hist: {} v: {}/{} must: {} pool-min: {}",
		__func__,
		res.proposed_time,
		pg_info.history.last_scrub_stamp,
		(bool)pg_info.stats.stats_invalid,
		conf()->osd_scrub_invalid_stats,
		(res.is_must == must_scrub_t::mandatory ? "y" : "n"),
		res.min_interval)
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

      dout(15) << __func__ << ": moving " << sjob->pgid
	       << " state: " << ScrubQueue::qu_state_text(sjob->state) << dendl;

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

std::string_view ScrubQueue::qu_state_text(qu_state_t st)
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

/**
 *  a note regarding 'to_scrub_copy':
 *  'to_scrub_copy' is a sorted set of all the ripe jobs from to_copy.
 *  As we usually expect to refer to only the first job in this set, we could
 *  consider an alternative implementation:
 *  - have collect_ripe_jobs() return the copied set without sorting it;
 *  - loop, performing:
 *    - use std::min_element() to find a candidate;
 *    - try that one. If not suitable, discard from 'to_scrub_copy'
 */
seastar::future<Scrub::schedule_result_t> ScrubQueue::select_pg_and_scrub(
  Scrub::ScrubPreconds&& preconds)
{
  using Scrub::schedule_result_t;
  dout(10) << __func__ << ": reg./pen. sizes: " << to_scrub.size() << " / "
	   << penalized.size() << dendl;

  utime_t now_is = time_now();

  preconds.time_permit = scrub_time_permit(now_is);
  preconds.load_is_low = scrub_load_below_threshold();
  preconds.only_deadlined = !preconds.time_permit || !preconds.load_is_low;

  //  create a list of candidates (copying, as otherwise creating a deadlock):
  //  - possibly restore penalized
  //  - (if we didn't handle directly) remove invalid jobs
  //  - create a copy of the to_scrub (possibly up to first not-ripe)
  //  - same for the penalized (although that usually be a waste)
  //  unlock, then try the lists

  // pardon all penalized jobs that have deadlined (or were updated)
  scan_penalized(restore_penalized, now_is);
  restore_penalized = false;

  // remove the 'updated' flag from all entries
  std::for_each(to_scrub.begin(),
		to_scrub.end(),
		[](const auto& jobref) -> void { jobref->updated = false; });

  // add failed scrub attempts to the penalized list
  move_failed_pgs(now_is);

  //  collect all valid & ripe jobs from the two lists. Note that we must copy,
  //  as when we use the lists we will not be holding jobs_lock (see
  //  explanation above)

  return seastar::do_with(
    collect_ripe_jobs(to_scrub, now_is),
    collect_ripe_jobs(penalized, now_is),
    std::move(preconds),
    [this, now_is](auto& to_scrub_copy, auto& penalized_copy, auto& preconds) {
      return select_from_group(to_scrub_copy, preconds, now_is)
	.then([this,
	       penalized_copy = std::move(penalized_copy),
	       to_scrub_copy = std::move(to_scrub_copy),
	       preconds,
	       now_is](
		auto result) mutable -> seastar::future<schedule_result_t> {
	  if (result != schedule_result_t::none_ready ||
	      penalized_copy.empty()) {
	    return seastar::make_ready_future<schedule_result_t>(result);
	  }
	  return seastar::make_ready_future<schedule_result_t>(
	    schedule_result_t::none_ready);
	  // return seastar::future<schedule_result_t>(result);
	  // try the penalized queue
	  return select_from_group(penalized_copy, preconds, now_is)
	    .then([this](
		    auto result) mutable -> seastar::future<schedule_result_t> {
	      restore_penalized = true;
	      return seastar::make_ready_future<schedule_result_t>(result);
	    });
	});
    });
}

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
  bool operator()(const ScrubQueue::ScrubJobRef& lhs,
		  const ScrubQueue::ScrubJobRef& rhs) const
  {
    return lhs->schedule.scheduled_at < rhs->schedule.scheduled_at;
  }
};
}  // namespace

// called under lock
ScrubQueue::ScrubQContainer ScrubQueue::collect_ripe_jobs(
  ScrubQContainer& group,
  utime_t time_now)
{
  rm_unregistered_jobs(group);

  // copy ripe jobs
  ScrubQueue::ScrubQContainer ripes;
  ripes.reserve(group.size());

  std::copy_if(group.begin(),
	       group.end(),
	       std::back_inserter(ripes),
	       [time_now](const auto& jobref) -> bool {
		 return jobref->schedule.scheduled_at <= time_now;
	       });
  std::sort(ripes.begin(), ripes.end(), cmp_sched_time_t{});

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (const auto& jobref : group) {
      if (jobref->schedule.scheduled_at > time_now) {
	dout(20) << __func__ << ": not ripe: " << jobref->pgid << " @ "
		 << jobref->schedule.scheduled_at << dendl;
      }
    }
  }

  return ripes;
}

// RRR should we keep preconds?

// not holding jobs_lock. 'group' is a copy of the actual list.
seastar::future<Scrub::schedule_result_t> ScrubQueue::select_from_group(
  ScrubQContainer& group,
  const Scrub::ScrubPreconds& preconds,
  utime_t now_is)
{
  using Scrub::schedule_result_t;
  using opt_sched_t = std::optional<Scrub::schedule_result_t>;
  logger().info("{}: jobs #: {} {}", __func__, group.size(), (void*)(&group));

  if (group.empty()) {
    return seastar::make_ready_future<schedule_result_t>(
      schedule_result_t::none_ready);
  }

  return seastar::do_with(
    group.begin(),
    std::move(preconds),
    [this, &group, now_is](auto& candidate_it, auto& preconds) {
      return seastar::repeat_until_value(
	[this, &candidate_it, &group, preconds, now_is]() mutable
	-> seastar::future<opt_sched_t> {
	  logger().debug("select_from_group():  XXXXX # {}",
			 group.size());	 // RRR to rm
	  if (group.size() == 0 || (candidate_it == group.end())) {
	    // the 'size==0' case is only if the group changed, which should
	    // should happen only during tests
	    return seastar::make_ready_future<opt_sched_t>(
	      schedule_result_t::none_ready);
	  }

	  auto& candidate = *candidate_it;
	  logger().debug("select_from_group(): -- jobs #: {} {} ",
			 group.size(),
			 (void*)(&group));

	  if (preconds.only_deadlined &&
	      (candidate->schedule.deadline.is_zero() ||
	       candidate->schedule.deadline >= now_is)) {
	    dout(15)
	      << "ScrubQueue::select_from_group(): not scheduling scrub for "
	      << candidate->pgid << " due to "
	      << (preconds.time_permit ? "high load" : "time not permitting")
	      << dendl;
	    return seastar::make_ready_future<opt_sched_t>(std::nullopt);
	  }

	  return osd_service
	    .initiate_a_scrub(candidate->pgid,
			      preconds.allow_requested_repair_only)
	    .then([this, &candidate_it](auto&& init_result) mutable
		  -> seastar::future<opt_sched_t> {
	      auto& candidate = *candidate_it;
	      switch (init_result) {

		case schedule_result_t::scrub_initiated:
		  // the happy path. We are done
		  dout(20) << "ScrubQueue::select_from_group(): initiated for "
			   << candidate->pgid << dendl;
		  logger().debug(
		    "ScrubQueue::select_from_group(): initiated for {}",
		    candidate->pgid);
		  return seastar::make_ready_future<opt_sched_t>(
		    schedule_result_t::scrub_initiated);

		case schedule_result_t::already_started:
		case schedule_result_t::preconditions:
		case schedule_result_t::bad_pg_state:
		  // continue with the next job
		  logger().debug(
		    "ScrubQueue::select_from_group(): failed "
		    "(state/cond/started) {}",
		    candidate->pgid);
		  break;

		case schedule_result_t::no_such_pg:
		  // The pg is no longer there
		  logger().debug(
		    "ScrubQueue::select_from_group(): failed (no pg) {}",
		    candidate->pgid);
		  break;

		case schedule_result_t::no_local_resources:
		  // failure to secure local resources. No point in trying the
		  // other PGs at this time. Note that this is not the same as
		  // replica resources failure!
		  logger().debug(
		    "ScrubQueue::select_from_group(): failed (local) {}",
		    candidate->pgid);
		  return seastar::make_ready_future<opt_sched_t>(
		    schedule_result_t::no_local_resources);

		case schedule_result_t::none_ready:
		  // can't happen. Just for the compiler.
		  logger().error(
		    "ScrubQueue::select_from_group(): failed !!! {}",
		    candidate->pgid);
		  return seastar::make_ready_future<opt_sched_t>(
		    schedule_result_t::none_ready);
	      };

	      ++candidate_it;
	      return seastar::make_ready_future<opt_sched_t>(std::nullopt);
	    });
	});
    });
}


ScrubQueue::scrub_schedule_t ScrubQueue::adjust_target_time(
  const sched_params_t& times) const
{
  ScrubQueue::scrub_schedule_t sched_n_dead{times.proposed_time,
					    times.proposed_time};

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    dout(20) << __func__ << ": min t: " << times.min_interval
	     << " osd: " << conf()->osd_scrub_min_interval
	     << " max t: " << times.max_interval
	     << " osd: " << conf()->osd_scrub_max_interval << dendl;

    dout(20) << "at " << sched_n_dead.scheduled_at << " ratio "
	     << conf()->osd_scrub_interval_randomize_ratio << dendl;
  }

  if (times.is_must == ScrubQueue::must_scrub_t::not_mandatory) {

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
  }

  dout(17) << __func__ << ": at (final) " << sched_n_dead.scheduled_at << " - "
	   << sched_n_dead.deadline << dendl;
  return sched_n_dead;
}

double ScrubQueue::scrub_sleep_time(bool must_scrub) const
{
  double regular_sleep_period = conf()->osd_scrub_sleep;

  if (must_scrub || scrub_time_permit(time_now())) {
    return regular_sleep_period;
  }

  // relevant if scrubbing started during allowed time, but continued into
  // forbidden hours
  double extended_sleep = conf()->osd_scrub_extended_sleep;
  dout(20) << __func__ << ": w/ extended sleep (" << extended_sleep << ")"
	   << dendl;
  return std::max(extended_sleep, regular_sleep_period);
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
    dout(20) << __func__ << ": penalized after screening: " << penalized.size()
	     << dendl;
  }
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
    dout(20) << __func__ << ": should run between week day "
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

void ScrubQueue::ScrubJob::dump(ceph::Formatter* f) const
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

  f->open_array_section("scrubs");

  std::for_each(to_scrub.cbegin(), to_scrub.cend(), [&f](const ScrubJobRef& j) {
    j->dump(f);
  });

  std::for_each(penalized.cbegin(),
		penalized.cend(),
		[&f](const ScrubJobRef& j) { j->dump(f); });

  f->close_section();
}

ScrubQueue::ScrubQContainer ScrubQueue::list_registered_jobs() const
{
  ScrubQueue::ScrubQContainer all_jobs;
  all_jobs.reserve(to_scrub.size() + penalized.size());
  dout(20) << __func__ << ": size: " << all_jobs.capacity() << dendl;

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
// CPU load average tracking

/**
 * to answer the question "is the CPU too busy to scrub?". we use two
 * data points:
 * - the "utilization" as reported by Seastar (remember: the CPU is almost
 * guaranteed to be 100% "busy" when running a reactor), and
 * - how that value compares with the "long term" (15m) average. Is the load
 *   improving?
 */
bool ScrubQueue::scrub_load_below_threshold() const
{
  if (current_loadavg < conf()->osd_scrub_load_threshold) {
    dout(20) << __func__ << ": loadavg  " << current_loadavg << " < max "
	     << conf()->osd_scrub_load_threshold << " = yes" << dendl;
    return true;
  }

  // allow scrub if below daily avg and currently decreasing
  if (current_loadavg < daily_loadavg && current_loadavg < filtrd_loadavg) {
    dout(20) << __func__ << ": loadavg " << current_loadavg
	     << " < daily_loadavg " << daily_loadavg << " and < 15m avg "
	     << filtrd_loadavg << " = yes" << dendl;
    return true;
  }

  dout(20) << __func__ << ": loadavg " << current_loadavg << " >= max "
	   << conf()->osd_scrub_load_threshold << " and ( >= daily_loadavg "
	   << daily_loadavg << " or >= 15m avg " << filtrd_loadavg << ") = no"
	   << dendl;

  return false;
}

namespace {

// query Seastar for the current load
std::optional<double> current_load()
{
  auto allgrp = seastar::scollectd::get_value_map();
  auto i = allgrp.find("reactor_utilization");
  logger().debug("{}: found {}",
		 __func__,
		 (i == allgrp.end() ? "nothing" : "+++"));
  if (i == allgrp.end()) {
    return std::nullopt;
  }
  const auto& metric_fam = i->second;

  for (const auto& [labels, metric] : metric_fam) {
    if (metric && metric->is_enabled()) {
      auto v = (*metric)();
      logger().debug("--- >> {} ", (double)(v.d()));
      return v.d();
    }
  }
  return std::nullopt;
}
}  // namespace

/**
 * Track CPU load average
 * The daily calculation should be replaced by a FIR filter.
 */
std::optional<double> ScrubQueue::update_load_average()
{
  int n_samples = 60 * 60 * 24;
  int hb_interval = conf()->osd_heartbeat_interval;
  if (hb_interval > 1) {
    n_samples /= hb_interval;
    if (n_samples < 1)
      n_samples = 1;
  }

  // get CPU load avg
  auto load = current_load();
  if (!load) {
    return std::nullopt;
  }
  current_loadavg = *load;

  /// \todo replace this with a FIR filter
  filtrd_loadavg = (filtrd_loadavg * 899 + current_loadavg) / 900;

  daily_loadavg =
    (daily_loadavg * (n_samples - 1) + current_loadavg) / n_samples;
  dout(17) << "heartbeat: daily_loadavg " << daily_loadavg << dendl;
  return 100 * current_loadavg;
}


// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob - scrub resource management

bool ScrubQueue::can_inc_scrubs() const
{
  if (scrubs_local + scrubs_remote < conf()->osd_max_scrubs) {
    return true;
  }

  dout(20) << __func__ << " == false. " << scrubs_local << " local + "
	   << scrubs_remote << " remote >= max " << conf()->osd_max_scrubs
	   << dendl;
  return false;
}

bool ScrubQueue::inc_scrubs_local()
{
  if (scrubs_local + scrubs_remote < conf()->osd_max_scrubs) {
    ++scrubs_local;
    return true;
  }

  dout(20) << __func__ << ": " << scrubs_local << " local + " << scrubs_remote
	   << " remote >= max " << conf()->osd_max_scrubs << dendl;
  return false;
}

void ScrubQueue::dec_scrubs_local()
{
  dout(20) << __func__ << ": " << scrubs_local << " -> " << (scrubs_local - 1)
	   << " (max " << conf()->osd_max_scrubs << ", remote " << scrubs_remote
	   << ")" << dendl;

  --scrubs_local;
  ceph_assert(scrubs_local >= 0);
}

bool ScrubQueue::inc_scrubs_remote()
{
  if (scrubs_local + scrubs_remote < conf()->osd_max_scrubs) {
    dout(20) << __func__ << ": " << scrubs_remote << " -> "
	     << (scrubs_remote + 1) << " (max " << conf()->osd_max_scrubs
	     << ", local " << scrubs_local << ")" << dendl;
    ++scrubs_remote;
    return true;
  }

  dout(20) << __func__ << ": " << scrubs_local << " local + " << scrubs_remote
	   << " remote >= max " << conf()->osd_max_scrubs << dendl;
  return false;
}

void ScrubQueue::dec_scrubs_remote()
{
  dout(20) << __func__ << ": " << scrubs_remote << " -> " << (scrubs_remote - 1)
	   << " (max " << conf()->osd_max_scrubs << ", local " << scrubs_local
	   << ")" << dendl;
  --scrubs_remote;
  ceph_assert(scrubs_remote >= 0);
}

void ScrubQueue::dump_scrub_reservations(ceph::Formatter* f) const
{
  f->dump_int("scrubs_local", scrubs_local);
  f->dump_int("scrubs_remote", scrubs_remote);
  f->dump_int("osd_max_scrubs", conf()->osd_max_scrubs);
}
