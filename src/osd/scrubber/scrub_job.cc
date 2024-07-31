// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_job.h"
#include "pg_scrubber.h"

using must_scrub_t = Scrub::must_scrub_t;
using ScrubQContainer = Scrub::ScrubQContainer;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
using sched_conf_t = Scrub::sched_conf_t;
using scrub_schedule_t = Scrub::scrub_schedule_t;
using ScrubJob = Scrub::ScrubJob;
using delay_ready_t = Scrub::delay_ready_t;


// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

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

ScrubJob::ScrubJob(CephContext* cct, const spg_t& pg, int node_id)
    : pgid{pg}
    , whoami{node_id}
    , cct{cct}
    , log_msg_prefix{fmt::format("osd.{} scrub-job:pg[{}]:", node_id, pgid)}
{}

// debug usage only
namespace std {
ostream& operator<<(ostream& out, const ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}
}  // namespace std


void ScrubJob::adjust_schedule(
    const Scrub::sched_params_t& suggested,
    const Scrub::sched_conf_t& app_conf,
    utime_t scrub_clock_now,
    delay_ready_t modify_ready_targets)
{
  dout(10) << fmt::format(
		  "{} current h.p.:{:c} conf:{} also-ready?{:c} "
		  "sjob@entry:{}",
		  suggested, high_priority ? 'y' : 'n', app_conf,
		  (modify_ready_targets == delay_ready_t::delay_ready) ? 'y'
								       : 'n',
		  *this)
	   << dendl;

  high_priority = (suggested.is_must == must_scrub_t::mandatory);
  utime_t adj_not_before = suggested.proposed_time;
  utime_t adj_target = suggested.proposed_time;
  schedule.deadline = adj_target;

  if (!high_priority) {
    // add a random delay to the proposed scheduled time - but only for periodic
    // scrubs that are not already eligible for scrubbing.
    if ((modify_ready_targets == delay_ready_t::delay_ready) ||
	adj_not_before > scrub_clock_now) {
      adj_target += app_conf.shallow_interval;
      double r = rand() / (double)RAND_MAX;
      adj_target +=
	  app_conf.shallow_interval * app_conf.interval_randomize_ratio * r;
    }

    // the deadline can be updated directly into the scrub-job
    if (app_conf.max_shallow) {
      schedule.deadline += *app_conf.max_shallow;
    } else {
      schedule.deadline = utime_t{};
    }

    if (adj_not_before < adj_target) {
      adj_not_before = adj_target;
    }
  }

  schedule.scheduled_at = adj_target;
  schedule.not_before = adj_not_before;
  dout(10) << fmt::format(
		  "adjusted: nb:{:s} target:{:s} deadline:{:s} ({})",
		  schedule.not_before, schedule.scheduled_at, schedule.deadline,
		  state_desc())
	   << dendl;
}


void ScrubJob::merge_and_delay(
    const scrub_schedule_t& aborted_schedule,
    delay_cause_t issue,
    requested_scrub_t updated_flags,
    utime_t scrub_clock_now)
{
  // merge the schedule targets:
  schedule.scheduled_at =
      std::min(aborted_schedule.scheduled_at, schedule.scheduled_at);
  high_priority = high_priority || updated_flags.must_scrub;
  delay_on_failure(5s, issue, scrub_clock_now);

  // the new deadline is the minimum of the two
  schedule.deadline = std::min(aborted_schedule.deadline, schedule.deadline);
}


void ScrubJob::delay_on_failure(
    std::chrono::seconds delay,
    Scrub::delay_cause_t delay_cause,
    utime_t scrub_clock_now)
{
  schedule.not_before =
      std::max(scrub_clock_now, schedule.not_before) + utime_t{delay};
  last_issue = delay_cause;
}

std::string ScrubJob::scheduling_state(utime_t now_is, bool is_deep_expected)
    const
{
  // if not registered, not a candidate for scrubbing on this OSD (or at all)
  if (!registered) {
    return "not registered for scrubbing";
  }
  if (!target_queued) {
    // if not currently queued - we are being scrubbed
    return "scrubbing";
  }

  // if the time has passed, we are surely in the queue
  if (now_is > schedule.not_before) {
    // we are never sure that the next scrub will indeed be shallow:
    return fmt::format("queued for {}scrub", (is_deep_expected ? "deep " : ""));
  }

  return fmt::format(
      "{}scrub scheduled @ {:s} ({:s})", (is_deep_expected ? "deep " : ""),
      schedule.not_before, schedule.scheduled_at);
}

std::ostream& ScrubJob::gen_prefix(std::ostream& out, std::string_view fn) const
{
  return out << log_msg_prefix << fn << ": ";
}

void ScrubJob::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << schedule.not_before;
  f->dump_stream("orig_sched_time") << schedule.scheduled_at;
  f->dump_stream("deadline") << schedule.deadline;
  f->dump_bool("forced",
	       schedule.scheduled_at == PgScrubber::scrub_must_stamp());
  f->close_section();
}
