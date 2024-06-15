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
    , log_msg_prefix{fmt::format("osd.{}: scrub-job:pg[{}]:", node_id, pgid)}
{}

// debug usage only
namespace std {
ostream& operator<<(ostream& out, const ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}
}  // namespace std


Scrub::scrub_schedule_t ScrubJob::adjust_target_time(
    const sched_conf_t& app_conf,
    const sched_params_t& suggested) const
{
  Scrub::scrub_schedule_t adjusted{
      suggested.proposed_time, suggested.proposed_time, suggested.proposed_time};

  if (suggested.is_must == Scrub::must_scrub_t::not_mandatory) {
    // unless explicitly requested, postpone the scrub with a random delay
    adjusted.scheduled_at += app_conf.shallow_interval;
    double r = rand() / (double)RAND_MAX;
    adjusted.scheduled_at +=
	app_conf.shallow_interval * app_conf.interval_randomize_ratio * r;

    if (app_conf.max_shallow) {
      adjusted.deadline += *app_conf.max_shallow;
    } else {
      adjusted.deadline = utime_t{};
    }

    if (adjusted.not_before < adjusted.scheduled_at) {
      adjusted.not_before = adjusted.scheduled_at;
    }

    dout(20) << fmt::format(
		    "not-must. Was:{:s} config:{} adjusted:{}",
		    suggested.proposed_time, app_conf, adjusted) << dendl;
  }
  // else - no log is needed. All relevant data will be logged by the caller

  return adjusted;
}


void ScrubJob::init_targets(
    const sched_params_t& suggested,
    const pg_info_t& info,
    const Scrub::sched_conf_t& aconf,
    utime_t scrub_clock_now)
{
  auto adjusted = adjust_target_time(aconf, suggested);
  high_priority = suggested.is_must == must_scrub_t::mandatory;
  update_schedule(adjusted, true);
}


void ScrubJob::update_schedule(
    const Scrub::scrub_schedule_t& adjusted,
    bool reset_failure_penalty)
{
  dout(15) << fmt::format(
		  "was: nb:{:s}({:s}). Called with: rest?{} {:s} ({})",
		  schedule.not_before, schedule.scheduled_at,
		  reset_failure_penalty, adjusted.scheduled_at,
		  state_desc())
	   << dendl;
  schedule.scheduled_at = adjusted.scheduled_at;
  schedule.deadline = adjusted.deadline;

  if (reset_failure_penalty || (schedule.not_before < schedule.scheduled_at)) {
    schedule.not_before = schedule.scheduled_at;
  }
  dout(10) << fmt::format(
		  "adjusted: nb:{:s} ({:s}) ({})", schedule.not_before,
		  schedule.scheduled_at, state_desc())
	   << dendl;
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
