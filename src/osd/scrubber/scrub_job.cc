// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_job.h"
#include "pg_scrubber.h"

using qu_state_t = Scrub::qu_state_t;
using must_scrub_t = Scrub::must_scrub_t;
using ScrubQContainer = Scrub::ScrubQContainer;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
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
    : RefCountedObject{cct}
    , pgid{pg}
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

void ScrubJob::update_schedule(
    const Scrub::scrub_schedule_t& adjusted,
    bool reset_failure_penalty)
{
  dout(15) << fmt::format(
		  "was: nb:{:s}({:s}). Called with: rest?{} {:s} ({})",
		  schedule.not_before, schedule.scheduled_at,
		  reset_failure_penalty, adjusted.scheduled_at,
		  registration_state())
	   << dendl;
  schedule.scheduled_at = adjusted.scheduled_at;
  schedule.deadline = adjusted.deadline;

  if (reset_failure_penalty || (schedule.not_before < schedule.scheduled_at)) {
    schedule.not_before = schedule.scheduled_at;
  }

  updated = true;
  dout(10) << fmt::format(
		  "adjusted: nb:{:s} ({:s}) ({})", schedule.not_before,
		  schedule.scheduled_at, registration_state())
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
  // if not in the OSD scheduling queues, not a candidate for scrubbing
  if (state != qu_state_t::registered) {
    return "no scrub is scheduled";
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

// clang-format off
std::string_view ScrubJob::qu_state_text(qu_state_t st)
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
