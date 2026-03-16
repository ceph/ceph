// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <string_view>
#include <algorithm>

#include "osd/scrubber_common.h"
#include "scrub_queue.h"
#include "crimson/common/log.h"
#include "crimson/osd/scrub/pg_scrubber.h"

SET_SUBSYS(osd);

using namespace ::std::chrono;
using namespace ::std::chrono_literals;
using namespace ::std::literals;

using must_scrub_t = crimson::osd::scrub::must_scrub_t;
using OSDRestrictions = crimson::osd::scrub::OSDRestrictions;
using ScrubJob = crimson::osd::scrub::ScrubJob;
using SchedEntry = crimson::osd::scrub::SchedEntry;


namespace crimson::osd::scrub {
// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue

/*
 * Remove the scrub job from the OSD scrub queue.
 * Caller should mark the Scrubber-owned job as 'not_registered'.
 */
void ScrubQueue::remove_from_osd_queue(spg_t pgid)
{
  LOG_PREFIX(ScrubQueue::remove_from_osd_queue);
  DEBUG("removing pg[{}] from OSD scrub queue", pgid);
  to_scrub.remove_by_class<spg_t>(pgid);
}


void ScrubQueue::enqueue_scrub_job(const ScrubJob& sjob)
{
  LOG_PREFIX(ScrubQueue::enqueue_scrub_job);
  DEBUG("enqueuing scrub job {}", sjob);
  to_scrub.enqueue(sjob.shallow_target.queued_element());
  to_scrub.enqueue(sjob.deep_target.queued_element());
}

void ScrubQueue::enqueue_target(const SchedTarget& trgt)
{
  LOG_PREFIX(ScrubQueue::enqueue_target);
  DEBUG("enqueuing scrub target {}",trgt);
  to_scrub.enqueue(trgt.queued_element());
}


void ScrubQueue::dequeue_target(spg_t pgid, scrub_level_t s_or_d)
{
  auto same_lvl = [s_or_d](const SchedEntry& e) { return e.level == s_or_d; };
  to_scrub.remove_if_by_class<spg_t, decltype(same_lvl)>(
      pgid, std::move(same_lvl), 1);
}


std::optional<SchedEntry> ScrubQueue::pop_ready_entry(
    EligibilityPred eligibility_pred,
    OSDRestrictions restrictions,
    utime_t time_now)
{
  LOG_PREFIX(ScrubQueue::pop_ready_entry);
  DEBUG("popping ready entry at time {}", time_now);
  auto eligible_filtr = [&, rst = restrictions](
				  const SchedEntry& e) -> bool {
      return eligibility_pred(e, rst, time_now);
  };

  if (!to_scrub.advance_time(time_now)) {
    // the clock was not advanced
    DEBUG(
       ": time now ({}) is earlier than the previous not-before "
       "cut-off time",
       time_now);
    // we still try to dequeue, mainly to handle possible corner cases
  }
  return to_scrub.dequeue_by_pred(eligible_filtr);
}


/**
 * the set of all PGs named by the entries in the queue (but only those
 * entries that satisfy the predicate)
 */
std::set<spg_t> ScrubQueue::get_pgs(const ScrubQueue::EntryPred& pred) const
{
  using acc_t = std::set<spg_t>;
  auto extract_pg =
      [pred](acc_t&& acc, const SchedEntry& se, bool is_eligible) {
	if (pred(se, is_eligible)) {
	  acc.insert(se.pgid);
	}
	return std::move(acc);
      };

  return to_scrub.accumulate<acc_t, decltype(extract_pg)>(
      std::move(extract_pg));
}


void ScrubQueue::for_each_job(
    std::function<void(const SchedEntry&)> fn,
    int max_jobs) const
{
  auto fn_call = [fn](const SchedEntry& e, bool) -> void { fn(e); };
  to_scrub.for_each_n<decltype(fn_call)>(std::move(fn_call), max_jobs);
}


void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
{
  ceph_assert(f != nullptr);
  const auto query_time = ceph_clock_now();
  Formatter::ArraySection all_scrubs_section{*f, "scrubs"};
  for_each_job(
      [&f, query_time](const SchedEntry& e) {
        Formatter::ObjectSection job_section{*f, "scrub"};
	f->dump_stream("pgid") << e.pgid;
	f->dump_stream("sched_time") << e.schedule.not_before;
	f->dump_stream("orig_sched_time") << e.schedule.scheduled_at;
	f->dump_bool(
	    "forced",
	    e.schedule.scheduled_at == PGScrubber::scrub_must_stamp());

        f->dump_stream("level") << (e.level == scrub_level_t::shallow
                                       ? "shallow"
                                       : "deep");
        f->dump_stream("urgency") << fmt::format("{}", e.urgency);
        f->dump_bool("eligible", e.schedule.not_before <= query_time);
        f->dump_stream("last_issue") << fmt::format("{}", e.last_issue);
      },
      std::numeric_limits<int>::max());
}

// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue - maintaining the 'blocked on a locked object' count 
// fix it according to BlockT
void ScrubQueue::clear_pg_scrub_blocked(spg_t blocked_pg)
{
  LOG_PREFIX(ScrubQueue::clear_pg_scrub_blocked);
  DEBUG("clearing blocked state for pg {}", blocked_pg);
  --blocked_scrubs_cnt;
  ceph_assert(blocked_scrubs_cnt >= 0);
}

void ScrubQueue::mark_pg_scrub_blocked(spg_t blocked_pg)
{
  LOG_PREFIX(ScrubQueue::mark_pg_scrub_blocked);
  DEBUG("marking pg {} as blocked", blocked_pg);
  ++blocked_scrubs_cnt;
}

int ScrubQueue::get_blocked_pgs_count() const
{
  return blocked_scrubs_cnt;
}
}