// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./osd_scrub_sched.h"

#include <string_view>
#include "osd/OSD.h"

#include "pg_scrubber.h"

using namespace ::std::chrono;
using namespace ::std::chrono_literals;
using namespace ::std::literals;

using must_scrub_t = Scrub::must_scrub_t;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
using ScrubJob = Scrub::ScrubJob;
using SchedEntry = ::Scrub::SchedEntry;



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
 * Remove the scrub job from the OSD scrub queue.
 * Caller should mark the Scrubber-owned job as 'not_registered'.
 */
void ScrubQueue::remove_from_osd_queue(spg_t pgid)
{
  dout(10) << fmt::format(
		  "removing pg[{}] from OSD scrub queue", pgid)
	   << dendl;
  std::unique_lock lck{jobs_lock};
  to_scrub.remove_by_class<spg_t>(pgid);
}


void ScrubQueue::enqueue_scrub_job(const Scrub::ScrubJob& sjob)
{
  std::unique_lock lck{jobs_lock};
  to_scrub.enqueue(sjob.shallow_target.queued_element());
  to_scrub.enqueue(sjob.deep_target.queued_element());
}

void ScrubQueue::enqueue_target(const Scrub::SchedTarget& trgt)
{
  std::unique_lock lck{jobs_lock};
  to_scrub.enqueue(trgt.queued_element());
}


void ScrubQueue::dequeue_target(spg_t pgid, scrub_level_t s_or_d)
{
  std::unique_lock lck{jobs_lock};
  remove_entry_unlocked(pgid, s_or_d);
}


std::optional<Scrub::SchedEntry> ScrubQueue::pop_ready_entry(
    EligibilityPred eligibility_pred,
    OSDRestrictions restrictions,
    utime_t time_now)
{
  auto eligible_filtr = [&, rst = restrictions](
				  const SchedEntry& e) -> bool {
      return eligibility_pred(e, rst, time_now);
  };

  std::unique_lock lck{jobs_lock};
  to_scrub.advance_time(time_now);
  return to_scrub.dequeue_by_pred(eligible_filtr);
}


/**
 * the set of all PGs named by the entries in the queue (but only those
 * entries that satisfy the predicate)
 */
std::set<spg_t> ScrubQueue::get_pgs(const ScrubQueue::EntryPred& pred) const
{
  std::lock_guard lck(jobs_lock);

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
    std::function<void(const Scrub::SchedEntry&)> fn,
    int max_jobs) const
{
  auto fn_call = [fn](const SchedEntry& e, bool) -> void { fn(e); };
  std::lock_guard lck(jobs_lock);
  to_scrub.for_each_n<decltype(fn_call)>(std::move(fn_call), max_jobs);
}


bool ScrubQueue::remove_entry_unlocked(spg_t pgid, scrub_level_t s_or_d)
{
  auto same_lvl = [s_or_d](const SchedEntry& e) { return e.level == s_or_d; };
  return to_scrub.remove_if_by_class<spg_t, decltype(same_lvl)>(
      pgid, std::move(same_lvl), 1);
}


void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
{
  ceph_assert(f != nullptr);
  const auto query_time = ceph_clock_now();
  f->open_array_section("scrubs");
  for_each_job(
      [&f, query_time](const Scrub::SchedEntry& e) {
	f->open_object_section("scrub");
	f->dump_stream("pgid") << e.pgid;
	f->dump_stream("sched_time") << e.schedule.not_before;
	f->dump_stream("orig_sched_time") << e.schedule.scheduled_at;
	f->dump_stream("deadline") << e.schedule.deadline;
	f->dump_bool(
	    "forced",
	    e.schedule.scheduled_at == PgScrubber::scrub_must_stamp());

        f->dump_stream("level") << (e.level == scrub_level_t::shallow
                                       ? "shallow"
                                       : "deep");
        f->dump_stream("urgency") << fmt::format("{}", e.urgency);
        f->dump_bool("eligible", e.schedule.not_before <= query_time);
        f->dump_bool("overdue", e.schedule.deadline < query_time);
        f->dump_stream("last_issue") << fmt::format("{}", e.last_issue);

	f->close_section();
      },
      std::numeric_limits<int>::max());
  f->close_section();
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
