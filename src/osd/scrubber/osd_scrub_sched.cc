// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./osd_scrub_sched.h"

#include <compare>
#include <shared_mutex>

#include "osd/OSD.h"

#include "pg_scrubber.h"
#include "scrub_queue.h"

using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;


// ////////////////////////////////////////////////////////////////////////// //
// SchedEntry

namespace Scrub {

std::weak_ordering cmp_ripe_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r)
{
  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
    return cmp;
  }
  // the 'utime_t' operator<=> is 'partial_ordering', it seems.
  if (auto cmp = std::weak_order(double(l.target), double(r.target));
      cmp != 0) {
    return cmp;
  }
  if (auto cmp = std::weak_order(double(l.not_before), double(r.not_before));
      cmp != 0) {
    return cmp;
  }
  if (l.level < r.level) {
    return std::weak_ordering::less;
  }
  return std::weak_ordering::greater;
}

std::weak_ordering cmp_future_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r)
{
  if (auto cmp = std::weak_order(double(l.not_before), double(r.not_before));
      cmp != 0) {
    return cmp;
  }
  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
    return cmp;
  }
  if (auto cmp = std::weak_order(double(l.target), double(r.target));
      cmp != 0) {
    return cmp;
  }
  if (l.level < r.level) {
    return std::weak_ordering::less;
  }
  return std::weak_ordering::greater;
}

std::weak_ordering
cmp_entries(utime_t t, const Scrub::SchedEntry& l, const Scrub::SchedEntry& r)
{
  bool l_ripe = l.is_ripe(t);
  bool r_ripe = r.is_ripe(t);
  if (l_ripe) {
    if (r_ripe) {
      return cmp_ripe_entries(l, r);
    }
    return std::weak_ordering::less;
  }
  if (r_ripe) {
    return std::weak_ordering::greater;
  }
  return cmp_future_entries(l, r);
}

}  // namespace Scrub


using SchedTarget = Scrub::SchedTarget;
using urgency_t = Scrub::urgency_t;
using delay_cause_t = Scrub::delay_cause_t;
using ScrubPreconds = Scrub::ScrubPreconds;

namespace {
utime_t add_double(utime_t t, double d)
{
  return utime_t{t.sec() + static_cast<int>(d), t.nsec()};
}
}  // namespace

namespace Scrub {
// both targets compared are assumed to be 'ripe', i.e. not_before is in the past
std::weak_ordering cmp_ripe_targets(
    const Scrub::SchedTarget& l,
    const Scrub::SchedTarget& r)
{
  return cmp_ripe_entries(l.queued_element(), r.queued_element());
}

std::weak_ordering cmp_future_targets(
    const Scrub::SchedTarget& l,
    const Scrub::SchedTarget& r)
{
  return cmp_ripe_entries(l.queued_element(), r.queued_element());
}

std::weak_ordering
cmp_targets(utime_t t, const Scrub::SchedTarget& l, const Scrub::SchedTarget& r)
{
  return cmp_entries(t, l.queued_element(), r.queued_element());
}
}  // namespace Scrub

bool Scrub::SchedEntry::is_ripe(utime_t now_is) const
{
  return urgency > urgency_t::off && now_is >= not_before;
}

void Scrub::SchedEntry::dump(std::string_view sect_name, ceph::Formatter* f)
    const
{
  f->open_object_section(sect_name);
  /// \todo improve the performance of u_time dumps here
  f->dump_stream("pg") << pgid;
  f->dump_stream("level")
      << (level == scrub_level_t::deep ? "deep" : "shallow");
  f->dump_stream("urgency") << fmt::format("{}", urgency);
  f->dump_stream("target") << target;
  f->dump_stream("not_before") << not_before;
  f->dump_stream("deadline") << deadline;
  f->close_section();
}


// ////////////////////////////////////////////////////////////////////////// //
// SchedTarget


// 'dout' definitions for SchedTarget & ScrubJob
#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix_target(_dout, this)

template <class T>
static ostream& _prefix_target(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout);
}

/**
 * A SchedTarget names both a PG to scrub and the level (deepness) of scrubbing
 */
SchedTarget::SchedTarget(
    spg_t pg_id,
    scrub_level_t scrub_level,
    int osd_num,
    CephContext* cct)
    : sched_info{pg_id, scrub_level}
    , cct{cct}
    , whoami{osd_num}
{
  ceph_assert(cct);
  m_log_prefix = fmt::format("osd.{} pg[{}] ScrubTrgt: ", whoami, pg_id.pgid);
}

std::ostream& SchedTarget::gen_prefix(std::ostream& out) const
{
  return out << m_log_prefix;
}

void SchedTarget::reset()
{
  // a bit convoluted, but the standard way to guarantee we keep the
  // same set of member defaults as the constructor
  *this = SchedTarget{sched_info.pgid, sched_info.level, whoami, cct};
}

bool SchedTarget::over_deadline(utime_t now_is) const
{
  return sched_info.urgency > urgency_t::off && now_is >= sched_info.deadline;
}

bool SchedTarget::is_periodic() const
{
  return sched_info.urgency == urgency_t::periodic_regular ||
	 sched_info.urgency == urgency_t::overdue;
}

utime_t SchedTarget::sched_time() const
{
  return sched_info.not_before;
}

void SchedTarget::depenalize()
{
  up_urgency_to(urgency_t::periodic_regular);
}

void SchedTarget::up_urgency_to(urgency_t u)
{
  sched_info.urgency = std::max(sched_info.urgency, u);
}

void SchedTarget::set_oper_shallow_target(
    scrub_type_t rpr,
    utime_t scrub_clock_now)
{
  ceph_assert(sched_info.level == scrub_level_t::shallow);
  ceph_assert(rpr != scrub_type_t::do_repair);
  ceph_assert(!in_queue);

  up_urgency_to(urgency_t::operator_requested);
  sched_info.target = std::min(scrub_clock_now, sched_info.target);
  sched_info.not_before = std::min(sched_info.not_before, scrub_clock_now);
  auto_repairing = false;
  last_issue = delay_cause_t::none;
}

void SchedTarget::set_oper_deep_target(
    scrub_type_t rpr,
    utime_t scrub_clock_now)
{
  ceph_assert(sched_info.level == scrub_level_t::deep);
  ceph_assert(!in_queue);

  if (rpr == scrub_type_t::do_repair) {
    up_urgency_to(urgency_t::must);
    do_repair = true;
  } else {
    up_urgency_to(urgency_t::operator_requested);
  }
  sched_info.target = std::min(scrub_clock_now, sched_info.target);
  sched_info.not_before = std::min(sched_info.not_before, scrub_clock_now);
  auto_repairing = false;
  last_issue = delay_cause_t::none;
  dout(20) << fmt::format(
		  "{}: repair?{} final:{}", __func__,
		  ((rpr == scrub_type_t::do_repair) ? "+" : "-"), *this)
	   << dendl;
}


void SchedTarget::update_as_shallow(
    const pg_info_t& pg_info,
    const Scrub::sched_conf_t& config,
    utime_t time_now)
{
  ceph_assert(sched_info.level == scrub_level_t::shallow);
  ceph_assert(!in_queue);

  if (is_required()) {
    // shouldn't be called for high-urgency scrubs
    return;
  }

  if (pg_info.stats.stats_invalid && config.mandatory_on_invalid) {
    sched_info.urgency = urgency_t::must;
    sched_info.target = time_now;
    sched_info.not_before = time_now;
    // we will force a deadline in this case
    if (config.max_shallow && *config.max_shallow > 0.1) {
      sched_info.deadline = add_double(time_now, *config.max_shallow);
    } else {
      sched_info.deadline = add_double(time_now, config.shallow_interval);
    }
  } else {
    auto base = pg_info.stats.stats_invalid ? time_now
					    : pg_info.history.last_scrub_stamp;
    sched_info.target = add_double(base, config.shallow_interval);
    // if in the past - do not delay. Otherwise - add a random delay
    if (sched_info.target > time_now) {
      double r = rand() / (double)RAND_MAX;
      sched_info.target +=
	  config.shallow_interval * config.interval_randomize_ratio * r;
    }
    sched_info.not_before = sched_info.target;
    sched_info.urgency = urgency_t::periodic_regular;

    if (config.max_shallow && *config.max_shallow > 0.1) {
      sched_info.deadline = add_double(sched_info.target, *config.max_shallow);

      if (time_now > sched_info.deadline) {
	sched_info.urgency = urgency_t::overdue;
      }
    } else {
      sched_info.deadline = utime_t::max();
    }
  }

  // does not match the original logic, but seems to be required
  // for testing (standalone/scrub-test):
  /// \todo fix the tests and remove this
  sched_info.deadline = add_double(sched_info.target, config.max_deep);
}

void SchedTarget::update_as_deep(
    const pg_info_t& pg_info,
    const Scrub::sched_conf_t& config,
    utime_t time_now)
{
  ceph_assert(sched_info.level == scrub_level_t::deep);
  ceph_assert(!in_queue);
  if (is_required()) {
    // shouldn't be called for high-urgency scrubs
    return;
  }

  // note that (based on existing code) we do not require an immediate
  // deep scrub if no stats are available (only a shallow one)
  auto base = pg_info.stats.stats_invalid
		  ? time_now
		  : pg_info.history.last_deep_scrub_stamp;

  sched_info.target = add_double(base, config.deep_interval);
  // if in the past - do not delay. Otherwise - add a random delay
  if (sched_info.target > time_now) {
    double r = rand() / (double)RAND_MAX;
    sched_info.target +=
	config.deep_interval * config.interval_randomize_ratio * r;
  }
  sched_info.not_before = sched_info.target;
  sched_info.deadline = add_double(sched_info.target, config.max_deep);

  sched_info.urgency = (time_now > sched_info.deadline)
			   ? urgency_t::overdue
			   : urgency_t::periodic_regular;
  auto_repairing = false;
}

void SchedTarget::push_nb_out(
    std::chrono::seconds delay,
    delay_cause_t delay_cause,
    utime_t scrub_clock_now)
{
  sched_info.not_before =
      std::max(scrub_clock_now, sched_info.not_before) + utime_t{delay};
  last_issue = delay_cause;
}

void SchedTarget::delay_on_pg_state(utime_t scrub_clock_now)
{
  // if not in a state to be scrubbed (active & clean) - we won't retry it
  // for some time
  const seconds delay =
      seconds(cct->_conf.get_val<int64_t>("osd_scrub_retry_pg_state"));
  push_nb_out(delay, delay_cause_t::pg_state, scrub_clock_now);
}

void SchedTarget::delay_on_level_not_allowed(utime_t scrub_clock_now)
{
  const seconds delay =
      seconds(cct->_conf.get_val<int64_t>("osd_scrub_retry_delay"));
  push_nb_out(delay, delay_cause_t::flags, scrub_clock_now);
}

/// \todo time the delay based on the wait for
/// the end of the forbidden hours.
void SchedTarget::delay_on_wrong_time(utime_t scrub_clock_now)
{
  // wrong time / day / load
  const seconds delay =
      seconds(cct->_conf.get_val<int64_t>("osd_scrub_retry_wrong_time"));
  push_nb_out(delay, delay_cause_t::time, scrub_clock_now);
}

void SchedTarget::delay_on_no_local_resrc(utime_t scrub_clock_now)
{
  // too many scrubs on our own OSD. The delay we introduce should be
  // minimal: after all, we expect all other PG tried to fail as well.
  // This should be revisited once we separate the resource-counters for
  // deep and shallow scrubs.
  push_nb_out(2s, delay_cause_t::local_resources, scrub_clock_now);
}

void SchedTarget::dump(std::string_view sect_name, ceph::Formatter* f) const
{
  f->open_object_section(sect_name);
  /// \todo improve the performance of u_time dumps here
  f->dump_stream("pg") << sched_info.pgid;
  f->dump_stream("level")
      << (sched_info.level == scrub_level_t::deep ? "deep" : "shallow");
  f->dump_stream("urgency") << fmt::format("{}", sched_info.urgency);
  f->dump_stream("target") << sched_info.target;
  f->dump_stream("not_before") << sched_info.not_before;
  f->dump_stream("deadline") << sched_info.deadline;
  f->dump_bool("auto_rpr", auto_repairing);
  f->dump_bool("forced", is_required());
  f->dump_stream("last_delay") << fmt::format("{}", last_issue);
  f->close_section();
}


// /////////////////////////////////////////////////////////////////////////
// ScrubJob

using ScrubJob = Scrub::ScrubJob;

ScrubJob::ScrubJob(
    ScrubQueueOps& osd_queue,
    CephContext* cct,
    const spg_t& pg,
    int node_id)
    : pgid{pg}
    , whoami{node_id}
    , cct{cct}
    , scrub_queue{osd_queue}
    , shallow_target{pg, scrub_level_t::shallow, node_id, cct}
    , deep_target{pg, scrub_level_t::deep, node_id, cct}
{
  m_log_msg_prefix = fmt::format("osd.{} pg[{}] ScrubJob:", whoami, pgid.pgid);
}

// debug usage only
ostream& operator<<(ostream& out, const ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}

std::ostream& ScrubJob::gen_prefix(std::ostream& out) const
{
  return out << m_log_msg_prefix;
}

void ScrubJob::dump(ceph::Formatter* f) const
{
  auto now_is = scrub_queue.scrub_clock_now();
  f->open_object_section("scheduling");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << get_sched_time(now_is);
  auto& nearest = closest_target(now_is);
  f->dump_stream("deadline") << nearest.sched_info.deadline;

  nearest.dump("nearest", f);
  shallow_target.dump("shallow_target", f);
  deep_target.dump("deep_target", f);
  f->dump_bool("forced", nearest.is_required());
  f->dump_bool("blocked", blocked);
  f->close_section();
}

scrub_level_t ScrubJob::the_other_level(scrub_level_t l)
{
  return (l == scrub_level_t::deep) ? scrub_level_t::shallow
				    : scrub_level_t::deep;
}


SchedTarget& ScrubJob::closest_target(utime_t scrub_clock_now)
{
  if (cmp_targets(scrub_clock_now, shallow_target, deep_target) < 0) {
    return shallow_target;
  } else {
    return deep_target;
  }
}

const SchedTarget& ScrubJob::closest_target(utime_t scrub_clock_now) const
{
  if (cmp_targets(scrub_clock_now, shallow_target, deep_target) < 0) {
    return shallow_target;
  } else {
    return deep_target;
  }
}

bool ScrubJob::in_queue() const
{
  return shallow_target.in_queue || deep_target.in_queue;
}


SchedTarget ScrubJob::get_moved_target(scrub_level_t s_or_d)
{
  auto& moved_trgt = get_target(s_or_d);
  SchedTarget cp = moved_trgt;
  ceph_assert(!cp.in_queue);
  moved_trgt.reset();
  return cp;
}

void ScrubJob::dequeue_entry(scrub_level_t lvl)
{
  scrub_queue.remove_entry(pgid, lvl);
  get_target(lvl).clear_queued();
}

int ScrubJob::dequeue_targets()
{
  const int in_q_count =
      (shallow_target.is_queued() ? 1 : 0) + (deep_target.is_queued() ? 1 : 0);
  scrub_queue.remove_entry(pgid, scrub_level_t::shallow);
  shallow_target.clear_queued();
  scrub_queue.remove_entry(pgid, scrub_level_t::deep);
  deep_target.clear_queued();
  return in_q_count;
}

SchedTarget& ScrubJob::dequeue_target(scrub_level_t s_or_d)
{
  auto& target = get_target(s_or_d);
  scrub_queue.remove_entry(pgid, s_or_d);
  target.clear_queued();
  return target;
}

/*
 * Note:
 * - this is the only targets-manipulating function that accepts disabled
 *   (urgency == off) targets;
 * - and (partially because of that), here is where we may decide to 'upgrade'
 *   the next shallow scrub to a deep scrub.
 */
void ScrubJob::init_and_queue_targets(
    const pg_info_t& info,
    const Scrub::sched_conf_t& aconf,
    utime_t scrub_clock_now)
{
  const int in_q_count = dequeue_targets();

  shallow_target.depenalize();
  shallow_target.update_as_shallow(info, aconf, scrub_clock_now);
  deep_target.depenalize();
  deep_target.update_as_deep(info, aconf, scrub_clock_now);

  // if 'randomly selected', we will modify the deep target to coincide
  // with the shallow one
  std::string log_as_updated = "";
  const bool upgrade_to_deep = (in_q_count == 0) &&
			       shallow_target.is_periodic() &&
			       deep_target.is_periodic() &&
			       (rand() / RAND_MAX) < aconf.deep_randomize_ratio;
  if (upgrade_to_deep && (deep_target.sched_info.not_before >
			  shallow_target.sched_info.not_before)) {
    deep_target.sched_info.target = std::min(
	shallow_target.sched_info.target, deep_target.sched_info.target);
    deep_target.sched_info.not_before = shallow_target.sched_info.not_before;
    shallow_target.sched_info.not_before = add_double(
	shallow_target.sched_info.not_before, aconf.shallow_interval);
    log_as_updated = " (updated)";
  }

  if (scrub_queue.queue_entries(
	  pgid, shallow_target.queued_element(),
	  deep_target.queued_element())) {
    shallow_target.set_queued();
    deep_target.set_queued();
    dout(15) << fmt::format(
		    "{}: {} targets removed from queue; added {} & {}{}",
		    __func__, in_q_count, shallow_target, deep_target,
		    log_as_updated)
	     << dendl;
  }
}


void ScrubJob::remove_from_osd_queue()
{
  const int in_q_count = dequeue_targets();
  shallow_target.disable();
  deep_target.disable();
  dout(15) << fmt::format(
		  "{}: {} targets removed and disabled", __func__, in_q_count)
	   << dendl;
}


void ScrubJob::mark_for_after_repair()
{
  auto now_is = scrub_queue.scrub_clock_now();

  //dequeue, then manipulate, the deep target
  scrub_queue.remove_entry(pgid, scrub_level_t::deep);
  deep_target.sched_info.urgency = urgency_t::after_repair;
  deep_target.sched_info.target = {0, 0};
  deep_target.sched_info.not_before = now_is;

  // requeue
  requeue_entry(scrub_level_t::deep);
}


/// \todo consider replacing with a call to 'PgScrubber::get_schedule()'
/// followed by the code in 'pg_stat_t::dump_scrub_schedule()'
std::string ScrubJob::scheduling_state() const
{
  auto now_is = scrub_queue.scrub_clock_now();
  auto& nearest = closest_target(now_is);
  if (!nearest.is_queued()) {
    return "no scrub is scheduled";
  }

  if (nearest.is_ripe(now_is)) {
    return fmt::format(
	"queued for {}scrub", (nearest.is_deep() ? "deep " : ""));
  }

  return fmt::format(
      "{}scrub scheduled @ {}", (nearest.is_deep() ? "deep " : ""),
      nearest.sched_time());
}

utime_t ScrubJob::get_sched_time(utime_t scrub_clock_now) const
{
  return closest_target(scrub_clock_now).sched_time();
}

void ScrubJob::requeue_entry(scrub_level_t level)
{
  if (scrubbing) {
    return;
  }
  auto& target = get_target(level);
  if (target.is_off()) {
    // which means that the PG is no longer "scrubable"
    return;
  }
  scrub_queue.cp_and_queue_target(target.queued_element());
  target.in_queue = true;
}


void ScrubJob::operator_forced_targets(
    scrub_level_t level,
    scrub_type_t scrub_type,
    utime_t now_is)
{
  // the dequeue might fail, as we might be scrubbing that same target now,
  // but that's OK
  dequeue_target(level);
  if (level == scrub_level_t::shallow) {
    shallow_target.set_oper_shallow_target(scrub_type, now_is);
  } else {
    deep_target.set_oper_deep_target(scrub_type, now_is);
  }
  requeue_entry(level);
}


void ScrubJob::operator_periodic_targets(
    scrub_level_t level,
    utime_t upd_stamp,
    const pg_info_t& info,
    const Scrub::sched_conf_t& aconf,
    utime_t scrub_clock_now)
{
  // the 'stamp' was "faked" to trigger a "periodic" scrub.
  auto& trgt = get_target(level);

  // if the target is in the queue, and has 'must' urgency - we are done
  if (trgt.is_queued() && trgt.is_required()) {
    dout(10) << fmt::format(
		    "{}: there is a higher urgency scrub in the queue",
		    __func__)
	     << dendl;
    return;
  }

  scrub_queue.remove_entry(pgid, level);
  trgt.clear_queued();

  trgt.up_urgency_to(urgency_t::periodic_regular);
  if (level == scrub_level_t::shallow) {
    trgt.sched_info.target = add_double(upd_stamp, aconf.shallow_interval);
    // we do set a deadline for the operator-induced scrubbing. That will
    // allow us to avoid some limiting preconditions.
    trgt.sched_info.deadline = add_double(
	upd_stamp, aconf.max_shallow.value_or(aconf.shallow_interval));
  } else {
    trgt.sched_info.target = add_double(upd_stamp, aconf.deep_interval);
    trgt.sched_info.deadline = add_double(upd_stamp, aconf.deep_interval);
  }

  trgt.sched_info.not_before =
      std::min(trgt.sched_info.not_before, scrub_clock_now);
  trgt.last_issue = delay_cause_t::none;
  if (scrub_clock_now > trgt.sched_info.deadline) {
    trgt.up_urgency_to(urgency_t::overdue);
  }
  requeue_entry(level);
}


/**
 * Handle a scrub aborted mid-execution.
 * State on entry:
 * - no target is in the queue (both were dequeued when the scrub started);
 * - both 'shallow' & 'deep' targets are valid - set for the next scrub;
 * Process:
 * - merge the failing target with the corresponding 'next' target;
 * - make sure 'not-before' is somewhat in the future;
 * - requeue both targets.
 *
 * \todo use the number of ripe jobs to determine the delay
 */
void ScrubJob::on_abort(
    SchedTarget&& aborted_target,
    delay_cause_t issue,
    utime_t now_is)
{
  scrubbing = false;
  auto& nxt_target = get_target(aborted_target.level());
  ++consec_aborts;
  const seconds delay = seconds(
      consec_aborts * cct->_conf.get_val<int64_t>("osd_scrub_retry_delay"));

  dout(15) << fmt::format(
		  "{}: pre-abort:{} next:{}", __func__, aborted_target,
		  nxt_target)
	   << dendl;

  // merge the targets:
  auto sched_to = std::min(
      aborted_target.queued_element().target,
      nxt_target.queued_element().target);
  auto delay_to = now_is + utime_t{delay};

  if (aborted_target.queued_element().urgency >
      nxt_target.queued_element().urgency) {
    nxt_target = aborted_target;
  }
  nxt_target.sched_info.target = sched_to;
  nxt_target.sched_info.not_before = delay_to;
  nxt_target.last_issue = issue;

  if (scrub_queue.queue_entries(
	  pgid, shallow_target.queued_element(),
	  deep_target.queued_element())) {
    shallow_target.set_queued();
    deep_target.set_queued();
  }
  dout(10) << fmt::format(
		  "{}: post [c.target/base:{}] [c.target/abrtd:{}] {}s delay",
		  __func__, nxt_target, aborted_target, delay.count())
	   << dendl;
}

/**
 * Handle a failure to secure the replicas' scrub resources.
 * State on entry:
 * - no target is in the queue (both were dequeued when the scrub started);
 * - both 'shallow' & 'deep' targets are valid - set for the next scrub;
 */
bool ScrubJob::on_reservation_failure(
    std::chrono::seconds penalty_period,
    SchedTarget&& aborted_target)
{
  bool trgts_demoted = false;

  ceph_assert(scrubbing);
  ceph_assert(!deep_target.in_queue);
  ceph_assert(!shallow_target.in_queue);

  const seconds delay =
      seconds{cct->_conf.get_val<int64_t>("osd_scrub_retry_busy_replicas")};

  scrubbing = false;
  auto& nxt_target = get_target(aborted_target.level());
  ++consec_aborts;

  dout(10) << fmt::format(
		  "{}: delay:{}s offending:{} planned:{}", __func__,
		  delay.count(), aborted_target, nxt_target)
	   << dendl;

  // merge the targets:
  auto sched_to =
      std::min(aborted_target.sched_info.target, nxt_target.sched_info.target);
  auto now_is = scrub_queue.scrub_clock_now();
  auto delay_to = now_is + utime_t{delay};

  if (aborted_target.sched_info.urgency > nxt_target.sched_info.urgency) {
    nxt_target = aborted_target;
  }
  nxt_target.sched_info.target = sched_to;
  nxt_target.sched_info.not_before = delay_to;
  nxt_target.last_issue = delay_cause_t::replicas;
  ceph_assert(!nxt_target.is_off());

  // now - if both targets are periodic, the
  // scrub_job will be penalized: its urgency will be demoted for a while.
  if (penalty_period.count() > 0 && shallow_target.is_periodic() &&
      deep_target.is_periodic()) {
    shallow_target.sched_info.urgency = urgency_t::penalized;
    deep_target.sched_info.urgency = urgency_t::penalized;
    penalized = true;
    penalized_until = now_is + utime_t{penalty_period};
    trgts_demoted = true;
  }
  scrub_queue.queue_entries(
      pgid, shallow_target.queued_element(), deep_target.queued_element());
  shallow_target.set_queued();
  deep_target.set_queued();
  dout(10) << fmt::format(
		  "{}: post [c.target/base:{}] [c.target/abrtd:{}] {}s delay",
		  __func__, nxt_target, aborted_target, delay.count())
	   << dendl;
  return trgts_demoted;
}

void ScrubJob::un_penalize()
{
  if (!penalized) {
    return;
  }
  // dequeue & requeue the targets:
  const int in_q_count = dequeue_targets();
  shallow_target.depenalize();
  deep_target.depenalize();

  if (scrub_queue.queue_entries(
	  pgid, shallow_target.queued_element(),
	  deep_target.queued_element())) {
    shallow_target.set_queued();
    deep_target.set_queued();
  }

  penalized = false;
  penalized_until = utime_t{0, 0};
  dout(15) << fmt::format(
		  "{}: {} targets dequeued. Now: {}", __func__, in_q_count,
		  *this)
	   << dendl;
}

std::string_view ScrubJob::registration_state() const
{
  return in_queue() ? "in-queue" : "not-queued";
}


/**
 * mark for a deep-scrub after the current scrub ended with errors.
 * Note that no need to requeue the target, as it will be requeued
 * when the scrub ends.
 */
void ScrubJob::mark_for_rescrubbing()
{
  ceph_assert(scrubbing);
  ceph_assert(!deep_target.in_queue);
  deep_target.auto_repairing = true;
  // no need to take existing deep_target contents into account,
  // as the only higher priority is 'after_repair', and we know no
  // repair took place while we were scrubbing.
  deep_target.sched_info.target = scrub_queue.scrub_clock_now();
  deep_target.sched_info.not_before = deep_target.sched_info.target;
  deep_target.sched_info.urgency = urgency_t::must;  // no need to use max(...)

  dout(10) << fmt::format(
		  "{}: need deep+a.r. after scrub errors. Target set to {}",
		  __func__, deep_target)
	   << dendl;
}


void ScrubJob::at_scrub_completion(
    const pg_info_t& pg_info,
    const sched_conf_t& aconf,
    utime_t scrub_clock_now)
{
  ceph_assert(!in_queue());

  shallow_target.depenalize();
  shallow_target.update_as_shallow(pg_info, aconf, scrub_clock_now);

  deep_target.depenalize();
  deep_target.update_as_deep(pg_info, aconf, scrub_clock_now);

  if (scrub_queue.queue_entries(
	  pgid, shallow_target.queued_element(),
	  deep_target.queued_element())) {
    shallow_target.set_queued();
    deep_target.set_queued();
    dout(10) << fmt::format(
		    "{}: requeued {} and {}", __func__, shallow_target,
		    deep_target)
	     << dendl;
  }
}


SchedTarget& ScrubJob::get_target(scrub_level_t lvl)
{
  return (lvl == scrub_level_t::deep) ? deep_target : shallow_target;
}

void ScrubJob::on_periods_change(
    const pg_info_t& info,
    const Scrub::sched_conf_t& aconf,
    utime_t scrub_clock_now)
{
  dout(10) << fmt::format(
		  "{}: before: {} and {} scrubbing:{}", __func__,
		  shallow_target, deep_target, scrubbing)
	   << dendl;
  if (scrubbing) {
    // both targets will be updated at the end of the scrub
    return;
  }

  bool should_unpenalize = penalized && (penalized_until < scrub_clock_now);

  if (shallow_target.is_periodic()) {
    if (shallow_target.is_queued()) {
      dequeue_target(scrub_level_t::shallow);
    }
    if (should_unpenalize) {
      shallow_target.depenalize();
    }
    shallow_target.update_as_shallow(info, aconf, scrub_clock_now);
    requeue_entry(scrub_level_t::shallow);
  }

  if (deep_target.is_periodic()) {
    if (deep_target.is_queued()) {
      dequeue_target(scrub_level_t::deep);
    }
    if (should_unpenalize) {
      deep_target.depenalize();
    }
    deep_target.update_as_deep(info, aconf, scrub_clock_now);
    requeue_entry(scrub_level_t::deep);
  }
  dout(10) << fmt::format(
		  "{}: after: {} and {}", __func__, shallow_target, deep_target)
	   << dendl;
}
