// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 smarttab

#include "./pg_scrubber.h"  // '.' notation used to affect clang-format order

#include <fmt/ranges.h>

#include <cmath>
#include <iostream>
#include <span>
#include <vector>

#include "debug.h"

#include "common/ceph_time.h"
#include "common/errno.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "messages/MOSDScrubReserve.h"
#include "osd/OSD.h"
#include "osd/PG.h"
#include "include/utime_fmt.h"
#include "osd/osd_types_fmt.h"

#include "ScrubStore.h"
#include "scrub_backend.h"
#include "scrub_machine.h"

using std::list;
using std::pair;
using std::stringstream;
using std::vector;
using namespace Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;

#define dout_context (m_osds->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

template <class T>
static ostream& _prefix(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout);
}

ostream& operator<<(ostream& out, const scrub_flags_t& sf)
{
  if (sf.auto_repair)
    out << " AUTO_REPAIR";
  if (sf.check_repair)
    out << " CHECK_REPAIR";
  if (sf.deep_scrub_on_error)
    out << " DEEP_SCRUB_ON_ERROR";

  return out;
}

void PgScrubber::on_replica_activate()
{
  dout(10) << __func__ << dendl;
  m_fsm->process_event(ReplicaActivate{});
}


/*
 * if the incoming message is from a previous interval, it must mean
 * PrimaryLogPG::on_change() was called when that interval ended. We can safely
 * discard the stale message.
 */
bool PgScrubber::check_interval(epoch_t epoch_to_verify) const
{
  return epoch_to_verify >= m_pg->get_same_interval_since();
}

bool PgScrubber::should_drop_message(OpRequestRef &op) const
{
  if (check_interval(op->sent_epoch)) {
    return false;
  } else {
    dout(10) << __func__ <<  ": discarding message " << *(op->get_req())
	     << " from prior interval, epoch " << op->sent_epoch
	     << ".  Current history.same_interval_since: "
             << m_pg->info.history.same_interval_since << dendl;
    return true;
  }
}

bool PgScrubber::is_message_relevant(epoch_t epoch_to_verify)
{
  if (!m_active) {
    // not scrubbing. We can assume that the scrub was already terminated, and
    // we can silently discard the incoming event.
    return false;
  }

  // is this a message from before we started this scrub?
  if (epoch_to_verify < m_epoch_start) {
    return false;
  }

  // has a new interval started?
  if (!check_interval(epoch_to_verify)) {
    // if this is a new interval, on_change() has already terminated that
    // old scrub.
    return false;
  }

  ceph_assert(is_primary());

  // were we instructed to abort?
  return verify_against_abort(epoch_to_verify);
}

bool PgScrubber::verify_against_abort(epoch_t epoch_to_verify)
{
  if (!should_abort()) {
    return true;
  }

  dout(10) << __func__ << " aborting. incoming epoch: " << epoch_to_verify
	   << " vs last-aborted: " << m_last_aborted << dendl;

  // if we were not aware of the abort before - kill the scrub.
  if (epoch_to_verify >= m_last_aborted) {
    m_fsm->process_event(FullReset{});
    m_last_aborted = std::max(epoch_to_verify, m_epoch_start);
  }
  return false;
}

bool PgScrubber::should_abort() const
{
  if (!ScrubJob::observes_noscrub_flags(m_active_target->urgency())) {
    // not aborting some types of high-priority scrubs
    return false;
  }

  // note: deep scrubs are allowed even if 'no-scrub' is set (but not
  // 'no-deepscrub')
  if (m_is_deep) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB) ||
	m_pg->pool.info.has_flag(pg_pool_t::FLAG_NODEEP_SCRUB)) {
      dout(10) << "nodeep_scrub set, aborting" << dendl;
      return true;
    }
  } else if (get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) ||
	     m_pg->pool.info.has_flag(pg_pool_t::FLAG_NOSCRUB)) {
    dout(10) << "noscrub set, aborting" << dendl;
    return true;
  }

  return false;
}

//   initiating state-machine events --------------------------------

/*
 * a note re the checks performed before sending scrub-initiating messages:
 *
 * For those scrub-initiation messages ('StartScrub') that
 * possibly were in the queue while the PG changed state and became unavailable
 * for scrubbing:
 *
 * The check_interval() catches all major changes to the PG. As for the other
 * conditions we may check (and see is_message_relevant() above):
 *
 * - we are not 'active' yet, so must not check against is_active(), and:
 *
 * - the 'abort' flags were just verified (when the triggering message was
 * queued). As those are only modified in human speeds - they need not be
 * queried again.
 *
 * Some of the considerations above are also relevant to the replica-side
 * initiation
 */

void PgScrubber::initiate_regular_scrub(epoch_t epoch_queued)
{
  dout(10) << fmt::format(
		  "{}: epoch:{} is PrimaryIdle:{}", __func__, epoch_queued,
		  m_fsm->is_primary_idle())
	   << dendl;

  // we may have lost our Primary status while the message languished in the
  // queue
  if (check_interval(epoch_queued)) {
    dout(10) << "scrubber event -->> StartScrub epoch: " << epoch_queued
	     << dendl;
    m_fsm->process_event(StartScrub{});
    dout(10) << "scrubber event --<< StartScrub" << dendl;
  }
}

void PgScrubber::advance_token()
{
  m_current_token++;
}

void PgScrubber::send_scrub_unblock(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (is_message_relevant(epoch_queued)) {
    m_fsm->process_event(Unblocked{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_scrub_resched(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (is_message_relevant(epoch_queued)) {
    m_fsm->process_event(InternalSchedScrub{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_start_replica(epoch_t epoch_queued,
				    Scrub::act_token_t token)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << " token: " << token << dendl;
  if (is_primary()) {
    // shouldn't happen. Ignore
    dout(1) << "got a replica scrub request while Primary!" << dendl;
    return;
  }

  if (check_interval(epoch_queued) && is_token_current(token)) {
    m_fsm->process_event(StartReplica{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_sched_replica(epoch_t epoch_queued,
				    Scrub::act_token_t token)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << " token: " << token << dendl;
  if (check_interval(epoch_queued) && is_token_current(token)) {
    m_fsm->process_event(SchedReplica{});  // retest for map availability
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::active_pushes_notification(epoch_t epoch_queued)
{
  // note: Primary only
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (is_message_relevant(epoch_queued)) {
    m_fsm->process_event(ActivePushesUpd{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::update_applied_notification(epoch_t epoch_queued)
{
  // note: Primary only
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (is_message_relevant(epoch_queued)) {
    m_fsm->process_event(UpdatesApplied{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::digest_update_notification(epoch_t epoch_queued)
{
  // note: Primary only
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (is_message_relevant(epoch_queued)) {
    m_fsm->process_event(DigestUpdate{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_local_map_done(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (is_message_relevant(epoch_queued)) {
    m_fsm->process_event(Scrub::IntLocalMapDone{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_replica_maps_ready(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (is_message_relevant(epoch_queued)) {
    m_fsm->process_event(GotReplicas{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_replica_pushes_upd(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (check_interval(epoch_queued)) {
    m_fsm->process_event(ReplicaPushesUpd{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_chunk_free(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (check_interval(epoch_queued)) {
    m_fsm->process_event(Scrub::SelectedChunkFree{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_chunk_busy(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (check_interval(epoch_queued)) {
    m_fsm->process_event(Scrub::ChunkIsBusy{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_get_next_chunk(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (is_message_relevant(epoch_queued)) {
    m_fsm->process_event(Scrub::NextChunk{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_scrub_is_finished(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;

  // can't check for "active"

  m_fsm->process_event(Scrub::ScrubFinished{});

  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_granted_by_reserver(const AsyncScrubResData& req)
{
  dout(10) << "scrubber event -->> granted_by_reserver" << dendl;
  if (check_interval(req.request_epoch)) {
    m_fsm->process_event(Scrub::ReserverGranted{req});
  }
  dout(10) << "scrubber event --<< granted_by_reserver" << dendl;
}

// -----------------

bool PgScrubber::is_reserving() const
{
  return m_fsm->is_reserving();
}

void PgScrubber::reset_epoch()
{
  dout(10) << __func__ << " state deep? " << state_test(PG_STATE_DEEP_SCRUB)
	   << dendl;
  m_fsm->assert_not_in_session();

  m_epoch_start = m_pg->get_same_interval_since();
  ceph_assert(m_is_deep == state_test(PG_STATE_DEEP_SCRUB));
  update_op_mode_text();
}

unsigned int PgScrubber::scrub_requeue_priority(
  Scrub::scrub_prio_t with_priority) const
{
  unsigned int qu_priority = m_flags.priority;

  if (with_priority == Scrub::scrub_prio_t::high_priority) {
    qu_priority =
      std::max(qu_priority,
	       (unsigned int)m_pg->get_cct()->_conf->osd_client_op_priority);
  }
  return qu_priority;
}

unsigned int PgScrubber::scrub_requeue_priority(
  Scrub::scrub_prio_t with_priority,
  unsigned int suggested_priority) const
{
  if (with_priority == Scrub::scrub_prio_t::high_priority) {
    suggested_priority =
      std::max(suggested_priority,
	       (unsigned int)m_pg->get_cct()->_conf->osd_client_op_priority);
  }
  return suggested_priority;
}

// ///////////////////////////////////////////////////////////////////// //
// scrub-op registration handling

/* on_new_interval
 *
 * Responsible for resetting any scrub state and releasing any resources.
 * Any inflight events will be ignored via check_interval/should_drop_message
 * or canceled.
 * Specifically:
 * - if Primary and in an active session - the IntervalChanged handler takes
 *   care of discarding the remote reservations, and transitioning out of
 *   Session. That resets both the scrubber and the FSM.
 * - if we are a reserved replica - we need to free ourselves;
 */
void PgScrubber::on_new_interval()
{
  dout(10) << fmt::format(
		  "{}: current role:{} active?{} q/a:{}", __func__,
		  (is_primary() ? "Primary" : "Replica/other"),
		  is_scrub_active(), is_queued_or_active())
	   << dendl;
  m_after_repair_scrub_required = false;
  m_fsm->process_event(IntervalChanged{});
  // the following asserts were added due to a past bug, where PG flags were
  // left set in some scenarios.
  ceph_assert(!is_queued_or_active());
  ceph_assert(!state_test(PG_STATE_SCRUBBING));
  ceph_assert(!state_test(PG_STATE_DEEP_SCRUB));
}

bool PgScrubber::is_scrub_registered() const
{
  return m_scrub_job && m_scrub_job->is_registered();
}

std::string_view PgScrubber::registration_state() const
{
  if (m_scrub_job) {
    return m_scrub_job->state_desc();
  }
  return "(no sched job)"sv;
}

void PgScrubber::rm_from_osd_scrubbing()
{
  if (m_scrub_job && m_scrub_job->is_registered()) {
    dout(15) << fmt::format(
		    "{}: prev. state: {}", __func__, registration_state())
	     << dendl;
    m_osds->get_scrub_services().remove_from_osd_queue(m_pg_id);
    m_scrub_job->clear_both_targets_queued();
    m_scrub_job->registered = false;
  }
}


void PgScrubber::update_targets(utime_t scrub_clock_now)
{
  const auto applicable_conf = populate_config_params();

  dout(10) << fmt::format(
		  "{}: config:{} job on entry:{}{}", __func__, applicable_conf,
		  *m_scrub_job,
		  m_pg->info.stats.stats_invalid ? " invalid-stats" : "")
	   << dendl;
  if (m_pg->info.stats.stats_invalid && applicable_conf.mandatory_on_invalid) {
    m_scrub_job->shallow_target.sched_info_ref().schedule.scheduled_at =
	scrub_clock_now;
    m_scrub_job->shallow_target.up_urgency_to(urgency_t::must_scrub);
  }

  // the next periodic scrubs:
  m_scrub_job->adjust_shallow_schedule(
      m_pg->info.history.last_scrub_stamp, applicable_conf, scrub_clock_now,
      delay_ready_t::delay_ready);
  m_scrub_job->adjust_deep_schedule(
      m_pg->info.history.last_deep_scrub_stamp, applicable_conf,
      scrub_clock_now, delay_ready_t::delay_ready);

  dout(10) << fmt::format("{}: adjusted:{}", __func__, *m_scrub_job) << dendl;
}


void PgScrubber::schedule_scrub_with_osd()
{
  ceph_assert(is_primary());
  ceph_assert(m_scrub_job);

  dout(20) << fmt::format(
		  "{}: state at entry: {}", __func__, m_scrub_job->state_desc())
	   << dendl;
  m_scrub_job->registered = true;
  update_scrub_job(delay_ready_t::delay_ready);
}


void PgScrubber::on_primary_active_clean()
{
  dout(10) << fmt::format(
		  "{}: reg state: {}", __func__, m_scrub_job->state_desc())
	   << dendl;
  m_fsm->process_event(PrimaryActivate{});
}


/*
 * A note re the call to publish_stats_to_osd() below:
 * - we are called from either request_rescrubbing() or scrub_requested().
 * Update: also from scrub_finish() & schedule_scrub_with_osd().
 * - in all cases - the schedule was modified, and needs to be published;
 * - we are a Primary.
 * - in the 1st case - the call is made as part of scrub_finish(), which
 *   guarantees that the PG is locked and the interval is still the same.
 * - in the 2nd case - we know the PG state and we know we are only called
 *   for a Primary.
 */
void PgScrubber::update_scrub_job(Scrub::delay_ready_t delay_ready)
{
  if (!is_primary() || !m_scrub_job) {
    dout(10) << fmt::format(
		    "{}: PG[{}]: not Primary or no scrub-job", __func__,
		    m_pg_id)
	     << dendl;
    return;
  }

  dout(15) << fmt::format("{}: job on entry:{}", __func__, *m_scrub_job)
	   << dendl;

  // if we were marked as 'not registered' - do not try to push into
  // the queue. And if we are already in the queue - dequeue.
  if (!m_scrub_job->is_registered()) {
    dout(10) << fmt::format("{}: PG[{}] not registered", __func__, m_pg_id)
	     << dendl;
    return;
  }
  ceph_assert(m_pg->is_locked());
  if (m_scrub_job->is_queued()) {
    // one or both of the targets are in the queue. Remove them.
    m_osds->get_scrub_services().remove_from_osd_queue(m_pg_id);
    m_scrub_job->clear_both_targets_queued();
    dout(20) << fmt::format(
		    "{}: PG[{}] dequeuing for an update", __func__, m_pg_id)
	     << dendl;
  }

  update_targets(ceph_clock_now());
  m_osds->get_scrub_services().enqueue_scrub_job(*m_scrub_job);
  m_scrub_job->set_both_targets_queued();
  m_pg->publish_stats_to_osd();
  dout(10) << fmt::format("{}: job on exit:{}", __func__, *m_scrub_job)
	   << dendl;
}


scrub_level_t PgScrubber::scrub_requested(
    scrub_level_t scrub_level,
    scrub_type_t scrub_type)
{
  const bool repair_requested = (scrub_type == scrub_type_t::do_repair);
  const bool deep_requested =
      (scrub_level == scrub_level_t::deep) || repair_requested;
  scrub_level = deep_requested ? scrub_level_t::deep : scrub_level_t::shallow;
  dout(10) << fmt::format(
		  "{}: {}{} scrub requested. "
		  "@entry:last-stamp:{:s},Registered?{}",
		  __func__,
		  (scrub_type == scrub_type_t::do_repair ? "repair + "
							 : "not-repair + "),
		  (deep_requested ? "deep" : "shallow"),
		  m_scrub_job->get_sched_time(), registration_state())
	   << dendl;

  // if we were marked as 'not registered' - do not try to push into
  // the queue.
  if (!m_scrub_job->is_registered()) {
    dout(10) << fmt::format(
		    "{}: pg[{}]: not registered for scrubbing on this OSD",
		    __func__, m_pg_id)
	     << dendl;
    return scrub_level_t::shallow;
  }

  // update the relevant SchedTarget (either shallow or deep). Set its urgency
  // to either operator_requested or must_repair. Push it into the queue
  auto& trgt = m_scrub_job->get_target(scrub_level);
  m_osds->get_scrub_services().dequeue_target(m_pg_id, scrub_level);
  m_scrub_job->operator_forced(scrub_level, scrub_type);
  m_osds->get_scrub_services().enqueue_target(trgt);

  return scrub_level;
}


void PgScrubber::recovery_completed()
{
  dout(15) << fmt::format(
		  "{}: is scrub required? {}", __func__,
		  m_after_repair_scrub_required)
	   << dendl;
  if (m_after_repair_scrub_required) {
    m_after_repair_scrub_required = false;
    m_osds->get_scrub_services().dequeue_target(m_pg_id, scrub_level_t::deep);
    auto& trgt = m_scrub_job->get_target(scrub_level_t::deep);
    trgt.up_urgency_to(urgency_t::after_repair);
    const auto clock_now = ceph_clock_now();
    trgt.sched_info.schedule.scheduled_at = clock_now;
    trgt.sched_info.schedule.not_before = clock_now;
    m_osds->get_scrub_services().enqueue_target(trgt);
  }
}


bool PgScrubber::is_after_repair_required() const
{
  return m_after_repair_scrub_required;
}


/**
 * mark for a deep-scrub after the current scrub ended with errors.
 * Note that no need to requeue the target, as it will be requeued
 * when the scrub ends.
 */
void PgScrubber::request_rescrubbing()
{
  dout(10) << fmt::format("{}: job on entry: {}", __func__, *m_scrub_job)
           << dendl;
  auto& trgt = m_scrub_job->get_target(scrub_level_t::deep);
  trgt.up_urgency_to(urgency_t::repairing);
  const auto clock_now = ceph_clock_now();
  trgt.sched_info.schedule.scheduled_at = clock_now;
  trgt.sched_info.schedule.not_before = clock_now;
}


/*
 * Implementation:
 * try to create the reservation object (which translates into asking the
 * OSD for a local scrub resource). The object returned is a
 * a wrapper around the actual reservation, and that object releases
 * the local resource automatically when reset.
 */
bool PgScrubber::reserve_local(const Scrub::SchedTarget& trgt)
{
  const bool is_hp = !ScrubJob::observes_max_concurrency(trgt.urgency());

  m_local_osd_resource = m_osds->get_scrub_services().inc_scrubs_local(is_hp);
  if (m_local_osd_resource) {
    dout(10) << __func__ << ": local resources reserved" << dendl;
    return true;
  }

  dout(15) << __func__ << ": failed to reserve local scrub resources" << dendl;
  return false;
}


Scrub::sched_conf_t PgScrubber::populate_config_params() const
{
  const pool_opts_t& pool_conf = m_pg->get_pgpool().info.opts;
  const auto& conf = get_pg_cct()->_conf;  // for brevity
  Scrub::sched_conf_t configs;

  // deep-scrub optimal interval
  configs.deep_interval =
      pool_conf.value_or(pool_opts_t::DEEP_SCRUB_INTERVAL, 0.0);
  if (configs.deep_interval <= 0.0) {
    configs.deep_interval = conf->osd_deep_scrub_interval;
  }

  // shallow-scrub interval
  configs.shallow_interval =
      pool_conf.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
  if (configs.shallow_interval <= 0.0) {
    configs.shallow_interval = conf->osd_scrub_min_interval;
  }

  // the max allowed delay between scrubs.
  // For deep scrubs - there is no equivalent of scrub_max_interval. Per the
  // documentation, once deep_scrub_interval has passed, we are already
  // "overdue", at least as far as the "ignore allowed load" window is
  // concerned.

  configs.max_deep = configs.deep_interval + configs.shallow_interval;

  auto max_shallow = pool_conf.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  if (max_shallow <= 0.0) {
    max_shallow = conf->osd_scrub_max_interval;
  }
  if (max_shallow > 0.0) {
    configs.max_shallow = max_shallow;
    // otherwise - we're left with the default nullopt
  }

  // but seems like our tests require: \todo fix!
  configs.max_deep =
      std::max(configs.max_shallow.value_or(0.0), configs.deep_interval);

  configs.interval_randomize_ratio = conf->osd_scrub_interval_randomize_ratio;
  configs.deep_randomize_ratio = conf.get_val<double>("osd_deep_scrub_interval_cv");
  configs.mandatory_on_invalid = conf->osd_scrub_invalid_stats;

  dout(15) << fmt::format("{}: updated config:{}", __func__, configs) << dendl;
  return configs;
}


// handling Asok's "scrub" & "deep-scrub" commands

namespace {
void asok_response_section(
    ceph::Formatter* f,
    bool is_periodic,
    scrub_level_t scrub_level,
    utime_t stamp = utime_t{})
{
  f->open_object_section("result");
  f->dump_bool("deep", (scrub_level == scrub_level_t::deep));
  f->dump_bool("must", !is_periodic);
  f->dump_stream("stamp") << stamp;
  f->close_section();
}
}  // namespace

// when asked to force a "periodic" scrub by faking the timestamps
void PgScrubber::on_operator_periodic_cmd(
    ceph::Formatter* f,
    scrub_level_t scrub_level,
    int64_t offset)
{
  const auto cnf = populate_config_params();
  dout(10) << fmt::format(
		  "{}: {} (cmd offset:{}) conf:{}", __func__,
		  (scrub_level == scrub_level_t::deep ? "deep" : "shallow"), offset,
		  cnf)
	   << dendl;

  // move the relevant time-stamp backwards - enough to trigger a scrub
  utime_t stamp = ceph_clock_now();
  if (offset > 0) {
    stamp -= offset;
  } else {
    double max_iv =
	(scrub_level == scrub_level_t::deep)
	    ? 2 * cnf.max_deep
	    : (cnf.max_shallow ? *cnf.max_shallow : cnf.shallow_interval);
    dout(20) << fmt::format(
		    "{}: stamp:{:s} ms:{}/{}/{}", __func__, stamp,
		    (cnf.max_shallow ? "ms+" : "ms-"),
		    (cnf.max_shallow ? *cnf.max_shallow : -999.99),
		    cnf.shallow_interval)
	     << dendl;
    stamp -= max_iv;
  }
  stamp -= 100.0;  // for good measure

  dout(10) << fmt::format("{}: stamp:{:s} ", __func__, stamp) << dendl;
  asok_response_section(f, true, scrub_level, stamp);

  if (scrub_level == scrub_level_t::deep) {
    const auto saved_shallow_stamp = m_pg->info.history.last_scrub_stamp;
    // this call sets both stamps
    m_pg->set_last_deep_scrub_stamp(stamp);
    // restore the shallow stamp, as otherwise it will be scheduled before
    // the deep, failing whatever test code called us (this is a test-only
    // interface).
    m_pg->set_last_scrub_stamp(saved_shallow_stamp);
  } else {
    m_pg->set_last_scrub_stamp(stamp);
  }
}

// when asked to force a high-priority scrub
void PgScrubber::on_operator_forced_scrub(
    ceph::Formatter* f,
    scrub_level_t scrub_level)
{
  auto deep_req = scrub_requested(scrub_level, scrub_type_t::not_repair);
  asok_response_section(f, false, deep_req);
}


// ----------------------------------------------------------------------------

bool PgScrubber::has_pg_marked_new_updates() const
{
  auto last_applied = m_pg->recovery_state.get_last_update_applied();
  dout(10) << __func__ << " recovery last: " << last_applied
	   << " vs. scrub's: " << m_subset_last_update << dendl;

  return last_applied >= m_subset_last_update;
}

void PgScrubber::set_subset_last_update(eversion_t e)
{
  m_subset_last_update = e;
  dout(15) << __func__ << " last-update: " << e << dendl;
}

void PgScrubber::on_applied_when_primary(const eversion_t& applied_version)
{
  // we are only interested in updates if we are the Primary, and in state
  // WaitLastUpdate
  if (m_fsm->is_accepting_updates() &&
      (applied_version >= m_subset_last_update)) {
    m_osds->queue_scrub_applied_update(m_pg, m_pg->is_scrub_blocking_ops());
    dout(15) << __func__ << " update: " << applied_version
	     << " vs. required: " << m_subset_last_update << dendl;
  }
}


namespace {

/**
 * an aux function to be used in select_range() below, to
 * select the correct chunk size based on the type of scrub
 */
int64_t size_from_conf(
    bool is_deep,
    const ceph::common::ConfigProxy& conf,
    const md_config_cacher_t<int64_t>& deep_opt,
    const md_config_cacher_t<int64_t>& shallow_opt)
{
  if (!is_deep) {
    auto sz = *shallow_opt;
    if (sz != 0) {
      // assuming '0' means that no distinction was yet configured between
      // deep and shallow scrubbing
      return sz;
    }
  }
  return *deep_opt;
}
}  // anonymous namespace

PgScrubber::scrubber_callback_cancel_token_t
PgScrubber::schedule_callback_after(
  ceph::timespan duration, scrubber_callback_t &&cb)
{
  std::lock_guard l(m_osds->sleep_lock);
  return m_osds->sleep_timer.add_event_after(
    duration,
    new LambdaContext(
      [this, pg=PGRef(m_pg), cb=std::move(cb), epoch=get_osdmap_epoch()] {
	pg->lock();
	if (check_interval(epoch)) {
	  cb();
	}
	pg->unlock();
      }));
}

void PgScrubber::cancel_callback(scrubber_callback_cancel_token_t token)
{
  std::lock_guard l(m_osds->sleep_lock);
  m_osds->sleep_timer.cancel_event(token);
}

LogChannelRef& PgScrubber::get_clog() const
{
  return m_osds->clog;
}

int PgScrubber::get_whoami() const
{
  return m_osds->whoami;
}

[[nodiscard]] bool PgScrubber::is_reservation_required() const
{
  ceph_assert(m_active_target);
  return ScrubJob::requires_reservation(m_active_target->urgency());
}

/*
 * The selected range is set directly into 'm_start' and 'm_end'
 * setting:
 * - m_subset_last_update
 * - m_max_end
 * - end
 * - start
 * returns:
 * - std::nullopt if the range is blocked
 * - otherwise, the number of objects in the selected range
 */
std::optional<uint64_t> PgScrubber::select_range()
{
  m_be->new_chunk();

  /* get the start and end of our scrub chunk
   *
   * Our scrub chunk has an important restriction we're going to need to
   * respect. We can't let head be start or end.
   * Using a half-open interval means that if end == head,
   * we'd scrub/lock head and the clone right next to head in different
   * chunks which would allow us to miss clones created between
   * scrubbing that chunk and scrubbing the chunk including head.
   * This isn't true for any of the other clones since clones can
   * only be created "just to the left of" head.  There is one exception
   * to this: promotion of clones which always happens to the left of the
   * left-most clone, but promote_object checks the scrubber in that
   * case, so it should be ok.  Also, it's ok to "miss" clones at the
   * left end of the range if we are a tier because they may legitimately
   * not exist (see _scrub).
   */

  const auto& conf = m_pg->get_cct()->_conf;
  dout(20) << fmt::format(
		  "{} {} mins: {}d {}s, max: {}d {}s", __func__,
		  (m_is_deep ? "D" : "S"),
		  *osd_scrub_chunk_min,
		  *osd_shallow_scrub_chunk_min,
		  *osd_scrub_chunk_max,
		  *osd_shallow_scrub_chunk_max)
	   << dendl;

  const int min_from_conf = static_cast<int>(size_from_conf(
      m_is_deep, conf, osd_scrub_chunk_min, osd_shallow_scrub_chunk_min));
  const int max_from_conf = static_cast<int>(size_from_conf(
      m_is_deep, conf, osd_scrub_chunk_max, osd_shallow_scrub_chunk_max));

  const int divisor = static_cast<int>(preemption_data.chunk_divisor());
  const int min_chunk_sz = std::max(3, min_from_conf / divisor);
  const int max_chunk_sz = std::max(min_chunk_sz, max_from_conf / divisor);

  dout(10) << fmt::format(
		  "{}: Min: {} Max: {} Div: {}", __func__, min_chunk_sz,
		  max_chunk_sz, divisor)
	   << dendl;

  hobject_t start = m_start;
  hobject_t candidate_end;
  std::vector<hobject_t> objects;
  int ret = m_pg->get_pgbackend()->objects_list_partial(
      start, min_chunk_sz, max_chunk_sz, &objects, &candidate_end);
  ceph_assert(ret >= 0);

  if (!objects.empty()) {

    hobject_t back = objects.back();
    while (candidate_end.is_head() && candidate_end == back.get_head()) {
      candidate_end = back;
      objects.pop_back();
      if (objects.empty()) {
	ceph_assert(0 ==
		    "Somehow we got more than 2 objects which"
		    "have the same head but are not clones");
      }
      back = objects.back();
    }

    if (candidate_end.is_head()) {
      ceph_assert(candidate_end != back.get_head());
      candidate_end = candidate_end.get_object_boundary();
    }

  } else {
    ceph_assert(candidate_end.is_max());
  }

  // is that range free for us? if not - we will be rescheduled later by whoever
  // triggered us this time

  if (!m_pg->_range_available_for_scrub(m_start, candidate_end)) {
    // we'll be requeued by whatever made us unavailable for scrub
    dout(10) << __func__ << ": scrub blocked somewhere in range "
	     << "[" << m_start << ", " << candidate_end << ")" << dendl;
    return std::nullopt;
  }

  m_end = candidate_end;
  if (m_end > m_max_end)
    m_max_end = m_end;

  dout(15) << __func__ << " range selected: " << m_start << " //// " << m_end
	   << " //// " << m_max_end << dendl;

  // debug: be 'blocked' if told so by the 'pg scrub_debug block' asok command
  if (m_debug_blockrange > 0) {
    m_debug_blockrange--;
    return std::nullopt;
  }
  return objects.size();
}

void PgScrubber::select_range_n_notify()
{
  get_counters_set().inc(scrbcnt_chunks_selected);
  auto num_chunk_objects = select_range();
  if (num_chunk_objects.has_value()) {
    // the next chunk to handle is not blocked
    dout(20) << __func__ << ": selection OK" << dendl;
    auto cost = get_scrub_cost(num_chunk_objects.value());
    m_osds->queue_scrub_chunk_free(m_pg, Scrub::scrub_prio_t::low_priority, cost);
  } else {
    // we will wait for the objects range to become available for scrubbing
    dout(10) << __func__ << ": selected chunk is busy" << dendl;
    m_osds->queue_scrub_chunk_busy(m_pg, Scrub::scrub_prio_t::low_priority);
    get_counters_set().inc(scrbcnt_chunks_busy);
  }
}

uint64_t PgScrubber::get_scrub_cost(uint64_t num_chunk_objects)
{
  const auto& conf = m_pg->get_cct()->_conf;
  if (op_queue_type_t::WeightedPriorityQueue == m_osds->osd->osd_op_queue_type()) {
    // if the osd_op_queue is WPQ, we will use the default osd_scrub_cost value
    return conf->osd_scrub_cost;
  }
  uint64_t cost = 0;
  double scrub_metadata_cost = m_osds->get_cost_per_io();
  if (m_is_deep) {
    auto pg_avg_object_size = m_pg->get_average_object_size();
    cost = conf->osd_scrub_event_cost + (num_chunk_objects
    * (scrub_metadata_cost + pg_avg_object_size));
    dout(20) << fmt::format("{} : deep-scrub cost = {}", __func__, cost) << dendl;
    return cost;
  } else {
    cost = conf->osd_scrub_event_cost + (num_chunk_objects *  scrub_metadata_cost);
    dout(20) << fmt::format("{} : shallow-scrub cost = {}", __func__, cost) << dendl;
    return cost;
  }
}

bool PgScrubber::write_blocked_by_scrub(const hobject_t& soid)
{
  if (soid < m_start || soid >= m_end) {
    return false;
  }

  get_counters_set().inc(scrbcnt_write_blocked);
  dout(20) << __func__ << " " << soid << " can preempt? "
	   << preemption_data.is_preemptable() << " already preempted? "
	   << preemption_data.was_preempted() << dendl;

  if (preemption_data.was_preempted()) {
    // otherwise - write requests arriving while 'already preempted' is set
    // but 'preemptable' is not - will not be allowed to continue, and will
    // not be requeued on time.
    return false;
  }

  if (preemption_data.is_preemptable()) {

    dout(10) << __func__ << " " << soid << " preempted" << dendl;

    // signal the preemption
    preemption_data.do_preempt();
    m_end = m_start;  // free the range we were scrubbing

    return false;
  }
  return true;
}

bool PgScrubber::range_intersects_scrub(const hobject_t& start,
					const hobject_t& end)
{
  // does [start, end] intersect [scrubber.start, scrubber.m_max_end)
  return (start < m_max_end && end >= m_start);
}

eversion_t PgScrubber::search_log_for_updates() const
{
  auto& projected = m_pg->projected_log.log;
  auto pi = find_if(projected.crbegin(),
		    projected.crend(),
		    [this](const auto& e) -> bool {
		      return e.soid >= m_start && e.soid < m_end;
		    });

  if (pi != projected.crend())
    return pi->version;

  // there was no relevant update entry in the log

  auto& log = m_pg->recovery_state.get_pg_log().get_log().log;
  auto p = find_if(log.crbegin(), log.crend(), [this](const auto& e) -> bool {
    return e.soid >= m_start && e.soid < m_end;
  });

  if (p == log.crend())
    return eversion_t{};
  else
    return p->version;
}

void PgScrubber::get_replicas_maps(bool replica_can_preempt)
{
  dout(10) << __func__ << " started in epoch/interval: " << m_epoch_start << "/"
	   << m_interval_start << " pg same_interval_since: "
	   << m_pg->info.history.same_interval_since << dendl;

  m_primary_scrubmap_pos.reset();

  // ask replicas to scan and send maps
  for (const auto& i : m_pg->get_actingset()) {

    if (i == m_pg_whoami)
      continue;

    m_maps_status.mark_replica_map_request(i);
    _request_scrub_map(i,
		       m_subset_last_update,
		       m_start,
		       m_end,
		       m_is_deep,
		       replica_can_preempt);
  }

  dout(10) << __func__ << " awaiting" << m_maps_status << dendl;
}

bool PgScrubber::was_epoch_changed() const
{
  // for crimson we have m_pg->get_info().history.same_interval_since
  dout(10) << __func__ << " epoch_start: " << m_interval_start
	   << " from pg: " << m_pg->get_history().same_interval_since << dendl;

  return m_interval_start < m_pg->get_history().same_interval_since;
}

void PgScrubber::mark_local_map_ready()
{
  m_maps_status.mark_local_map_ready();
}

bool PgScrubber::are_all_maps_available() const
{
  return m_maps_status.are_all_maps_available();
}

std::string PgScrubber::dump_awaited_maps() const
{
  return m_maps_status.dump();
}

void PgScrubber::update_op_mode_text()
{
  auto visible_repair = state_test(PG_STATE_REPAIR);
  m_mode_desc =
    (visible_repair ? "repair" : (m_is_deep ? "deep-scrub" : "scrub"));

  dout(10) << __func__
	   << ": repair: visible: " << (visible_repair ? "true" : "false")
	   << ", internal: " << (m_is_repair ? "true" : "false")
	   << ". Displayed: " << m_mode_desc << dendl;
}

std::string_view PgScrubber::get_op_mode_text() const
{
  return m_mode_desc;
}

void PgScrubber::_request_scrub_map(pg_shard_t replica,
				    eversion_t version,
				    hobject_t start,
				    hobject_t end,
				    bool deep,
				    bool allow_preemption)
{
  ceph_assert(replica != m_pg_whoami);
  dout(10) << __func__ << " scrubmap from osd." << replica
	   << (deep ? " deep" : " shallow") << dendl;

  auto repscrubop = new MOSDRepScrub(spg_t(m_pg->info.pgid.pgid, replica.shard),
				     version,
				     get_osdmap_epoch(),
				     m_pg->get_last_peering_reset(),
				     start,
				     end,
				     deep,
				     allow_preemption,
				     m_flags.priority,
				     m_pg->ops_blocked_by_scrub());

  // default priority. We want the replica-scrub processed prior to any recovery
  // or client io messages (we are holding a lock!)
  m_osds->send_message_osd_cluster(replica.osd, repscrubop, get_osdmap_epoch());
}

// only called on interval change. Both DBs are to be removed.
void PgScrubber::cleanup_store(ObjectStore::Transaction* t)
{
  if (!m_store)
    return;

  struct OnComplete : Context {
    std::unique_ptr<Scrub::Store> store;
    explicit OnComplete(std::unique_ptr<Scrub::Store>&& store)
	: store(std::move(store))
    {}
    void finish(int) override {}
  };
  m_store->cleanup(t);
  t->register_on_complete(new OnComplete(std::move(m_store)));
  ceph_assert(!m_store);
}


void PgScrubber::reinit_scrub_store()
{
  // Entering, 0 to 3 of the following objects(*) may exist:
  // ((*)'objects' here: both code objects (the ScrubStore object) and
  // actual Object Store objects).
  // 1. The ScrubStore object itself.
  // 2,3. The two special hobjects in the coll (the PG data) holding the last
  //      scrub's results.
  //
  // The Store object can be deleted and recreated, as a way to guarantee
  // no junk is left. We won't do it here, but we will clear the at_level_t
  // structures.
  // The hobjects: possibly. The shallow DB object is always cleared. The
  // deep one - only if running a deep scrub.
  ObjectStore::Transaction t;
  if (m_store) {
    dout(10) << __func__ << " reusing existing store" << dendl;
    m_store->flush(&t);
  } else {
    dout(10) << __func__ << " creating new store" << dendl;
    m_store = std::make_unique<Scrub::Store>(
	*this, *m_pg->osd->store, &t, m_pg->info.pgid, m_pg->coll);
  }

  // regardless of whether the ScrubStore object was recreated or reused, we need to
  // (possibly) clear the actual DB objects in the Object Store.
  m_store->reinit(&t, m_active_target->level());
  m_pg->osd->store->queue_transaction(m_pg->ch, std::move(t), nullptr);
}


void PgScrubber::on_init()
{
  // going upwards from 'inactive'
  ceph_assert(!is_scrub_active());
  m_pg->reset_objects_scrubbed();
  preemption_data.reset();
  m_interval_start = m_pg->get_history().same_interval_since;
  dout(10) << __func__ << " start same_interval:" << m_interval_start << dendl;

  m_be = std::make_unique<ScrubBackend>(
    *this,
    *m_pg,
    m_pg_whoami,
    m_is_repair,
    m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow,
    m_pg->get_actingset());

  // create or reuse the 'known errors' store
  reinit_scrub_store();

  m_start = m_pg->info.pgid.pgid.get_hobj_start();
  m_active = true;
  ++m_sessions_counter;
  // publish the session counter and the fact the we are scrubbing.
  m_pg->publish_stats_to_osd();
}


void PgScrubber::on_replica_init()
{
  dout(10) << __func__ << " called with 'active' "
	   << (m_active ? "set" : "cleared") << dendl;
  if (!m_active) {
    m_be = std::make_unique<ScrubBackend>(
      *this, *m_pg, m_pg_whoami, m_is_repair,
      m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow);
    m_active = true;
    ++m_sessions_counter;
  }
}


int PgScrubber::build_primary_map_chunk()
{
  epoch_t map_building_since = m_pg->get_osdmap_epoch();
  dout(20) << __func__ << ": initiated at epoch " << map_building_since
	   << dendl;

  auto ret = build_scrub_map_chunk(m_be->get_primary_scrubmap(),
				   m_primary_scrubmap_pos,
				   m_start,
				   m_end,
				   m_is_deep);

  if (ret == -EINPROGRESS) {
    // reschedule another round of asking the backend to collect the scrub data
    m_osds->queue_for_scrub_resched(m_pg, Scrub::scrub_prio_t::low_priority);
  }
  return ret;
}


int PgScrubber::build_replica_map_chunk()
{
  dout(10) << __func__ << " interval start: " << m_interval_start
	   << " current token: " << m_current_token
	   << " epoch: " << m_epoch_start << " deep: " << m_is_deep << dendl;

  ceph_assert(m_be);

  auto ret = build_scrub_map_chunk(replica_scrubmap,
				   replica_scrubmap_pos,
				   m_start,
				   m_end,
				   m_is_deep);

  switch (ret) {

    case -EINPROGRESS:
      // must wait for the backend to finish. No external event source.
      // (note: previous version used low priority here. Now switched to using
      // the priority of the original message)
      m_osds->queue_for_rep_scrub_resched(m_pg,
					  m_replica_request_priority,
					  m_flags.priority,
					  m_current_token);
      break;

    case 0: {
      // finished!

      auto required_fixes = m_be->replica_clean_meta(replica_scrubmap,
						     m_end.is_max(),
						     m_start,
						     get_snap_mapper_accessor());
      // actuate snap-mapper changes:
      apply_snap_mapper_fixes(required_fixes);

      // the local map has been created. Send it to the primary.
      // Note: once the message reaches the Primary, it may ask us for another
      // chunk - and we better be done with the current scrub. The clearing of
      // state must be complete before we relinquish the PG lock.

      send_replica_map(prep_replica_map_msg(PreemptionNoted::no_preemption));
      dout(15) << fmt::format("{}: chunk map sent", __func__) << dendl;
    }
    break;

    default:
      // build_scrub_map_chunk() signalled an error
      // Pre-Pacific code ignored this option, treating it as a success.
      // Now: "regular" I/O errors were already handled by the backend (by
      // setting the error flag in the scrub-map). We are left with the
      // "unknown" error case - and we have no mechanism to handle it.
      // Thus - we must abort.
      dout(1) << "Error! Aborting. ActiveReplica::react(SchedReplica) Ret: "
	      << ret << dendl;
      ceph_abort_msg("backend error");
      break;
  };

  return ret;
}

int PgScrubber::build_scrub_map_chunk(ScrubMap& map,
				      ScrubMapBuilder& pos,
				      hobject_t start,
				      hobject_t end,
				      bool deep)
{
  dout(10) << __func__ << " [" << start << "," << end << ") "
	   << " pos " << pos << " Deep: " << deep << dendl;

  // start
  while (pos.empty()) {

    pos.deep = deep;
    map.valid_through = m_pg->info.last_update;

    // objects
    vector<ghobject_t> rollback_obs;
    pos.ret = m_pg->get_pgbackend()->objects_list_range(start,
							end,
							&pos.ls,
							&rollback_obs);
    dout(10) << __func__ << " while pos empty " << pos.ret << dendl;
    if (pos.ret < 0) {
      dout(5) << "objects_list_range error: " << pos.ret << dendl;
      return pos.ret;
    }
    dout(10) << __func__ << " pos.ls.empty()? " << (pos.ls.empty() ? "+" : "-")
	     << dendl;
    if (pos.ls.empty()) {
      break;
    }
    m_pg->_scan_rollback_obs(rollback_obs);
    pos.pos = 0;
    return -EINPROGRESS;
  }

  // scan objects
  while (!pos.done()) {

    int r = m_pg->get_pgbackend()->be_scan_list(map, pos);
    dout(30) << __func__ << " BE returned " << r << dendl;
    if (r == -EINPROGRESS) {
      dout(20) << __func__ << " in progress" << dendl;
      return r;
    }
  }

  // finish
  dout(20) << __func__ << " finishing" << dendl;
  ceph_assert(pos.done());
  repair_oinfo_oid(map);

  dout(20) << __func__ << " done, got " << map.objects.size() << " items"
	   << dendl;
  return 0;
}

/// \todo consider moving repair_oinfo_oid() back to the backend
void PgScrubber::repair_oinfo_oid(ScrubMap& smap)
{
  for (auto i = smap.objects.rbegin(); i != smap.objects.rend(); ++i) {

    const hobject_t& hoid = i->first;
    ScrubMap::object& o = i->second;

    if (o.attrs.find(OI_ATTR) == o.attrs.end()) {
      continue;
    }
    object_info_t oi;
    try {
      oi.decode(o.attrs[OI_ATTR]);
    } catch (...) {
      continue;
    }

    if (oi.soid != hoid) {
      ObjectStore::Transaction t;
      OSDriver::OSTransaction _t(m_pg->osdriver.get_transaction(&t));

      m_osds->clog->error()
        << "osd." << m_pg_whoami << " found object info error on pg " << m_pg_id
        << " oid " << hoid << " oid in object info: " << oi.soid
        << "...repaired";
      // Fix object info
      oi.soid = hoid;
      bufferlist bl;
      encode(oi,
             bl,
             m_pg->get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));

      o.attrs[OI_ATTR] = std::move(bl);

      t.setattr(m_pg->coll, ghobject_t(hoid), OI_ATTR, bl);
      int r = m_pg->osd->store->queue_transaction(m_pg->ch, std::move(t));
      if (r != 0) {
        derr << __func__ << ": queue_transaction got " << cpp_strerror(r)
             << dendl;
      }
    }
  }
}


void PgScrubber::run_callbacks()
{
  std::list<Context*> to_run;
  to_run.swap(m_callbacks);

  for (auto& tr : to_run) {
    tr->complete(0);
  }
}

void PgScrubber::persist_scrub_results(inconsistent_objs_t&& all_errors)
{
  dout(10) << __func__ << " " << all_errors.size() << " errors" << dendl;

  for (auto& e : all_errors) {
    std::visit([this](auto& e) { m_store->add_error(m_pg->pool.id, e); }, e);
  }

  ObjectStore::Transaction t;
  m_store->flush(&t);
  m_osds->store->queue_transaction(m_pg->ch, std::move(t), nullptr);
}

void PgScrubber::apply_snap_mapper_fixes(
  const std::vector<snap_mapper_fix_t>& fix_list)
{
  dout(15) << __func__ << " " << fix_list.size() << " fixes" << dendl;

  if (fix_list.empty()) {
    return;
  }

  ObjectStore::Transaction t;
  OSDriver::OSTransaction t_drv(m_pg->osdriver.get_transaction(&t));

  for (auto& [fix_op, hoid, snaps, bogus_snaps] : fix_list) {

    if (fix_op != snap_mapper_op_t::add) {

      // must remove the existing snap-set before inserting the correct one
      if (auto r = m_pg->snap_mapper.remove_oid(hoid, &t_drv); r < 0) {

	derr << __func__ << ": remove_oid returned " << cpp_strerror(r)
	     << dendl;
	if (fix_op == snap_mapper_op_t::update) {
	  // for inconsistent snapmapper objects (i.e. for
	  // snap_mapper_op_t::inconsistent), we don't fret if we can't remove
	  // the old entries
	  ceph_abort();
	}
      }

      m_osds->clog->error() << fmt::format(
	"osd.{} found snap mapper error on pg {} oid {} snaps in mapper: {}, "
	"oi: "
	"{} ...repaired",
	m_pg_whoami,
	m_pg_id,
	hoid,
	bogus_snaps,
	snaps);

    } else {

      m_osds->clog->error() << fmt::format(
	"osd.{} found snap mapper error on pg {} oid {} snaps missing in "
	"mapper, should be: {} ...repaired",
	m_pg_whoami,
	m_pg_id,
	hoid,
	snaps);
    }

    // now - insert the correct snap-set
    m_pg->snap_mapper.add_oid(hoid, snaps, &t_drv);
  }

  // wait for repair to apply to avoid confusing other bits of the system.
  {
    dout(15) << __func__ << " wait on repair!" << dendl;

    ceph::condition_variable my_cond;
    ceph::mutex my_lock = ceph::make_mutex("PG::_scan_snaps my_lock");
    int e = 0;
    bool done{false};

    t.register_on_applied_sync(new C_SafeCond(my_lock, my_cond, &done, &e));

    if (e = m_pg->osd->store->queue_transaction(m_pg->ch, std::move(t));
	e != 0) {
      derr << __func__ << ": queue_transaction got " << cpp_strerror(e)
	   << dendl;
    } else {
      std::unique_lock l{my_lock};
      my_cond.wait(l, [&done] { return done; });
      ceph_assert(m_pg->osd->store);  // RRR why?
    }
    dout(15) << __func__ << " wait on repair - done" << dendl;
  }
}

void PgScrubber::maps_compare_n_cleanup()
{
  m_pg->add_objects_scrubbed_count(std::ssize(m_be->get_primary_scrubmap().objects));

  auto required_fixes =
    m_be->scrub_compare_maps(m_end.is_max(), get_snap_mapper_accessor());
  if (!required_fixes.inconsistent_objs.empty()) {
    if (state_test(PG_STATE_REPAIR)) {
      dout(10) << __func__ << ": discarding scrub results (repairing)" << dendl;
    } else {
      // perform the ordered scrub-store I/O:
      persist_scrub_results(std::move(required_fixes.inconsistent_objs));
    }
  }

  // actuate snap-mapper changes:
  apply_snap_mapper_fixes(required_fixes.snap_fix_list);

  auto chunk_err_counts = m_be->get_error_counts();
  m_shallow_errors += chunk_err_counts.shallow_errors;
  m_deep_errors += chunk_err_counts.deep_errors;

  m_start = m_end;
  run_callbacks();

  // requeue the writes from the chunk that just finished
  requeue_waiting();
}

Scrub::preemption_t& PgScrubber::get_preemptor()
{
  return preemption_data;
}

/*
 * Process note: called for the arriving "give me your map, replica!" request.
 * Unlike the original implementation, we do not requeue the Op waiting for
 * updates. Instead - we trigger the FSM.
 */
void PgScrubber::replica_scrub_op(OpRequestRef op)
{
  op->mark_started();
  auto msg = op->get_req<MOSDRepScrub>();
  dout(10) << __func__ << " pg:" << m_pg->pg_id
	   << " Msg: map_epoch:" << msg->map_epoch
	   << " min_epoch:" << msg->min_epoch << " deep?" << msg->deep << dendl;

  if (should_drop_message(op)) {
    return;
  }

  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos = ScrubMapBuilder{};

  m_replica_min_epoch = msg->min_epoch;
  m_start = msg->start;
  m_end = msg->end;
  m_max_end = msg->end;
  m_is_deep = msg->deep;
  m_interval_start = m_pg->info.history.same_interval_since;
  m_replica_request_priority = msg->high_priority
				 ? Scrub::scrub_prio_t::high_priority
				 : Scrub::scrub_prio_t::low_priority;
  m_flags.priority = msg->priority ? msg->priority : m_pg->get_scrub_priority();

  preemption_data.reset();
  preemption_data.force_preemptability(msg->allow_preemption);

  replica_scrubmap_pos.reset();	 // needed? RRR

  set_queued_or_active();
  advance_token();
  const auto& conf = m_pg->get_cct()->_conf;
  const int max_from_conf = size_from_conf(
    m_is_deep, conf, osd_scrub_chunk_max, osd_shallow_scrub_chunk_max);
  auto cost = get_scrub_cost(max_from_conf);
  m_osds->queue_for_rep_scrub(m_pg,
			      m_replica_request_priority,
			      m_flags.priority,
			      m_current_token,
			      cost);
}

void PgScrubber::set_op_parameters(ScrubPGPreconds pg_cond)
{
  dout(10) << fmt::format("{}: target: {}", __func__, *m_active_target)
	   << dendl;

  set_queued_or_active();  // we are fully committed now.

  // write down the epoch of starting a new scrub. Will be used
  // to discard stale messages from previous aborted scrubs.
  m_epoch_start = m_pg->get_osdmap_epoch();

  // set the session attributes, as coded in m_flags, m_is_deep and m_is_repair

  m_flags.check_repair = m_active_target->urgency() == urgency_t::after_repair;
  m_is_deep = m_active_target->sched_info.level == scrub_level_t::deep;

  state_set(PG_STATE_SCRUBBING);
  if (m_is_deep) {
    state_set(PG_STATE_DEEP_SCRUB);
  }

  m_flags.auto_repair = m_is_deep && pg_cond.can_autorepair;

  // m_is_repair is set for all repair cases - for operator-requested
  // repairs, for deep-scrubs initiated automatically after a shallow scrub
  // that has ended with repairable error, and for 'repair-on-the-go' (i.e.
  // deep-scrub with the auto_repair configuration flag set). m_is_repair value
  // determines the scrubber behavior (especially the scrubber backend's).
  //
  // PG_STATE_REPAIR, on the other hand, is only used for status reports (inc.
  // the PG status as appearing in the logs), and would not be turned on for
  // 'on the go' - only after errors to be repair are found.
  m_is_repair = m_flags.auto_repair ||
		ScrubJob::is_repair_implied(m_active_target->urgency());
  ceph_assert(!m_is_repair || m_is_deep);  // repair implies deep-scrub
  if (ScrubJob::is_repair_implied(m_active_target->urgency())) {
    state_set(PG_STATE_REPAIR);
    update_op_mode_text();
  }


  // 'deep-on-error' is set for periodic shallow scrubs, if allowed
  // by the environment
  if (!m_is_deep && pg_cond.can_autorepair &&
      m_active_target->urgency() == urgency_t::periodic_regular) {
    m_flags.deep_scrub_on_error = true;
    dout(10) << fmt::format(
		    "{}: auto repair with scrubbing, rescrub if errors found",
		    __func__)
	     << dendl;
  } else {
    m_flags.deep_scrub_on_error = false;
  }

  m_flags.priority = m_pg->get_scrub_priority();

  // The publishing here is required for tests synchronization.
  // The PG state flags were modified.
  m_pg->publish_stats_to_osd();
}


ScrubMachineListener::MsgAndEpoch PgScrubber::prep_replica_map_msg(
  PreemptionNoted was_preempted)
{
  dout(10) << __func__ << " min epoch:" << m_replica_min_epoch << dendl;

  auto reply = make_message<MOSDRepScrubMap>(
    spg_t(m_pg->info.pgid.pgid, m_pg->get_primary().shard),
    m_replica_min_epoch,
    m_pg_whoami);

  reply->preempted = (was_preempted == PreemptionNoted::preempted);
  ::encode(replica_scrubmap, reply->get_data());

  return ScrubMachineListener::MsgAndEpoch{reply, m_replica_min_epoch};
}

void PgScrubber::send_replica_map(const MsgAndEpoch& preprepared)
{
  m_pg->send_cluster_message(m_pg->get_primary().osd,
			     preprepared.m_msg,
			     preprepared.m_epoch,
			     false);
}

void PgScrubber::send_preempted_replica()
{
  auto reply = make_message<MOSDRepScrubMap>(
    spg_t{m_pg->info.pgid.pgid, m_pg->get_primary().shard},
    m_replica_min_epoch,
    m_pg_whoami);

  reply->preempted = true;
  ::encode(replica_scrubmap,
	   reply->get_data());	// skipping this crashes the scrubber
  m_pg->send_cluster_message(m_pg->get_primary().osd,
			     reply,
			     m_replica_min_epoch,
			     false);
}

/*
 *  - if the replica lets us know it was interrupted, we mark the chunk as
 * interrupted. The state-machine will react to that when all replica maps are
 * received.
 *  - when all maps are received, we signal the FSM with the GotReplicas event
 * (see scrub_send_replmaps_ready()). Note that due to the no-reentrancy
 * limitations of the FSM, we do not 'process' the event directly. Instead - it
 * is queued for the OSD to handle.
 */
void PgScrubber::map_from_replica(OpRequestRef op)
{
  auto m = op->get_req<MOSDRepScrubMap>();
  dout(15) << __func__ << " " << *m << dendl;

  if (should_drop_message(op)) {
    return;
  }

  // note: we check for active() before map_from_replica() is called. Thus, we
  // know m_be is initialized
  m_be->decode_received_map(m->from, *m);

  auto [is_ok, err_txt] = m_maps_status.mark_arriving_map(m->from);
  if (!is_ok) {
    // previously an unexpected map was triggering an assert. Now, as scrubs can
    // be aborted at any time, the chances of this happening have increased, and
    // aborting is not justified
    dout(1) << __func__ << err_txt << " from OSD " << m->from << dendl;
    return;
  }

  if (m->preempted) {
    dout(10) << __func__ << " replica was preempted, setting flag" << dendl;
    preemption_data.do_preempt();
  }

  if (m_maps_status.are_all_maps_available()) {
    dout(15) << __func__ << " all repl-maps available" << dendl;
    m_osds->queue_scrub_got_repl_maps(m_pg, m_pg->is_scrub_blocking_ops());
  }
}

/**
 * route incoming replica-reservations requests/responses to the
 * appropriate handler.
 * As the ReplicaReservations object is to be owned by the ScrubMachine, we
 * send the relevant messages to the ScrubMachine.
 */
void PgScrubber::handle_scrub_reserve_msgs(OpRequestRef op)
{
  dout(10) << fmt::format("{}: {}", __func__, *op->get_req()) << dendl;
  op->mark_started();
  if (should_drop_message(op)) {
    return;
  }
  auto m = op->get_req<MOSDScrubReserve>();
  switch (m->type) {
    case MOSDScrubReserve::REQUEST:
      m_fsm->process_event(ReplicaReserveReq{op, m->from});
      break;
    case MOSDScrubReserve::GRANT:
      m_fsm->process_event(ReplicaGrant{op, m->from});
      break;
    case MOSDScrubReserve::REJECT:
      m_fsm->process_event(ReplicaReject{op, m->from});
      break;
    case MOSDScrubReserve::RELEASE:
      m_fsm->process_event(ReplicaRelease{op, m->from});
      break;
  }
}

void PgScrubber::set_queued_or_active()
{
  m_queued_or_active = true;
}

void PgScrubber::clear_queued_or_active()
{
  if (m_queued_or_active) {
    m_queued_or_active = false;
    // and just in case snap trimming was blocked by the aborted scrub
    m_pg->snap_trimmer_scrub_complete();
  }
}

bool PgScrubber::is_queued_or_active() const
{
  return m_queued_or_active;
}

void PgScrubber::set_scrub_blocked(utime_t since)
{
  ceph_assert(!m_scrub_job->blocked);
  // we are called from a time-triggered lambda,
  // thus - not under PG-lock
  PGRef pg = m_osds->osd->lookup_lock_pg(m_pg_id);
  ceph_assert(pg); // 'this' here should not exist if the PG was removed
  m_osds->get_scrub_services().mark_pg_scrub_blocked(m_pg_id);
  m_scrub_job->blocked_since = since;
  m_scrub_job->blocked = true;
  m_pg->publish_stats_to_osd();
  pg->unlock();
}

void PgScrubber::clear_scrub_blocked()
{
  ceph_assert(m_scrub_job->blocked);
  m_osds->get_scrub_services().clear_pg_scrub_blocked(m_pg_id);
  m_scrub_job->blocked = false;
  m_pg->publish_stats_to_osd();
}

void PgScrubber::flag_reservations_failure()
{
  dout(10) << __func__ << dendl;
  // delay the next invocation of the scrubber on this target. Note that
  // we use 'delay_both_targets_t::yes', as requeue_penalized() knows not to
  // penalize the sister target if its priority is higher.
  requeue_penalized(
      m_active_target->level(), delay_both_targets_t::yes,
      delay_cause_t::replicas, ceph_clock_now());
}

/*
 * note: only called for the Primary.
 */
void PgScrubber::scrub_finish()
{
  dout(10) << __func__ << " before flags: " << m_flags << ". repair state: "
	   << (state_test(PG_STATE_REPAIR) ? "repair" : "no-repair")
	   << ". deep_scrub_on_error: " << m_flags.deep_scrub_on_error << dendl;

  ceph_assert(m_pg->is_locked());
  ceph_assert(is_queued_or_active());

  // if the repair request comes from auto-repair and large number of errors,
  // we would like to cancel auto-repair
  if (m_is_repair && m_flags.auto_repair &&
      m_be->authoritative_peers_count() >
	static_cast<int>(m_pg->cct->_conf->osd_scrub_auto_repair_num_errors)) {

    dout(10) << __func__ << " undoing the repair" << dendl;
    state_clear(PG_STATE_REPAIR);  // not expected to be set, anyway
    m_is_repair = false;
    update_op_mode_text();
  }

  m_be->update_repair_status(m_is_repair);

  // if a regular scrub had errors within the limit, do a deep scrub to auto
  // repair
  bool do_auto_scrub = false;
  if (m_flags.deep_scrub_on_error && m_be->authoritative_peers_count() &&
      m_be->authoritative_peers_count() <=
	static_cast<int>(m_pg->cct->_conf->osd_scrub_auto_repair_num_errors)) {
    ceph_assert(!m_is_deep);
    do_auto_scrub = true;
    dout(15) << __func__ << " Try to auto repair after scrub errors" << dendl;
  }

  m_flags.deep_scrub_on_error = false;

  // type-specific finish (can tally more errors)
  _scrub_finish();

  /// \todo fix the relevant scrub test so that we would not need the extra log
  /// line here (even if the following 'if' is false)

  if (m_be->authoritative_peers_count()) {

    auto err_msg = fmt::format("{} {} {} missing, {} inconsistent objects",
			       m_pg->info.pgid,
			       m_mode_desc,
			       m_be->m_missing.size(),
			       m_be->m_inconsistent.size());

    dout(2) << err_msg << dendl;
    m_osds->clog->error() << fmt::to_string(err_msg);
  }

  // note that the PG_STATE_REPAIR might have changed above
  if (m_be->authoritative_peers_count() && m_is_repair) {

    state_clear(PG_STATE_CLEAN);
    // we know we have a problem, so it's OK to set the user-visible flag
    // even if we only reached here via auto-repair
    state_set(PG_STATE_REPAIR);
    update_op_mode_text();
    m_be->update_repair_status(true);
    m_fixed_count += m_be->scrub_process_inconsistent();
  }

  bool has_error = (m_be->authoritative_peers_count() > 0) && m_is_repair;

  {
    stringstream oss;
    oss << m_pg->info.pgid.pgid << " " << m_mode_desc << " ";
    int total_errors = m_shallow_errors + m_deep_errors;
    if (total_errors)
      oss << total_errors << " errors";
    else
      oss << "ok";
    if (!m_is_deep && m_pg->info.stats.stats.sum.num_deep_scrub_errors)
      oss << " ( " << m_pg->info.stats.stats.sum.num_deep_scrub_errors
	  << " remaining deep scrub error details lost)";
    if (m_is_repair)
      oss << ", " << m_fixed_count << " fixed";
    if (total_errors)
      m_osds->clog->error(oss);
    else
      m_osds->clog->debug(oss);
  }

  // Since we don't know which errors were fixed, we can only clear them
  // when every one has been fixed.
  if (m_is_repair) {
    dout(15) << fmt::format("{}: {} errors. {} errors fixed",
			    __func__,
			    m_shallow_errors + m_deep_errors,
			    m_fixed_count)
	     << dendl;
    if (m_fixed_count == m_shallow_errors + m_deep_errors) {

      ceph_assert(m_is_deep);
      m_shallow_errors = 0;
      m_deep_errors = 0;
      dout(20) << __func__ << " All may be fixed" << dendl;

    } else if (has_error) {

      // a recovery will be initiated (below). Arrange for a deep-scrub
      // after the recovery, to get the updated error counts.
      m_after_repair_scrub_required = true;
      dout(20) << fmt::format(
		      "{}: setting for deep-scrub-after-repair ({} errors. {} "
		      "errors fixed)",
		      __func__, m_shallow_errors + m_deep_errors, m_fixed_count)
	       << dendl;

    } else if (m_shallow_errors || m_deep_errors) {

      // We have errors but nothing can be fixed, so there is no repair
      // possible.
      state_set(PG_STATE_FAILED_REPAIR);
      dout(10) << __func__ << " " << (m_shallow_errors + m_deep_errors)
	       << " error(s) present with no repair possible" << dendl;
    }
  }

  {
    // finish up
    ObjectStore::Transaction t;
    m_pg->recovery_state.update_stats(
      [this](auto& history, auto& stats) {
	dout(10) << "m_pg->recovery_state.update_stats() errors:"
		 << m_shallow_errors << "/" << m_deep_errors << " deep? "
		 << m_is_deep << dendl;
	utime_t now = ceph_clock_now();
	history.last_scrub = m_pg->recovery_state.get_info().last_update;
	history.last_scrub_stamp = now;
	if (m_is_deep) {
	  history.last_deep_scrub = m_pg->recovery_state.get_info().last_update;
	  history.last_deep_scrub_stamp = now;
	}

	if (m_is_deep) {
	  if ((m_shallow_errors == 0) && (m_deep_errors == 0)) {
	    history.last_clean_scrub_stamp = now;
	  }
	  stats.stats.sum.num_shallow_scrub_errors = m_shallow_errors;
	  stats.stats.sum.num_deep_scrub_errors = m_deep_errors;
	  auto omap_stats = m_be->this_scrub_omapstats();
	  stats.stats.sum.num_large_omap_objects =
	    omap_stats.large_omap_objects;
	  stats.stats.sum.num_omap_bytes = omap_stats.omap_bytes;
	  stats.stats.sum.num_omap_keys = omap_stats.omap_keys;
	  dout(19) << "scrub_finish shard " << m_pg_whoami
		   << " num_omap_bytes = " << stats.stats.sum.num_omap_bytes
		   << " num_omap_keys = " << stats.stats.sum.num_omap_keys
		   << dendl;
	} else {
	  stats.stats.sum.num_shallow_scrub_errors = m_shallow_errors;
	  // XXX: last_clean_scrub_stamp doesn't mean the pg is not inconsistent
	  // because of deep-scrub errors
	  if (m_shallow_errors == 0) {
	    history.last_clean_scrub_stamp = now;
	  }
	}

	stats.stats.sum.num_scrub_errors =
	  stats.stats.sum.num_shallow_scrub_errors +
	  stats.stats.sum.num_deep_scrub_errors;

	if (m_flags.check_repair) {
	  m_flags.check_repair = false;
	  if (m_pg->info.stats.stats.sum.num_scrub_errors) {
	    state_set(PG_STATE_FAILED_REPAIR);
	    dout(10) << "scrub_finish "
		     << m_pg->info.stats.stats.sum.num_scrub_errors
		     << " error(s) still present after re-scrub" << dendl;
	  }
	}
	return true;
      },
      &t);
    int tr = m_osds->store->queue_transaction(m_pg->ch, std::move(t), nullptr);
    ceph_assert(tr == 0);
  }

  if (has_error) {
    m_pg->queue_peering_event(PGPeeringEventRef(
      std::make_shared<PGPeeringEvent>(get_osdmap_epoch(),
				       get_osdmap_epoch(),
				       PeeringState::DoRecovery())));
  } else {
    m_is_repair = false;
    state_clear(PG_STATE_REPAIR);
    update_op_mode_text();
  }

  cleanup_on_finish();
  if (do_auto_scrub) {
    request_rescrubbing();
  }
  // determine the next scrub time
  update_scrub_job(delay_ready_t::delay_ready);

  if (m_pg->is_active() && m_pg->is_primary()) {
    m_pg->recovery_state.share_pg_info();
  }
}


void PgScrubber::on_digest_updates()
{
  dout(10) << __func__ << " #pending: " << num_digest_updates_pending << " "
	   << (m_end.is_max() ? " <last chunk>" : " <mid chunk>")
	   << (is_queued_or_active() ? "" : " ** not marked as scrubbing **")
	   << dendl;

  if (num_digest_updates_pending > 0) {
    // do nothing for now. We will be called again when new updates arrive
    return;
  }

  // got all updates, and finished with this chunk. Any more?
  if (m_end.is_max()) {
    m_osds->queue_scrub_is_finished(m_pg);
  } else {
    // go get a new chunk (via "requeue")
    preemption_data.reset();
    m_osds->queue_scrub_next_chunk(m_pg, m_pg->is_scrub_blocking_ops());
  }
}


/**
 * The scrub session was aborted. We are left with two sets of parameters
 * as to when the next scrub of this PG should take place, and what should
 * it be like. One set of parameters is the one that was used to start the
 * scrub, and that was 'frozen' by set_op_parameters(). It has its own
 * scheduling target, priority, not-before, etc'.
 * The other set is the updated state of the current scrub-job. It may
 * have had its priority, flags, or schedule modified in the meantime.
 * And - it does not (at least initially, i.e. immediately after
 * set_op_parameters()), have high priority.
 */
void PgScrubber::on_mid_scrub_abort(Scrub::delay_cause_t issue)
{
  if (!m_scrub_job->is_registered()) {
    dout(10) << fmt::format(
		    "{}: PG not registered for scrubbing on this OSD. Won't "
		    "requeue!",
		    __func__)
	     << dendl;
    return;
  }

  dout(10) << fmt::format(
		  "{}: executing target: {}. Session flags: {} up-to-date job: "
		  "{}",
		  __func__, *m_active_target, m_flags, *m_scrub_job)
	   << dendl;

  // copy the aborted target
  const auto aborted_target = *m_active_target;
  m_active_target.reset();

  const auto scrub_clock_now = ceph_clock_now();
  auto& current_targ = m_scrub_job->get_target(aborted_target.level());
  ceph_assert(!current_targ.queued);

  // merge the aborted target with the current one
  auto& curr_sched = current_targ.sched_info.schedule;
  auto& abrt_sched = aborted_target.sched_info.schedule;

  current_targ.sched_info.urgency =
      std::max(current_targ.urgency(), aborted_target.urgency());
  curr_sched.scheduled_at =
      std::min(curr_sched.scheduled_at, abrt_sched.scheduled_at);
  curr_sched.deadline = std::min(curr_sched.deadline, abrt_sched.deadline);
  curr_sched.not_before =
      std::min(curr_sched.not_before, abrt_sched.not_before);

  dout(10) << fmt::format(
		  "{}: merged target (before delay): {}", __func__,
		  current_targ)
	   << dendl;

  // affect a delay, as there was a failure mid-scrub
  m_scrub_job->delay_on_failure(current_targ.level(), issue, scrub_clock_now);

  // reinstate both targets in the queue
  m_osds->get_scrub_services().enqueue_target(current_targ);
  current_targ.queued = true;

  // our sister target is off the queue, too:
  auto& sister = m_scrub_job->get_target(
      aborted_target.level() == scrub_level_t::deep ? scrub_level_t::shallow
						    : scrub_level_t::deep);
  if (!sister.queued) {
    m_osds->get_scrub_services().enqueue_target(sister);
    sister.queued = true;
  }
}


void PgScrubber::requeue_penalized(
    scrub_level_t s_or_d,
    delay_both_targets_t delay_both,
    Scrub::delay_cause_t cause,
    utime_t scrub_clock_now)
{
  if (!m_scrub_job->is_registered()) {
    dout(10) << fmt::format(
		    "{}: PG not registered for scrubbing on this OSD. Won't "
		    "requeue!",
		    __func__)
	     << dendl;
    return;
  }
  /// \todo fix the 5s' to use a cause-specific delay parameter
  auto& trgt = m_scrub_job->delay_on_failure(s_or_d, cause, scrub_clock_now);
  ceph_assert(!trgt.queued);
  m_osds->get_scrub_services().enqueue_target(trgt);
  trgt.queued = true;

  if (delay_both == delay_both_targets_t::yes) {
    const auto sister_level = (s_or_d == scrub_level_t::deep)
				  ? scrub_level_t::shallow
				  : scrub_level_t::deep;
    auto& trgt2 = m_scrub_job->get_target(sister_level);
    // do not delay if the other target has higher urgency
    if (trgt2.urgency() > trgt.urgency()) {
      dout(10) << fmt::format(
		      "{}: not delaying the other target (urgency: {})",
		      __func__, trgt2.urgency())
	       << dendl;
      return;
    }
    if (trgt2.queued) {
      m_osds->get_scrub_services().dequeue_target(m_pg_id, sister_level);
      trgt2.queued = false;
    }
    m_scrub_job->delay_on_failure(sister_level, cause, scrub_clock_now);
    m_osds->get_scrub_services().enqueue_target(trgt2);
    trgt2.queued = true;
  }
}


Scrub::schedule_result_t PgScrubber::start_scrub_session(
    scrub_level_t s_or_d,
    Scrub::OSDRestrictions osd_restrictions,
    Scrub::ScrubPGPreconds pg_cond)
{
  auto& trgt = m_scrub_job->get_target(s_or_d);
  dout(10) << fmt::format(
		  "{}: pg[{}] {} {} target: {}", __func__, m_pg_id,
		  (m_pg->is_active() ? "<active>" : "<not-active>"),
		  (m_pg->is_clean() ? "<clean>" : "<not-clean>"), trgt)
	   << dendl;
  // mark our target as not-in-queue. If any error is encountered - that
  // target must be requeued!
  trgt.queued = false;

  if (is_queued_or_active()) {
    dout(10) << __func__ << ": scrub already in progress" << dendl;
    // no need to requeue
    return schedule_result_t::target_specific_failure;
  }

  // a few checks. If failing - the 'not-before' is modified, and the target
  // is requeued.
  auto clock_now = ceph_clock_now();

  if (!is_primary() || !m_pg->is_active()) {
    // the PG is not expected to be 'registered' in this state. And we should
    // not attempt to queue it.
    dout(10) << __func__ << ": cannot scrub (not an active primary)"
	     << dendl;
    return schedule_result_t::target_specific_failure;
  }

  // for all other failures - we must reinstate our entry in the Scrub Queue.
  // For some of the failures, we will also delay the 'other' target.
  if (!m_pg->is_clean()) {
    dout(10) << fmt::format(
		    "{}: cannot scrub (not clean). Registered?{:c}", __func__,
		    m_scrub_job->is_registered() ? 'Y' : 'n')
	     << dendl;
    requeue_penalized(
	s_or_d, delay_both_targets_t::yes, delay_cause_t::pg_state, clock_now);
    return schedule_result_t::target_specific_failure;
  }

  if (state_test(PG_STATE_SNAPTRIM) || state_test(PG_STATE_SNAPTRIM_WAIT)) {
    // note that the trimmer checks scrub status when setting 'snaptrim_wait'
    // (on the transition from NotTrimming to Trimming/WaitReservation),
    // i.e. some time before setting 'snaptrim'.
    dout(10) << __func__ << ": cannot scrub while snap-trimming" << dendl;
    requeue_penalized(
	s_or_d, delay_both_targets_t::yes, delay_cause_t::snap_trimming,
	clock_now);
    return schedule_result_t::target_specific_failure;
  }

  // is there a 'no-scrub' flag set for the initiated scrub level? note:
  // won't affect operator-initiated (and some other types of) scrubs.
  if (ScrubJob::observes_noscrub_flags(trgt.urgency())) {
    if (trgt.is_shallow()) {
      if (!pg_cond.allow_shallow) {
	// can't scrub at all
	dout(10) << __func__ << ": shallow not allowed" << dendl;
	requeue_penalized(
	    s_or_d, delay_both_targets_t::no, delay_cause_t::flags, clock_now);
	return schedule_result_t::target_specific_failure;
      }
    } else if (!pg_cond.allow_deep) {
      // can't scrub at all
      dout(10) << __func__ << ": deep not allowed" << dendl;
      requeue_penalized(
	  s_or_d, delay_both_targets_t::no, delay_cause_t::flags, clock_now);
      return schedule_result_t::target_specific_failure;
    }
  }

  // restricting shallow scrubs of PGs that have deep errors:
  if (pg_cond.has_deep_errors && trgt.is_shallow()) {
    if (trgt.urgency() < urgency_t::operator_requested) {
      // if there are deep errors, we should have scheduled a deep scrub first.
      // If we are here trying to perform a shallow scrub, it means that for some
      // reason that deep scrub failed to be initiated. We will not try a shallow
      // scrub until this is solved.
      dout(10) << __func__ << ": Regular scrub skipped due to deep-scrub errors"
	       << dendl;
      requeue_penalized(
	  s_or_d, delay_both_targets_t::no, delay_cause_t::pg_state, clock_now);
      return schedule_result_t::target_specific_failure;
    } else {
      // we will honor the request anyway, but will report the issue
      m_osds->clog->error() << fmt::format(
	  "osd.{} pg {} Regular scrub request, deep-scrub details will be lost",
	  m_osds->whoami, m_pg_id);
    }
  }

  // if only explicitly requested repairing is allowed - skip other types
  // of scrubbing
  if (osd_restrictions.allow_requested_repair_only &&
      ScrubJob::observes_recovery(trgt.urgency())) {
    dout(10) << __func__
	     << ": skipping this PG as repairing was not explicitly "
		"requested for it"
	     << dendl;
    requeue_penalized(
	s_or_d, delay_both_targets_t::yes, delay_cause_t::scrub_params,
	clock_now);
    return schedule_result_t::target_specific_failure;
  }

  // try to reserve the local OSD resources. If failing: no harm. We will
  // be retried by the OSD later on.
  if (!reserve_local(trgt)) {
    dout(10) << __func__ << ": failed to reserve locally" << dendl;
    requeue_penalized(
	s_or_d, delay_both_targets_t::yes, delay_cause_t::local_resources,
	clock_now);
    return schedule_result_t::osd_wide_failure;
  }

  // can commit now to the specific scrub details, as nothing will
  // stop the scrub

  // An interrupted recovery repair could leave this set.
  state_clear(PG_STATE_REPAIR);

  m_active_target = trgt;
  set_op_parameters(pg_cond);
  // dequeue the PG's "other" target
  m_osds->get_scrub_services().remove_from_osd_queue(m_pg_id);
  m_scrub_job->clear_both_targets_queued();

  // clear all special handling urgency/flags from the target that is
  // executing now.
  trgt.reset();

  // using the OSD queue, as to not execute the scrub code as part of the tick.
  dout(10) << __func__ << ": queueing" << dendl;
  m_osds->queue_for_scrub(m_pg, Scrub::scrub_prio_t::low_priority);
  return schedule_result_t::scrub_initiated;
}


///\todo modify the fields dumped here to match the new scrub-job structure
void PgScrubber::dump_scrubber(
    ceph::Formatter* f) const
{
  f->open_object_section("scrubber");

  if (m_active_target) {
    f->dump_bool("active", true);
    dump_active_scrubber(f);
  } else {
    f->dump_bool("active", false);
    const auto now_is = ceph_clock_now();
    const auto& earliest = m_scrub_job->earliest_target(now_is);
    f->dump_bool("must_scrub", earliest.is_high_priority());
    f->dump_bool(
	"must_deep_scrub", m_scrub_job->deep_target.is_high_priority());
    // the following data item is deprecated. Will be replaced by a set
    // of reported attributes that match the updated scrub-job state.
    f->dump_bool("must_repair", earliest.urgency() == urgency_t::must_repair);

    f->dump_stream("scrub_reg_stamp")
	<< earliest.sched_info.schedule.not_before;
    auto sched_state = m_scrub_job->scheduling_state(now_is);
    f->dump_string("schedule", sched_state);
  }

  if (m_publish_sessions) {
    // this is a test-only feature. It is not expected to be used in production.
    // The 'test_sequence' is an ever-increasing number used by tests.
    f->dump_int("test_sequence", m_sessions_counter);
  }

  f->close_section();
}


void PgScrubber::dump_active_scrubber(ceph::Formatter* f) const
{
  f->dump_stream("epoch_start") << m_interval_start;
  f->dump_stream("start") << m_start;
  f->dump_stream("end") << m_end;
  f->dump_stream("max_end") << m_max_end;
  f->dump_stream("subset_last_update") << m_subset_last_update;
  f->dump_bool("deep", m_active_target->is_deep());

  // dump the scrub-type flags
  f->dump_bool("req_scrub", m_active_target->is_high_priority());
  f->dump_bool("auto_repair", m_flags.auto_repair);
  f->dump_bool("check_repair", m_flags.check_repair);
  f->dump_bool("deep_scrub_on_error", m_flags.deep_scrub_on_error);
  f->dump_unsigned("priority", m_flags.priority);

  f->dump_int("shallow_errors", m_shallow_errors);
  f->dump_int("deep_errors", m_deep_errors);
  f->dump_int("fixed", m_fixed_count);
  {
    f->open_array_section("waiting_on_whom");
    for (const auto& p : m_maps_status.get_awaited()) {
      f->dump_stream("shard") << p;
    }
    f->close_section();
  }
  if (m_scrub_job->blocked) {
    f->dump_string("schedule", "blocked");
  } else {
    f->dump_string("schedule", "scrubbing");
  }
}

pg_scrubbing_status_t PgScrubber::get_schedule() const
{
  if (!m_scrub_job) {
    return pg_scrubbing_status_t{};
  }

  dout(25) << fmt::format(
		  "{}: active:{} blocked:{}", __func__, m_active,
		  m_scrub_job->blocked)
	   << dendl;

  auto now_is = ceph_clock_now();

  if (m_active) {
    // report current scrub info, including updated duration

    if (m_scrub_job->blocked) {
      // a bug. An object is held locked.
      int32_t blocked_for =
	  (utime_t{now_is} - m_scrub_job->blocked_since).sec();
      return pg_scrubbing_status_t{
	  utime_t{},
	  blocked_for,
	  pg_scrub_sched_status_t::blocked,
	  true,	 // active
	  (m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow),
	  false};

    } else {
      int32_t dur_seconds =
	  duration_cast<seconds>(m_fsm->get_time_scrubbing()).count();
      return pg_scrubbing_status_t{
	  utime_t{},
	  dur_seconds,
	  pg_scrub_sched_status_t::active,
	  true,	 // active
	  (m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow),
	  false /* is periodic? unknown, actually */};
    }
  }
  if (!m_scrub_job->is_registered()) {
    return pg_scrubbing_status_t{
	utime_t{},
	0,
	pg_scrub_sched_status_t::not_queued,
	false,
	scrub_level_t::shallow,
	false};
  }

  // not taking 'no-*scrub' flags into account here.
  const auto first_ready = m_scrub_job->earliest_eligible(now_is);
  if (first_ready) {
    const auto& targ = first_ready->get();
    return pg_scrubbing_status_t{
	targ.sched_info.schedule.not_before,
	0,
	pg_scrub_sched_status_t::queued,
	false,
	(targ.is_deep() ? scrub_level_t::deep : scrub_level_t::shallow),
	!targ.is_high_priority()};
  }

  // both targets are not ready yet
  const auto targ = m_scrub_job->earliest_target();
  return pg_scrubbing_status_t{
      targ.sched_info.schedule.not_before,
      0,
      pg_scrub_sched_status_t::scheduled,
      false,
      (targ.is_deep() ? scrub_level_t::deep : scrub_level_t::shallow),
      !targ.is_high_priority()};
}


void PgScrubber::handle_query_state(ceph::Formatter* f)
{
  dout(15) << __func__ << dendl;

  f->open_object_section("scrub");
  f->dump_stream("scrubber.epoch_start") << m_interval_start;
  f->dump_bool("scrubber.active", m_active);
  f->dump_stream("scrubber.start") << m_start;
  f->dump_stream("scrubber.end") << m_end;
  f->dump_stream("scrubber.max_end") << m_max_end;
  f->dump_stream("scrubber.subset_last_update") << m_subset_last_update;
  f->dump_bool("scrubber.deep", m_is_deep);
  {
    f->open_array_section("scrubber.waiting_on_whom");
    for (const auto& p : m_maps_status.get_awaited()) {
      f->dump_stream("shard") << p;
    }
    f->close_section();
  }

  f->dump_string("comment", "DEPRECATED - may be removed in the next release");

  f->close_section();
}

PgScrubber::~PgScrubber()
{
  m_fsm->process_event(IntervalChanged{});
  m_scrub_job.reset();
}

PgScrubber::PgScrubber(PG* pg)
    : m_pg{pg}
    , m_pg_id{pg->pg_id}
    , m_osds{m_pg->osd}
    , m_pg_whoami{pg->pg_whoami}
    , osd_scrub_chunk_max{m_osds->cct->_conf, "osd_scrub_chunk_max"}
    , osd_shallow_scrub_chunk_max{m_osds->cct->_conf,
				  "osd_shallow_scrub_chunk_max"}
    , osd_scrub_chunk_min{m_osds->cct->_conf, "osd_scrub_chunk_min"}
    , osd_shallow_scrub_chunk_min{m_osds->cct->_conf,
				  "osd_shallow_scrub_chunk_min"}
    , osd_stats_update_period_scrubbing{
	m_osds->cct->_conf, "osd_stats_update_period_scrubbing"}
    , osd_stats_update_period_not_scrubbing{
	m_osds->cct->_conf, "osd_stats_update_period_not_scrubbing"}
    , preemption_data{pg}
{
  m_fsm = std::make_unique<ScrubMachine>(m_pg, this);
  m_fsm->initiate();
  m_scrub_job.emplace(m_osds->cct, m_pg->pg_id, m_osds->get_nodeid());
}

void PgScrubber::set_scrub_duration(std::chrono::milliseconds duration)
{
  dout(20) << fmt::format("{}: to {}", __func__, duration) << dendl;
  double dur_ms = double(duration.count());
  m_pg->recovery_state.update_stats([=](auto& history, auto& stats) {
    stats.last_scrub_duration = ceill(dur_ms / 1000.0);
    stats.scrub_duration = dur_ms;
    return true;
  });
}

PerfCounters& PgScrubber::get_counters_set() const
{
  return *m_osds->get_scrub_services().get_perf_counters(
      (m_pg->pool.info.is_replicated() ? pg_pool_t::TYPE_REPLICATED
				       : pg_pool_t::TYPE_ERASURE),
      (m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow));
}

void PgScrubber::cleanup_on_finish()
{
  dout(10) << __func__ << dendl;
  clear_pgscrub_state();

  // PG state flags changed:
  m_pg->publish_stats_to_osd();
}

/*
 * note: does not access the state-machine
 */
void PgScrubber::clear_pgscrub_state()
{
  dout(10) << __func__ << dendl;
  ceph_assert(m_pg->is_locked());

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);

  m_local_osd_resource.reset();
  requeue_waiting();

  reset_internal_state();
  m_flags = scrub_flags_t{};

  // type-specific state clear
  _scrub_clear_state();
}

void PgScrubber::replica_handling_done()
{
  dout(10) << __func__ << dendl;

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);

  reset_internal_state();
}

std::chrono::milliseconds PgScrubber::get_scrub_sleep_time() const
{
  return m_osds->get_scrub_services().scrub_sleep_time(
      ceph_clock_now(),
      !ScrubJob::observes_allowed_hours(m_active_target->urgency()));
}

void PgScrubber::queue_for_scrub_resched(Scrub::scrub_prio_t prio)
{
  m_osds->queue_for_scrub_resched(m_pg, prio);
}

/*
 * note: performs run_callbacks()
 * note: reservations-related variables are not reset here
 */
void PgScrubber::reset_internal_state()
{
  dout(10) << __func__ << dendl;

  preemption_data.reset();
  m_maps_status.reset();

  m_start = hobject_t{};
  m_end = hobject_t{};
  m_max_end = hobject_t{};
  m_subset_last_update = eversion_t{};
  m_shallow_errors = 0;
  m_deep_errors = 0;
  m_fixed_count = 0;

  run_callbacks();

  num_digest_updates_pending = 0;
  m_primary_scrubmap_pos.reset();
  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos.reset();

  m_active = false;
  clear_queued_or_active();
  ++m_sessions_counter;
  m_be.reset();
}

bool PgScrubber::is_token_current(Scrub::act_token_t received_token)
{
  if (received_token == 0 || received_token == m_current_token) {
    return true;
  }
  dout(5) << __func__ << " obsolete token (" << received_token << " vs current "
	  << m_current_token << dendl;

  return false;
}

const OSDMapRef& PgScrubber::get_osdmap() const
{
  return m_pg->get_osdmap();
}

LoggerSinkSet& PgScrubber::get_logger() const { return *m_osds->clog.get(); }

ostream& operator<<(ostream& out, const PgScrubber& scrubber)
{
  return out << scrubber.m_flags;
}

std::ostream& PgScrubber::gen_prefix(std::ostream& out) const
{
  if (m_pg) {
    return m_pg->gen_prefix(out) << "scrubber<" << m_fsm_state_name << ">: ";
  } else {
    return out << " scrubber [" << m_pg_id << "]: ";
  }
}

void PgScrubber::log_cluster_warning(const std::string& warning) const
{
  m_osds->clog->do_log(CLOG_WARN, warning);
}


ostream& PgScrubber::show_concise(ostream& out) const
{
  /*
  * 'show_concise()' is only used when calling operator<< thru the ScrubPgIF,
  * i.e. only by the PG when creating a standard log entry.
  *
  * desired outcome (only relevant for Primaries):
  *
  * if scrubbing:
  *   (urgency,flags)
  *   or (if blocked)
  *   (*blocked*,urgency,flags)
  *
  * if not scrubbing:
  *   either nothing (if only periodic scrubs are scheduled)
  *   or [next-scrub: effective-lvl, urgency]
  */
  if (!is_primary()) {
    return out;
  }

  if (m_active) {
    const auto flags_txt = fmt::format("{}", m_flags);
    const std::string sep = (flags_txt.empty() ? "" : ",");
    if (m_active_target) {
      return out << fmt::format(
		 "({}{}{}{})", (m_scrub_job->blocked ? "*blocked*," : ""),
		 m_active_target->urgency(), sep, flags_txt);
    } else {
      // only expected in a couple of messages during scrub termination
      return out << fmt::format(
		 "(teardown{}{}{})", (m_scrub_job->blocked ? "-*blocked*" : ""),
		 sep, flags_txt);
    }
  }

  // not actively scrubbing now. Show some info about the next scrub
  const auto now_is = ceph_clock_now();
  const auto& next_scrub = m_scrub_job->earliest_target(now_is);
  if (!next_scrub.is_high_priority()) {
    // no interesting flags to report
    return out;
  }
  return out << fmt::format(
	     "[next-scrub:{},{:10.10}]", (next_scrub.is_deep() ? "dp" : "sh"),
	     next_scrub.urgency());
}

int PgScrubber::asok_debug(std::string_view cmd,
			   std::string param,
			   Formatter* f,
			   stringstream& ss)
{
  dout(10) << __func__ << " cmd: " << cmd << " param: " << param << dendl;

  if (cmd == "block") {
    // 'm_debug_blockrange' causes the next 'select_range' to report a blocked
    // object
    m_debug_blockrange = 10;  // >1, so that will trigger fast state reports

  } else if (cmd == "unblock") {
    // send an 'unblock' event, as if a blocked range was freed
    m_debug_blockrange = 0;
    m_fsm->process_event(Unblocked{});

  } else if ((cmd == "set") || (cmd == "unset")) {

    if (param == "sessions") {
      // set/reset the inclusion of the scrub sessions counter in 'query' output
      m_publish_sessions = (cmd == "set");

    } else if (param == "block") {
      if (cmd == "set") {
	// set a flag that will cause the next 'select_range' to report a
	// blocked object
	m_debug_blockrange = 10;  // >1, so that will trigger fast state reports
      } else {
	// send an 'unblock' event, as if a blocked range was freed
	m_debug_blockrange = 0;
	m_fsm->process_event(Unblocked{});
      }
    }
  }

  return 0;
}

/*
 * Note: under PG lock
 */
void PgScrubber::update_scrub_stats(ceph::coarse_real_clock::time_point now_is)
{
  using clock = ceph::coarse_real_clock;
  using namespace std::chrono;

  const seconds period_active = seconds(*osd_stats_update_period_scrubbing);
  if (!period_active.count()) {
    // a way for the operator to disable these stats updates
    return;
  }
  const seconds period_inactive = seconds(
      *osd_stats_update_period_not_scrubbing +
      m_pg_id.pgid.m_seed % 30);

  // determine the required update period, based on our current state
  auto period{period_inactive};
  if (m_active) {
    period = m_debug_blockrange ? 2s : period_active;
  }

  /// \todo use the date library (either the one included in Arrow or directly)
  /// to get the formatting of the time_points.

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 25>()) {
    // will only create the debug strings if required
    char buf[50];
    auto printable_last = fmt::localtime(clock::to_time_t(m_last_stat_upd));
    strftime(buf, sizeof(buf), "%Y-%m-%dT%T", &printable_last);
    dout(20) << fmt::format("{}: period: {}/{}-> {} last:{}",
			    __func__,
			    period_active,
			    period_inactive,
			    period,
			    buf)
	     << dendl;
  }

  if (now_is - m_last_stat_upd > period) {
    m_pg->publish_stats_to_osd();
    m_last_stat_upd = now_is;
  }
}


// ///////////////////// preemption_data_t //////////////////////////////////

PgScrubber::preemption_data_t::preemption_data_t(PG* pg) : m_pg{pg},
  osd_scrub_max_preemptions{pg->cct->_conf, "osd_scrub_max_preemptions"}
{
  m_left = *osd_scrub_max_preemptions;
}

void PgScrubber::preemption_data_t::reset()
{
  std::lock_guard<ceph::mutex> lk{m_preemption_lock};

  m_preemptable = false;
  m_preempted = false;
  m_left = *osd_scrub_max_preemptions;
  m_size_divisor = 1;
}

namespace Scrub {

// ///////////////////// MapsCollectionStatus ////////////////////////////////

auto MapsCollectionStatus::mark_arriving_map(pg_shard_t from)
  -> std::tuple<bool, std::string_view>
{
  auto fe =
    std::find(m_maps_awaited_for.begin(), m_maps_awaited_for.end(), from);
  if (fe != m_maps_awaited_for.end()) {
    // we are indeed waiting for a map from this replica
    m_maps_awaited_for.erase(fe);
    return std::tuple{true, ""sv};
  } else {
    return std::tuple{false, " unsolicited scrub-map"sv};
  }
}

void MapsCollectionStatus::reset()
{
  *this = MapsCollectionStatus{};
}

std::string MapsCollectionStatus::dump() const
{
  std::string all;
  for (const auto& rp : m_maps_awaited_for) {
    all.append(rp.get_osd() + " "s);
  }
  return all;
}

ostream& operator<<(ostream& out, const MapsCollectionStatus& sf)
{
  out << " [ ";
  for (const auto& rp : sf.m_maps_awaited_for) {
    out << rp.get_osd() << " ";
  }
  if (!sf.m_local_map_ready) {
    out << " local ";
  }
  return out << " ] ";
}

}  // namespace Scrub
