// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 smarttab

#include "./pg_scrubber.h"  // '.' notation used to affect clang-format order

#include <cmath>
#include <cstddef>
#include <iostream>
#include <vector>

#include <fmt/ranges.h>

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
#include "osd/scrubber_common.h"

#include "ScrubStore.h"
#include "scrub_backend.h"
#include "scrub_machine.h"
#include "scrub_queue.h"

using std::list;
using std::pair;
using std::stringstream;
using std::vector;
using namespace Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;

using schedule_result_t = Scrub::schedule_result_t;
using ScrubPGPreconds = Scrub::ScrubPGPreconds;
using OSDRestrictions = Scrub::OSDRestrictions;
using loop_token_t = Scrub::loop_token_t;

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
  if (sf.required)
    out << " REQ_SCRUB";

  return out;
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

  dout(10) << fmt::format(
		  "{}: aborting. Incoming epoch: {} vs. last-aborted: {}",
		  __func__, epoch_to_verify, m_last_aborted)
	   << dendl;

  // if we were not aware of the abort before - kill the scrub.
  if (epoch_to_verify >= m_last_aborted) {
    at_scrub_failure(delay_cause_t::aborted);
    scrub_clear_state();
    m_last_aborted = std::max(epoch_to_verify, m_epoch_start);
    assert_targets_invariant();
  }
  return false;
}

bool PgScrubber::should_abort() const
{
  ceph_assert(m_flags.required == !m_active_target->is_periodic());
  if (m_active_target->is_required()) {
    return false;  // not stopping 'required' scrubs for configuration changes
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
 * For those ('StartScrub', 'AfterRepairScrub') scrub-initiation messages that
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
 * ('StartReplica' & 'StartReplicaNoWait').
 */

void PgScrubber::initiate_regular_scrub(epoch_t epoch_queued)
{
  dout(15) << __func__ << " epoch: " << epoch_queued << dendl;
  // we may have lost our Primary status while the message languished in the
  // queue
  if (check_interval(epoch_queued)) {
    dout(10) << "scrubber event -->> StartScrub epoch: " << epoch_queued
	     << dendl;
    reset_epoch(epoch_queued);
    m_fsm->process_event(StartScrub{});
    dout(10) << "scrubber event --<< StartScrub" << dendl;
  } else {
    clear_queued_or_active();  // also restarts snap trimming
  }
}

void PgScrubber::initiate_scrub_after_repair(epoch_t epoch_queued)
{
  dout(15) << __func__ << " epoch: " << epoch_queued << dendl;
  // we may have lost our Primary status while the message languished in the
  // queue
  if (check_interval(epoch_queued)) {
    dout(10) << "scrubber event -->> AfterRepairScrub epoch: " << epoch_queued
	     << dendl;
    reset_epoch(epoch_queued);
    m_fsm->process_event(AfterRepairScrub{});
    dout(10) << "scrubber event --<< AfterRepairScrub" << dendl;
  } else {
    clear_queued_or_active();  // also restarts snap trimming
  }
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
    // save us some time by not waiting for updates if there are none
    // to wait for. Affects the transition from NotActive into either
    // ReplicaWaitUpdates or ActiveReplica.
    if (pending_active_pushes())
      m_fsm->process_event(StartReplica{});
    else
      m_fsm->process_event(StartReplicaNoWait{});
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

void PgScrubber::send_remotes_reserved(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  // note: scrub is not active yet
  if (check_interval(epoch_queued)) {
    m_fsm->process_event(RemotesReserved{});
  }
  dout(10) << "scrubber event --<< " << __func__ << dendl;
}

void PgScrubber::send_reservation_failure(epoch_t epoch_queued)
{
  dout(10) << "scrubber event -->> " << __func__ << " epoch: " << epoch_queued
	   << dendl;
  if (check_interval(epoch_queued)) {  // do not check for 'active'!
    m_fsm->process_event(ReservationFailure{});
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

// -----------------

bool PgScrubber::is_reserving() const
{
  return m_fsm->is_reserving();
}

void PgScrubber::reset_epoch(epoch_t epoch_queued)
{
  dout(10) << __func__ << " state deep? " << state_test(PG_STATE_DEEP_SCRUB)
	   << dendl;
  m_fsm->assert_not_active();

  m_epoch_start = epoch_queued;
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
 */
void PgScrubber::on_new_interval()
{
  dout(10) << fmt::format(
		  "{}: current role:{} active?{} q/a:{}", __func__,
		  (is_primary() ? "Primary" : "Replica/other"),
		  is_scrub_active(), is_queued_or_active())
	   << dendl;
  m_fsm->process_event(NewIntervalReset{});
  ceph_assert(!m_active_target);
  rm_from_osd_scrubbing();
}

bool PgScrubber::is_scrub_registered() const
{
  return m_scrub_job && m_scrub_job->in_queue();
}

std::string_view PgScrubber::registration_state() const
{
  ceph_assert(m_scrub_job);
  return m_scrub_job->registration_state();
}

void PgScrubber::rm_from_osd_scrubbing()
{
  if (m_scrub_job) {
    m_scrub_job->remove_from_osd_queue();
  }
}

void PgScrubber::on_pg_activate()
{
  ceph_assert(is_primary());
  ceph_assert(m_scrub_job);
  ceph_assert(!is_queued_or_active());

  auto pre_reg = m_scrub_job->registration_state();
  auto applicable_conf =
      m_scrub_queue.populate_config_params(m_pg->get_pgpool().info.opts);
  m_scrub_job->init_and_queue_targets(
      m_pg->info, applicable_conf, m_scrub_queue.scrub_clock_now());

  dout(10) << fmt::format(
		  "{}: Primary. <{:.5}> --> <{:.5}> ({})", __func__, pre_reg,
		  m_scrub_job->registration_state(), *m_scrub_job)
	   << dendl;
}


scrub_level_t PgScrubber::scrub_requested(
    scrub_level_t scrub_level,
    scrub_type_t scrub_type)
{
  auto now_is = m_scrub_queue.scrub_clock_now();
  const bool deep_requested = (scrub_level == scrub_level_t::deep) ||
			      (scrub_type == scrub_type_t::do_repair);
  dout(5) << fmt::format(
		 "{}: {} {} scrub requested. Prev stamp: {}. Registered? {}",
		 __func__,
		 (scrub_type == scrub_type_t::do_repair ? " repair-scrub "
							: " not-repair "),
		 (deep_requested ? "Deep" : "Shallow"),
		 m_scrub_job->get_sched_time(now_is), registration_state())
	  << dendl;

  auto deduced_level =
      deep_requested ? scrub_level_t::deep : scrub_level_t::shallow;
  m_scrub_job->operator_forced_targets(deduced_level, scrub_type, now_is);
  return deduced_level;
}

void PgScrubber::recovery_completed()
{
  dout(15) << fmt::format("{}: is scrub required? {}", __func__,
                          m_after_repair_scrub_required)
           << dendl;
  if (m_after_repair_scrub_required) {
    m_after_repair_scrub_required = false;
    m_scrub_job->mark_for_after_repair();
  }
}


void PgScrubber::recalc_schedule()
{
  ceph_assert(m_pg->is_locked());

  // Note: if we are in the middle of scrub initiation - we must wait for
  // that to fail - and then redo the rescheduling (if succeeding in
  // starting a scrub - there will be a rescheduling anyway when that
  // scrub completes).
  // Otherwise - we might end up with multiple targets for the same
  // PG (more than the two).

  auto applicable_conf = m_osds->get_scrub_services().populate_config_params(
      m_pg->get_pgpool().info.opts);

  m_scrub_job->on_periods_change(
      m_pg->info, applicable_conf, m_scrub_queue.scrub_clock_now());
}


bool PgScrubber::reserve_local()
{
  // try to create the reservation object (which translates into asking the
  // OSD for the local scrub resource). If failing - undo it immediately

  m_local_osd_resource.emplace(m_osds);
  if (m_local_osd_resource->is_reserved()) {
    dout(15) << __func__ << ": local resources reserved" << dendl;
    return true;
  }

  dout(10) << __func__ << ": failed to reserve local scrub resources" << dendl;
  m_local_osd_resource.reset();
  return false;
}


void PgScrubber::start_scrubbing(
    scrub_level_t lvl,
    loop_token_t loop_id,
    OSDRestrictions env_restrictions,
    ScrubPGPreconds pg_cond)
{
  auto& trgt = m_scrub_job->get_target(lvl);
  dout(10) << fmt::format(
		  "{}: pg[{}] {} {} target: {}", __func__, m_pg_id,
		  (m_pg->is_active() ? "<active>" : "<not-active>"),
		  (m_pg->is_clean() ? "<clean>" : "<not-clean>"), trgt)
	   << dendl;

  // mark our target as not-in-queue. If any error is encountered - that
  // target must be requeued!
  trgt.clear_queued();

  ceph_assert(!is_queued_or_active());
  ceph_assert(!trgt.is_off());

  m_schedloop_step.emplace(m_scrub_queue, loop_id);
  auto clock_now = m_scrub_queue.scrub_clock_now();

  // a few checks. If failing - the 'not-before' is modified, and the target
  // is requeued.
  // We then either instruct the ScrubQueue to try the next ready PG, or -
  // for some failure modes - to stop attempting to scrub any more PGs in the
  // OSD tick.

  auto res_code = schedule_result_t::ok_thus_far;

  if (env_restrictions.only_deadlined && trgt.is_periodic() &&
      !trgt.over_deadline(clock_now)) {
    dout(15) << fmt::format(
		    "not scheduling scrub for pg[{}] due to {}", m_pg_id,
		    (env_restrictions.time_permit ? "high load"
						  : "time not permitting"))
	     << dendl;
    trgt.delay_on_wrong_time(clock_now);
    res_code = schedule_result_t::target_failure;

  } else if (
      state_test(PG_STATE_SNAPTRIM) || state_test(PG_STATE_SNAPTRIM_WAIT)) {
    // note that the trimmer checks scrub status when setting 'snaptrim_wait'
    // (on the transition from NotTrimming to Trimming/WaitReservation),
    // i.e. some time before setting 'snaptrim'.
    dout(10) << __func__ << ": cannot scrub while snap-trimming" << dendl;
    trgt.delay_on_pg_state(clock_now);
    res_code = schedule_result_t::target_failure;

  } else if (auto validation_err =
		 validate_scrub_mode(clock_now, trgt, pg_cond);
	     validation_err) {

    // based on the combination of the requested scrub flags, the osd/pool
    // configuration and the PG status -
    // the stars do not align for starting a scrub for this PG at this time.
    // The reason was already reported by validate_scrub_mode().
    dout(10) << __func__ << ": failed to initiate a scrub" << dendl;
    res_code = validation_err.value();

  } else if (!reserve_local()) {

    // Failed in reserving the local OSD resources (i.e. this OSD is already
    // performing osd_max_scrubs concurrent scrubs).
    // No point in trying any other PG at this tick.
    dout(10) << __func__ << ": failed to reserve locally" << dendl;
    // note that we are not trying to re-sort (i.e. - not changing the
    // 'not-before')
    res_code = schedule_result_t::failure;
  }

  switch (res_code) {
    case schedule_result_t::target_failure:
      m_scrub_job->requeue_entry(lvl);
      // go on to try some other ready-to-scrub PG
      m_schedloop_step->go_for_next_in_queue();
      return;
    case schedule_result_t::failure:
    default:
      m_scrub_job->requeue_entry(lvl);
      m_schedloop_step->conclude_candidates_selection();
      return;
    case schedule_result_t::ok_thus_far:
      // go on and scrub this PG
      break;
  }

  // we are now committed to scrubbing this PG
  set_op_parameters(trgt, pg_cond);
  m_scrub_job->scrubbing = true;
  dout(10) << fmt::format("{}: scrubbing target {}", __func__, *m_active_target)
	   << dendl;
  assert_targets_invariant();
  m_osds->queue_for_scrub(m_pg, Scrub::scrub_prio_t::low_priority);
}


/*
 * We are presented with the specific scheduling target that was chosen by the
 * OSD - i.e., we have a specific PG to scrub and the presumed type of scrub
 * (remember that each PG has two entries - deep and shallow - in the scrubbing
 * queue).
 *
 * Are we prevented from going on with this specific level of scrub?
 * If so, we will return a 'failure' result, and will modify the target's
 * 'not before'. The caller will requeue.
 */
std::optional<Scrub::schedule_result_t> PgScrubber::validate_scrub_mode(
    utime_t scrub_clock_now,
    Scrub::SchedTarget& trgt,
    const Scrub::ScrubPGPreconds& pg_cond)
{
  using schedule_result_t = Scrub::schedule_result_t;
  if (trgt.is_required()) {
    // 'initiated' scrubs
    dout(10) << fmt::format(
		    "{}: initiated (\"must\") scrub (target:{} pg:{})",
		    __func__, trgt, pg_cond)
	     << dendl;

    if (trgt.is_shallow() && pg_cond.has_deep_errors) {
      // we will honor the request anyway, but will report the issue
      m_osds->clog->error() << fmt::format(
	  "osd.{} pg {} Regular scrub request, deep-scrub details will be lost",
	  m_osds->whoami, m_pg_id);
    }
    return std::nullopt;  // no error
  }

  // --------  a periodic scrub
  dout(10) << fmt::format(
		  "{}: periodic target:{} pg:{}", __func__, trgt, pg_cond)
	   << dendl;

  // if a shallow target:
  if (trgt.is_shallow()) {
    if (!pg_cond.allow_shallow) {
      // can't scrub at all
      dout(10) << __func__ << ": shallow not allowed" << dendl;
      trgt.delay_on_level_not_allowed(scrub_clock_now);
      return schedule_result_t::target_failure;
    }

    // if there are deep errors, we should have scheduled a deep scrub first.
    // If we are here trying to perform a shallow scrub, it means that for some
    // reason that deep scrub failed to be initiated. We will not try a shallow
    // scrub until this is solved.
    if (pg_cond.has_deep_errors) {
      dout(10) << __func__ << ": Regular scrub skipped due to deep-scrub errors"
	       << dendl;
      trgt.delay_on_deep_errors(scrub_clock_now);
      return schedule_result_t::target_failure;
    }

    return std::nullopt;  // no error;
  }

  // A deep target:
  if (!pg_cond.allow_deep) {
    dout(10) << __func__ << ": deep not allowed" << dendl;
    trgt.delay_on_level_not_allowed(scrub_clock_now);
    return schedule_result_t::target_failure;
  }
  return std::nullopt;
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
int size_from_conf(
    bool is_deep,
    const ceph::common::ConfigProxy& conf,
    std::string_view deep_opt,
    std::string_view shallow_opt)
{
  if (!is_deep) {
    auto sz = conf.get_val<int64_t>(shallow_opt);
    if (sz != 0) {
      // assuming '0' means that no distinction was yet configured between
      // deep and shallow scrubbing
      return static_cast<int>(sz);
    }
  }
  return static_cast<int>(conf.get_val<int64_t>(deep_opt));
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

LogChannelRef &PgScrubber::get_clog() const
{
  return m_osds->clog;
}

int PgScrubber::get_whoami() const
{
  return m_osds->whoami;
}

/*
 * The selected range is set directly into 'm_start' and 'm_end'
 * setting:
 * - m_subset_last_update
 * - m_max_end
 * - end
 * - start
 */
bool PgScrubber::select_range()
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
		  conf.get_val<int64_t>("osd_scrub_chunk_min"),
		  conf.get_val<int64_t>("osd_shallow_scrub_chunk_min"),
		  conf.get_val<int64_t>("osd_scrub_chunk_max"),
		  conf.get_val<int64_t>("osd_shallow_scrub_chunk_max"))
	   << dendl;

  const int min_from_conf = size_from_conf(
      m_is_deep, conf, "osd_scrub_chunk_min", "osd_shallow_scrub_chunk_min");
  const int max_from_conf = size_from_conf(
      m_is_deep, conf, "osd_scrub_chunk_max", "osd_shallow_scrub_chunk_max");

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
    return false;
  }

  m_end = candidate_end;
  if (m_end > m_max_end)
    m_max_end = m_end;

  dout(15) << __func__ << " range selected: " << m_start << " //// " << m_end
	   << " //// " << m_max_end << dendl;

  // debug: be 'blocked' if told so by the 'pg scrub_debug block' asok command
  if (m_debug_blockrange > 0) {
    m_debug_blockrange--;
    return false;
  }
  return true;
}

void PgScrubber::select_range_n_notify()
{
  if (select_range()) {
    // the next chunk to handle is not blocked
    dout(20) << __func__ << ": selection OK" << dendl;
    m_osds->queue_scrub_chunk_free(m_pg, Scrub::scrub_prio_t::low_priority);

  } else {
    // we will wait for the objects range to become available for scrubbing
    dout(10) << __func__ << ": selected chunk is busy" << dendl;
    m_osds->queue_scrub_chunk_busy(m_pg, Scrub::scrub_prio_t::low_priority);
  }
}

bool PgScrubber::write_blocked_by_scrub(const hobject_t& soid)
{
  if (soid < m_start || soid >= m_end) {
    return false;
  }

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

void PgScrubber::on_init()
{
  // going upwards from 'inactive'
  ceph_assert(!is_scrub_active());
  m_pg->reset_objects_scrubbed();
  preemption_data.reset();
  m_interval_start = m_pg->get_history().same_interval_since;
  dout(10) << __func__ << " start same_interval:" << m_interval_start << dendl;

  // as this PG has managed to secure replica-scrub resources, we can let the
  // OSD know that the 'scrub-initiation loop' can be stopped
  ceph_assert(m_schedloop_step);
  dout(5) << fmt::format(
		 "{}: scrub-initiation loop wasn't empty ({})", __func__,
		 *m_schedloop_step)
	  << dendl;
  m_schedloop_step->conclude_candidates_selection();
  m_schedloop_step.reset();

  m_be = std::make_unique<ScrubBackend>(
      *this, *m_pg, m_pg_whoami, m_is_repair,
      m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow,
      m_pg->get_actingset());

  //  create a new store
  ObjectStore::Transaction t;
  cleanup_store(&t);
  m_store.reset(
      Scrub::Store::create(m_pg->osd->store, &t, m_pg->info.pgid, m_pg->coll));
  m_pg->osd->store->queue_transaction(m_pg->ch, std::move(t), nullptr);

  m_start = m_pg->info.pgid.pgid.get_hobj_start();
  m_active = true;
  ++m_sessions_counter;
  // publish the session counter and the fact the we are scrubbing.
  m_pg->publish_stats_to_osd();
}


void PgScrubber::on_repl_reservation_failure()
{
  dout(10) << __func__ << dendl;
  m_scrub_job->on_reservation_failure(
      std::move(*m_active_target), ceph_clock_now());
  m_active_target.reset();

  // trigger the next attempt by the OSD to select a PG to scrub:
  ceph_assert(m_schedloop_step);
  m_schedloop_step->go_for_next_in_queue();
  m_schedloop_step.reset();

  clear_pgscrub_state();
}


void PgScrubber::at_scrub_failure(delay_cause_t issue)
{
  // assuming we can still depend on the 'scrubbing' flag being set;
  // Also on Queued&Active.

  // if there is a 'next' target - it might have higher priority than
  // what was just run. Let's merge the two.
  ceph_assert(m_active_target);
  m_scrub_job->on_abort(std::move(*m_active_target), issue, ceph_clock_now());
  m_active_target.reset();
}

void PgScrubber::on_backend_error()
{
  dout(5) << fmt::format("{}: @ start: {}", __func__, *m_scrub_job) << dendl;
  at_scrub_failure(delay_cause_t::backend_error);
  clear_pgscrub_state();
}

/*
 * process:
 *  - merge the active target (the one that we are scrubbing) back into one
 *    of the two jobs' targets.
 *    Note: no need to push the not-before of the active target. The abort
 *    is "blameless".
 *  - make sure the other target is also dequeued.
 */
void PgScrubber::interval_when_active()
{
  dout(10) << fmt::format(
		  "{}: @ start: {} (a-t? {})", __func__, *m_scrub_job,
		  (m_active_target ? "yes" : "no"))
	   << dendl;
  m_scrub_job->remove_from_osd_queue();
  if (m_active_target) {
    m_scrub_job->merge_active_back(
	std::move(*m_active_target), delay_cause_t::interval, ceph_clock_now());
    m_active_target.reset();
  }
  // both targets are now dequeued. They will only be requeued once we are "activated
  // primary" once more.

  clear_pgscrub_state();
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
      // chunk - and we better be done with the current scrub. Thus - the
      // preparation of the reply message is separate, and we clear the scrub
      // state before actually sending it.

      auto reply = prep_replica_map_msg(PreemptionNoted::no_preemption);
      replica_handling_done();
      dout(15) << __func__ << " chunk map sent " << dendl;
      send_replica_map(reply);
    } break;

    default:
      // negative retval: build_scrub_map_chunk() signalled an error
      // Pre-Pacific code ignored this option, treating it as a success.
      // \todo Add an error flag in the returning message.
      dout(1) << "Error! Aborting. ActiveReplica::react(SchedReplica) Ret: "
	      << ret << dendl;
      replica_handling_done();
      // only in debug mode for now:
      assert(false && "backend error");
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
    bufferlist bl;
    bl.push_back(o.attrs[OI_ATTR]);
    object_info_t oi;
    try {
      oi.decode(bl);
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
      bl.clear();
      encode(oi,
             bl,
             m_pg->get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));

      bufferptr bp(bl.c_str(), bl.length());
      o.attrs[OI_ATTR] = bp;

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
  ceph_assert(m_store);

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
      ceph_assert(m_pg->osd->store);
    }
    dout(15) << __func__ << " wait on repair - done" << dendl;
  }
}

void PgScrubber::maps_compare_n_cleanup()
{
  m_pg->add_objects_scrubbed_count(m_be->get_primary_scrubmap().objects.size());

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
  m_osds->queue_for_rep_scrub(m_pg,
			      m_replica_request_priority,
			      m_flags.priority,
			      m_current_token);
}

void PgScrubber::set_op_parameters(
    Scrub::SchedTarget& trgt,
    const Scrub::ScrubPGPreconds& pg_cond)
{
  dout(10) << fmt::format("{}: {}: conditions: {}", __func__, trgt, pg_cond)
	   << dendl;

  // we are now committed to scrubbing this pg. The 'scheduling target' being
  // passed here is the one we have chosen to scrub. It will be moved into
  // m_active_target, and a new target object will be created in the ScrubJob,
  // to be used for scheduling the next scrub of this level.

  set_queued_or_active();
  m_active_target = m_scrub_job->get_moved_target(trgt.level());
  // remove our sister target from the queue
  // (note: dequeue_entry() calls remove_entry(), which locks the queue)
  m_scrub_job->dequeue_entry(ScrubJob::the_other_level(m_active_target->level()));

  // write down the epoch of starting a new scrub. Will be used
  // to discard stale messages from previous aborted scrubs.
  m_epoch_start = m_pg->get_osdmap_epoch();

  dout(7) << fmt::format("{}: m_flags.cr: {} | periodic? {} is_deep:{} | pg_conf.canar:{}",
                                __func__, (m_active_target->urgency() == urgency_t::after_repair), m_active_target->is_periodic(),
                                m_active_target->is_deep(), pg_cond.can_autorepair)
                << dendl; // RRR - dev only


  m_flags.check_repair = m_active_target->urgency() == urgency_t::after_repair;
  bool can_auto_repair =
      m_active_target->is_deep() && m_active_target->is_periodic() && pg_cond.can_autorepair;
  if (can_auto_repair) {
    // maintaining an existing log line
    dout(20) << __func__ << ": auto repair with deep scrubbing" << dendl;
  }

  m_flags.auto_repair = can_auto_repair || m_active_target->get_auto_repair();

  dout(17) << fmt::format("{}: auto_repair:{}", __func__, m_flags.auto_repair)
           << dendl;

  if (m_active_target->is_periodic()) {
    // lower urgency
    m_flags.required = false;
    m_flags.priority = m_pg->get_scrub_priority();
  } else {
    // 'required' scrubs
    m_flags.required = true;
    m_flags.priority = get_pg_cct()->_conf->osd_requested_scrub_priority;
  }

  // 'deep-on-error' is set for periodic shallow scrubs, if allowed
  // by the environment
  if (m_active_target->is_shallow() && pg_cond.can_autorepair && m_active_target->is_periodic()) {
    m_flags.deep_scrub_on_error = true;
    dout(10) << fmt::format(
		    "{}: auto repair with scrubbing, rescrub if errors found",
		    __func__)
	     << dendl;
  }

  state_set(PG_STATE_SCRUBBING);

  // will we be deep-scrubbing?
  if (m_active_target->is_deep()) {
    state_set(PG_STATE_DEEP_SCRUB);
    m_is_deep = true;
  } else {
    m_is_deep = false;
  }

  // m_is_repair is set for either 'must_repair' or 'repair-on-the-go' (i.e.
  // deep-scrub with the auto_repair configuration flag set). m_is_repair value
  // determines the scrubber behavior.
  //
  // PG_STATE_REPAIR, on the other hand, is only used for status reports (inc.
  // the PG status as appearing in the logs).
  m_is_repair = m_active_target->get_do_repair() || m_flags.auto_repair;
  if (m_active_target->get_do_repair()) {
    state_set(PG_STATE_REPAIR);
    update_op_mode_text();
  }

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
 *     interrupted. The state-machine will react to that when all replica maps
 *     are received.
 *  - when all maps are received, we signal the FSM with the GotReplicas event
 *    (see scrub_send_replmaps_ready()). Note that due to the no-reentrancy
 *     limitations of the FSM, we do not 'process' the event directly. Instead -
 *     it is queued for the OSD to handle.
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

void PgScrubber::dec_scrubs_remote()
{
  m_osds->get_scrub_services().resource_bookkeeper().dec_scrubs_remote();
}

void PgScrubber::advance_token()
{
  m_current_token++;
}

void PgScrubber::handle_scrub_reserve_request(OpRequestRef op)
{
  dout(10) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();
  auto request_ep = op->sent_epoch;
  dout(20) << fmt::format("{}: request_ep:{} recovery:{}",
			  __func__,
			  request_ep,
			  m_osds->is_recovery_active())
	   << dendl;

  if (should_drop_message(op)) {
    return;
  }

  /* here is a convenient point to acknowledge the fact that we are not the
   * Primary, and thus can be sure that any possible scrub-scheduling state
   * (created in our possible past as the Primary and kept
   * in the two sched-targets) is obsolete and can be discarded.
   */
  m_scrub_job->reset_schedule();

  /* The primary may unilaterally restart the scrub process without notifying
   * replicas.  Unconditionally clear any existing state prior to handling
   * the new reservation. */
  m_fsm->process_event(FullReset{});

  bool granted{false};
  if (m_pg->cct->_conf->osd_scrub_during_recovery ||
      !m_osds->is_recovery_active()) {

    granted = m_osds->get_scrub_services().resource_bookkeeper().inc_scrubs_remote();
    if (granted) {
      m_fsm->process_event(ReplicaGrantReservation{});
    } else {
      dout(20) << __func__ << ": failed to reserve remotely" << dendl;
    }
  } else {
    dout(10) << __func__ << ": recovery is active; not granting" << dendl;
  }

  dout(10) << __func__ << " reserved? " << (granted ? "yes" : "no") << dendl;

  Message* reply = new MOSDScrubReserve(
    spg_t(m_pg->info.pgid.pgid, m_pg->get_primary().shard),
    request_ep,
    granted ? MOSDScrubReserve::GRANT : MOSDScrubReserve::REJECT,
    m_pg_whoami);

  m_osds->send_message_osd_cluster(reply, op->get_req()->get_connection());
}

void PgScrubber::handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << " " << *op->get_req() << dendl;
  {
    if (m_debug_deny_replica) {
      // debug/UT code
      dout(10) << fmt::format("{}: debug_deny_replica set - denying", __func__)
	       << dendl;
      m_debug_deny_replica = false;
      m_reservations->release_replica(from, m_pg->get_osdmap_epoch());
      handle_scrub_reserve_reject(op, from);
      return;
    }
  }
  op->mark_started();

  if (m_reservations.has_value()) {
    m_reservations->handle_reserve_grant(op, from);
  } else {
    dout(20) << __func__ << ": late/unsolicited reservation grant from osd "
	 << from << " (" << op << ")" << dendl;
  }
}

void PgScrubber::handle_scrub_reserve_reject(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  if (m_reservations.has_value()) {
    // there is an active reservation process. No action is required otherwise.
    m_reservations->handle_reserve_reject(op, from);
  }
}

void PgScrubber::handle_scrub_reserve_release(OpRequestRef op)
{
  dout(10) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  /*
   * this specific scrub session has terminated. All incoming events carrying
   *  the old tag will be discarded.
   */
  m_fsm->process_event(FullReset{});
}

void PgScrubber::discard_replica_reservations()
{
  dout(10) << __func__ << dendl;
  if (m_reservations.has_value()) {
    m_reservations->discard_all();
  }
}

void PgScrubber::clear_scrub_reservations()
{
  dout(10) << __func__ << dendl;
  m_reservations.reset();	  // the remote reservations
  m_local_osd_resource.reset();	  // the local reservation
}

void PgScrubber::message_all_replicas(int32_t opcode, std::string_view op_text)
{
  ceph_assert(m_pg->recovery_state.get_backfill_targets().empty());

  std::vector<pair<int, Message*>> messages;
  messages.reserve(m_pg->get_actingset().size());

  epoch_t epch = get_osdmap_epoch();

  for (auto& p : m_pg->get_actingset()) {

    if (p == m_pg_whoami)
      continue;

    dout(10) << "scrub requesting " << op_text << " from osd." << p
	     << " Epoch: " << epch << dendl;
    Message* m = new MOSDScrubReserve(spg_t(m_pg->info.pgid.pgid, p.shard),
				      epch,
				      opcode,
				      m_pg_whoami);
    messages.push_back(std::make_pair(p.osd, m));
  }

  if (!messages.empty()) {
    m_osds->send_message_osd_cluster(messages, epch);
  }
}

void PgScrubber::unreserve_replicas()
{
  dout(10) << __func__ << dendl;
  m_reservations.reset();
}

void PgScrubber::on_replica_reservation_timeout()
{
  if (m_reservations) {
    m_reservations->handle_no_reply_timeout();
  }
}

void PgScrubber::set_reserving_now()
{
  m_osds->get_scrub_services().set_reserving_now();
}

void PgScrubber::clear_reserving_now()
{
  m_osds->get_scrub_services().clear_reserving_now();
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

void PgScrubber::err_cnt_to_clog() const
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

void PgScrubber::clear_scrub_blocked()
{
  ceph_assert(m_scrub_job->blocked);
  m_osds->get_scrub_services().clear_pg_scrub_blocked(m_pg_id);
  m_scrub_job->blocked = false;
  m_pg->publish_stats_to_osd();
}

/*
 * note: only called for the Primary.
 */
void PgScrubber::scrub_finish()
{
  dout(10) << fmt::format(
		  "{}: flags at start:{} pg-state:{}", __func__, m_flags,
		  (state_test(PG_STATE_REPAIR) ? "repair" : "no-repair"))
	   << dendl;
  ceph_assert(m_pg->is_locked());
  ceph_assert(is_queued_or_active());
  assert_targets_invariant();

  // if the repair request comes from auto-repair and large number of errors,
  // we would like to cancel auto-repair
  if (m_is_repair && m_flags.auto_repair &&
      m_be->authoritative_peers_count() >
	  static_cast<int>(
	      m_pg->cct->_conf->osd_scrub_auto_repair_num_errors)) {

    dout(10) << __func__ << ": undoing the repair" << dendl;
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
	  static_cast<int>(
	      m_pg->cct->_conf->osd_scrub_auto_repair_num_errors)) {
    ceph_assert(!m_is_deep);
    do_auto_scrub = true;
    dout(15) << __func__ << ": Try to auto repair after scrub errors" << dendl;
  }

  m_flags.deep_scrub_on_error = false;

  // type-specific finish (can tally more errors)
  _scrub_finish();

  /// \todo fix the relevant scrub test so that we would not need the extra log
  /// line here (even if the following 'if' is false)

  if (m_be->authoritative_peers_count()) {

    auto err_msg = fmt::format(
	"{} {} {} missing, {} inconsistent objects", m_pg->info.pgid,
	m_mode_desc, m_be->m_missing.size(), m_be->m_inconsistent.size());

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
  err_cnt_to_clog();

  // Since we don't know which errors were fixed, we can only clear them
  // when every one has been fixed.
  if (m_is_repair) {
    dout(15) << fmt::format(
		    "{}: {} errors. {} errors fixed", __func__,
		    m_shallow_errors + m_deep_errors, m_fixed_count)
	     << dendl;
    if (m_fixed_count == m_shallow_errors + m_deep_errors) {

      ceph_assert(m_is_deep);
      m_shallow_errors = 0;
      m_deep_errors = 0;
      dout(20) << __func__ << " All may be fixed" << dendl;

    } else if (has_error) {

      // Deep scrub in order to get corrected error counts
      dout(10) << fmt::format(
		      "{}: the repair will be followed by a deep-scrub",
		      __func__)
	       << dendl;
      m_after_repair_scrub_required = true;
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
    // mark_for_rescrubbing() only affects the deep target, thus
    // at_scrub_completion() is still needed.
    m_scrub_job->mark_for_rescrubbing();
  }

  auto applicable_conf = m_osds->get_scrub_services().populate_config_params(
      m_pg->get_pgpool().info.opts);
  /// \todo the ceph_clock_now() should be replaced by the mockable ScrubQueue
  /// clock
  m_scrub_job->at_scrub_completion(
      m_pg->get_pg_info(ScrubberPasskey()), applicable_conf, ceph_clock_now());
  m_active_target.reset();

  // m_active & queued-or-active are both cleared now

  if (m_pg->is_active() && m_pg->is_primary()) {
    m_pg->recovery_state.share_pg_info();
  }
  assert_targets_invariant();
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

// handling Asok's "scrub" & "deep_scrub" commands

namespace {
void asok_response_section(
    ceph::Formatter* f,
    bool is_periodic,
    scrub_level_t scrub_level
    /*const char* section_value*/)
{
  f->open_object_section("result");
  f->dump_bool("deep", (scrub_level == scrub_level_t::deep));
  f->dump_bool("must", !is_periodic);
  f->close_section();
}
}  // namespace

// when asked to force a "periodic" scrub by faking the timestamps
void PgScrubber::on_operator_periodic_cmd(
    ceph::Formatter* f,
    scrub_level_t scrub_level,
    int64_t offset)
{
  auto cnf = m_scrub_queue.populate_config_params(m_pg->get_pgpool().info.opts);
  dout(10) << fmt::format(
		  "{}: {} (cmd offset:{}) conf:{}", __func__,
		  (scrub_level == scrub_level_t::deep ? "deep" : "shallow"), offset,
		  cnf)
	   << dendl;

  // move the relevant time-stamp backwards - enough to trigger a scrub

  utime_t now_is = m_scrub_queue.scrub_clock_now();
  utime_t stamp = now_is;

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
  asok_response_section(f, true, scrub_level);

  if (scrub_level == scrub_level_t::deep) {
    m_pg->set_last_deep_scrub_stamp(stamp);
  }
  // and in both cases:
  m_pg->set_last_scrub_stamp(stamp);

  // use the newly-updated set of timestamps to schedule a scrub
  m_scrub_job->operator_periodic_targets(
      scrub_level, stamp, m_pg->get_pg_info(ScrubberPasskey()), cnf, now_is);
}

// when asked to force a high-priority scrub
void PgScrubber::on_operator_forced_scrub(
    ceph::Formatter* f,
    scrub_level_t scrub_level)
{
  auto deep_req = scrub_requested(scrub_level, scrub_type_t::not_repair);
  asok_response_section(f, false, deep_req);
}

void PgScrubber::dump_scrubber(ceph::Formatter* f) const
{
  f->open_object_section("scrubber");

  if (m_active) {
    f->dump_bool("active", true);
    dump_active_scrubber(f, state_test(PG_STATE_DEEP_SCRUB));
  } else {
    f->dump_bool("active", false);
    auto now_is = m_scrub_queue.scrub_clock_now();
    auto& closest = m_scrub_job->closest_target(now_is);
    f->dump_bool("must_scrub", closest.is_required());
    f->dump_stream("scrub_reg_stamp") << m_scrub_job->get_sched_time(now_is);

    auto sched_state = m_scrub_job->scheduling_state();
    m_scrub_job->dump(f);
    f->dump_string("schedule", sched_state);
  }

  if (m_publish_sessions) {
    // an ever-increasing number used by tests
    f->dump_int("test_sequence", m_sessions_counter);
  }

  f->close_section();
}

void PgScrubber::dump_active_scrubber(ceph::Formatter* f, bool is_deep) const
{
  f->dump_stream("epoch_start") << m_interval_start;
  f->dump_stream("start") << m_start;
  f->dump_stream("end") << m_end;
  f->dump_stream("max_end") << m_max_end;
  f->dump_stream("subset_last_update") << m_subset_last_update;
  // note that m_is_deep will be set some time after PG_STATE_DEEP_SCRUB is
  // asserted. Thus, using the latter.
  f->dump_bool("deep", is_deep);

  // dump the scrub-type flags
  f->dump_bool("req_scrub", m_flags.required);
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

  auto now_is = m_scrub_queue.scrub_clock_now();

  if (m_active) {
    // report current scrub info, including updated duration
    if (m_scrub_job->blocked) {
      // a bug. An object is held locked.
      int32_t blocked_for = (now_is - m_scrub_job->blocked_since).sec();
      return pg_scrubbing_status_t{
	  utime_t{},
	  blocked_for,
	  pg_scrub_sched_status_t::blocked,
	  true,	 // active
	  (m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow),
	  !m_flags.required};

    } else {
      int32_t duration = (now_is - scrub_begin_stamp).sec();
      return pg_scrubbing_status_t{
	  utime_t{},
	  duration,
	  pg_scrub_sched_status_t::active,
	  true,	 // active
	  (m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow),
	  !m_flags.required /* is periodic? */};
    }
  }

  // we are not active. Ask the scrub job to report the nearest scheduling
  // target.
  return m_scrub_job->get_schedule(now_is);
}

PgScrubber::~PgScrubber()
{
  if (m_scrub_job) {
    // make sure the OSD won't try to scrub this one just now
    m_scrub_job->remove_from_osd_queue();
    m_scrub_job.reset();
  }
}

PgScrubber::PgScrubber(PG* pg, ScrubQueue& osd_scrubq)
    : m_pg{pg}
    , m_pg_id{pg->pg_id}
    , m_osds{m_pg->osd}
    , m_scrub_queue{osd_scrubq}
    , m_pg_whoami{pg->pg_whoami}
    , preemption_data{pg}
{
  m_fsm = std::make_unique<ScrubMachine>(m_pg, this);
  m_fsm->initiate();

  m_scrub_job = std::make_unique<Scrub::ScrubJob>(
      m_scrub_queue, m_osds->cct, m_pg->pg_id, m_osds->get_nodeid());
}

void PgScrubber::set_scrub_begin_time()
{
  scrub_begin_stamp = ceph_clock_now();
  //scrub_begin_stamp = m_scrub_queue.scrub_clock_now(); // RRR
  m_osds->clog->debug() << fmt::format(
    "{} {} starts",
    m_pg->info.pgid.pgid,
    m_mode_desc);
}

void PgScrubber::set_scrub_duration()
{
  utime_t stamp = ceph_clock_now();
  //scrub_begin_stamp = m_scrub_queue.scrub_clock_now(); // RRR
  utime_t duration = stamp - scrub_begin_stamp;
  m_pg->recovery_state.update_stats([=](auto& history, auto& stats) {
    stats.last_scrub_duration = ceill(duration.to_msec() / 1000.0);
    stats.scrub_duration = double(duration);
    return true;
  });
}

void PgScrubber::reserve_replicas()
{
  dout(10) << __func__ << dendl;
  m_reservations.emplace(
    m_pg, m_pg_whoami, m_scrub_job.get(), m_pg->get_cct()->_conf);
}

// note: only called for successful scrubs
void PgScrubber::cleanup_on_finish()
{
  dout(10) << __func__ << dendl;
  ceph_assert(m_pg->is_locked());

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);
  m_scrub_job->consec_aborts = 0;
  m_pg->publish_stats_to_osd();

  clear_scrub_reservations();
  requeue_waiting();

  reset_internal_state();
  m_flags = scrub_flags_t{};

  // type-specific state clear
  _scrub_clear_state();
  // PG state flags changed:
  m_pg->publish_stats_to_osd();
}

// uses process_event(), so must be invoked externally
void PgScrubber::scrub_clear_state()
{
  dout(10) << __func__ << dendl;

  clear_pgscrub_state();
  m_fsm->process_event(FullReset{});
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

  state_clear(PG_STATE_REPAIR);

  clear_scrub_reservations();
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
  return m_osds->get_scrub_services().required_sleep_time(
    m_flags.required);
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
  m_scrub_job->scrubbing = false;
  m_schedloop_step.reset();

  clear_queued_or_active();
  ++m_sessions_counter;
  m_be.reset();
}

void PgScrubber::assert_targets_invariant() const
{
  // No lock here. The relevant PG itself is locked.
  // But note that the OSD might manipulate the queue (popping the top) in
  // parallel. Thus, the following is only a sanity check.

  const auto queued_cnt = m_scrub_queue.count_queued(m_pg_id);
  ceph_assert(queued_cnt <= 3); // that's a real no-no, regardless of state
  ssize_t enabled_cnt = 0;

  if (m_scrub_job) {
    const auto& s_target = m_scrub_job->get_target(scrub_level_t::shallow);
    const auto& d_target = m_scrub_job->get_target(scrub_level_t::deep);
    enabled_cnt = (s_target.is_off() ? 0 : 1) + (d_target.is_off() ? 0 : 1);
    //all_enabled_cnt = enabled_cnt + (m_active_target ? 1 : 0);
  }

  // if we are active - we must have real targets
  /// \todo replace the following condition with a 'PG should be in the queue'
  if (is_queued_or_active() || enabled_cnt || queued_cnt) {
    if (is_queued_or_active()) {
      // while scrubbing, the targets are supposed to be dequeued. The active one
      // should be copied into the 'm_active_target' member
      ceph_assert(queued_cnt == 0);
      ceph_assert(enabled_cnt == 2 || (enabled_cnt >= 1 && m_active_target));
    } else {
      // we might be in a transition into 'active'.
      // we are in the OSD queue, but not scrubbing. we might be in a
      // transition into 'active'. Either both targets are queued and active,
      // one of them was pulled out.
      if (queued_cnt != 2 || enabled_cnt != 2) {
	dout(2) << fmt::format(
		       "{}: failure: queued_cnt={} enabled_cnt={} a: {}"
		       " mact:{}",
		       __func__, queued_cnt, enabled_cnt,
		       (m_active_target ? 1 : 0), m_active)
		<< dendl;
      }
      ceph_assert(queued_cnt == 2);
      ceph_assert(enabled_cnt == 2);
    }
  } else {
    // not in the OSD queue. No targets should be enabled nor queued.
    ceph_assert(enabled_cnt == 0);
    ceph_assert(queued_cnt == 0);
  }
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

ostream &operator<<(ostream &out, const PgScrubber &scrubber) {
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

// 'show_concise()' is only used when calling operator<< thru the ScrubPgIF
// interface - i.e. only by the PG
ostream& PgScrubber::show_concise(ostream& out) const
{
  // desired outcome:
  // Only relevant for Primaries!
  // if active:
  //   (shallow|deep,urgency[,mandatory][,auto-rpr][,chk-rpr][,deep-on-error])
  // if not:
  //   either nothing (if only periodic scrubs are scheduled)
  //   or [next scrub: effective-lvl, urgency, rpr,

  if (!is_primary()) {
    return out;
  }
  if (m_active) {
    if (m_active_target) {
      return out << fmt::format(
		 "({},{}{:4.4}{})", m_is_deep ? "deep" : "shallow",
		 (m_scrub_job->blocked ? "-*blocked*" : ""),
		 (*m_active_target).urgency(), m_flags);
    } else {
      return out << fmt::format(
		 "({},{}{}-inac)", m_is_deep ? "deep" : "shallow",
		 (m_scrub_job->blocked ? "-*blocked*" : ""), m_flags);
    }
  }

  auto now_is = m_scrub_queue.scrub_clock_now();
  auto& nscrub = m_scrub_job->closest_target(now_is);
  if (nscrub.is_periodic()) {
    // no interesting flags to be reported
    return out;
  }

  return out << fmt::format(
	     " [next-scrub:{},{:4.4}{}{}]", (nscrub.is_deep() ? "dp" : "sh"),
	     nscrub.urgency(), (nscrub.get_do_repair() ? ",rpr" : ""),
	     (nscrub.get_auto_repair() ? ",auto" : ""));
}

int PgScrubber::asok_debug(
    std::string_view prefix,
    std::string_view cmd,
    std::string_view param,
    Formatter* f,
    std::stringstream& ss)
{
  dout(10) << fmt::format(
		  "asok_debug: prefix={}, cmd={}, param={}", prefix, cmd, param)
	   << dendl;

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
    f->open_object_section("result");
    f->dump_bool("success", true);
    f->close_section();
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

  const seconds period_active = seconds(m_pg->get_cct()->_conf.get_val<int64_t>(
    "osd_stats_update_period_scrubbing"));
  if (!period_active.count()) {
    // a way for the operator to disable these stats updates
    return;
  }
  auto base_inactive_upd = seconds(m_pg->get_cct()->_conf.get_val<int64_t>(
      "osd_stats_update_period_not_scrubbing"));
  // a period set to < 5 seconds means we are running a test. In that case -
  // do not "randomize" the period based on the PG ID
  const seconds period_inactive =
      (base_inactive_upd > 5s)
	  ? base_inactive_upd + seconds(m_pg_id.pgid.m_seed % 30)
	  : base_inactive_upd;

  // determine the required update period, based on our current state
  auto period{period_inactive};
  if (m_active) {
    period = m_debug_blockrange ? 2s : period_active;
  }

  /// \todo use the date library (either the one included in Arrow or directly)
  /// to get the formatting of the time_points.

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    // will only create the debug strings if required
    char buf[50];
    auto printable_last = fmt::localtime(clock::to_time_t(m_last_stat_upd));
    strftime(buf, sizeof(buf), "%Y-%m-%dT%T", &printable_last);
    dout(20) << fmt::format(
		    "{}: period: {}/{}-> {} last:{}", __func__, period_active,
		    period_inactive, period, buf)
	     << dendl;
  }

  if (now_is - m_last_stat_upd > period) {
    m_pg->publish_stats_to_osd();
    m_last_stat_upd = now_is;
  }
}


// ///////////////////// preemption_data_t //////////////////////////////////

PgScrubber::preemption_data_t::preemption_data_t(PG* pg) : m_pg{pg}
{
  m_left = static_cast<int>(
    m_pg->get_cct()->_conf.get_val<uint64_t>("osd_scrub_max_preemptions"));
}

void PgScrubber::preemption_data_t::reset()
{
  std::lock_guard<ceph::mutex> lk{m_preemption_lock};

  m_preemptable = false;
  m_preempted = false;
  m_left = static_cast<int>(
    m_pg->cct->_conf.get_val<uint64_t>("osd_scrub_max_preemptions"));
  m_size_divisor = 1;
}


// ///////////////////// ReplicaReservations //////////////////////////////////
namespace Scrub {

void ReplicaReservations::release_replica(pg_shard_t peer, epoch_t epoch)
{
  auto m = new MOSDScrubReserve(spg_t(m_pg_info.pgid.pgid, peer.shard),
				epoch,
				MOSDScrubReserve::RELEASE,
				m_pg->pg_whoami);
  m_osds->send_message_osd_cluster(peer.osd, m, epoch);
}

ReplicaReservations::ReplicaReservations(
  PG* pg,
  pg_shard_t whoami,
  Scrub::ScrubJob* scrubjob,
  const ConfigProxy& conf)
    : m_pg{pg}
    , m_acting_set{pg->get_actingset()}
    , m_osds{m_pg->get_pg_osd(ScrubberPasskey())}
    , m_pending{static_cast<int>(m_acting_set.size()) - 1}
    , m_pg_info{m_pg->get_pg_info(ScrubberPasskey())}
    , m_scrub_job{scrubjob}
    , m_conf{conf}
{
  epoch_t epoch = m_pg->get_osdmap_epoch();
  m_log_msg_prefix = fmt::format(
      "osd.{} ep: {} scrubber::ReplicaReservations pg[{}]: ", m_osds->whoami,
      epoch, pg->pg_id);

  m_timeout = conf.get_val<std::chrono::milliseconds>(
      "osd_scrub_slow_reservation_response");

  if (m_pending <= 0) {
    // A special case of no replicas.
    // just signal the scrub state-machine to continue
    send_all_done();

  } else {
    // send the reservation requests
    for (auto p : m_acting_set) {
      if (p == whoami)
	continue;
      auto m = new MOSDScrubReserve(
	spg_t(m_pg_info.pgid.pgid, p.shard), epoch, MOSDScrubReserve::REQUEST,
	m_pg->pg_whoami);
      m_osds->send_message_osd_cluster(p.osd, m, epoch);
      m_waited_for_peers.push_back(p);
      dout(10) << __func__ << ": reserve " << p.osd << dendl;
    }
  }
}

void ReplicaReservations::send_all_done()
{
  // stop any pending timeout timer
  m_osds->queue_for_scrub_granted(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::send_reject()
{
  m_osds->queue_for_scrub_denied(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::discard_all()
{
  dout(10) << __func__ << ": " << m_reserved_peers << dendl;

  m_had_rejections = true;  // preventing late-coming responses from triggering
			    // events
  m_reserved_peers.clear();
  m_waited_for_peers.clear();
}

/*
 * The following holds when update_latecomers() is called:
 * - we are still waiting for replies from some of the replicas;
 * - we might have already set a timer. If so, we should restart it.
 * - we might have received responses from 50% of the replicas.
 */
std::optional<ReplicaReservations::tpoint_t>
ReplicaReservations::update_latecomers(tpoint_t now_is)
{
  if (m_reserved_peers.size() > m_waited_for_peers.size()) {
    // at least half of the replicas have already responded. Time we flag
    // latecomers.
    return now_is + m_timeout;
  } else {
    return std::nullopt;
  }
}

ReplicaReservations::~ReplicaReservations()
{
  m_had_rejections = true;  // preventing late-coming responses from triggering
			    // events

  // send un-reserve messages to all reserved replicas. We do not wait for
  // answer (there wouldn't be one). Other incoming messages will be discarded
  // on the way, by our owner.
  epoch_t epoch = m_pg->get_osdmap_epoch();

  for (auto& p : m_reserved_peers) {
    release_replica(p, epoch);
  }
  m_reserved_peers.clear();

  // note: the release will follow on the heels of the request. When tried
  // otherwise, grants that followed a reject arrived after the whole scrub
  // machine-state was reset, causing leaked reservations.
  for (auto& p : m_waited_for_peers) {
    release_replica(p, epoch);
  }
  m_waited_for_peers.clear();
}

/**
 *  @ATTN we would not reach here if the ReplicaReservation object managed by
 * the scrubber was reset.
 */
void ReplicaReservations::handle_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << ": granted by " << from << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is
    // cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  // are we forced to reject the reservation?
  if (m_had_rejections) {

    dout(10) << __func__ << ": rejecting late-coming reservation from " << from
	     << dendl;
    release_replica(from, m_pg->get_osdmap_epoch());

  } else if (std::find(m_reserved_peers.begin(),
		       m_reserved_peers.end(),
		       from) != m_reserved_peers.end()) {

    dout(10) << __func__ << ": already had osd." << from << " reserved"
	     << dendl;

  } else {

    dout(10) << __func__ << ": osd." << from << " scrub reserve = success"
	     << dendl;
    m_reserved_peers.push_back(from);

    // was this response late?
    auto now_is = clock::now();
    if (m_timeout_point && (now_is > *m_timeout_point)) {
      m_osds->clog->warn() << fmt::format(
	"osd.{} scrubber pg[{}]: late reservation from osd.{}",
	m_osds->whoami,
	m_pg->pg_id,
	from);
      m_timeout_point.reset();
    } else {
      // possibly set a timer to warn about late-coming reservations
      m_timeout_point = update_latecomers(now_is);
    }

    if (--m_pending == 0) {
      send_all_done();
    }
  }
}

void ReplicaReservations::handle_reserve_reject(OpRequestRef op,
						pg_shard_t from)
{
  dout(10) << __func__ << ": rejected by " << from << dendl;
  dout(15) << __func__ << ": " << *op->get_req() << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is
    // cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  if (m_had_rejections) {

    // our failure was already handled when the first rejection arrived
    dout(15) << __func__ << ": ignoring late-coming rejection from " << from
	     << dendl;

  } else if (std::find(m_reserved_peers.begin(),
		       m_reserved_peers.end(),
		       from) != m_reserved_peers.end()) {

    dout(10) << __func__ << ": already had osd." << from << " reserved"
	     << dendl;

  } else {

    dout(10) << __func__ << ": osd." << from << " scrub reserve = fail"
	     << dendl;
    m_had_rejections = true;  // preventing any additional notifications
    send_reject();
  }
}

void ReplicaReservations::handle_no_reply_timeout()
{
  dout(1) << fmt::format(
	       "{}: timeout! no reply from {}", __func__, m_waited_for_peers)
	  << dendl;

  // treat reply timeout as if a REJECT was received
  m_had_rejections = true;  // preventing any additional notifications
  send_reject();
}

std::ostream& ReplicaReservations::gen_prefix(std::ostream& out) const
{
  return out << m_log_msg_prefix;
}


// ///////////////////// LocalReservation //////////////////////////////////

// note: no dout()s in LocalReservation functions. Client logs interactions.
LocalReservation::LocalReservation(OSDService* osds) : m_osds{osds}
{
  if (m_osds->get_scrub_services().resource_bookkeeper().inc_scrubs_local()) {
    // a failure is signalled by not having m_holding_local_reservation set
    m_holding_local_reservation = true;
  }
}

LocalReservation::~LocalReservation()
{
  if (m_holding_local_reservation) {
    m_holding_local_reservation = false;
    m_osds->get_scrub_services().resource_bookkeeper().dec_scrubs_local();
  }
}

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
