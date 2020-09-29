// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_scrubber.h"

#include <iostream>
#include <vector>

#include "debug.h"

#include "common/errno.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "messages/MOSDScrub.h"
#include "messages/MOSDScrubReserve.h"

#include "OSD.h"
#include "ScrubStore.h"
#include "scrub_machine.h"

using namespace Scrub;
using Scrub::ScrubMachine;
using namespace std::chrono;
using namespace std::chrono_literals;


#define dout_context (m_pg->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << " scrbr.pg(~" << m_pg->pg_id << "~) "


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
  // RRR - mine
  if (sf.marked_must)
    out << " MARKED_AS_MUST";

  return out;
}

ostream& operator<<(ostream& out, const requested_scrub_t& sf)
{
  if (sf.must_repair)
    out << " planned MUST_REPAIR";
  if (sf.auto_repair)
    out << " planned AUTO_REPAIR";
  if (sf.check_repair)
    out << " planned CHECK_REPAIR";
  if (sf.deep_scrub_on_error)
    out << " planned DEEP_SCRUB_ON_ERROR";
  if (sf.must_deep_scrub)
    out << " planned MUST_DEEP_SCRUB";
  if (sf.must_scrub)
    out << " planned MUST_SCRUB";
  if (sf.time_for_deep)
    out << " planned TIME_FOR_DEEP";
  if (sf.need_auto)
    out << " planned NEED_AUTO";
  if (sf.req_scrub)
    out << " planned REQ_SCRUB";

  return out;
}

/*
 * are we still a clean & healthy scrubbing primary?
 *
 * relevant only after the initial sched_scrub
 */
bool PgScrubber::is_event_relevant(epoch_t queued) const
{
  return is_primary() && m_pg->is_active() && m_pg->is_clean() && is_scrub_active() &&
	 !was_epoch_changed() && (!queued || !m_pg->pg_has_reset_since(queued));

  // shouldn't we check was_epoch_changed() (i.e. use m_epoch_start)? RRR
}

/**
 * check the 'no scrub' configuration options.
 */
bool PgScrubber::should_abort_scrub(epoch_t queued) const
{
  dout(7) << __func__ << "(): ep:" << queued << " required: " << m_flags.required
	  << " noscrub: " << get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) << " / "
	  << m_pg->pool.info.has_flag(pg_pool_t::FLAG_NOSCRUB) << dendl;

  if (!is_primary() || !m_pg->is_active() ||
      (queued && m_pg->pg_has_reset_since(queued))) {
    return true;
  }

  if (m_flags.required) {
    return false;  // not stopping 'required' scrubs for configuration changes
  }

  if (state_test(PG_STATE_DEEP_SCRUB)) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB) ||
	m_pg->pool.info.has_flag(pg_pool_t::FLAG_NODEEP_SCRUB)) {
      dout(10) << "nodeep_scrub set, aborting" << dendl;
      return true;
    }
  } else if (state_test(PG_STATE_SCRUBBING)) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) ||
	m_pg->pool.info.has_flag(pg_pool_t::FLAG_NOSCRUB)) {
      dout(10) << "noscrub set, aborting" << dendl;
      return true;
    }
  }

  return false;
}

void PgScrubber::send_start_scrub()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  if (should_abort_scrub(epoch_t(0))) {
    dout(7) << __func__ << " aborting!" << dendl;
    scrub_clear_state(false);
  } else {
    m_fsm->my_states();
    // ceph_assert(state_downcast<const NotActive*>() != 0);
    m_fsm->process_event(StartScrub{});
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_start_after_rec()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  m_fsm->my_states();
  m_fsm->process_event(AfterRecoveryScrub{});
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_scrub_unblock()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  if (should_abort_scrub(epoch_t(0))) {

    dout(7) << __func__ << " aborting!" << dendl;
    scrub_clear_state(false);

  } else if (is_scrub_active()) {

    m_fsm->my_states();
    m_fsm->process_event(Unblocked{});

  } else {
    dout(7) << __func__ << " ignored as scrub not active" << dendl;
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_scrub_resched()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  if (should_abort_scrub(epoch_t(0))) {
    dout(7) << __func__ << " aborting!" << dendl;
    scrub_clear_state(false);
  } else if (is_scrub_active()) {
    m_fsm->my_states();
    m_fsm->process_event(InternalSchedScrub{});
  } else {
    // no need to send anything
    dout(7) << __func__ << " event no longer relevant" << dendl;
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_start_replica()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  m_fsm->my_states();
  m_fsm->process_event(StartReplica{});
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_sched_replica()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  m_fsm->my_states();
  m_fsm->process_event(SchedReplica{});	 // retest for map availability
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::active_pushes_notification()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  if (should_abort_scrub(epoch_t(0))) {
    dout(7) << __func__ << " aborting!" << dendl;
    scrub_clear_state(false);
  } else {
    m_fsm->my_states();
    m_fsm->process_event(ActivePushesUpd{});
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::update_applied_notification(epoch_t epoch_queued)
{
  dout(7) << "RRRRRRR -->> " << __func__ << "() ep: " << epoch_queued << dendl;
  if (should_abort_scrub(epoch_queued)) {
    dout(7) << __func__ << " aborting!" << dendl;
    scrub_clear_state(false);
  } else {
    m_fsm->my_states();
    m_fsm->process_event(UpdatesApplied{});
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::digest_update_notification()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  m_fsm->my_states();
  if (is_event_relevant(epoch_t(0))) {
    m_fsm->process_event(DigestUpdate{});
  } else {
    // no need to send anything
    dout(7) << __func__ << " event no longer relevant" << dendl;
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_epoch_changed()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  if (is_scrub_active()) {
    m_fsm->my_states();
    m_fsm->process_event(EpochChanged{});
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_replica_maps_ready()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  m_fsm->my_states();
  if (is_scrub_active()) {
    m_fsm->process_event(GotReplicas{});
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_remotes_reserved()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  m_fsm->my_states();
  m_fsm->process_event(RemotesReserved{});  // do not check for 'active'!
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_reservation_failure()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  m_fsm->my_states();
  m_fsm->process_event(ReservationFailure{});  // do not check for 'active'!
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

bool PgScrubber::is_scrub_active() const
{
  dout(7) << " " << __func__ << " actv? " << m_active << "pg:" << m_pg->pg_id << dendl;
  return m_active;
}

bool PgScrubber::is_chunky_scrub_active() const
{
  dout(10) << "RRRRRRR -->> " << __func__ << " actv? " << m_active << dendl;
  return is_scrub_active();
}

bool PgScrubber::is_reserving() const
{
  return m_fsm->is_reserving();
}


void PgScrubber::reset_epoch(epoch_t epoch_queued)
{
  dout(10) << __func__ << " PG( " << m_pg->pg_id
	   << (m_pg->is_primary() ? ") prm" : ") rpl") << " epoch: " << epoch_queued
	   << " state deep? " << state_test(PG_STATE_DEEP_SCRUB) << dendl;

  dout(7) << __func__ << " STATE_SCR? " << state_test(PG_STATE_SCRUBBING) << dendl;
  m_epoch_queued = epoch_queued;
  m_needs_sleep = true;

  m_fsm->assert_not_active();

  m_is_deep = state_test(PG_STATE_DEEP_SCRUB);
}

// RRR verify the cct used here is the same one used by the osd when accessing the conf
unsigned int PgScrubber::scrub_requeue_priority(Scrub::scrub_prio_t with_priority) const
{
  unsigned int qu_priority = m_flags.priority;

  if (with_priority == Scrub::scrub_prio_t::high_priority) {
    qu_priority =
      std::max(qu_priority, (unsigned int)m_pg->cct->_conf->osd_client_op_priority);
  }
  return qu_priority;
}

// RRR verify the cct used here is the same one used by the osd when accessing the conf
unsigned int PgScrubber::scrub_requeue_priority(Scrub::scrub_prio_t with_priority,
						unsigned int suggested_priority) const
{
  if (with_priority == Scrub::scrub_prio_t::high_priority) {
    suggested_priority = std::max(suggested_priority,
				  (unsigned int)m_pg->cct->_conf->osd_client_op_priority);
  }
  return suggested_priority;
}

// ///////////////////////////////////////////////////////////////////// //
// scrub op registration handling

bool PgScrubber::is_scrub_registered() const
{
  return !m_scrub_reg_stamp.is_zero();
}

void PgScrubber::reg_next_scrub(requested_scrub_t& request_flags, bool is_explicit)
{
  dout(10) << __func__ << ": explicit: " << is_explicit
	   << " planned.m.s: " << request_flags.must_scrub
	   << ": planned.n.a.: " << request_flags.need_auto
	   << " stamp: " << m_pg->info.history.last_scrub_stamp << dendl;

  if (!is_primary()) {
    dout(10) << __func__ << ": not a primary!" << dendl;
    return;
  }

  ceph_assert(!is_scrub_registered());

  utime_t reg_stamp;
  bool must = false;

  if (request_flags.must_scrub || request_flags.need_auto ||
      is_explicit /*|| m_scrubber->m_flags.marked_must*/) {
    // Set the smallest time that isn't utime_t()
    reg_stamp = PgScrubber::scrub_must_stamp();
    must = true;
  } else if (m_pg->info.stats.stats_invalid &&
	     m_pg->cct->_conf->osd_scrub_invalid_stats) {
    reg_stamp = ceph_clock_now();
    must = true;
  } else {
    reg_stamp = m_pg->info.history.last_scrub_stamp;
  }

  dout(9) << __func__ << " pg(" << m_pg_id << ") must: " << must
	  << " mm:" << m_flags.marked_must << " flags: " << request_flags
	  << " stamp: " << reg_stamp << dendl;

  // note down the sched_time, so we can locate this scrub, and remove it
  // later on.
  double scrub_min_interval = 0;
  double scrub_max_interval = 0;
  m_pg->pool.info.opts.get(pool_opts_t::SCRUB_MIN_INTERVAL, &scrub_min_interval);
  m_pg->pool.info.opts.get(pool_opts_t::SCRUB_MAX_INTERVAL, &scrub_max_interval);

  m_scrub_reg_stamp = m_osds->reg_pg_scrub(m_pg->info.pgid, reg_stamp, scrub_min_interval,
					   scrub_max_interval, must);
  dout(10) << __func__ << " pg(" << m_pg_id << ") register next scrub, scrub time "
	   << m_scrub_reg_stamp << ", must = " << (int)must << dendl;
}

void PgScrubber::unreg_next_scrub()
{
  if (is_scrub_registered()) {
    m_osds->unreg_pg_scrub(m_pg->info.pgid, m_scrub_reg_stamp);
    m_scrub_reg_stamp = utime_t{};
  }
}

void PgScrubber::scrub_requested(bool deep,
				 bool repair,
				 bool need_auto,
				 requested_scrub_t& req_flags)
{
  dout(9) << __func__ << " pg(" << m_pg_id << ") d/r/na:" << deep << repair << need_auto
	  << " existing-" << m_scrub_reg_stamp << " ## " << is_scrub_registered()
	  << dendl;

  /* debug code */ {
    /* debug code */ std::string format;
    /* debug code */ auto f = Formatter::create(format, "json-pretty", "json-pretty");
    /* debug code */ m_osds->dump_scrub_reservations(f);
    /* debug code */ std::stringstream o;
    /* debug code */ f->flush(o);
    /* debug code */ dout(9) << __func__ << " b4_unreg " << o.str() << dendl;
    /* debug code */ delete f;
  /* debug code */ }

  unreg_next_scrub();

  if (need_auto) {
    req_flags.need_auto = true;
  } else {
    req_flags.must_scrub = true;
    req_flags.must_deep_scrub = deep || repair;
    req_flags.must_repair = repair;
    // User might intervene, so clear this
    req_flags.need_auto = false;
    req_flags.req_scrub = true;
  }

  /* debug code */ dout(9) << __func__ << " pg(" << m_pg_id << ") planned:" << req_flags
			   << dendl;
  /* debug code */ {
    /* debug code */ std::string format;
    /* debug code */ auto f = Formatter::create(format, "json-pretty", "json-pretty");
    /* debug code */ m_osds->dump_scrub_reservations(f);
    /* debug code */ std::stringstream o;
    /* debug code */ f->flush(o);
    /* debug code */ dout(9) << __func__ << " b4_reg " << o.str() << dendl;
    /* debug code */ delete f;
  /* debug code */ }

  reg_next_scrub(req_flags, true);

  /* debug code */ {
    /* debug code */ std::string format;
    /* debug code */ auto f = Formatter::create(format, "json-pretty", "json-pretty");
    /* debug code */ m_osds->dump_scrub_reservations(f);
    /* debug code */ std::stringstream o;
    /* debug code */ f->flush(o);
    /* debug code */ dout(9) << __func__ << " af_unreg " << o.str() << dendl;
    /* debug code */ delete f;
  /* debug code */ }
}

bool PgScrubber::reserve_local()
{
  // try to create the reservation object (which translates into asking the
  // OSD for the local scrub resource). If failing - just undo it immediately

  m_local_osd_resource.emplace(m_pg, m_osds);
  if (!m_local_osd_resource->is_reserved()) {
    m_local_osd_resource.reset();
    return false;
  }

  return true;
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
}

/*
 * setting:
 * - m_subset_last_update
 * - m_max_end
 * - end
 * - start
 * By:
 * - setting tentative range based on conf and divisor
 * - requesting a partial list of elements from the backend;
 * - handling some head/clones issues
 * - ...
 *
 * The selected range is set directly into 'm_start' and 'm_end'
 */
bool PgScrubber::select_range()
{
  m_primary_scrubmap = ScrubMap{};
  m_received_maps.clear();

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
  int min_idx = std::max<int64_t>(
    3, m_pg->get_cct()->_conf->osd_scrub_chunk_min / preemption_data.chunk_divisor());

  int max_idx = std::max<int64_t>(min_idx, m_pg->get_cct()->_conf->osd_scrub_chunk_max /
					     preemption_data.chunk_divisor());

  // why mixing 'int' and int64_t? RRR

  dout(10) << __func__ << " Min: " << min_idx << " Max: " << max_idx
	   << " Div: " << preemption_data.chunk_divisor() << dendl;

  hobject_t start = m_start;
  hobject_t candidate_end;
  std::vector<hobject_t> objects;
  int ret = m_pg->get_pgbackend()->objects_list_partial(start, min_idx, max_idx, &objects,
							&candidate_end);
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

  dout(8) << __func__ << " range selected: " << m_start << " //// " << m_end << " //// "
	  << m_max_end << dendl;
  return true;
}


bool PgScrubber::write_blocked_by_scrub(const hobject_t& soid)
{
  if (soid < m_start || soid >= m_end) {
    return false;
  }

  dout(10) << __func__ << " " << soid << " can preempt? "
	   << preemption_data.is_preemptable() << dendl;
  dout(10) << __func__ << " " << soid << " already? " << preemption_data.was_preempted()
	   << dendl;

  if (preemption_data.is_preemptable()) {

    if (!preemption_data.was_preempted()) {
      dout(8) << __func__ << " " << soid << " preempted" << dendl;

      // signal the preemption
      preemption_data.do_preempt();

    } else {
      dout(10) << __func__ << " " << soid << " already preempted" << dendl;
    }
    return false;
  }
  return true;
}

bool PgScrubber::range_intersects_scrub(const hobject_t& start, const hobject_t& end)
{
  // does [start, end] intersect [scrubber.start, scrubber.m_max_end)
  return (start < m_max_end && end >= m_start);
}

/**
 *  if we are required to sleep:
 *	arrange a callback sometimes later.
 *	be sure to be able to identify a stale callback.
 *  Otherwise: perform a requeue (i.e. - rescheduling thru the OSD queue)
 *    anyway.
 */
void PgScrubber::add_delayed_scheduling()
{
  milliseconds sleep_time{0ms};
  if (m_needs_sleep) {
    double scrub_sleep = 1000.0 * m_osds->osd->scrub_sleep_time(m_flags.marked_must);
    dout(10) << __func__ << " sleep: " << scrub_sleep << dendl;
    sleep_time = milliseconds{long(scrub_sleep)};
  }
  dout(7) << __func__ << " sleep: " << sleep_time.count() << " needed? " << m_needs_sleep
	  << dendl;

  if (sleep_time.count()) {
    // schedule a transition for some 'sleep_time' ms in the future

    m_needs_sleep = false;
    m_sleep_started_at = ceph_clock_now();

    // the 'delayer' for crimson is different. Will be factored out.

    spg_t pgid = m_pg->get_pgid();
    auto callbk = new LambdaContext([osds = m_osds, pgid, scrbr = this](int r) mutable {
      PGRef pg = osds->osd->lookup_lock_pg(pgid);
      if (!pg) {
	// no PG now, so we are forced to use the OSD's context
	lgeneric_subdout(g_ceph_context, osd, 10)
	  << "scrub_requeue_callback: Could not find "
	  << "PG " << pgid << " can't complete scrub requeue after sleep" << dendl;

	// RRR ask how to send this error message
	return;
      }
      scrbr->m_needs_sleep = true;
      lgeneric_dout(scrbr->get_pg_cct(), 7)
	<< "scrub_requeue_callback: slept for "
	<< ceph_clock_now() - scrbr->m_sleep_started_at << ", re-queuing scrub" << dendl;

      scrbr->m_sleep_started_at = utime_t{};
      osds->queue_for_scrub_resched(
	&(*pg), /* RRR  should it be high? */ Scrub::scrub_prio_t::high_priority);
      pg->unlock();
    });

    m_osds->sleep_timer.add_event_after(sleep_time.count() / 1000.0f,
					callbk);  // RRR why 'float'?

  } else {
    // just a requeue
    m_osds->queue_for_scrub_resched(m_pg, Scrub::scrub_prio_t::high_priority);
  }
}

/**
 *  walk the log to find the latest update that affects our chunk
 */
eversion_t PgScrubber::search_log_for_updates() const
{
  auto& projected = m_pg->projected_log.log;
  auto pi = find_if(
    projected.crbegin(), projected.crend(),
    [this](const auto& e) -> bool { return e.soid >= m_start && e.soid < m_end; });

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

/**
 * \retval have we asked at least one replica?
 * 'false' means we are configured with no replicas, and
 * should expect no maps to arrive.
 */
bool PgScrubber::get_replicas_maps(bool replica_can_preempt)
{
  dout(10) << __func__ << " epoch_start: " << m_epoch_start
	   << " pg sis: " << m_pg->info.history.same_interval_since << dendl;

  bool do_have_replicas = false;

  m_primary_scrubmap_pos.reset();

  // ask replicas to scan and send maps
  for (const auto& i : m_pg->get_acting_recovery_backfill()) {

    if (i == m_pg_whoami)
      continue;

    do_have_replicas = true;
    m_maps_status.mark_replica_map_request(i);
    _request_scrub_map(i, m_subset_last_update, m_start, m_end, m_is_deep,
		       replica_can_preempt);
  }

  dout(7) << __func__ << " awaiting" << m_maps_status << dendl;
  return do_have_replicas;
}

bool PgScrubber::was_epoch_changed() const
{
  // for crimson we have m_pg->get_info().history.same_interval_since
  dout(10) << __func__ << " epoch_start: " << m_epoch_start
	   << " from pg: " << m_pg->get_history().same_interval_since << dendl;

  /// RRR \todo ask: why are we using the same_interval? it's OK for the primary, but
  /// for the replica?

  return m_epoch_start != m_pg->get_history().same_interval_since;
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

void PgScrubber::_request_scrub_map(pg_shard_t replica,
				    eversion_t version,
				    hobject_t start,
				    hobject_t end,
				    bool deep,
				    bool allow_preemption)
{
  ceph_assert(replica != m_pg_whoami);
  dout(10) << " scrubmap from osd." << replica << " deep " << (int)deep << dendl;

  dout(9) << __func__ << " rep: " << replica << " epos: " << m_pg->get_osdmap_epoch()
	  << " / " << m_pg->get_last_peering_reset() << dendl;

  // RRR do NOT use curmap_ epoch instead of m_pg->get_osdmap_epoch()

  MOSDRepScrub* repscrubop = new MOSDRepScrub(
    spg_t(m_pg->info.pgid.pgid, replica.shard), version, m_pg->get_osdmap_epoch(),
    m_pg->get_last_peering_reset(), start, end, deep, allow_preemption, m_flags.priority,
    m_pg->ops_blocked_by_scrub());

  // default priority, we want the rep scrub processed prior to any recovery
  // or client io messages (we are holding a lock!)
  m_osds->send_message_osd_cluster(replica.osd, repscrubop, get_osdmap_epoch());
}

void PgScrubber::cleanup_store(ObjectStore::Transaction* t)
{
  if (!m_store)
    return;

  struct OnComplete : Context {
    std::unique_ptr<Scrub::Store> store;
    explicit OnComplete(std::unique_ptr<Scrub::Store>&& store) : store(std::move(store))
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

  preemption_data.reset();
  m_pg->publish_stats_to_osd();
  m_epoch_start = m_pg->get_history().same_interval_since;

  dout(8) << __func__ << " start same_interval:" << m_epoch_start << dendl;

  //  create a new store
  {
    ObjectStore::Transaction t;
    cleanup_store(&t);
    m_store.reset(
      Scrub::Store::create(m_pg->osd->store, &t, m_pg->info.pgid, m_pg->coll));
    m_pg->osd->store->queue_transaction(m_pg->ch, std::move(t), nullptr);
  }

  m_start = m_pg->info.pgid.pgid.get_hobj_start();
  m_active = true;
}

void PgScrubber::on_replica_init()
{
  ceph_assert(!m_active);
  m_active = true;
}

void PgScrubber::_scan_snaps(ScrubMap& smap)
{
  hobject_t head;
  SnapSet snapset;

  // Test qa/standalone/scrub/osd-scrub-snaps.sh uses this message to verify
  // caller using clean_meta_map(), and it works properly.
  dout(20) << __func__ << " start" << dendl;

  for (auto i = smap.objects.rbegin(); i != smap.objects.rend(); ++i) {

    const hobject_t& hoid = i->first;
    ScrubMap::object& o = i->second;

    dout(20) << __func__ << " " << hoid << dendl;

    ceph_assert(!hoid.is_snapdir());
    if (hoid.is_head()) {
      // parse the SnapSet
      bufferlist bl;
      if (o.attrs.find(SS_ATTR) == o.attrs.end()) {
	continue;
      }
      bl.push_back(o.attrs[SS_ATTR]);
      auto p = bl.cbegin();
      try {
	decode(snapset, p);
      } catch (...) {
	continue;
      }
      head = hoid.get_head();
      continue;
    }

    if (hoid.snap < CEPH_MAXSNAP) {
      // check and if necessary fix snap_mapper
      if (hoid.get_head() != head) {
	derr << __func__ << " no head for " << hoid << " (have " << head << ")" << dendl;
	continue;
      }
      set<snapid_t> obj_snaps;
      auto p = snapset.clone_snaps.find(hoid.snap);
      if (p == snapset.clone_snaps.end()) {
	derr << __func__ << " no clone_snaps for " << hoid << " in " << snapset << dendl;
	continue;
      }
      obj_snaps.insert(p->second.begin(), p->second.end());
      set<snapid_t> cur_snaps;
      int r = m_pg->snap_mapper.get_snaps(hoid, &cur_snaps);
      if (r != 0 && r != -ENOENT) {
	derr << __func__ << ": get_snaps returned " << cpp_strerror(r) << dendl;
	ceph_abort();
      }
      if (r == -ENOENT || cur_snaps != obj_snaps) {
	ObjectStore::Transaction t;
	OSDriver::OSTransaction _t(m_pg->osdriver.get_transaction(&t));
	if (r == 0) {
	  r = m_pg->snap_mapper.remove_oid(hoid, &_t);
	  if (r != 0) {
	    derr << __func__ << ": remove_oid returned " << cpp_strerror(r) << dendl;
	    ceph_abort();
	  }
	  m_pg->osd->clog->error()
	    << "osd." << m_pg->osd->whoami << " found snap mapper error on pg "
	    << m_pg->info.pgid << " oid " << hoid << " snaps in mapper: " << cur_snaps
	    << ", oi: " << obj_snaps << "...repaired";
	} else {
	  m_pg->osd->clog->error()
	    << "osd." << m_pg->osd->whoami << " found snap mapper error on pg "
	    << m_pg->info.pgid << " oid " << hoid << " snaps missing in mapper"
	    << ", should be: " << obj_snaps << " was " << cur_snaps << " r " << r
	    << "...repaired";
	}
	m_pg->snap_mapper.add_oid(hoid, obj_snaps, &_t);

	// wait for repair to apply to avoid confusing other bits of the system.
	{
	  // RRR is this the mechanism I should use with the FSM?
	  dout(7) << __func__ << " RRR is this OK? wait on repair!" << dendl;

	  ceph::condition_variable my_cond;
	  ceph::mutex my_lock = ceph::make_mutex("PG::_scan_snaps my_lock");
	  int e = 0;
	  bool done;

	  t.register_on_applied_sync(new C_SafeCond(my_lock, my_cond, &done, &e));

	  // RRR 'e' is sent to on_applied but discarded?

	  e = m_pg->osd->store->queue_transaction(m_pg->ch, std::move(t));
	  if (e != 0) {
	    derr << __func__ << ": queue_transaction got " << cpp_strerror(e) << dendl;
	  } else {
	    std::unique_lock l{my_lock};
	    my_cond.wait(l, [&done] { return done; });
	  }
	}
      }
    }
  }
}

int PgScrubber::build_primary_map_chunk()
{
  auto ret = build_scrub_map_chunk(m_primary_scrubmap, m_primary_scrubmap_pos, m_start,
				   m_end, m_is_deep);

  if (ret == -EINPROGRESS)
    m_osds->queue_for_scrub_resched(m_pg, Scrub::scrub_prio_t::high_priority);

  return ret;
}

int PgScrubber::build_replica_map_chunk()
{
  dout(10) << __func__ << " epoch start: " << m_epoch_start << " ep q: " << m_epoch_queued
	   << dendl;
  dout(10) << __func__ << " deep: " << m_is_deep << dendl;

  auto ret = build_scrub_map_chunk(replica_scrubmap, replica_scrubmap_pos, m_start, m_end,
				   m_is_deep);

  if (ret == 0) {

    // finished!
    // In case we restarted smaller chunk, clear old data

    ScrubMap for_meta_scrub;
    m_cleaned_meta_map.clear_from(m_start);
    m_cleaned_meta_map.insert(replica_scrubmap);
    clean_meta_map(for_meta_scrub);
    _scan_snaps(for_meta_scrub);
  }

  /// RRR \todo ask: original code used low priority here. I am using the original
  /// message priority.
  if (ret == -EINPROGRESS)
    requeue_replica(m_replica_request_priority);

  return ret;
}


int PgScrubber::build_scrub_map_chunk(
  ScrubMap& map, ScrubMapBuilder& pos, hobject_t start, hobject_t end, bool deep)
{
  dout(10) << __func__ << " [" << start << "," << end << ") "
	   << " pos " << pos << " Deep: " << deep << dendl;

  // start
  while (pos.empty()) {

    pos.deep = deep;
    map.valid_through = m_pg->info.last_update;

    // objects
    vector<ghobject_t> rollback_obs;
    pos.ret =
      m_pg->get_pgbackend()->objects_list_range(start, end, &pos.ls, &rollback_obs);
    dout(10) << __func__ << " while pos empty " << pos.ret << dendl;
    if (pos.ret < 0) {
      dout(5) << "objects_list_range error: " << pos.ret << dendl;
      return pos.ret;
    }
    dout(10) << __func__ << " pos.ls.empty()? " << (pos.ls.empty() ? "+" : "-") << dendl;
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
    dout(10) << __func__ << " be r " << r << dendl;
    if (r == -EINPROGRESS) {
      dout(8 /*20*/) << __func__ << " in progress" << dendl;
      return r;
    }
  }

  // finish
  dout(8 /*20*/) << __func__ << " finishing" << dendl;
  ceph_assert(pos.done());  // how can this fail? RRR
  m_pg->_repair_oinfo_oid(map);

  dout(8 /*20*/) << __func__ << " done, got " << map.objects.size() << " items" << dendl;
  return 0;
}

/*!
 * \todo describe what we are doing here
 *
 * @param for_meta_scrub
 */
void PgScrubber::clean_meta_map(ScrubMap& for_meta_scrub)
{
  if (m_end.is_max() || m_cleaned_meta_map.objects.empty()) {
    m_cleaned_meta_map.swap(for_meta_scrub);
  } else {
    auto iter = m_cleaned_meta_map.objects.end();
    --iter;  // not empty, see 'if' clause
    auto begin = m_cleaned_meta_map.objects.begin();
    if (iter->first.has_snapset()) {
      ++iter;
    } else {
      while (iter != begin) {
	auto next = iter--;
	if (next->first.get_head() != iter->first.get_head()) {
	  ++iter;
	  break;
	}
      }
    }
    for_meta_scrub.objects.insert(begin, iter);
    m_cleaned_meta_map.objects.erase(begin, iter);
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

void PgScrubber::done_comparing_maps()
{
  scrub_compare_maps();
  m_start = m_end;
  run_callbacks();
  requeue_waiting();
}

Scrub::preemption_t* PgScrubber::get_preemptor()
{
  return &preemption_data;
}

void PgScrubber::requeue_replica(Scrub::scrub_prio_t is_high_priority)
{
  dout(10) << "-" << __func__ << dendl;
  m_osds->queue_for_rep_scrub_resched(m_pg, is_high_priority, m_flags.priority);
}

/**
 * Called for the arriving "give me your map, replica!" request. Unlike
 * the original implementation, we do not requeue the Op waiting for
 * updates. Instead - we trigger the FSM.
 */
void PgScrubber::replica_scrub_op(OpRequestRef op)
{
  auto msg = op->get_req<MOSDRepScrub>();

  dout(7) << "PgScrubber::replica_scrub(op, ...) " << m_pg->pg_id << " / ["
	  << m_pg->pg_id.pgid << " / " << m_pg->pg_id.pgid.m_pool << " / "
	  << m_pg->pg_id.pgid.m_seed << " ] // " << m_pg_whoami << dendl;
  dout(17) << __func__ << " m  ep: " << msg->map_epoch
	   << " better be >= " << m_pg->info.history.same_interval_since << dendl;
  dout(17) << __func__ << " minep: " << msg->min_epoch << dendl;
  dout(17) << __func__ << " m-deep: " << msg->deep << dendl;

  if (msg->map_epoch < m_pg->info.history.same_interval_since) {
    dout(10) << "replica_scrub_op discarding old replica_scrub from " << msg->map_epoch
	     << " < " << m_pg->info.history.same_interval_since << dendl;
    return;
  }

  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos = ScrubMapBuilder{};

  // m_replica_epoch_start is overwritten if requeued waiting for active pushes
  m_replica_epoch_start = m_pg->info.history.same_interval_since;
  m_replica_min_epoch = msg->min_epoch;
  m_start = msg->start;
  m_end = msg->end;
  m_max_end = msg->end;
  m_is_deep = msg->deep;
  m_epoch_start = m_pg->info.history.same_interval_since;
  m_replica_request_priority = static_cast<Scrub::scrub_prio_t>(msg->high_priority);
  m_flags.priority = msg->priority ? msg->priority : m_pg->get_scrub_priority();

  preemption_data.reset();
  preemption_data.force_preemptability(msg->allow_preemption);

  replica_scrubmap_pos.reset();

  // make sure the FSM is at NotActive
  m_fsm->assert_not_active();

  m_osds->queue_for_rep_scrub(m_pg, m_replica_request_priority, m_flags.priority);
}

void PgScrubber::replica_scrub(epoch_t epoch_queued)
{
  dout(7) << "replica_scrub(epoch,) RRRRRRR:\t" << m_pg->pg_id << dendl;
  dout(7) << __func__ << " epoch queued: " << epoch_queued << dendl;
  dout(7) << __func__ << " epst: " << m_epoch_start
	  << " better be >= " << m_pg->info.history.same_interval_since << dendl;
  dout(7) << __func__ << " m_is_deep: " << m_is_deep << dendl;

  if (m_pg->pg_has_reset_since(epoch_queued)) {
    // RRR verify
    dout(7) << "replica_scrub(epoch,) - reset!" << dendl;
    send_epoch_changed();
    return;
  }

  if (was_epoch_changed()) {
    // RRR verify
    dout(7) << "replica_scrub(epoch,) - epoch!" << dendl;
    send_epoch_changed();  // RRR
    return;
  }

  // RR check directly for epoch number?

  if (is_primary()) {
    // we will never get here unless the epoch changed.
    send_epoch_changed();
    // a bug. RRR assert
    return;
  }

  send_start_replica();
}

void PgScrubber::replica_scrub_resched(epoch_t epoch_queued)
{
  dout(7) << "replica_scrub_resched(epoch,) RRRRRRRRRRRR:\t"
	  << (was_epoch_changed() ? "<epoch!>" : "<>") << dendl;
  dout(7) << "replica_scrub(epoch,) RRRRRRR:\t" << m_pg->pg_id << dendl;
  dout(7) << __func__ << " m_is_deep: " << m_is_deep << dendl;
  dout(7) << __func__ << " epst: " << m_epoch_start
	  << " better be >= " << m_pg->info.history.same_interval_since << dendl;

  if (m_pg->pg_has_reset_since(epoch_queued)) {
    // RRR verify
    dout(7) << "replica_scrub(epoch,) - reset!" << dendl;
    send_epoch_changed();
    return;
  }

  if (was_epoch_changed()) {
    // RRR verify
    dout(7) << "replica_scrub(epoch,) - epoch!" << dendl;
    send_epoch_changed();
    return;
  }

  if (is_primary()) {
    // we will never get here unless the epoch changed.
    send_epoch_changed();
    // a bug. RRR assert
    return;
  }

  send_sched_replica();
}

void PgScrubber::queue_pushes_update(Scrub::scrub_prio_t is_high_priority)
{
  dout(10) << __func__ << ": queueing" << dendl;
  m_osds->queue_scrub_pushes_update(m_pg, is_high_priority);
}

void PgScrubber::queue_pushes_update(bool with_priority)
{
  dout(10) << __func__ << ": queueing" << dendl;
  m_osds->queue_scrub_pushes_update(m_pg,
				    static_cast<Scrub::scrub_prio_t>(with_priority));
}

void PgScrubber::set_op_parameters(requested_scrub_t& request)
{
  dout(10) << __func__ << " input: " << request << dendl;

  m_flags.check_repair = request.check_repair;
  m_flags.auto_repair = request.auto_repair || request.need_auto;
  m_flags.marked_must = request.must_scrub;
  m_flags.required = request.req_scrub;

  m_flags.priority = (request.must_scrub || request.need_auto)
		       ? get_pg_cct()->_conf->osd_requested_scrub_priority
		       : m_pg->get_scrub_priority();
  // RRR which CCT to use here?

  state_set(PG_STATE_SCRUBBING);

  // will we be deep-scrubbing?
  bool must_deep_scrub =
    request.must_deep_scrub || request.need_auto || request.time_for_deep;
  if (must_deep_scrub) {
    state_set(PG_STATE_DEEP_SCRUB);
  }

  if (request.must_repair || m_flags.auto_repair) {
    state_set(PG_STATE_REPAIR);
  }

  // the publishing here seems to be required for tests synchronization
  m_pg->publish_stats_to_osd();

  m_flags.deep_scrub_on_error = request.deep_scrub_on_error;

  dout(10) << __func__ << " output 1: " << m_flags << " priority: " << m_flags.priority
	   << dendl;
  dout(10) << __func__
	   << " output 2: " << (state_test(PG_STATE_DEEP_SCRUB) ? "deep" : "shallow")
	   << dendl;
  dout(10) << __func__
	   << " output 3: " << (m_flags.deep_scrub_on_error ? "+deepOnE" : "-deepOnE")
	   << dendl;
  dout(10) << __func__
	   << " output 4: " << (state_test(PG_STATE_REPAIR) ? "repair" : "-no-rep-")
	   << dendl;

  request = requested_scrub_t{};
}

/**
 *  RRR \todo ask why we collect from acting+recovery+backfill, but use the size of
 *  only the acting set
 */
void PgScrubber::scrub_compare_maps()
{
  dout(7) << __func__ << " has maps, analyzing" << dendl;

  // construct authoritative scrub map for type-specific scrubbing
  m_cleaned_meta_map.insert(m_primary_scrubmap);
  map<hobject_t, pair<std::optional<uint32_t>, std::optional<uint32_t>>> missing_digest;

  map<pg_shard_t, ScrubMap*> maps;
  maps[m_pg_whoami] = &m_primary_scrubmap;

  for (const auto& i : m_pg->get_acting_recovery_backfill()) {
    if (i == m_pg_whoami)
      continue;
    dout(2) << __func__ << " replica " << i << " has "
	    << m_received_maps[i].objects.size() << " items" << dendl;
    maps[i] = &m_received_maps[i];
  }

  set<hobject_t> master_set;

  // Construct master set
  for (const auto map : maps) {
    for (const auto& i : map.second->objects) {
      master_set.insert(i.first);
    }
  }

  stringstream ss;
  m_pg->get_pgbackend()->be_omap_checks(maps, master_set, m_omap_stats, ss);

  if (!ss.str().empty()) {
    m_osds->clog->warn(ss);
  }

  if (m_pg->recovery_state.get_acting().size() > 1) {

    // RRR add a comment here

    dout(10) << __func__ << "  comparing replica scrub maps" << dendl;

    // Map from object with errors to good peer
    map<hobject_t, list<pg_shard_t>> authoritative;

    dout(2) << __func__ << m_pg->get_primary() << " has "
	    << m_primary_scrubmap.objects.size() << " items" << dendl;

    ss.str("");
    ss.clear();

    m_pg->get_pgbackend()->be_compare_scrubmaps(
      maps, master_set, state_test(PG_STATE_REPAIR), m_missing, m_inconsistent,
      authoritative, missing_digest, m_shallow_errors, m_deep_errors, m_store.get(),
      m_pg->info.pgid, m_pg->recovery_state.get_acting(), ss);
    dout(2) << ss.str() << dendl;

    if (!ss.str().empty()) {
      m_osds->clog->error(ss);
    }

    for (auto& [hobj, shrd_list] : authoritative) {
      list<pair<ScrubMap::object, pg_shard_t>> good_peers;
      for (const auto shrd : shrd_list) {
	good_peers.emplace_back(maps[shrd]->objects[hobj], shrd);
      }

      m_authoritative.emplace(hobj, good_peers);
    }

    for (const auto& [hobj, shrd_list] : authoritative) {

      // RRR a comment?

      m_cleaned_meta_map.objects.erase(hobj);
      m_cleaned_meta_map.objects.insert(*(maps[shrd_list.back()]->objects.find(hobj)));
    }
  }

  ScrubMap for_meta_scrub;
  clean_meta_map(for_meta_scrub);  // RRR understand what's the meaning of cleaning here

  // ok, do the pg-type specific scrubbing

  // (Validates consistency of the object info and snap sets)
  scrub_snapshot_metadata(for_meta_scrub, missing_digest);

  // RRR ask: this comment??
  // Called here on the primary can use an authoritative map if it isn't the primary
  _scan_snaps(for_meta_scrub);

  if (!m_store->empty()) {

    if (state_test(PG_STATE_REPAIR)) {
      dout(10) << __func__ << ": discarding scrub results" << dendl;
      m_store->flush(nullptr);
    } else {
      dout(10) << __func__ << ": updating scrub object" << dendl;
      ObjectStore::Transaction t;
      m_store->flush(&t);
      m_pg->osd->store->queue_transaction(m_pg->ch, std::move(t), nullptr);
    }
  }
}

void PgScrubber::replica_update_start_epoch()
{
  dout(10) << __func__ << " start:" << m_pg->info.history.same_interval_since << dendl;
  m_replica_epoch_start = m_pg->info.history.same_interval_since;
}

/**
 * Send the requested map back to the primary (or - if we
 * were preempted - let the primary know).
 */
void PgScrubber::send_replica_map(bool was_preempted)
{
  dout(10) << __func__ << " min epoch:" << m_replica_min_epoch
	   << " ep start:" << m_replica_epoch_start << dendl;

  MOSDRepScrubMap* reply =
    new MOSDRepScrubMap(spg_t(m_pg->info.pgid.pgid, m_pg->get_primary().shard),
			m_replica_min_epoch, m_pg_whoami);

  reply->preempted = was_preempted;
  ::encode(replica_scrubmap, reply->get_data());

  m_osds->send_message_osd_cluster(m_pg->get_primary().osd, reply, m_replica_min_epoch);
}

/**
 *  - if the replica lets us know it was interrupted, we mark the chunk as interrupted.
 *    The state-machine will react to that when all replica maps are received.
 *  - when all maps are received, we signal the FSM with the GotReplicas event (see
 * scrub_send_replmaps_ready()). Note that due to the no-reentrancy limitations of the
 * FSM, we do not 'process' the event directly. Instead - it is queued for the OSD to
 * handle (well - the incoming message is marked for fast dispatching, which is an even
 * better reason for handling it via the queue).
 */
void PgScrubber::map_from_replica(OpRequestRef op)
{
  auto m = op->get_req<MOSDRepScrubMap>();
  dout(7) << __func__ << " " << *m << dendl;

  if (m->map_epoch < m_pg->info.history.same_interval_since) {
    dout(10) << __func__ << " discarding old from " << m->map_epoch << " < "
	     << m_pg->info.history.same_interval_since << dendl;
    return;
  }

  op->mark_started();

  auto p = const_cast<bufferlist&>(m->get_data()).cbegin();

  m_received_maps[m->from].decode(p, m_pg->info.pgid.pool());
  dout(10) << "map version is " << m_received_maps[m->from].valid_through << dendl;
  // dout(10) << __func__ << " waiting_on _whom was " << waiting_on _whom << dendl;

  [[maybe_unused]] auto [is_ok, err_txt] = m_maps_status.mark_arriving_map(m->from);
  ceph_assert(is_ok);  // and not an error message, as this was the original code

  if (m->preempted) {
    dout(10) << __func__ << " replica was preempted, setting flag" << dendl;
    ceph_assert(preemption_data.is_preemptable());  // otherwise - how dare the replica!
    preemption_data.do_preempt();
  }

  if (m_maps_status.are_all_maps_available()) {
    dout(10) << __func__ << " osd-queuing GotReplicas" << dendl;
    m_osds->queue_scrub_got_repl_maps(m_pg, m_pg->is_scrub_blocking_ops());
  }
}

/// we are a replica being asked by the Primary to reserve OSD resources for
/// scrubbing
void PgScrubber::handle_scrub_reserve_request(OpRequestRef op)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  if (m_remote_osd_resource.has_value() && m_remote_osd_resource->is_reserved()) {
    dout(10) << __func__ << " ignoring reserve request: Already reserved" << dendl;
    return;
  }

  bool granted{false};

  // RRR check the 'cct' used here
  if (m_pg->cct->_conf->osd_scrub_during_recovery || !m_osds->is_recovery_active()) {

    m_remote_osd_resource.emplace(m_pg, m_osds);
    // OSD resources allocated?
    granted = m_remote_osd_resource->is_reserved();
    if (!granted) {
      // just forget it
      m_remote_osd_resource.reset();
      dout(20) << __func__ << ": failed to reserve remotely" << dendl;
    }
  }

  dout(7) << __func__ << " reserved? " << (granted ? "yes" : "no") << dendl;

  auto m = op->get_req<MOSDScrubReserve>();
  Message* reply = new MOSDScrubReserve(
    spg_t(m_pg->info.pgid.pgid, m_pg->get_primary().shard), m->map_epoch,
    granted ? MOSDScrubReserve::GRANT : MOSDScrubReserve::REJECT, m_pg_whoami);

  m_osds->send_message_osd_cluster(reply, op->get_req()->get_connection());
}

void PgScrubber::handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  if (m_reservations.has_value()) {
    m_reservations->handle_reserve_grant(op, from);
  } else {
    derr << __func__ << ": replica scrub reservations that will be leaked!" << dendl;
  }
}

void PgScrubber::handle_scrub_reserve_reject(OpRequestRef op, pg_shard_t from)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  if (m_reservations.has_value()) {
    m_reservations->handle_reserve_reject(op, from);
  } else {
    ;  // No active reservation process. No action is required.
  }
}

void PgScrubber::handle_scrub_reserve_release(OpRequestRef op)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();
  m_remote_osd_resource.reset();
}

void PgScrubber::clear_scrub_reservations()
{
  dout(7) << __func__ << dendl;
  m_reservations.reset();	  // the remote reservations
  m_local_osd_resource.reset();	  // the local reservation
  m_remote_osd_resource.reset();  // we as replica reserved for a Primary
}

/**
 * send a replica (un)reservation request to the acting set
 *
 * @param opcode - one of (as yet untyped) MOSDScrubReserve::REQUEST
 *                  or MOSDScrubReserve::RELEASE
 */
void PgScrubber::message_all_replicas(int32_t opcode, std::string_view op_text)
{
  ceph_assert(m_pg->recovery_state.get_backfill_targets()
		.empty());  // RRR ask: (the code was copied as is) Why checking here?

  std::vector<std::pair<int, Message*>> messages;
  messages.reserve(m_pg->get_actingset().size());

  epoch_t epch = get_osdmap_epoch();

  for (auto& p : m_pg->get_actingset()) {

    if (p == m_pg_whoami)
      continue;

    dout(10) << "scrub requesting " << op_text << " from osd." << p << " ep: " << epch
	     << dendl;
    Message* m = new MOSDScrubReserve(spg_t(m_pg->info.pgid.pgid, p.shard), epch, opcode,
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

[[nodiscard]] bool PgScrubber::scrub_process_inconsistent()
{
  dout(7) << __func__ << ": checking authoritative" << dendl;

  bool repair = state_test(PG_STATE_REPAIR);
  const bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));
  dout(8) << __func__ << " sdeep: " << deep_scrub << " is_d: " << m_is_deep
	  << " repair: " << repair << dendl;

  // authoritative only store objects which are missing or inconsistent.
  if (!m_authoritative.empty()) {

    stringstream ss;
    ss << m_pg->info.pgid << " " << mode << " " << m_missing.size() << " missing, "
       << m_inconsistent.size() << " inconsistent objects";
    dout(2) << ss.str() << dendl;
    m_osds->clog->error(ss);

    if (repair) {
      state_clear(PG_STATE_CLEAN);

      for (const auto& [hobj, shrd_list] : m_authoritative) {

	auto missing_entry = m_missing.find(hobj);

	if (missing_entry != m_missing.end()) {
	  m_pg->repair_object(hobj, shrd_list, missing_entry->second);
	  m_fixed_count += missing_entry->second.size();
	}

	if (m_inconsistent.count(hobj)) {
	  m_pg->repair_object(hobj, shrd_list, m_inconsistent[hobj]);
	  m_fixed_count += missing_entry->second.size();
	}
      }
    }
  }
  return (!m_authoritative.empty() && repair);
}

/*
 * note: only called for the Primary.
 */
void PgScrubber::scrub_finish()
{
  dout(7) << __func__ << " flags b4: " << m_flags
	  << " dsonerr: " << m_flags.deep_scrub_on_error << dendl;
  dout(7) << __func__ << " deep: state " << state_test(PG_STATE_DEEP_SCRUB) << dendl;
  dout(7) << __func__ << " deep: var " << m_is_deep << dendl;
  dout(7) << __func__
	  << " repair in p.na/p.ar/flags: " << m_pg->m_planned_scrub.auto_repair
	  << m_pg->m_planned_scrub.need_auto << m_flags.auto_repair << dendl;
  dout(7) << __func__ << " auth sz " << m_authoritative.size() << dendl;

  bool do_auto_scrub = false;

  ceph_assert(m_pg->is_locked());

  // if the repair request comes from auto-repair and large number of errors,
  // we would like to cancel auto-repair

  bool repair = state_test(PG_STATE_REPAIR);
  if (repair && m_flags.auto_repair &&
      m_authoritative.size() > m_pg->cct->_conf->osd_scrub_auto_repair_num_errors) {

    dout(7) << __func__ << " undoing the repair" << dendl;
    state_clear(PG_STATE_REPAIR);
    repair = false;
  }

  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));

  // if a regular scrub had errors within the limit, do a deep scrub to auto repair
  if (m_flags.deep_scrub_on_error && m_authoritative.size() &&
      m_authoritative.size() <= m_pg->cct->_conf->osd_scrub_auto_repair_num_errors) {
    ceph_assert(!deep_scrub);  // RRR what guarantees that?
    do_auto_scrub = true;
    dout(8 /*20*/) << __func__ << " Try to auto repair after scrub errors" << dendl;
  }

  // also - if we have a fake (unit-testing related) request:
  // for UT: if (/*m_flags.deep_scrub_on_error &&*/ m_pg->pg_id.pgid.m_seed == 4 &&
  // for UT:     (--fake_count == 0)) {
  // for UT:  dout(7) << __func__ << " faking errors RRRR" << dendl;
  // for UT:  // for qa tests, 28/7/20: do_auto_scrub = true;
  // for UT: }

  m_flags.deep_scrub_on_error =
    false;  // RRR probably wrong. Maybe should change the planned_...

  // type-specific finish (can tally more errors)
  _scrub_finish();

  dout(7) << __func__ << ": af _scrub_finish(). flags: " << m_flags << dendl;
  dout(7) << __func__ << ": af state-deep-scrub: " << state_test(PG_STATE_DEEP_SCRUB)
	  << dendl;

  bool has_error = scrub_process_inconsistent();
  dout(10) << __func__ << ": from scrub_p_inc: " << has_error << dendl;

  {
    stringstream oss;
    oss << m_pg->info.pgid.pgid << " " << mode << " ";
    int total_errors = m_shallow_errors + m_deep_errors;
    if (total_errors)
      oss << total_errors << " errors";
    else
      oss << "ok";
    if (!deep_scrub && m_pg->info.stats.stats.sum.num_deep_scrub_errors)
      oss << " ( " << m_pg->info.stats.stats.sum.num_deep_scrub_errors
	  << " remaining deep scrub error details lost)";
    if (repair)
      oss << ", " << m_fixed_count << " fixed";
    if (total_errors)
      m_osds->clog->error(oss);
    else
      m_osds->clog->debug(oss);
  }

  dout(7) << __func__ << ": status: " << (has_error ? " he " : "-")
	  << (repair ? " rp " : "-") << dendl;

  // Since we don't know which errors were fixed, we can only clear them
  // when every one has been fixed.
  if (repair) {
    if (m_fixed_count == m_shallow_errors + m_deep_errors) {

      ceph_assert(deep_scrub);
      m_shallow_errors = 0;
      m_deep_errors = 0;
      dout(8 /*20*/) << __func__ << " All may be fixed" << dendl;

    } else if (has_error) {

      // Deep scrub in order to get corrected error counts
      m_pg->scrub_after_recovery = true;
      m_pg->m_planned_scrub.req_scrub =
	m_pg->m_planned_scrub.req_scrub || m_flags.required;

      dout(8 /*20*/) << __func__ << " Current 'required': " << m_flags.required
		     << " Planned 'req_scrub': "
		     << m_pg->m_planned_scrub.req_scrub
		     //<< " Set scrub_after_recovery, req_scrub= " << saved_req_scrub
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
      [this, deep_scrub](auto& history, auto& stats) {
	dout(10) << "m_pg->recovery_state.update_stats()" << dendl;
	utime_t now = ceph_clock_now();
	history.last_scrub = m_pg->recovery_state.get_info().last_update;
	history.last_scrub_stamp = now;
	if (m_is_deep) {
	  history.last_deep_scrub = m_pg->recovery_state.get_info().last_update;
	  history.last_deep_scrub_stamp = now;
	}

	if (deep_scrub) {
	  if ((m_shallow_errors == 0) && (m_deep_errors == 0))
	    history.last_clean_scrub_stamp = now;
	  stats.stats.sum.num_shallow_scrub_errors = m_shallow_errors;
	  stats.stats.sum.num_deep_scrub_errors = m_deep_errors;
	  stats.stats.sum.num_large_omap_objects = m_omap_stats.large_omap_objects;
	  stats.stats.sum.num_omap_bytes = m_omap_stats.omap_bytes;
	  stats.stats.sum.num_omap_keys = m_omap_stats.omap_keys;
	  dout(10 /*25*/) << "scrub_finish shard " << m_pg_whoami
			  << " num_omap_bytes = " << stats.stats.sum.num_omap_bytes
			  << " num_omap_keys = " << stats.stats.sum.num_omap_keys
			  << dendl;
	} else {
	  stats.stats.sum.num_shallow_scrub_errors = m_shallow_errors;
	  // XXX: last_clean_scrub_stamp doesn't mean the pg is not inconsistent
	  // because of deep-scrub errors
	  if (m_shallow_errors == 0)
	    history.last_clean_scrub_stamp = now;
	}
	stats.stats.sum.num_scrub_errors = stats.stats.sum.num_shallow_scrub_errors +
					   stats.stats.sum.num_deep_scrub_errors;
	if (m_flags.check_repair) {
	  m_flags.check_repair = false;
	  if (m_pg->info.stats.stats.sum.num_scrub_errors) {
	    state_set(PG_STATE_FAILED_REPAIR);
	    dout(10) << "scrub_finish " << m_pg->info.stats.stats.sum.num_scrub_errors
		     << " error(s) still present after re-scrub" << dendl;
	  }
	}
	return true;
      },
      &t);
    int tr = m_osds->store->queue_transaction(m_pg->ch, std::move(t), nullptr);
    ceph_assert(tr == 0);

    if (!m_pg->snap_trimq.empty()) {
      dout(10) << "scrub finished, requeuing snap_trimmer" << dendl;
      m_pg->snap_trimmer_scrub_complete();
    }
  }

  if (has_error) {
    m_pg->queue_peering_event(PGPeeringEventRef(std::make_shared<PGPeeringEvent>(
      get_osdmap_epoch(), get_osdmap_epoch(), PeeringState::DoRecovery())));
  } else {
    state_clear(PG_STATE_REPAIR);
  }

  cleanup_on_finish();
  if (do_auto_scrub) {
    scrub_requested(false, false, true, m_pg->m_planned_scrub);
  }

  if (m_pg->is_active() && m_pg->is_primary()) {
    m_pg->recovery_state.share_pg_info();
  }
}

Scrub::FsmNext PgScrubber::on_digest_updates()
{
  dout(7) << __func__ << " #pending: " << num_digest_updates_pending << " are we done? "
	  << num_digest_updates_pending << (m_end.is_max() ? " <L> " : " <nl> ") << dendl;

  if (num_digest_updates_pending == 0) {

    // got all updates, and finished with this chunk. Any more?
    if (m_end.is_max()) {
      scrub_finish();
      return Scrub::FsmNext::goto_notactive;
    } else {
      // go get a new chunk (via "requeue")
      preemption_data.reset();
      return Scrub::FsmNext::next_chunk;
    }
  } else {
    return Scrub::FsmNext::do_discard;
  }
}

void PgScrubber::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrubber");
  f->dump_stream("epoch_start") << m_epoch_start;
  f->dump_bool("active", m_active);
  if (m_active) {
    // f->dump_string("state", state_string(state));
    f->dump_stream("start") << m_start;
    f->dump_stream("end") << m_end;
    f->dump_stream("m_max_end") << m_max_end;
    f->dump_stream("subset_last_update") << m_subset_last_update;
    f->dump_bool("deep", m_is_deep);
    f->dump_bool("must_scrub", m_flags.marked_must);
    // RRR f->dump_bool("must_deep_scrub", must_deep_scrub);
    // RRR f->dump_bool("must_repair", must_repair);
    // RRR f->dump_bool("need_auto", need_auto);
    f->dump_bool("req_scrub", m_flags.required);
    // RRR f->dump_bool("time_for_deep", time_for_deep);
    f->dump_bool("auto_repair", m_flags.auto_repair);
    f->dump_bool("check_repair", m_flags.check_repair);
    f->dump_bool("deep_scrub_on_error", m_flags.deep_scrub_on_error);
    f->dump_stream("scrub_reg_stamp") << m_scrub_reg_stamp;  // utime_t
    // f->dump_stream("waiting_on_whom") << waiting_on_whom;   // set<pg_shard_t>
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
  }
  f->close_section();
}


void PgScrubber::handle_query_state(ceph::Formatter* f)
{
  dout(7) << __func__ << dendl;

  f->open_object_section("scrub");
  f->dump_stream("scrubber.epoch_start") << m_epoch_start;
  f->dump_bool("scrubber.active", m_active);
  // RRR needed? f->dump_string("scrubber.state",
  // PG::Scrubber::state_string(scrubber.state));
  f->dump_stream("scrubber.start") << m_start;
  f->dump_stream("scrubber.end") << m_end;
  f->dump_stream("scrubber.m_max_end") << m_max_end;
  f->dump_stream("scrubber.m_subset_last_update") << m_subset_last_update;
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
  dout(7) << __func__ << dendl;
}

PgScrubber::PgScrubber(PG* pg)
    : m_pg{pg}
    , m_pg_id{pg->pg_id}
    , m_osds{static_cast<OSDService*>(m_pg->osd)}
    , m_pg_whoami{pg->pg_whoami}
    , m_epoch_queued{0}
    , preemption_data{pg}
{
  dout(11) << " creating PgScrubber for " << pg->pg_id << " / [" << pg->pg_id.pgid
	   << " / " << pg->pg_id.pgid.m_pool << " / " << pg->pg_id.pgid.m_seed << " ] // "
	   << m_pg_whoami << dendl;
  m_fsm = std::make_unique<ScrubMachine>(m_pg, this);
  m_fsm->initiate();
}

void PgScrubber::reserve_replicas()
{
  dout(10) << __func__ << dendl;
  m_reservations.emplace(m_pg, this, m_pg_whoami);
}

//  called only for normal end-of-scrub, and only for a Primary
void PgScrubber::cleanup_on_finish()
{
  dout(7) << __func__ << dendl;
  ceph_assert(m_pg->is_locked());

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);
  m_pg->publish_stats_to_osd();

  m_reservations.reset();
  m_local_osd_resource.reset();

  m_pg->requeue_ops(m_pg->waiting_for_scrub);

  reset_internal_state();
  // type-specific state clear
  _scrub_clear_state();
}

// uses process_event(), so must be invoked externally
void PgScrubber::scrub_clear_state(bool keep_repair_state)
{
  dout(7) << __func__ << dendl;

  clear_pgscrub_state(keep_repair_state);
  m_fsm->process_event(FullReset{});
}

/*
 * note: does not access the state-machine
 */
void PgScrubber::clear_pgscrub_state(bool keep_repair_state)
{
  dout(7) << __func__ << dendl;
  ceph_assert(m_pg->is_locked());

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);
  if (!keep_repair_state)
    state_clear(PG_STATE_REPAIR);

  clear_scrub_reservations();
  m_pg->publish_stats_to_osd();

  m_pg->requeue_ops(m_pg->waiting_for_scrub);

  reset_internal_state();

  // type-specific state clear
  _scrub_clear_state();
}

void PgScrubber::replica_handling_done()
{
  dout(7) << __func__ << dendl;

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);

  // make sure we cleared the reservations!

  preemption_data.reset();
  m_maps_status.reset();
  m_received_maps.clear();

  m_start = hobject_t{};
  m_end = hobject_t{};
  m_max_end = hobject_t{};
  m_subset_last_update = eversion_t{};
  m_shallow_errors = 0;
  m_deep_errors = 0;
  m_fixed_count = 0;
  m_omap_stats = (const struct omap_stat_t){0};

  run_callbacks();
  m_inconsistent.clear();
  m_missing.clear();
  m_authoritative.clear();
  num_digest_updates_pending = 0;
  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos.reset();

  m_cleaned_meta_map = ScrubMap{};
  m_needs_sleep = true;
  m_sleep_started_at = utime_t{};

  m_active = false;
  m_pg->publish_stats_to_osd();
}


/*
 * note: performs run_callbacks()
 * note: reservations-related variables are not reset here
 */
void PgScrubber::reset_internal_state()
{
  dout(7) << __func__ << dendl;

  preemption_data.reset();
  m_maps_status.reset();
  m_received_maps.clear();

  m_start = hobject_t{};
  m_end = hobject_t{};
  m_max_end = hobject_t{};
  m_subset_last_update = eversion_t{};
  m_shallow_errors = 0;
  m_deep_errors = 0;
  m_fixed_count = 0;
  m_omap_stats = (const struct omap_stat_t){0};

  run_callbacks();

  m_inconsistent.clear();
  m_missing.clear();
  m_authoritative.clear();
  num_digest_updates_pending = 0;
  m_primary_scrubmap = ScrubMap{};
  m_primary_scrubmap_pos.reset();
  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos.reset();
  m_cleaned_meta_map = ScrubMap{};
  m_needs_sleep = true;
  m_sleep_started_at = utime_t{};

  m_flags = scrub_flags_t{};

  m_active = false;
}

const OSDMapRef& PgScrubber::get_osdmap() const
{
  return m_pg->get_osdmap();  // RRR understand why we cannot use curmap_
}

ostream& operator<<(ostream& out, const PgScrubber& scrubber)
{
  return out << scrubber.m_flags;
}

ostream& PgScrubber::show(ostream& out) const
{
  return out << " [ " << m_pg_id << ": " << /*for now*/ m_flags << " ] ";
}

// ///////////////////// preemption_data_t //////////////////////////////////

PgScrubber::preemption_data_t::preemption_data_t(PG* pg) : m_pg{pg}
{
  m_left = static_cast<int>(
    m_pg->get_cct()->_conf.get_val<uint64_t>("osd_scrub_max_preemptions"));
}

void PgScrubber::preemption_data_t::reset()
{
  std::lock_guard<std::mutex> lk{m_preemption_lock};

  m_preemptable = false;
  m_preempted = false;
  m_left =
    static_cast<int>(m_pg->cct->_conf.get_val<uint64_t>("osd_scrub_max_preemptions"));
  m_size_divisor = 1;
}


// ///////////////////// ReplicaReservations //////////////////////////////////
namespace Scrub {

void ReplicaReservations::release_replica(pg_shard_t peer, epoch_t epoch)
{
  dout(7) << __func__ << " <ReplicaReservations> release-> " << peer << dendl;

  Message* m = new MOSDScrubReserve(spg_t(m_pg->info.pgid.pgid, peer.shard), epoch,
				    MOSDScrubReserve::RELEASE, m_pg->pg_whoami);
  m_osds->send_message_osd_cluster(peer.osd, m, epoch);
}

ReplicaReservations::ReplicaReservations(PG* pg, PgScrubber* scrubber, pg_shard_t whoami)
    : m_pg{pg}
    , m_scrubber{scrubber}
    , m_acting_set{pg->get_actingset()}
    , m_osds{static_cast<OSDService*>(m_pg->osd)}
    , m_pending{static_cast<int>(m_acting_set.size()) - 1}
{
  epoch_t epoch = m_pg->get_osdmap_epoch();

  // handle the special case of no replicas
  if (!m_pending) {
    // just signal the scrub state-machine to continue
    send_all_done();

  } else {

    for (auto p : m_acting_set) {
      if (p == whoami)
	continue;
      Message* m = new MOSDScrubReserve(spg_t(m_pg->info.pgid.pgid, p.shard), epoch,
					MOSDScrubReserve::REQUEST, m_pg->pg_whoami);
      m_osds->send_message_osd_cluster(p.osd, m, epoch);
      m_waited_for_peers.push_back(p);
      dout(7) << __func__ << " <ReplicaReservations> reserve<-> " << p.osd << dendl;
    }
  }
}

void ReplicaReservations::send_all_done()
{
  /// \todo find out: which priority should we use here?
  m_osds->queue_for_scrub_granted(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::send_reject()
{
  /// \todo find out: which priority should we use here?
  m_osds->queue_for_scrub_denied(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::release_all()
{
  dout(7) << __func__ << " " << m_reserved_peers << dendl;

  m_had_rejections = true;  // preventing late-coming responses from triggering events
  epoch_t epoch = m_pg->get_osdmap_epoch();

  for (auto p : m_reserved_peers) {
    release_replica(p, epoch);
  }
  m_reserved_peers.clear();

  // note: the release will follow on the heels of the request. When tried otherwise,
  // grants that followed a reject arrived after the whole scrub machine-state was
  // reset, causing leaked reservations.
  if (m_pending) {
    for (auto p : m_waited_for_peers) {
      release_replica(p, epoch);
    }
  }
  m_waited_for_peers.clear();
}

ReplicaReservations::~ReplicaReservations()
{
  m_had_rejections = true;  // preventing late-coming responses from triggering events

  // send un-reserve messages to all reserved replicas. We do not wait for answer (there
  // wouldn't be one). Other incoming messages will be discarded on the way, by our
  // owner.
  release_all();
}

/**
 *  \ATTN we would not reach here if the ReplicaReservation object managed by the
 * scrubber was reset.
 */
void ReplicaReservations::handle_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(7) << __func__ << " <ReplicaReservations> granted-> " << from << dendl;
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  // are we forced to reject the reservation?
  if (m_had_rejections) {

    dout(10) << " rejecting late-coming reservation from " << from << dendl;
    release_replica(from, m_pg->get_osdmap_epoch());

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    dout(10) << " already had osd." << from << " reserved" << dendl;

  } else {

    dout(10) << " osd." << from << " scrub reserve = success" << dendl;
    m_reserved_peers.push_back(from);
    if (--m_pending == 0) {
      send_all_done();
    }
  }
}

void ReplicaReservations::handle_reserve_reject(OpRequestRef op, pg_shard_t from)
{
  dout(7) << __func__ << " <ReplicaReservations> rejected-> " << from << dendl;
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  if (m_had_rejections) {

    // our failure was already handled when the first rejection arrived
    dout(10) << " ignoring late-coming rejection from " << from << dendl;

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    dout(10) << " already had osd." << from << " reserved" << dendl;

  } else {

    dout(10) << " osd." << from << " scrub reserve = fail" << dendl;
    m_had_rejections = true;  // preventing any additional notifications
    --m_pending;	      // not sure we need this bookkeeping anymore
    send_reject();
  }
}

// ///////////////////// LocalReservation //////////////////////////////////

LocalReservation::LocalReservation(PG* pg, OSDService* osds)
    : m_pg{pg}	// holding the "whole PG" for dout() sake
    , m_osds{osds}
{
  if (!m_osds->inc_scrubs_local()) {
    dout(7) << __func__ << ": failed to reserve locally " << dendl;
    // the failure is signalled by not having m_holding_local_reservation set
    return;
  }

  dout(20) << __func__ << ": local OSD scrub resources reserved" << dendl;
  m_holding_local_reservation = true;
}

void LocalReservation::early_release()
{
  if (m_holding_local_reservation) {
    m_holding_local_reservation = false;
    m_osds->dec_scrubs_local();
    dout(20) << __func__ << ": local OSD scrub resources freed" << dendl;
  }
}

LocalReservation::~LocalReservation()
{
  early_release();
}


// ///////////////////// ReservedByRemotePrimary ///////////////////////////////

ReservedByRemotePrimary::ReservedByRemotePrimary(PG* pg, OSDService* osds)
    : m_pg{pg}	// holding the "whole PG" for dout() sake
    , m_osds{osds}
{
  if (!m_osds->inc_scrubs_remote()) {
    dout(7) << __func__ << ": failed to reserve at Primary request" << dendl;
    // the failure is signalled by not having m_reserved_by_remote_primary set
    return;
  }

  dout(20) << __func__ << ": scrub resources reserved at Primary request" << dendl;
  m_reserved_by_remote_primary = true;
}

void ReservedByRemotePrimary::early_release()
{
  dout(20) << "ReservedByRemotePrimary::" << __func__ << ": "
	   << m_reserved_by_remote_primary << dendl;
  if (m_reserved_by_remote_primary) {
    m_reserved_by_remote_primary = false;
    m_osds->dec_scrubs_remote();
    dout(20) << __func__ << ": scrub resources held for Primary were freed" << dendl;
  }
}

ReservedByRemotePrimary::~ReservedByRemotePrimary()
{
  early_release();
}

// ///////////////////// MapsCollectionStatus ////////////////////////////////

auto MapsCollectionStatus::mark_arriving_map(pg_shard_t from)
  -> std::tuple<bool, std::string_view>
{
  auto fe = std::find(m_maps_awaited_for.begin(), m_maps_awaited_for.end(), from);
  if (fe != m_maps_awaited_for.end()) {
    // we are indeed waiting for a map from this replica
    m_maps_awaited_for.erase(fe);
    return std::tuple{true, ""sv};
  } else {
    return std::tuple{false, "unsolicited scrub-map"sv};
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
