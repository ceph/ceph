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


#define dout_context (pg_->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "scrbr.pg(" << pg_->pg_id << ") "


ostream& operator<<(ostream& out, const scrub_flags_t& sf)
{
  if (sf.auto_repair)
    out << " AUTO_REPAIR";
  if (sf.check_repair)
    out << " CHECK_REPAIR";
  if (sf.deep_scrub_on_error)
    out << " DEEP_SCRUB_ON_ERROR";
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

  return out;
}

/*
 * are we still a clean & healthy scrubbing primary?
 *
 * relevant only after the initial sched_scrub
 */
bool PgScrubber::is_event_relevant(epoch_t queued)
{
  return is_primary() && pg_->is_active() && pg_->is_clean() &&
	 is_scrub_active() && /* note that original checked for
				 state_test(PG_STATE_SCRUBBING). What we do here is wrong
				 for start/sched events */
	 !was_epoch_changed() && (!queued || !pg_->pg_has_reset_since(queued));

  // shouldn't we check was_epoch_changed() (i.e. use epoch_start_)? RRR
}

void PgScrubber::send_start_scrub()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  fsm_->my_states();
  // ceph_assert(state_downcast<const NotActive*>() != 0);
  fsm_->process_event(StartScrub{});
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_scrub_unblock()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  if (is_scrub_active()) {
    fsm_->my_states();
    fsm_->process_event(Unblocked{});
    dout(7) << "RRRRRRR --<< " << __func__ << dendl;
  }
}

void PgScrubber::send_sched_scrub()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  fsm_->my_states();
  fsm_->process_event(SchedScrub{});
  dout(7) << "RRRRRRR --<< " << __func__ << " debug state: "
	  << (state_test(PG_STATE_SCRUBBING) ? "pg_state_scrubbing already set"
					     : "no pg_state_scrubbing")
	  << dendl;
}

void PgScrubber::send_scrub_resched()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  if (is_scrub_active()) {
    fsm_->my_states();
    fsm_->process_event(InternalSchedScrub{});
    dout(7) << "RRRRRRR --<< " << __func__ << dendl;
  }
}

// void PgScrubber::send_full_reset()
//{
//  dout(7) << "RRRRRRR -->> "  << __func__ <<  dendl;
//  fsm_->my_states();
//  fsm_->process_event(FullReset{});
//  dout(7) << "RRRRRRR --<< "  << __func__ <<  dendl;
//}

void PgScrubber::send_start_replica()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  fsm_->my_states();
  fsm_->process_event(StartReplica{});
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_sched_replica()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  fsm_->my_states();
  fsm_->process_event(SchedReplica{});	// RRR which does what?
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::active_pushes_notification()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  fsm_->my_states();
  fsm_->process_event(ActivePushesUpd{});
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::update_applied_notification()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  fsm_->my_states();
  fsm_->process_event(UpdatesApplied{});
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::digest_update_notification()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  fsm_->my_states();
  if (is_event_relevant(epoch_t(0))) {
    fsm_->process_event(DigestUpdate{});
  } else {
    // no need to send anything
    dout(7) << __func__ << " RRRRRR event no longer relevant" << dendl;
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_epoch_changed()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  if (is_scrub_active()) {
    fsm_->my_states();
    fsm_->process_event(EpochChanged{});
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}

void PgScrubber::send_replica_maps_ready()
{
  dout(7) << "RRRRRRR -->> " << __func__ << dendl;
  fsm_->my_states();
  if (is_scrub_active()) {
    fsm_->process_event(GotReplicas{});
  }
  dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}


bool PgScrubber::is_scrub_active() const
{
  dout(7) << " " << __func__ << " actv? " << active_ << "pg:" << pg_->pg_id << dendl;
  return active_;
}

bool PgScrubber::is_chunky_scrub_active() const
{
  dout(10) << "RRRRRRR -->> " << __func__ << " actv? " << active_ << dendl;
  return is_scrub_active();
}


void PgScrubber::reset_epoch(epoch_t epoch_queued)
{
  dout(10) << __func__ << " PG( " << pg_->pg_id << (pg_->is_primary() ? ") prm" : ") rpl")
	   << " epoch: " << epoch_queued << " state deep? "
	   << state_test(PG_STATE_DEEP_SCRUB) << dendl;

  epoch_queued_ = epoch_queued;
  needs_sleep_ = true;

  // trying not to: fsm_ = std::make_unique<ScrubMachine>(pg_, this);
  // trying not to: fsm_->initiate();

  // replace with verifying that we have the FSM quiescent:
  fsm_->assert_not_active();

  is_deep_ = state_test(PG_STATE_DEEP_SCRUB);
}

// RRR verify the cct used here is the same one used by the osd when accessing the conf
unsigned int PgScrubber::scrub_requeue_priority(Scrub::scrub_prio_t with_priority) const
{
  unsigned int qu_priority = flags_.priority;

  if (with_priority == Scrub::scrub_prio_t::high_priority) {
    qu_priority =
      std::max(qu_priority, (unsigned int)pg_->cct->_conf->osd_client_op_priority);
  }
  return qu_priority;
}

// ///////////////////////////////////////////////////////////////////// //
// scrub op registration handling

bool PgScrubber::is_scrub_registered() const
{
  return !scrub_reg_stamp.is_zero();
}

void PgScrubber::reg_next_scrub(requested_scrub_t& request_flags, bool is_explicit)
{
  dout(10) << __func__ << ": explicit: " << is_explicit
	   << " planned.m.s: " << request_flags.must_scrub
	   << ": planned.n.a.: " << request_flags.need_auto
	   << " stamp: " << pg_->info.history.last_scrub_stamp << dendl;

  if (!is_primary())
    return;

  ceph_assert(!is_scrub_registered());

  utime_t reg_stamp;
  bool must = false;

  if (request_flags.must_scrub || request_flags.need_auto ||
      is_explicit /*|| scrubber_->flags_.marked_must*/) {
    // Set the smallest time that isn't utime_t()
    reg_stamp = PgScrubber::scrub_must_stamp();
    must = true;
  } else if (pg_->info.stats.stats_invalid && pg_->cct->_conf->osd_scrub_invalid_stats) {
    reg_stamp = ceph_clock_now();
    must = true;
  } else {
    reg_stamp = pg_->info.history.last_scrub_stamp;
  }

  dout(9) << __func__ << " pg(" << pg_id_ << ") must: " << must
	  << " mm:" << flags_.marked_must << " flags: " << request_flags
	  << " stamp: " << reg_stamp << dendl;

  // note down the sched_time, so we can locate this scrub, and remove it
  // later on.
  double scrub_min_interval = 0;
  double scrub_max_interval = 0;
  pg_->pool.info.opts.get(pool_opts_t::SCRUB_MIN_INTERVAL, &scrub_min_interval);
  pg_->pool.info.opts.get(pool_opts_t::SCRUB_MAX_INTERVAL, &scrub_max_interval);

  scrub_reg_stamp = osds_->reg_pg_scrub(pg_->info.pgid, reg_stamp, scrub_min_interval,
					scrub_max_interval, must);
  dout(10) << __func__ << " pg(" << pg_id_ << ") register next scrub, scrub time "
	   << scrub_reg_stamp << ", must = " << (int)must << dendl;
}

void PgScrubber::unreg_next_scrub()
{
  if (is_scrub_registered()) {
    osds_->unreg_pg_scrub(pg_->info.pgid, scrub_reg_stamp);
    scrub_reg_stamp = utime_t{};
  }
}

void PgScrubber::scrub_requested(bool deep,
				 bool repair,
				 bool need_auto,
				 requested_scrub_t& req_flags)
{
  dout(9) << __func__ << " pg(" << pg_id_ << ") d/r/na:" << deep << repair << need_auto
	  << " existing-" << scrub_reg_stamp << " ## " << is_scrub_registered() << dendl;

  /* debug code */ {
    /* debug code */ std::string format;
    /* debug code */ auto f = Formatter::create(format, "json-pretty", "json-pretty");
    /* debug code */ osds_->dump_scrub_reservations(f);
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
    req_scrub = true;
  }

  /* debug code */ dout(9) << __func__ << " pg(" << pg_id_ << ") planned:" << req_flags
			   << dendl;
  /* debug code */ {
    /* debug code */ std::string format;
    /* debug code */ auto f = Formatter::create(format, "json-pretty", "json-pretty");
    /* debug code */ osds_->dump_scrub_reservations(f);
    /* debug code */ std::stringstream o;
    /* debug code */ f->flush(o);
    /* debug code */ dout(9) << __func__ << " b4_reg " << o.str() << dendl;
    /* debug code */ delete f;
  /* debug code */ }

  reg_next_scrub(req_flags, true);

  /* debug code */ {
    /* debug code */ std::string format;
    /* debug code */ auto f = Formatter::create(format, "json-pretty", "json-pretty");
    /* debug code */ osds_->dump_scrub_reservations(f);
    /* debug code */ std::stringstream o;
    /* debug code */ f->flush(o);
    /* debug code */ dout(9) << __func__ << " af_unreg " << o.str() << dendl;
    /* debug code */ delete f;
  /* debug code */ }
}

bool PgScrubber::reserve_local_n_remotes()
{
  if (!osds_->inc_scrubs_local()) {
    dout(10) << __func__ << ": failed to reserve locally " << dendl;
    return false;
  }

  dout(20) << __func__ << ": reserved locally, reserving replicas" << dendl;
  local_reserved = true;
  reserved_peers.insert(pg_whoami_);
  scrub_reserve_replicas();
  return true;
}

// ----------------------------------------------------------------------------

bool PgScrubber::has_pg_marked_new_updates() const
{
  auto last_applied = pg_->recovery_state.get_last_update_applied();
  dout(10) << __func__ << " recovery last: " << last_applied
	   << " vs. scrub's: " << subset_last_update << dendl;

  return last_applied >= subset_last_update;
}

void PgScrubber::set_subset_last_update(eversion_t e)
{
  subset_last_update = e;
}

/* the whole block (circa l. 2710) that sets:
 * - subset_last_update
 * - max_end
 * - end
 * - start
 * By:
 * - setting tentative range based on conf and divisor
 * - requesting a partial list of elements from the backend;
 * - handling some head/clones issues
 * -
 *
 * The selected range is set directly into 'start_' and 'end_'
 *
 */
bool PgScrubber::select_range()
{
  primary_scrubmap = ScrubMap{};
  received_maps.clear();

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
    3, pg_->get_cct()->_conf->osd_scrub_chunk_min / preemption_data.chunk_divisor());

  int max_idx = std::max<int64_t>(min_idx, pg_->get_cct()->_conf->osd_scrub_chunk_max /
					     preemption_data.chunk_divisor());

  // why mixing 'int' and int64_t? RRR

  dout(10) << __func__ << " Min: " << min_idx << " Max: " << max_idx
	   << " Div: " << preemption_data.chunk_divisor() << dendl;

  hobject_t start = start_;
  hobject_t candidate_end;
  std::vector<hobject_t> objects;
  int ret = pg_->get_pgbackend()->objects_list_partial(start, min_idx, max_idx, &objects,
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

  if (!pg_->_range_available_for_scrub(start_, candidate_end)) {
    // we'll be requeued by whatever made us unavailable for scrub
    dout(10) << __func__ << ": scrub blocked somewhere in range "
	     << "[" << start_ << ", " << candidate_end << ")" << dendl;
    return false;
  }

  end_ = candidate_end;
  if (end_ > max_end)
    max_end = end_;

  dout(8) << __func__ << " range selected: " << start_ << " //// " << end_ << " //// "
	  << max_end << dendl;
  return true;
}


bool PgScrubber::write_blocked_by_scrub(const hobject_t& soid)
{
  if (soid < start_ || soid >= end_) {
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
  // does [start, end] intersect [scrubber.start, scrubber.max_end)
  return (start < max_end && end >= start_);
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
  if (needs_sleep_) {

    double scrub_sleep = 1000.0 * osds_->osd->scrub_sleep_time(flags_.marked_must);
    // RRR make sure I understand the units
    dout(10) << __func__ << " sleep: " << scrub_sleep << dendl;
    sleep_time = milliseconds{long(scrub_sleep)};
  }
  dout(7) << __func__ << " sleep: " << sleep_time.count() << " needed? " << needs_sleep_
	  << dendl;

  if (sleep_time.count()) {

    // schedule a transition some sleep_time ms in the future

    needs_sleep_ = false;
    sleep_started_at = ceph_clock_now();

    // the 'delayer' for crimson is different. Will be factored out.

    spg_t pgid = pg_->get_pgid();
    auto callbk = new LambdaContext([osds = osds_, pgid, scrbr = this](int r) mutable {
      PGRef pg = osds->osd->lookup_lock_pg(pgid);
      if (!pg) {
	// no PG now, so we are forced to use the OSD's context
	//          lgeneric_dout(osds->osd->cct, 20)
	//            << "scrub_requeue_callback: Could not find "
	//            << "PG " << pgid << " can't complete scrub requeue after sleep"
	//            << dendl;

	// RRR ask how to send this error message
	return;
      }
      scrbr->needs_sleep_ = true;
      lgeneric_dout(scrbr->get_pg_cct(), 7)
	<< "scrub_requeue_callback: slept for "
	<< ceph_clock_now() - scrbr->sleep_started_at << ", re-queuing scrub with state "
	<< "XX" /*state*/ << dendl;

      scrbr->sleep_started_at = utime_t{};  // check where are we making use of that one
      osds->queue_for_scrub_resched(&(*pg), /* RRR */ Scrub::scrub_prio_t::high_priority);
      pg->unlock();
    });

    osds_->sleep_timer.add_event_after(sleep_time.count() / 1000.0f,
				       callbk);	 // why 'float'?

  } else {
    // just a requeue
    osds_->queue_for_scrub_resched(pg_, Scrub::scrub_prio_t::high_priority);
  }
}

/**
 *  walk the log to find the latest update that affects our chunk
 */
eversion_t PgScrubber::search_log_for_updates() const
{
  auto& projected = pg_->projected_log.log;
  auto pi =
    find_if(projected.crbegin(), projected.crend(),
	    [this](const auto& e) -> bool { return e.soid >= start_ && e.soid < end_; });

  if (pi != projected.crend())
    return pi->version;

  // (there was no relevant update entry in the log)

  auto& log = pg_->recovery_state.get_pg_log().get_log().log;
  auto p = find_if(log.crbegin(), log.crend(), [this](const auto& e) -> bool {
    return e.soid >= start_ && e.soid < end_;
  });

  if (p == log.crend())
    return eversion_t{};
  else
    return p->version;
}


bool PgScrubber::get_replicas_maps(bool replica_can_preempt)
{
  dout(10) << __func__ << " epoch_start: " << epoch_start_
	   << " pg sis: " << pg_->info.history.same_interval_since << dendl;

  primary_scrubmap_pos.reset();
  waiting_on_whom.insert(pg_whoami_);

  // ask replicas to scan and send maps
  for (pg_shard_t i : pg_->get_acting_recovery_backfill()) {

    if (i == pg_whoami_)
      continue;

    waiting_on_whom.insert(i);
    _request_scrub_map(i, subset_last_update, start_, end_, is_deep_,
		       replica_can_preempt);
  }

  dout(7) << __func__ << " waiting_on_whom " << waiting_on_whom << dendl;
  return (waiting_on_whom.size() > 1);
}

bool PgScrubber::was_epoch_changed()
{
  // for crimson we have pg_->get_info().history.same_interval_since
  dout(10) << __func__ << " epoch_start: " << epoch_start_
	   << " from pg: " << pg_->get_history().same_interval_since << dendl;

  // RRR why am I using the same_interval? it's OK for the primary, but for the replica?

  return (epoch_start_ != pg_->get_history().same_interval_since);
}


void PgScrubber::_request_scrub_map(pg_shard_t replica,
				    eversion_t version,
				    hobject_t start,
				    hobject_t end,
				    bool deep,
				    bool allow_preemption)
{
  ceph_assert(replica != pg_whoami_);
  dout(10) << " scrubmap from osd." << replica << " deep " << (int)deep << dendl;

  dout(9) << __func__ << " rep: " << replica << " epos: " << pg_->get_osdmap_epoch()
	  << " / " << pg_->get_last_peering_reset() << dendl;

  // RRR do NOT use curmap_ epoch instead of pg_->get_osdmap_epoch()

  MOSDRepScrub* repscrubop = new MOSDRepScrub(
    spg_t(pg_->info.pgid.pgid, replica.shard), version, pg_->get_osdmap_epoch(),
    pg_->get_last_peering_reset(), start, end, deep, allow_preemption, flags_.priority,
    pg_->ops_blocked_by_scrub());

  // default priority, we want the rep scrub processed prior to any recovery
  // or client io messages (we are holding a lock!)
  osds_->send_message_osd_cluster(replica.osd, repscrubop, get_osdmap_epoch());
}

void PgScrubber::cleanup_store(ObjectStore::Transaction* t)
{
  if (!store_)
    return;

  struct OnComplete : Context {
    std::unique_ptr<Scrub::Store> store;
    explicit OnComplete(std::unique_ptr<Scrub::Store>&& store) : store(std::move(store))
    {}
    void finish(int) override {}
  };
  store_->cleanup(t);
  t->register_on_complete(new OnComplete(std::move(store_)));
  ceph_assert(!store_);
}

void PgScrubber::on_init()
{
  // going upwards from 'inactive'

  ceph_assert(!is_scrub_active());

  preemption_data.reset();
  pg_->publish_stats_to_osd();
  epoch_start_ = pg_->get_history().same_interval_since;

  dout(8) << __func__ << " start same_interval:" << epoch_start_ << dendl;

  //  create a new store
  {
    ObjectStore::Transaction t;
    cleanup_store(&t);
    store_.reset(Scrub::Store::create(pg_->osd->store, &t, pg_->info.pgid, pg_->coll));
    pg_->osd->store->queue_transaction(pg_->ch, std::move(t), nullptr);
  }

  start_ = pg_->info.pgid.pgid.get_hobj_start();
  active_ = true;
}

void PgScrubber::on_replica_init()
{
  ceph_assert(!active_);

  // RRR do we need to do anything re the store?
  // RRR preemption state reset
  // RRR epoch_start_ ??

  active_ = true;
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
      int r = pg_->snap_mapper.get_snaps(hoid, &cur_snaps);
      if (r != 0 && r != -ENOENT) {
	derr << __func__ << ": get_snaps returned " << cpp_strerror(r) << dendl;
	ceph_abort();
      }
      if (r == -ENOENT || cur_snaps != obj_snaps) {
	ObjectStore::Transaction t;
	OSDriver::OSTransaction _t(pg_->osdriver.get_transaction(&t));
	if (r == 0) {
	  r = pg_->snap_mapper.remove_oid(hoid, &_t);
	  if (r != 0) {
	    derr << __func__ << ": remove_oid returned " << cpp_strerror(r) << dendl;
	    ceph_abort();
	  }
	  pg_->osd->clog->error()
	    << "osd." << pg_->osd->whoami << " found snap mapper error on pg "
	    << pg_->info.pgid << " oid " << hoid << " snaps in mapper: " << cur_snaps
	    << ", oi: " << obj_snaps << "...repaired";
	} else {
	  pg_->osd->clog->error()
	    << "osd." << pg_->osd->whoami << " found snap mapper error on pg "
	    << pg_->info.pgid << " oid " << hoid << " snaps missing in mapper"
	    << ", should be: " << obj_snaps << " was " << cur_snaps << " r " << r
	    << "...repaired";
	}
	pg_->snap_mapper.add_oid(hoid, obj_snaps, &_t);

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

	  e = pg_->osd->store->queue_transaction(pg_->ch, std::move(t));
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
  auto ret =
    build_scrub_map_chunk(primary_scrubmap, primary_scrubmap_pos, start_, end_, is_deep_);

  if (ret == -EINPROGRESS)
    osds_->queue_for_scrub_resched(pg_, Scrub::scrub_prio_t::high_priority);

  return ret;
}

int PgScrubber::build_replica_map_chunk(bool qu_priority)
{
  dout(10) << __func__ << " epoch start: " << epoch_start_ << " ep q: " << epoch_queued_
	   << dendl;
  dout(10) << __func__ << " deep: " << is_deep_ << dendl;

  auto ret =
    build_scrub_map_chunk(replica_scrubmap, replica_scrubmap_pos, start_, end_, is_deep_);

  if (ret == 0) {

    // finished!
    // In case we restarted smaller chunk, clear old data

    ScrubMap for_meta_scrub;
    cleaned_meta_map.clear_from(start_);
    cleaned_meta_map.insert(replica_scrubmap);
    clean_meta_map(for_meta_scrub);
    _scan_snaps(for_meta_scrub);
  }

  /// RRR \todo ask: original code used low priority here. I am using the original message
  /// priority.
  if (ret == -EINPROGRESS)
    requeue_replica(replica_request_priority_);

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
    map.valid_through = pg_->info.last_update;

    // objects
    vector<ghobject_t> rollback_obs;
    pos.ret =
      pg_->get_pgbackend()->objects_list_range(start, end, &pos.ls, &rollback_obs);
    dout(10) << __func__ << " while pos empty " << pos.ret << dendl;
    if (pos.ret < 0) {
      dout(5) << "objects_list_range error: " << pos.ret << dendl;
      return pos.ret;
    }
    dout(10) << __func__ << " pos.ls.empty()? " << (pos.ls.empty() ? "+" : "-") << dendl;
    if (pos.ls.empty()) {
      break;
    }
    pg_->_scan_rollback_obs(rollback_obs);
    pos.pos = 0;
    return -EINPROGRESS;
  }

  // scan objects
  // dout(10) << __func__ << " pos.done()? " << (pos.done()?"+":"-") << dendl;
  while (!pos.done()) {
    int r = pg_->get_pgbackend()->be_scan_list(map, pos);
    dout(10) << __func__ << " be r " << r << dendl;
    if (r == -EINPROGRESS) {
      dout(8 /*20*/) << __func__ << " in progress" << dendl;
      return r;
    }
  }

  // finish
  dout(8 /*20*/) << __func__ << " finishing" << dendl;
  ceph_assert(pos.done());  // how can this fail? RRR
  pg_->_repair_oinfo_oid(map);

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
  if (end_.is_max() || cleaned_meta_map.objects.empty()) {
    cleaned_meta_map.swap(for_meta_scrub);
  } else {
    auto iter = cleaned_meta_map.objects.end();
    --iter;  // not empty, see 'if' clause
    auto begin = cleaned_meta_map.objects.begin();
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
    cleaned_meta_map.objects.erase(begin, iter);
  }
}

void PgScrubber::run_callbacks()
{
  std::list<Context*> to_run;
  to_run.swap(callbacks_);

  for (auto& tr : to_run) {
    tr->complete(0);
  }
}

void PgScrubber::done_comparing_maps()
{
  scrub_compare_maps();
  start_ = end_;
  run_callbacks();
  requeue_waiting();
}

Scrub::preemption_t* PgScrubber::get_preemptor()
{
  return &preemption_data;
}


std::chrono::milliseconds PgScrubber::fetch_sleep_needed()
{
  double scrub_sleep = /*1000.0 * */ osds_->osd->scrub_sleep_time(
    flags_.marked_must);  // RRR make sure I understand the units

  dout(10) << __func__ << " sleep: " << scrub_sleep << dendl;
  return std::chrono::milliseconds{long(1000.0f * scrub_sleep)};
}

void PgScrubber::requeue_replica(Scrub::scrub_prio_t is_high_priority)
{
  dout(10) << "-" << __func__ << dendl;
  osds_->queue_for_rep_scrub_resched(pg_, is_high_priority, flags_.priority);
}

/**
 * Called for the arriving "give me your map, replica!" request. Unlike
 * the original implementation, we do not requeue the Op waiting for
 * updates. Instead - we trigger the FSM.
 *
 */
void PgScrubber::replica_scrub_op(OpRequestRef op)
{
  auto msg = op->get_req<MOSDRepScrub>();

  dout(7) << "PgScrubber::replica_scrub(op, ...) " << pg_->pg_id << " / ["
	  << pg_->pg_id.pgid << " / " << pg_->pg_id.pgid.m_pool << " / "
	  << pg_->pg_id.pgid.m_seed << " ] // " << pg_whoami_ << dendl;
  dout(17) << __func__ << " m  ep: " << msg->map_epoch
	   << " better be >= " << pg_->info.history.same_interval_since << dendl;
  dout(17) << __func__ << " minep: " << msg->min_epoch << dendl;
  dout(17) << __func__ << " m-deep: " << msg->deep << dendl;

  if (msg->map_epoch < pg_->info.history.same_interval_since) {
    dout(10) << "replica_scrub_op discarding old replica_scrub from " << msg->map_epoch
	     << " < " << pg_->info.history.same_interval_since << dendl;
    return;
  }

  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos = ScrubMapBuilder{};

  // replica_epoch_start_ is overwritten if requeued waiting for active pushes
  replica_epoch_start_ = pg_->info.history.same_interval_since;
  replica_min_epoch_ = msg->min_epoch;
  start_ = msg->start;
  end_ = msg->end;
  max_end = msg->end;
  is_deep_ = msg->deep;
  epoch_start_ = pg_->info.history.same_interval_since;
  replica_request_priority_ = static_cast<Scrub::scrub_prio_t>(msg->high_priority);
  flags_.priority = msg->priority ? msg->priority : pg_->get_scrub_priority();

  preemption_data.reset();
  preemption_data.force_preemptability(msg->allow_preemption);

  replica_scrubmap_pos.reset();

  // make sure the FSM is at NotActive
  fsm_->assert_not_active();

  osds_->queue_for_rep_scrub(pg_, replica_request_priority_, flags_.priority);
}

void PgScrubber::replica_scrub(epoch_t epoch_queued)
{
  dout(7) << "replica_scrub(epoch,) RRRRRRR:\t" << pg_->pg_id << dendl;
  dout(7) << __func__ << " epoch queued: " << epoch_queued << dendl;
  dout(7) << __func__ << " epst: " << epoch_start_
	  << " better be >= " << pg_->info.history.same_interval_since << dendl;
  dout(7) << __func__ << " is_deep_: " << is_deep_ << dendl;

  if (pg_->pg_has_reset_since(epoch_queued)) {
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
  dout(7) << "replica_scrub(epoch,) RRRRRRR:\t" << pg_->pg_id << dendl;
  dout(7) << __func__ << " is_deep_: " << is_deep_ << dendl;
  dout(7) << __func__ << " epst: " << epoch_start_
	  << " better be >= " << pg_->info.history.same_interval_since << dendl;

  if (pg_->pg_has_reset_since(epoch_queued)) {
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
  osds_->queue_scrub_pushes_update(pg_, is_high_priority);
}

void PgScrubber::queue_pushes_update(bool with_priority)
{
  dout(10) << __func__ << ": queueing" << dendl;
  osds_->queue_scrub_pushes_update(pg_, static_cast<Scrub::scrub_prio_t>(with_priority));
}

/*
 *
 */
void PgScrubber::set_op_parameters(requested_scrub_t& request)
{
  dout(10) << __func__ << " input: " << request << dendl;

  flags_.check_repair = request.check_repair;
  flags_.auto_repair = request.auto_repair || request.need_auto;
  flags_.marked_must = request.must_scrub;

  flags_.priority = (request.must_scrub || request.need_auto)
		      ? get_pg_cct()->_conf->osd_requested_scrub_priority
		      : pg_->get_scrub_priority();
  // RRR which CCT to use here?

  state_set(PG_STATE_SCRUBBING);

  // will we be deep-scrubbing?
  bool must_deep_scrub = request.must_deep_scrub || request.need_auto;
  if (must_deep_scrub) {
    state_set(PG_STATE_DEEP_SCRUB);
  }

  if (request.must_repair || flags_.auto_repair) {
    state_set(PG_STATE_REPAIR);
  }

  flags_.deep_scrub_on_error = request.deep_scrub_on_error;

  dout(10) << __func__ << " output 1: " << flags_ << " priority: " << flags_.priority
	   << dendl;
  dout(10) << __func__
	   << " output 2: " << (state_test(PG_STATE_DEEP_SCRUB) ? "deep" : "shallow")
	   << dendl;
  dout(10) << __func__
	   << " output 3: " << (flags_.deep_scrub_on_error ? "+deepOnE" : "-deepOnE")
	   << dendl;
  dout(10) << __func__
	   << " output 4: " << (state_test(PG_STATE_REPAIR) ? "repair" : "-no-rep-")
	   << dendl;

  request = requested_scrub_t{};
}

void PgScrubber::scrub_compare_maps()
{
  dout(7) << __func__ << " has maps, analyzing" << dendl;

  // construct authoritative scrub map for type-specific scrubbing
  cleaned_meta_map.insert(primary_scrubmap);
  map<hobject_t, pair<std::optional<uint32_t>, std::optional<uint32_t>>> missing_digest;

  map<pg_shard_t, ScrubMap*> maps;
  maps[pg_whoami_] = &primary_scrubmap;

  for (const auto& i : pg_->get_acting_recovery_backfill()) {
    if (i == pg_whoami_)
      continue;
    dout(2) << __func__ << " replica " << i << " has " << received_maps[i].objects.size()
	    << " items" << dendl;
    maps[i] = &received_maps[i];
  }

  set<hobject_t> master_set;

  // Construct master set
  for (const auto map : maps) {
    for (const auto& i : map.second->objects) {
      master_set.insert(i.first);
    }
  }

  stringstream ss;
  pg_->get_pgbackend()->be_omap_checks(maps, master_set, omap_stats, ss);

  if (!ss.str().empty()) {
    osds_->clog->warn(ss);
  }

  if (pg_->recovery_state.get_acting().size() > 1) {

    // RRR add a comment here

    dout(10) << __func__ << "  comparing replica scrub maps" << dendl;

    // Map from object with errors to good peer
    map<hobject_t, list<pg_shard_t>> authoritative;

    dout(2) << __func__ << pg_->get_primary() << " has "
	    << primary_scrubmap.objects.size() << " items" << dendl;

    ss.str("");
    ss.clear();

    pg_->get_pgbackend()->be_compare_scrubmaps(
      maps, master_set, state_test(PG_STATE_REPAIR), missing, inconsistent, authoritative,
      missing_digest, shallow_errors, deep_errors, store_.get(), pg_->info.pgid,
      pg_->recovery_state.get_acting(), ss);
    dout(2) << ss.str() << dendl;

    if (!ss.str().empty()) {
      osds_->clog->error(ss);
    }

    for (auto& [hobj, shrd_list] : authoritative) {
      list<pair<ScrubMap::object, pg_shard_t>> good_peers;
      for (const auto shrd : shrd_list) {
	good_peers.emplace_back(maps[shrd]->objects[hobj], shrd);
      }

      authoritative_.emplace(hobj, good_peers);
    }

    for (const auto& [hobj, shrd_list] : authoritative) {

      // RRR a comment?

      cleaned_meta_map.objects.erase(hobj);
      cleaned_meta_map.objects.insert(*(maps[shrd_list.back()]->objects.find(hobj)));
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

  if (!store_->empty()) {

    if (state_test(PG_STATE_REPAIR)) {
      dout(10) << __func__ << ": discarding scrub results" << dendl;
      store_->flush(nullptr);
    } else {
      dout(10) << __func__ << ": updating scrub object" << dendl;
      ObjectStore::Transaction t;
      store_->flush(&t);
      pg_->osd->store->queue_transaction(pg_->ch, std::move(t), nullptr);
    }
  }
}

void PgScrubber::replica_update_start_epoch()
{
  dout(10) << __func__ << " start:" << pg_->info.history.same_interval_since << dendl;
  replica_epoch_start_ = pg_->info.history.same_interval_since;
}


/**
 * Send the requested map back to the primary (or - if we
 * were preempted - let the primary know).
 */
void PgScrubber::send_replica_map(bool was_preempted)
{
  dout(7) << __func__ << " min:" << replica_min_epoch_
	  << " mstrt:" << replica_epoch_start_ << dendl;

  MOSDRepScrubMap* reply = new MOSDRepScrubMap(
    spg_t(pg_->info.pgid.pgid, pg_->get_primary().shard), replica_min_epoch_, pg_whoami_);

  reply->preempted = was_preempted;
  ::encode(replica_scrubmap, reply->get_data());

  osds_->send_message_osd_cluster(pg_->get_primary().osd, reply, replica_min_epoch_);
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

  if (m->map_epoch < pg_->info.history.same_interval_since) {
    dout(10) << __func__ << " discarding old from " << m->map_epoch << " < "
	     << pg_->info.history.same_interval_since << dendl;
    return;
  }

  op->mark_started();

  auto p = const_cast<bufferlist&>(m->get_data()).cbegin();

  received_maps[m->from].decode(p, pg_->info.pgid.pool());
  dout(10) << "map version is " << received_maps[m->from].valid_through << dendl;

  dout(10) << __func__ << " waiting_on_whom was " << waiting_on_whom << dendl;

  ceph_assert(waiting_on_whom.count(m->from));
  waiting_on_whom.erase(m->from);

  if (m->preempted) {
    dout(10) << __func__ << " replica was preempted, setting flag" << dendl;
    ceph_assert(preemption_data.is_preemptable());  // otherwise - how dare the replica!
    preemption_data.do_preempt();
  }

  if (waiting_on_whom.empty()) {
    dout(10) << __func__ << " osd-queuing GotReplicas" << dendl;
    osds_->queue_scrub_got_repl_maps(pg_, pg_->ops_blocked_by_scrub());
  }
}

void PgScrubber::handle_scrub_reserve_request(OpRequestRef op)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  if (remote_reserved) {
    dout(10) << __func__ << " ignoring reserve request: Already reserved" << dendl;
    return;
  }

  // RRR check the 'cct' used here
  if ((pg_->cct->_conf->osd_scrub_during_recovery || !osds_->is_recovery_active()) &&
      osds_->inc_scrubs_remote()) {
    remote_reserved = true;
  } else {
    dout(20) << __func__ << ": failed to reserve remotely" << dendl;
    remote_reserved = false;
  }

  dout(7) << __func__ << " reserved? " << (remote_reserved ? "yes" : "no") << dendl;

  auto m = op->get_req<MOSDScrubReserve>();
  Message* reply = new MOSDScrubReserve(
    spg_t(pg_->info.pgid.pgid, pg_->get_primary().shard), m->map_epoch,
    remote_reserved ? MOSDScrubReserve::GRANT : MOSDScrubReserve::REJECT, pg_whoami_);

  osds_->send_message_osd_cluster(reply, op->get_req()->get_connection());
}

void PgScrubber::handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  if (!local_reserved) {
    dout(10) << "ignoring obsolete scrub reserve reply" << dendl;
    return;
  }

  if (reserved_peers.find(from) != reserved_peers.end()) {
    dout(10) << " already had osd." << from << " reserved" << dendl;
  } else {
    dout(10) << " osd." << from << " scrub reserve = success" << dendl;
    reserved_peers.insert(from);
    pg_->sched_scrub();
  }
}

void PgScrubber::handle_scrub_reserve_reject(OpRequestRef op, pg_shard_t from)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();

  if (!local_reserved) {
    dout(10) << "ignoring obsolete scrub reserve reply" << dendl;
    return;
  }

  if (reserved_peers.find(from) != reserved_peers.end()) {
    dout(10) << " already had osd." << from << " reserved" << dendl;
  } else {
    /* One decline stops this pg from being scheduled for scrubbing. */
    dout(10) << " osd." << from << " scrub reserve = fail" << dendl;
    reserve_failed = true;
    pg_->sched_scrub();
  }
}

void PgScrubber::handle_scrub_reserve_release(OpRequestRef op)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();
  clear_scrub_reserved();
}

void PgScrubber::clear_scrub_reserved()
{
  dout(7) << __func__ << " " << local_reserved << " <-LR/RR-> " << remote_reserved
	  << dendl;

  reserved_peers.clear();
  reserve_failed = false;

  if (local_reserved) {
    local_reserved = false;
    osds_->dec_scrubs_local();
  }
  if (remote_reserved) {
    remote_reserved = false;
    osds_->dec_scrubs_remote();
  }
}

/*!
 * send a replica (un)reservation request to the acting set
 *
 * @param opcode - one of (as yet untyped) MOSDScrubReserve::REQUEST
 *                  or MOSDScrubReserve::RELEASE
 */
void PgScrubber::message_all_replicas(int32_t opcode, std::string_view op_text)
{
  ceph_assert(pg_->recovery_state.get_backfill_targets()
		.empty());  // RRR ask: (the code was copied as is) Why checking here?

  std::vector<std::pair<int, Message*>> messages;
  messages.reserve(pg_->get_actingset().size());

  epoch_t epch = get_osdmap_epoch();

  for (auto& p : pg_->get_actingset()) {

    if (p == pg_whoami_)
      continue;

    dout(10) << "scrub requesting " << op_text << " from osd." << p << " ep: " << epch
	     << dendl;
    Message* m =
      new MOSDScrubReserve(spg_t(pg_->info.pgid.pgid, p.shard), epch, opcode, pg_whoami_);
    messages.push_back(std::make_pair(p.osd, m));
  }

  if (!messages.empty()) {
    osds_->send_message_osd_cluster(messages, epch);
  }
}

void PgScrubber::scrub_reserve_replicas()
{
  dout(10) << __func__ << dendl;
  message_all_replicas(MOSDScrubReserve::REQUEST, "reservation");
}

/*
 * note that we do not check for the actual existence of registrations
 * before we message all our replicas. We should probably consider tracking existing
 * registrations.
 */
void PgScrubber::scrub_unreserve_replicas()
{
  dout(10) << __func__ << dendl;
  message_all_replicas(MOSDScrubReserve::RELEASE, "unreservation");
}

bool PgScrubber::scrub_process_inconsistent()
{
  dout(7) << __func__ << ": checking authoritative" << dendl;

  bool repair = state_test(PG_STATE_REPAIR);
  const bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));
  dout(8) << __func__ << " sdeep: " << deep_scrub << " is_d: " << is_deep_
	  << " repair: " << repair << dendl;

  // authoritative only store objects which are missing or inconsistent.
  if (!authoritative_.empty()) {

    stringstream ss;
    ss << pg_->info.pgid << " " << mode << " " << missing.size() << " missing, "
       << inconsistent.size() << " inconsistent objects";
    dout(2) << ss.str() << dendl;
    osds_->clog->error(ss);

    if (repair) {
      state_clear(PG_STATE_CLEAN);

      for (const auto& [hobj, shrd_list] : authoritative_) {

	auto missing_entry = missing.find(hobj);

	if (missing_entry != missing.end()) {
	  pg_->repair_object(hobj, shrd_list, missing_entry->second);
	  fixed_count += missing_entry->second.size();
	}

	if (inconsistent.count(hobj)) {
	  pg_->repair_object(hobj, shrd_list, inconsistent[hobj]);
	  fixed_count += missing_entry->second.size();
	}
      }
    }
  }
  return (!authoritative_.empty() && repair);
}

/*
 * note: only called for the Primary.
 */
void PgScrubber::scrub_finish()
{
  dout(7) << __func__ << " flags b4: " << flags_
	  << " dsonerr: " << flags_.deep_scrub_on_error << dendl;
  dout(7) << __func__ << " deep: state " << state_test(PG_STATE_DEEP_SCRUB) << dendl;
  dout(7) << __func__ << " deep: var " << is_deep_ << dendl;
  dout(7) << __func__ << " repair in p.na/p.ar/flags: " << pg_->planned_scrub_.auto_repair
	  << pg_->planned_scrub_.need_auto << flags_.auto_repair << dendl;
  dout(7) << __func__ << " auth sz " << authoritative_.size() << dendl;

  bool do_auto_scrub = false;

  ceph_assert(pg_->is_locked());

  // if the repair request comes from auto-repair and large number of errors,
  // we would like to cancel auto-repair

  bool repair = state_test(PG_STATE_REPAIR);
  if (repair && flags_.auto_repair &&
      authoritative_.size() > pg_->cct->_conf->osd_scrub_auto_repair_num_errors) {

    dout(7) << __func__ << " undoing the repair" << dendl;
    state_clear(PG_STATE_REPAIR);
    repair = false;
  }

  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));

  // if a regular scrub had errors within the limit, do a deep scrub to auto repair
  if (flags_.deep_scrub_on_error && authoritative_.size() &&
      authoritative_.size() <= pg_->cct->_conf->osd_scrub_auto_repair_num_errors) {
    ceph_assert(!deep_scrub);  // RRR what guarantees that?
    do_auto_scrub = true;
    dout(8 /*20*/) << __func__ << " Try to auto repair after scrub errors" << dendl;
  }

  // also - if we have a fake (unit-testing related) request:
  // for UT: if (/*flags_.deep_scrub_on_error &&*/ pg_->pg_id.pgid.m_seed == 4 &&
  // for UT:     (--fake_count == 0)) {
  // for UT:  dout(7) << __func__ << " faking errors RRRR" << dendl;
  // for UT:  // for qa tests, 28/7/20: do_auto_scrub = true;
  // for UT: }

  flags_.deep_scrub_on_error =
    false;  // RRR probably wrong. Maybe should change the planned_...

  // type-specific finish (can tally more errors)
  _scrub_finish();

  // dout(11) << __func__ << ": af _scrub_finish(). flags: " << flags_ << dendl;

  bool has_error = scrub_process_inconsistent();
  // dout(10) << __func__ << ": from scrub_p_inc: " << has_error << dendl;

  {
    stringstream oss;
    oss << pg_->info.pgid.pgid << " " << mode << " ";
    int total_errors = shallow_errors + deep_errors;
    if (total_errors)
      oss << total_errors << " errors";
    else
      oss << "ok";
    if (!deep_scrub && pg_->info.stats.stats.sum.num_deep_scrub_errors)
      oss << " ( " << pg_->info.stats.stats.sum.num_deep_scrub_errors
	  << " remaining deep scrub error details lost)";
    if (repair)
      oss << ", " << fixed_count << " fixed";
    if (total_errors)
      osds_->clog->error(oss);
    else
      osds_->clog->debug(oss);
  }

  dout(7) << __func__ << ": status: " << (has_error ? " he " : "-")
	  << (repair ? " rp " : "-") << dendl;

  // Since we don't know which errors were fixed, we can only clear them
  // when every one has been fixed.
  if (repair) {
    if (fixed_count == shallow_errors + deep_errors) {
      ceph_assert(deep_scrub);
      shallow_errors = 0;
      deep_errors = 0;
      dout(8 /*20*/) << __func__ << " All may be fixed" << dendl;
    } else if (has_error) {
      // Deep scrub in order to get corrected error counts
      pg_->scrub_after_recovery = true;
      saved_req_scrub = req_scrub;
      dout(8 /*20*/) << __func__
		     << " Set scrub_after_recovery, req_scrub= " << saved_req_scrub
		     << dendl;
    } else if (shallow_errors || deep_errors) {
      // We have errors but nothing can be fixed, so there is no repair
      // possible.
      state_set(PG_STATE_FAILED_REPAIR);
      dout(10) << __func__ << " " << (shallow_errors + deep_errors)
	       << " error(s) present with no repair possible" << dendl;
    }
  }

  {
    // finish up
    ObjectStore::Transaction t;
    pg_->recovery_state.update_stats(
      [this, deep_scrub](auto& history, auto& stats) {
	dout(10) << "pg_->recovery_state.update_stats()" << dendl;
	utime_t now = ceph_clock_now();
	history.last_scrub = pg_->recovery_state.get_info().last_update;
	history.last_scrub_stamp = now;
	if (is_deep_) {
	  history.last_deep_scrub = pg_->recovery_state.get_info().last_update;
	  history.last_deep_scrub_stamp = now;
	}

	if (deep_scrub) {
	  if ((shallow_errors == 0) && (deep_errors == 0))
	    history.last_clean_scrub_stamp = now;
	  stats.stats.sum.num_shallow_scrub_errors = shallow_errors;
	  stats.stats.sum.num_deep_scrub_errors = deep_errors;
	  stats.stats.sum.num_large_omap_objects = omap_stats.large_omap_objects;
	  stats.stats.sum.num_omap_bytes = omap_stats.omap_bytes;
	  stats.stats.sum.num_omap_keys = omap_stats.omap_keys;
	  dout(10 /*25*/) << "scrub_finish shard " << pg_whoami_
			  << " num_omap_bytes = " << stats.stats.sum.num_omap_bytes
			  << " num_omap_keys = " << stats.stats.sum.num_omap_keys
			  << dendl;
	} else {
	  stats.stats.sum.num_shallow_scrub_errors = shallow_errors;
	  // XXX: last_clean_scrub_stamp doesn't mean the pg is not inconsistent
	  // because of deep-scrub errors
	  if (shallow_errors == 0)
	    history.last_clean_scrub_stamp = now;
	}
	stats.stats.sum.num_scrub_errors = stats.stats.sum.num_shallow_scrub_errors +
					   stats.stats.sum.num_deep_scrub_errors;
	if (flags_.check_repair) {
	  flags_.check_repair = false;
	  if (pg_->info.stats.stats.sum.num_scrub_errors) {
	    state_set(PG_STATE_FAILED_REPAIR);
	    dout(10) << "scrub_finish " << pg_->info.stats.stats.sum.num_scrub_errors
		     << " error(s) still present after re-scrub" << dendl;
	  }
	}
	return true;
      },
      &t);
    int tr = osds_->store->queue_transaction(pg_->ch, std::move(t), nullptr);
    ceph_assert(tr == 0);

    if (!pg_->snap_trimq.empty()) {
      dout(10) << "scrub finished, requeuing snap_trimmer" << dendl;
      pg_->snap_trimmer_scrub_complete();
    }
  }

  // dout(7) << __func__ << " scrub_after_recovery: " << pg_->scrub_after_recovery <<
  // dendl;

  if (has_error) {
    pg_->queue_peering_event(PGPeeringEventRef(std::make_shared<PGPeeringEvent>(
      get_osdmap_epoch(), get_osdmap_epoch(), PeeringState::DoRecovery())));
  } else {
    state_clear(PG_STATE_REPAIR);
  }

  cleanup_on_finish();
  if (do_auto_scrub) {
    scrub_requested(false, false, true, pg_->planned_scrub_);
  }

  if (pg_->is_active()) {
    // dout(7) << __func__ << " after19 " << dendl;
    pg_->recovery_state.share_pg_info();
  }
  // dout(7) << __func__ << " after2" << dendl;
}


Scrub::FsmNext PgScrubber::on_digest_updates()
{
  dout(7) << __func__ << " #pending: " << num_digest_updates_pending << " are we done? "
	  << num_digest_updates_pending << (end_.is_max() ? " <L> " : " <nl> ") << dendl;

  if (num_digest_updates_pending == 0) {

    // got all updates, and finished with this chunk. Any more?
    if (end_.is_max()) {
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

void PgScrubber::handle_query_state(Formatter* f)
{
  dout(7) << __func__ << dendl;

  f->open_object_section("scrub");
  f->dump_stream("scrubber.epoch_start") << epoch_start_;
  f->dump_bool("scrubber.active", active_);
  // RRR needed? f->dump_string("scrubber.state",
  // PG::Scrubber::state_string(scrubber.state));
  f->dump_stream("scrubber.start") << start_;
  f->dump_stream("scrubber.end") << end_;
  f->dump_stream("scrubber.max_end") << max_end;
  f->dump_stream("scrubber.subset_last_update") << subset_last_update;
  f->dump_bool("scrubber.deep", is_deep_);

  {
    f->open_array_section("scrubber.waiting_on_whom");
    for (const auto& p : waiting_on_whom) {
      f->dump_stream("shard") << p;
    }
    f->close_section();
  }

  f->close_section();
}

PgScrubber::~PgScrubber()
{
  dout(7) << __func__ << dendl;
}

PgScrubber::PgScrubber(PG* pg)
    : pg_{pg}
    , pg_id_{pg->pg_id}
    , osds_{static_cast<OSDService*>(pg_->osd)}
    , pg_whoami_{pg->pg_whoami}
    , epoch_queued_{0}
    , preemption_data{pg}
{
  dout(11) << " creating PgScrubber for " << pg->pg_id << " / [" << pg->pg_id.pgid
	   << " / " << pg->pg_id.pgid.m_pool << " / " << pg->pg_id.pgid.m_seed << " ] // "
	   << pg_whoami_ << dendl;
  fsm_ = std::make_unique<ScrubMachine>(pg_, this);
  fsm_->initiate();
}

//  called only for normal end-of-scrub
void PgScrubber::cleanup_on_finish()
{
  dout(7) << __func__ << dendl;
  ceph_assert(pg_->is_locked());

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);
  pg_->publish_stats_to_osd();

  // RRR this part should be handled by the FSM
  if (local_reserved) {
    osds_->dec_scrubs_local();
    local_reserved = false;
    reserved_peers.clear();
  }
  scrub_unreserve_replicas();
  pg_->requeue_ops(pg_->waiting_for_scrub);

  reset();

  // type-specific state clear
  _scrub_clear_state();
}


// uses process_event(), so must be invoked externally RRR
void PgScrubber::scrub_clear_state(bool has_error)
{
  dout(7) << __func__ << dendl;
  ceph_assert(pg_->is_locked());

  state_clear(PG_STATE_SCRUBBING);
  if (!has_error)
    state_clear(PG_STATE_REPAIR);
  state_clear(PG_STATE_DEEP_SCRUB);

  pg_->publish_stats_to_osd();
  req_scrub = false;

  // local -> nothing.

  // RRR this part should be handled by the FSM
  if (local_reserved) {
    osds_->dec_scrubs_local();
    local_reserved = false;
    reserved_peers.clear();
  }

  pg_->requeue_ops(pg_->waiting_for_scrub);

  reset();
  fsm_->process_event(FullReset{});

  active_ = false;

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
  waiting_on_whom.clear();
  received_maps.clear();

  start_ = hobject_t{};
  end_ = hobject_t{};
  max_end = hobject_t{};
  subset_last_update = eversion_t{};
  shallow_errors = 0;
  deep_errors = 0;
  fixed_count = 0;
  omap_stats = (const struct omap_stat_t){0};

  run_callbacks();
  inconsistent.clear();
  missing.clear();
  authoritative_.clear();
  num_digest_updates_pending = 0;
  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos.reset();

  cleaned_meta_map = ScrubMap{};
  needs_sleep_ = true;
  sleep_started_at = utime_t{};

  active_ = false;
  pg_->publish_stats_to_osd();

  // dout(7) << "RRRRRRR --<< " << __func__ << dendl;
}


// clear all state
/*
 * note: reservations-related variables are not reset here
 */
void PgScrubber::reset()
{
  dout(7) << __func__ << dendl;

  preemption_data.reset();
  waiting_on_whom.clear();
  received_maps.clear();

  start_ = hobject_t{};
  end_ = hobject_t{};
  max_end = hobject_t{};
  subset_last_update = eversion_t{};
  shallow_errors = 0;
  deep_errors = 0;
  fixed_count = 0;
  omap_stats = (const struct omap_stat_t){0};

  run_callbacks();
  inconsistent.clear();
  missing.clear();
  authoritative_.clear();
  num_digest_updates_pending = 0;
  primary_scrubmap = ScrubMap{};
  primary_scrubmap_pos.reset();
  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos.reset();
  cleaned_meta_map = ScrubMap{};
  needs_sleep_ = true;
  sleep_started_at = utime_t{};

  active_ = false;
}

const OSDMapRef& PgScrubber::get_osdmap() const
{
  return pg_->get_osdmap();  // RRR understand why we cannot use curmap_
}

ostream& operator<<(ostream& out, const PgScrubber& scrubber)
{
  return out << scrubber.flags_;
}

// ///////////////////// preemption_data_t //////////////////////////////////

PgScrubber::preemption_data_t::preemption_data_t(PG* pg) : pg_{pg}
{
  left_ = static_cast<int>(
    pg_->get_cct()->_conf.get_val<uint64_t>("osd_scrub_max_preemptions"));
}

void PgScrubber::preemption_data_t::reset()
{
  std::lock_guard<std::mutex> lk{preemption_lock_};

  preemptable_ = false;
  preempted_ = false;
  left_ =
    static_cast<int>(pg_->cct->_conf.get_val<uint64_t>("osd_scrub_max_preemptions"));
  size_divisor_ = 1;
}
