// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "PaxosService.h"
#include "common/Clock.h"
#include "Monitor.h"
#include "MonitorDBStore.h"


#include "common/config.h"
#include "include/assert.h"
#include "common/Formatter.h"

#include "mon/MonOpRequest.h"

#define dout_subsys ceph_subsys_paxos
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, paxos, service_name, get_first_committed(), get_last_committed())
static ostream& _prefix(std::ostream *_dout, Monitor *mon, Paxos *paxos, string service_name,
			version_t fc, version_t lc) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").paxosservice(" << service_name << " " << fc << ".." << lc << ") ";
}

bool PaxosService::dispatch(MonOpRequestRef op)
{
  assert(op->is_type_service() || op->is_type_command());
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  op->mark_event("psvc:dispatch");

  dout(10) << "dispatch " << m << " " << *m
	   << " from " << m->get_orig_source_inst()
	   << " con " << m->get_connection() << dendl;

  if (mon->is_shutdown()) {
    return true;
  }

  // make sure this message isn't forwarded from a previous election epoch
  if (m->rx_election_epoch &&
      m->rx_election_epoch < mon->get_epoch()) {
    dout(10) << " discarding forwarded message from previous election epoch "
	     << m->rx_election_epoch << " < " << mon->get_epoch() << dendl;
    return true;
  }

  // make sure the client is still connected.  note that a proxied
  // connection will be disconnected with a null message; don't drop
  // those.  also ignore loopback (e.g., log) messages.
  if (m->get_connection() &&
      !m->get_connection()->is_connected() &&
      m->get_connection() != mon->con_self &&
      m->get_connection()->get_messenger() != NULL) {
    dout(10) << " discarding message from disconnected client "
	     << m->get_source_inst() << " " << *m << dendl;
    return true;
  }

  // make sure our map is readable and up to date
  if (!is_readable(m->version)) {
    dout(10) << " waiting for paxos -> readable (v" << m->version << ")" << dendl;
    wait_for_readable(op, new C_RetryMessage(this, op), m->version);
    return true;
  }

  // preprocess
  if (preprocess_query(op)) 
    return true;  // easy!

  // leader?
  if (!mon->is_leader()) {
    mon->forward_request_leader(op);
    return true;
  }
  
  // writeable?
  if (!is_writeable()) {
    dout(10) << " waiting for paxos -> writeable" << dendl;
    wait_for_writeable(op, new C_RetryMessage(this, op));
    return true;
  }

  // update
  if (prepare_update(op)) {
    double delay = 0.0;
    if (should_propose(delay)) {
      if (delay == 0.0) {
	propose_pending();
      } else {
	// delay a bit
	if (!proposal_timer) {
	  proposal_timer = new C_Propose(this);
	  dout(10) << " setting proposal_timer " << proposal_timer << " with delay of " << delay << dendl;
	  mon->timer.add_event_after(delay, proposal_timer);
	} else { 
	  dout(10) << " proposal_timer already set" << dendl;
	}
      }
    } else {
      dout(10) << " not proposing" << dendl;
    }
  }     
  return true;
}

void PaxosService::refresh(bool *need_bootstrap)
{
  // update cached versions
  cached_first_committed = mon->store->get(get_service_name(), first_committed_name);
  cached_last_committed = mon->store->get(get_service_name(), last_committed_name);

  version_t new_format = get_value("format_version");
  if (new_format != format_version) {
    dout(1) << __func__ << " upgraded, format " << format_version << " -> " << new_format << dendl;
    on_upgrade();
  }
  format_version = new_format;

  dout(10) << __func__ << dendl;

  update_from_paxos(need_bootstrap);
}

void PaxosService::post_refresh()
{
  dout(10) << __func__ << dendl;

  post_paxos_update();

  if (mon->is_peon() && !waiting_for_finished_proposal.empty()) {
    finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);
  }
}

bool PaxosService::should_propose(double& delay)
{
  // simple default policy: quick startup, then some damping.
  if (get_last_committed() <= 1)
    delay = 0.0;
  else {
    utime_t now = ceph_clock_now(g_ceph_context);
    if ((now - paxos->last_commit_time) > g_conf->paxos_propose_interval)
      delay = (double)g_conf->paxos_min_wait;
    else
      delay = (double)(g_conf->paxos_propose_interval + paxos->last_commit_time
		       - now);
  }
  return true;
}


void PaxosService::propose_pending()
{
  dout(10) << "propose_pending" << dendl;
  assert(have_pending);
  assert(!proposing);
  assert(mon->is_leader());
  assert(is_active());

  if (proposal_timer) {
    dout(10) << " canceling proposal_timer " << proposal_timer << dendl;
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = NULL;
  }

  /**
   * @note What we contirbute to the pending Paxos transaction is
   *	   obtained by calling a function that must be implemented by
   *	   the class implementing us.  I.e., the function
   *	   encode_pending will be the one responsible to encode
   *	   whatever is pending on the implementation class into a
   *	   bufferlist, so we can then propose that as a value through
   *	   Paxos.
   */
  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();

  if (should_stash_full())
    encode_full(t);

  encode_pending(t);
  have_pending = false;

  if (format_version > 0) {
    t->put(get_service_name(), "format_version", format_version);
  }

  // apply to paxos
  proposing = true;
  paxos->queue_pending_finisher(new C_Committed(this));
  paxos->trigger_propose();
}

bool PaxosService::should_stash_full()
{
  version_t latest_full = get_version_latest_full();
  /* @note The first member of the condition is moot and it is here just for
   *	   clarity's sake. The second member would end up returing true
   *	   nonetheless because, in that event,
   *	      latest_full == get_trim_to() == 0.
   */
  return (!latest_full ||
	  (latest_full <= get_trim_to()) ||
	  (get_last_committed() - latest_full > (unsigned)g_conf->paxos_stash_full_interval));
}

void PaxosService::restart()
{
  dout(10) << "restart" << dendl;
  if (proposal_timer) {
    dout(10) << " canceling proposal_timer " << proposal_timer << dendl;
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);

  if (have_pending) {
    discard_pending();
    have_pending = false;
  }
  proposing = false;

  on_restart();
}

void PaxosService::election_finished()
{
  dout(10) << "election_finished" << dendl;

  finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);

  // make sure we update our state
  _active();
}

void PaxosService::_active()
{
  if (is_proposing()) {
    dout(10) << "_acting - proposing" << dendl;
    return;
  }
  if (!is_active()) {
    dout(10) << "_active - not active" << dendl;
    wait_for_active_ctx(new C_Active(this));
    return;
  }
  dout(10) << "_active" << dendl;

  // create pending state?
  if (mon->is_leader() && is_active()) {
    dout(7) << "_active creating new pending" << dendl;
    if (!have_pending) {
      create_pending();
      have_pending = true;
    }

    if (get_last_committed() == 0) {
      // create initial state
      create_initial();
      propose_pending();
      return;
    }
  } else {
    if (!mon->is_leader()) {
      dout(7) << __func__ << " we are not the leader, hence we propose nothing!" << dendl;
    } else if (!is_active()) {
      dout(7) << __func__ << " we are not active, hence we propose nothing!" << dendl;
    }
  }

  // wake up anyone who came in while we were proposing.  note that
  // anyone waiting for the previous proposal to commit is no longer
  // on this list; it is on Paxos's.
  finish_contexts(g_ceph_context, waiting_for_finished_proposal, 0);

  if (is_active() && mon->is_leader())
    upgrade_format();

  // NOTE: it's possible that this will get called twice if we commit
  // an old paxos value.  Implementations should be mindful of that.
  if (is_active())
    on_active();
}


void PaxosService::shutdown()
{
  cancel_events();

  if (proposal_timer) {
    dout(10) << " canceling proposal_timer " << proposal_timer << dendl;
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);

  on_shutdown();
}

void PaxosService::maybe_trim()
{
  if (!is_writeable())
    return;

  version_t trim_to = get_trim_to();
  if (trim_to < get_first_committed())
    return;

  version_t to_remove = trim_to - get_first_committed();
  if (g_conf->paxos_service_trim_min > 0 &&
      to_remove < (version_t)g_conf->paxos_service_trim_min) {
    dout(10) << __func__ << " trim_to " << trim_to << " would only trim " << to_remove
	     << " < paxos_service_trim_min " << g_conf->paxos_service_trim_min << dendl;
    return;
  }

  if (g_conf->paxos_service_trim_max > 0 &&
      to_remove > (version_t)g_conf->paxos_service_trim_max) {
    dout(10) << __func__ << " trim_to " << trim_to << " would only trim " << to_remove
	     << " > paxos_service_trim_max, limiting to " << g_conf->paxos_service_trim_max
	     << dendl;
    trim_to = get_first_committed() + g_conf->paxos_service_trim_max;
    to_remove = trim_to - get_first_committed();
  }

  dout(10) << __func__ << " trimming to " << trim_to << ", " << to_remove << " states" << dendl;
  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
  trim(t, get_first_committed(), trim_to);
  put_first_committed(t, trim_to);

  // let the service add any extra stuff
  encode_trim_extra(t, trim_to);

  paxos->trigger_propose();
}

void PaxosService::trim(MonitorDBStore::TransactionRef t,
			version_t from, version_t to)
{
  dout(10) << __func__ << " from " << from << " to " << to << dendl;
  assert(from != to);

  for (version_t v = from; v < to; ++v) {
    dout(20) << __func__ << " " << v << dendl;
    t->erase(get_service_name(), v);

    string full_key = mon->store->combine_strings("full", v);
    if (mon->store->exists(get_service_name(), full_key)) {
      dout(20) << __func__ << " " << full_key << dendl;
      t->erase(get_service_name(), full_key);
    }
  }
  if (g_conf->mon_compact_on_trim) {
    dout(20) << " compacting prefix " << get_service_name() << dendl;
    t->compact_range(get_service_name(), stringify(from - 1), stringify(to));
    t->compact_range(get_service_name(),
		     mon->store->combine_strings(full_prefix_name, from - 1),
		     mon->store->combine_strings(full_prefix_name, to));
  }
}

