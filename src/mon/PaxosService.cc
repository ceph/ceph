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

#define dout_subsys ceph_subsys_paxos
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, paxos, service_name)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, Paxos *paxos, string service_name) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").paxosservice(" << service_name << ") ";
}

bool PaxosService::dispatch(PaxosServiceMessage *m)
{
  dout(10) << "dispatch " << *m << " from " << m->get_orig_source_inst() << dendl;

  // make sure this message isn't forwarded from a previous election epoch
  if (m->rx_election_epoch &&
      m->rx_election_epoch < mon->get_epoch()) {
    dout(10) << " discarding forwarded message from previous election epoch "
	     << m->rx_election_epoch << " < " << mon->get_epoch() << dendl;
    m->put();
    return true;
  }

  // make sure the client is still connected.  note that a proxied
  // connection will be disconnected with a null message; don't drop
  // those.  also ignore loopback (e.g., log) messages.
  if (!m->get_connection()->is_connected() &&
      m->get_connection() != mon->messenger->get_loopback_connection() &&
      m->get_connection()->get_messenger() != NULL) {
    dout(10) << " discarding message from disconnected client "
	     << m->get_source_inst() << " " << *m << dendl;
    m->put();
    return true;
  }

  // make sure our map is readable and up to date
  if (!is_readable(m->version)) {
    dout(10) << " waiting for paxos -> readable (v" << m->version << ")" << dendl;
    wait_for_readable(new C_RetryMessage(this, m), m->version);
    return true;
  }

  // make sure service has latest from paxos.
  update_from_paxos();

  // preprocess
  if (preprocess_query(m)) 
    return true;  // easy!

  // leader?
  if (!mon->is_leader()) {
    mon->forward_request_leader(m);
    return true;
  }
  
  // writeable?
  if (!is_writeable()) {
    dout(10) << " waiting for paxos -> writeable" << dendl;
    wait_for_writeable(new C_RetryMessage(this, m));
    return true;
  }

  // update
  if (prepare_update(m)) {
    double delay = 0.0;
    if (should_propose(delay)) {
      if (delay == 0.0) {
	propose_pending();
      } else {
	// delay a bit
	if (!proposal_timer) {
	  dout(10) << " setting propose timer with delay of " << delay << dendl;
	  proposal_timer = new C_Propose(this);
	  mon->timer.add_event_after(delay, proposal_timer);
	} else { 
	  dout(10) << " propose timer already set" << dendl;
	}
      }
    } else {
      dout(10) << " not proposing" << dendl;
    }
  }     
  return true;
}

void PaxosService::scrub()
{
  dout(10) << __func__ << dendl;
  if (!mon->store->exists(get_service_name(), "conversion_first"))
    return;

  version_t cf = mon->store->get(get_service_name(), "conversion_first");
  version_t fc = get_first_committed();

  dout(10) << __func__ << " conversion_first " << cf
	   << " first committed " << fc << dendl;

  MonitorDBStore::Transaction t;
  if (cf < fc) {
    trim(&t, cf, fc);
  }
  t.erase(get_service_name(), "conversion_first");
  mon->store->apply_transaction(t);
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
  assert(mon->is_leader());
  assert(is_active());
  if (!is_active())
    return;

  if (proposal_timer) {
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  /**
   * @note The value we propose is encoded in a bufferlist, passed to 
   *	   Paxos::propose_new_value and it is obtained by calling a 
   *	   function that must be implemented by the class implementing us.
   *	   I.e., the function encode_pending will be the one responsible
   *	   to encode whatever is pending on the implementation class into a
   *	   bufferlist, so we can then propose that as a value through Paxos.
   */
  MonitorDBStore::Transaction t;
  bufferlist bl;

  update_trim();
  if (should_stash_full())
    encode_full(&t);

  if (should_trim()) {
    encode_trim(&t);
  }

  encode_pending(&t);
  have_pending = false;

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t.dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  t.encode(bl);

  // apply to paxos
  proposing = true;
  paxos->propose_new_value(bl, new C_Committed(this));
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
	  (get_version() - latest_full > (unsigned)g_conf->paxos_stash_full_interval));
}

void PaxosService::restart()
{
  dout(10) << "restart" << dendl;
  if (proposal_timer) {
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);

  on_restart();
}

void PaxosService::election_finished()
{
  dout(10) << "election_finished" << dendl;

  if (proposal_timer) {
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  if (have_pending) {
    discard_pending();
    have_pending = false;
  }
  proposing = false;

  finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);

  // make sure we update our state
  if (is_active())
    _active();
  else
    wait_for_active(new C_Active(this));
}

void PaxosService::_active()
{
  if (!is_active()) {
    dout(10) << "_active - not active" << dendl;
    wait_for_active(new C_Active(this));
    return;
  }
  dout(10) << "_active" << dendl;

  // pull latest from paxos
  update_from_paxos();

  scrub();

  // create pending state?
  if (mon->is_leader() && is_active()) {
    dout(7) << "_active creating new pending" << dendl;
    if (!have_pending) {
      create_pending();
      have_pending = true;
    }

    if (get_version() == 0) {
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

  // NOTE: it's possible that this will get called twice if we commit
  // an old paxos value.  Implementations should be mindful of that.
  if (is_active())
    on_active();
}


void PaxosService::shutdown()
{
  cancel_events();

  if (proposal_timer) {
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  finish_contexts(g_ceph_context, waiting_for_finished_proposal, -EAGAIN);
}

void PaxosService::put_version(MonitorDBStore::Transaction *t,
			       const string& prefix, version_t ver,
			       bufferlist& bl)
{
  ostringstream os;
  os << ver;
  string key = mon->store->combine_strings(prefix, os.str());
  t->put(get_service_name(), key, bl);
}

int PaxosService::get_version(const string& prefix, version_t ver,
			      bufferlist& bl)
{
  ostringstream os;
  os << ver;
  string key = mon->store->combine_strings(prefix, os.str());
  return mon->store->get(get_service_name(), key, bl);
}

void PaxosService::trim(MonitorDBStore::Transaction *t,
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
  }
}

void PaxosService::encode_trim(MonitorDBStore::Transaction *t)
{
  version_t first_committed = get_first_committed();
  version_t latest_full = get_version("full", "latest");
  version_t trim_to = get_trim_to();

  dout(10) << __func__ << " " << trim_to << " (was " << first_committed << ")"
	   << ", latest full " << latest_full << dendl;

  if (first_committed >= trim_to)
    return;

  version_t trim_to_max = trim_to;
  if ((g_conf->paxos_service_trim_max > 0)
      && (trim_to - first_committed > (size_t)g_conf->paxos_service_trim_max)) {
    trim_to_max = first_committed + g_conf->paxos_service_trim_max;
  }

  dout(10) << __func__ << " trimming versions " << first_committed
           << " to " << trim_to_max << dendl;

  trim(t, first_committed, trim_to_max);
  put_first_committed(t, trim_to_max);

  if (trim_to_max == trim_to)
    set_trim_to(0);
}

