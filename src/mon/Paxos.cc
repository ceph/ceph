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

#include <sstream>
#include "Paxos.h"
#include "Monitor.h"
#include "MonitorDBStore.h"

#include "messages/MMonPaxos.h"

#include "common/config.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"

#define dout_subsys ceph_subsys_paxos
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, mon->name, mon->rank, paxos_name, state, first_committed, last_committed)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, const string& name,
		        int rank, const string& paxos_name, int state,
			version_t first_committed, version_t last_committed)
{
  return *_dout << "mon." << name << "@" << rank
		<< "(" << mon->get_state_name() << ")"
		<< ".paxos(" << paxos_name << " " << Paxos::get_statename(state)
		<< " c " << first_committed << ".." << last_committed
		<< ") ";
}

MonitorDBStore *Paxos::get_store()
{
  return mon->store;
}

void Paxos::read_and_prepare_transactions(MonitorDBStore::TransactionRef tx,
					  version_t first, version_t last)
{
  dout(10) << __func__ << " first " << first << " last " << last << dendl;
  for (version_t v = first; v <= last; ++v) {
    dout(30) << __func__ << " apply version " << v << dendl;
    bufferlist bl;
    int err = get_store()->get(get_name(), v, bl);
    assert(err == 0);
    assert(bl.length());
    decode_append_transaction(tx, bl);
  }
  dout(15) << __func__ << " total versions " << (last-first) << dendl;
}

void Paxos::init()
{
  // load paxos variables from stable storage
  last_pn = get_store()->get(get_name(), "last_pn");
  accepted_pn = get_store()->get(get_name(), "accepted_pn");
  last_committed = get_store()->get(get_name(), "last_committed");
  first_committed = get_store()->get(get_name(), "first_committed");

  dout(10) << __func__ << " last_pn: " << last_pn << " accepted_pn: "
	   << accepted_pn << " last_committed: " << last_committed
	   << " first_committed: " << first_committed << dendl;

  dout(10) << "init" << dendl;
  assert(is_consistent());
}

void Paxos::init_logger()
{
  PerfCountersBuilder pcb(g_ceph_context, "paxos", l_paxos_first, l_paxos_last);
  pcb.add_u64_counter(l_paxos_start_leader, "start_leader", "Starts in leader role");
  pcb.add_u64_counter(l_paxos_start_peon, "start_peon", "Starts in peon role");
  pcb.add_u64_counter(l_paxos_restart, "restart", "Restarts");
  pcb.add_u64_counter(l_paxos_refresh, "refresh", "Refreshes");
  pcb.add_time_avg(l_paxos_refresh_latency, "refresh_latency", "Refresh latency");
  pcb.add_u64_counter(l_paxos_begin, "begin", "Started and handled begins");
  pcb.add_u64_avg(l_paxos_begin_keys, "begin_keys", "Keys in transaction on begin");
  pcb.add_u64_avg(l_paxos_begin_bytes, "begin_bytes", "Data in transaction on begin");
  pcb.add_time_avg(l_paxos_begin_latency, "begin_latency", "Latency of begin operation");
  pcb.add_u64_counter(l_paxos_commit, "commit",
      "Commits", "cmt");
  pcb.add_u64_avg(l_paxos_commit_keys, "commit_keys", "Keys in transaction on commit");
  pcb.add_u64_avg(l_paxos_commit_bytes, "commit_bytes", "Data in transaction on commit");
  pcb.add_time_avg(l_paxos_commit_latency, "commit_latency",
      "Commit latency", "clat");
  pcb.add_u64_counter(l_paxos_collect, "collect", "Peon collects");
  pcb.add_u64_avg(l_paxos_collect_keys, "collect_keys", "Keys in transaction on peon collect");
  pcb.add_u64_avg(l_paxos_collect_bytes, "collect_bytes", "Data in transaction on peon collect");
  pcb.add_time_avg(l_paxos_collect_latency, "collect_latency", "Peon collect latency");
  pcb.add_u64_counter(l_paxos_collect_uncommitted, "collect_uncommitted", "Uncommitted values in started and handled collects");
  pcb.add_u64_counter(l_paxos_collect_timeout, "collect_timeout", "Collect timeouts");
  pcb.add_u64_counter(l_paxos_accept_timeout, "accept_timeout", "Accept timeouts");
  pcb.add_u64_counter(l_paxos_lease_ack_timeout, "lease_ack_timeout", "Lease acknowledgement timeouts");
  pcb.add_u64_counter(l_paxos_lease_timeout, "lease_timeout", "Lease timeouts");
  pcb.add_u64_counter(l_paxos_store_state, "store_state", "Store a shared state on disk");
  pcb.add_u64_avg(l_paxos_store_state_keys, "store_state_keys", "Keys in transaction in stored state");
  pcb.add_u64_avg(l_paxos_store_state_bytes, "store_state_bytes", "Data in transaction in stored state");
  pcb.add_time_avg(l_paxos_store_state_latency, "store_state_latency", "Storing state latency");
  pcb.add_u64_counter(l_paxos_share_state, "share_state", "Sharings of state");
  pcb.add_u64_avg(l_paxos_share_state_keys, "share_state_keys", "Keys in shared state");
  pcb.add_u64_avg(l_paxos_share_state_bytes, "share_state_bytes", "Data in shared state");
  pcb.add_u64_counter(l_paxos_new_pn, "new_pn", "New proposal number queries");
  pcb.add_time_avg(l_paxos_new_pn_latency, "new_pn_latency", "New proposal number getting latency");
  logger = pcb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

void Paxos::dump_info(Formatter *f)
{
  f->open_object_section("paxos");
  f->dump_unsigned("first_committed", first_committed);
  f->dump_unsigned("last_committed", last_committed);
  f->dump_unsigned("last_pn", last_pn);
  f->dump_unsigned("accepted_pn", accepted_pn);
  f->close_section();
}

// ---------------------------------

// PHASE 1

// leader
void Paxos::collect(version_t oldpn)
{
  // we're recoverying, it seems!
  state = STATE_RECOVERING;
  assert(mon->is_leader());

  // reset the number of lasts received
  uncommitted_v = 0;
  uncommitted_pn = 0;
  uncommitted_value.clear();
  peer_first_committed.clear();
  peer_last_committed.clear();

  // look for uncommitted value
  if (get_store()->exists(get_name(), last_committed+1)) {
    version_t v = get_store()->get(get_name(), "pending_v");
    version_t pn = get_store()->get(get_name(), "pending_pn");
    if (v && pn && v == last_committed + 1) {
      uncommitted_pn = pn;
    } else {
      dout(10) << "WARNING: no pending_pn on disk, using previous accepted_pn " << accepted_pn
	       << " and crossing our fingers" << dendl;
      uncommitted_pn = accepted_pn;
    }
    uncommitted_v = last_committed+1;

    get_store()->get(get_name(), last_committed+1, uncommitted_value);
    assert(uncommitted_value.length());
    dout(10) << "learned uncommitted " << (last_committed+1)
	     << " pn " << uncommitted_pn
	     << " (" << uncommitted_value.length() << " bytes) from myself" 
	     << dendl;

    logger->inc(l_paxos_collect_uncommitted);
  }

  // pick new pn
  accepted_pn = get_new_proposal_number(MAX(accepted_pn, oldpn));
  accepted_pn_from = last_committed;
  num_last = 1;
  dout(10) << "collect with pn " << accepted_pn << dendl;

  // send collect
  for (set<int>::const_iterator p = mon->get_quorum().begin();
       p != mon->get_quorum().end();
       ++p) {
    if (*p == mon->rank) continue;
    
    MMonPaxos *collect = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_COLLECT,
				       ceph_clock_now(g_ceph_context));
    collect->last_committed = last_committed;
    collect->first_committed = first_committed;
    collect->pn = accepted_pn;
    mon->messenger->send_message(collect, mon->monmap->get_inst(*p));
  }

  // set timeout event
  collect_timeout_event = new C_CollectTimeout(this);
  mon->timer.add_event_after(g_conf->mon_accept_timeout_factor *
			     g_conf->mon_lease,
			     collect_timeout_event);
}


// peon
void Paxos::handle_collect(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_collect");

  MMonPaxos *collect = static_cast<MMonPaxos*>(op->get_req());
  dout(10) << "handle_collect " << *collect << dendl;

  assert(mon->is_peon()); // mon epoch filter should catch strays

  // we're recoverying, it seems!
  state = STATE_RECOVERING;

  if (collect->first_committed > last_committed+1) {
    dout(2) << __func__
            << " leader's lowest version is too high for our last committed"
            << " (theirs: " << collect->first_committed
            << "; ours: " << last_committed << ") -- bootstrap!" << dendl;
    op->mark_paxos_event("need to bootstrap");
    mon->bootstrap();
    return;
  }

  // reply
  MMonPaxos *last = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_LAST,
				  ceph_clock_now(g_ceph_context));
  last->last_committed = last_committed;
  last->first_committed = first_committed;
  
  version_t previous_pn = accepted_pn;

  // can we accept this pn?
  if (collect->pn > accepted_pn) {
    // ok, accept it
    accepted_pn = collect->pn;
    accepted_pn_from = collect->pn_from;
    dout(10) << "accepting pn " << accepted_pn << " from " 
	     << accepted_pn_from << dendl;
  
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    t->put(get_name(), "accepted_pn", accepted_pn);

    dout(30) << __func__ << " transaction dump:\n";
    JSONFormatter f(true);
    t->dump(&f);
    f.flush(*_dout);
    *_dout << dendl;

    logger->inc(l_paxos_collect);
    logger->inc(l_paxos_collect_keys, t->get_keys());
    logger->inc(l_paxos_collect_bytes, t->get_bytes());
    utime_t start = ceph_clock_now(NULL);

    get_store()->apply_transaction(t);

    utime_t end = ceph_clock_now(NULL);
    logger->tinc(l_paxos_collect_latency, end - start);
  } else {
    // don't accept!
    dout(10) << "NOT accepting pn " << collect->pn << " from " << collect->pn_from
	     << ", we already accepted " << accepted_pn
	     << " from " << accepted_pn_from << dendl;
  }
  last->pn = accepted_pn;
  last->pn_from = accepted_pn_from;

  // share whatever committed values we have
  if (collect->last_committed < last_committed)
    share_state(last, collect->first_committed, collect->last_committed);

  // do we have an accepted but uncommitted value?
  //  (it'll be at last_committed+1)
  bufferlist bl;
  if (collect->last_committed <= last_committed &&
      get_store()->exists(get_name(), last_committed+1)) {
    get_store()->get(get_name(), last_committed+1, bl);
    assert(bl.length() > 0);
    dout(10) << " sharing our accepted but uncommitted value for " 
	     << last_committed+1 << " (" << bl.length() << " bytes)" << dendl;
    last->values[last_committed+1] = bl;

    version_t v = get_store()->get(get_name(), "pending_v");
    version_t pn = get_store()->get(get_name(), "pending_pn");
    if (v && pn && v == last_committed + 1) {
      last->uncommitted_pn = pn;
    } else {
      // previously we didn't record which pn a value was accepted
      // under!  use the pn value we just had...  :(
      dout(10) << "WARNING: no pending_pn on disk, using previous accepted_pn " << previous_pn
	       << " and crossing our fingers" << dendl;
      last->uncommitted_pn = previous_pn;
    }

    logger->inc(l_paxos_collect_uncommitted);
  }

  // send reply
  collect->get_connection()->send_message(last);
}

/**
 * @note This is Okay. We share our versions between peer_last_committed and
 *	 our last_committed (inclusive), and add their bufferlists to the
 *	 message. It will be the peer's job to apply them to its store, as
 *	 these bufferlists will contain raw transactions.
 *	 This function is called by both the Peon and the Leader. The Peon will
 *	 share the state with the Leader during handle_collect(), sharing any
 *	 values the leader may be missing (i.e., the leader's last_committed is
 *	 lower than the peon's last_committed). The Leader will share the state
 *	 with the Peon during handle_last(), if the peon's last_committed is
 *	 lower than the leader's last_committed.
 */
void Paxos::share_state(MMonPaxos *m, version_t peer_first_committed,
			version_t peer_last_committed)
{
  assert(peer_last_committed < last_committed);

  dout(10) << "share_state peer has fc " << peer_first_committed 
	   << " lc " << peer_last_committed << dendl;
  version_t v = peer_last_committed + 1;

  // include incrementals
  uint64_t bytes = 0;
  for ( ; v <= last_committed; v++) {
    if (get_store()->exists(get_name(), v)) {
      get_store()->get(get_name(), v, m->values[v]);
      assert(m->values[v].length());
      dout(10) << " sharing " << v << " ("
	       << m->values[v].length() << " bytes)" << dendl;
      bytes += m->values[v].length() + 16;  // paxos_ + 10 digits = 16
    }
  }
  logger->inc(l_paxos_share_state);
  logger->inc(l_paxos_share_state_keys, m->values.size());
  logger->inc(l_paxos_share_state_bytes, bytes);

  m->last_committed = last_committed;
}

/**
 * Store on disk a state that was shared with us
 *
 * Basically, we received a set of version. Or just one. It doesn't matter.
 * What matters is that we have to stash it in the store. So, we will simply
 * write every single bufferlist into their own versions on our side (i.e.,
 * onto paxos-related keys), and then we will decode those same bufferlists
 * we just wrote and apply the transactions they hold. We will also update
 * our first and last committed values to point to the new values, if need
 * be. All all this is done tightly wrapped in a transaction to ensure we
 * enjoy the atomicity guarantees given by our awesome k/v store.
 */
bool Paxos::store_state(MMonPaxos *m)
{
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  map<version_t,bufferlist>::iterator start = m->values.begin();
  bool changed = false;

  // build map of values to store
  // we want to write the range [last_committed, m->last_committed] only.
  if (start != m->values.end() &&
      start->first > last_committed + 1) {
    // ignore everything if values start in the future.
    dout(10) << "store_state ignoring all values, they start at " << start->first
	     << " > last_committed+1" << dendl;
    start = m->values.end();
  }

  // push forward the start position on the message's values iterator, up until
  // we run out of positions or we find a position matching 'last_committed'.
  while (start != m->values.end() && start->first <= last_committed) {
    ++start;
  }

  // make sure we get the right interval of values to apply by pushing forward
  // the 'end' iterator until it matches the message's 'last_committed'.
  map<version_t,bufferlist>::iterator end = start;
  while (end != m->values.end() && end->first <= m->last_committed) {
    last_committed = end->first;
    ++end;
  }

  if (start == end) {
    dout(10) << "store_state nothing to commit" << dendl;
  } else {
    dout(10) << "store_state [" << start->first << ".." 
	     << last_committed << "]" << dendl;
    t->put(get_name(), "last_committed", last_committed);

    // we should apply the state here -- decode every single bufferlist in the
    // map and append the transactions to 't'.
    map<version_t,bufferlist>::iterator it;
    for (it = start; it != end; ++it) {
      // write the bufferlist as the version's value
      t->put(get_name(), it->first, it->second);
      // decode the bufferlist and append it to the transaction we will shortly
      // apply.
      decode_append_transaction(t, it->second);
    }

    // discard obsolete uncommitted value?
    if (uncommitted_v && uncommitted_v <= last_committed) {
      dout(10) << " forgetting obsolete uncommitted value " << uncommitted_v
	       << " pn " << uncommitted_pn << dendl;
      uncommitted_v = 0;
      uncommitted_pn = 0;
      uncommitted_value.clear();
    }
  }
  if (!t->empty()) {
    dout(30) << __func__ << " transaction dump:\n";
    JSONFormatter f(true);
    t->dump(&f);
    f.flush(*_dout);
    *_dout << dendl;

    logger->inc(l_paxos_store_state);
    logger->inc(l_paxos_store_state_bytes, t->get_bytes());
    logger->inc(l_paxos_store_state_keys, t->get_keys());
    utime_t start = ceph_clock_now(NULL);

    get_store()->apply_transaction(t);

    utime_t end = ceph_clock_now(NULL);
    logger->tinc(l_paxos_store_state_latency, end - start);

    // refresh first_committed; this txn may have trimmed.
    first_committed = get_store()->get(get_name(), "first_committed");

    _sanity_check_store();
    changed = true;
  }

  return changed;
}

void Paxos::_sanity_check_store()
{
  version_t lc = get_store()->get(get_name(), "last_committed");
  assert(lc == last_committed);
}


// leader
void Paxos::handle_last(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_last");
  MMonPaxos *last = static_cast<MMonPaxos*>(op->get_req());
  bool need_refresh = false;
  int from = last->get_source().num();

  dout(10) << "handle_last " << *last << dendl;

  if (!mon->is_leader()) {
    dout(10) << "not leader, dropping" << dendl;
    return;
  }

  // note peer's first_ and last_committed, in case we learn a new
  // commit and need to push it to them.
  peer_first_committed[from] = last->first_committed;
  peer_last_committed[from] = last->last_committed;

  if (last->first_committed > last_committed + 1) {
    dout(5) << __func__
            << " mon." << from
	    << " lowest version is too high for our last committed"
            << " (theirs: " << last->first_committed
            << "; ours: " << last_committed << ") -- bootstrap!" << dendl;
    op->mark_paxos_event("need to bootstrap");
    mon->bootstrap();
    return;
  }

  assert(g_conf->paxos_kill_at != 1);

  // store any committed values if any are specified in the message
  need_refresh = store_state(last);

  assert(g_conf->paxos_kill_at != 2);

  // is everyone contiguous and up to date?
  for (map<int,version_t>::iterator p = peer_last_committed.begin();
       p != peer_last_committed.end();
       ++p) {
    if (p->second + 1 < first_committed && first_committed > 1) {
      dout(5) << __func__
	      << " peon " << p->first
	      << " last_committed (" << p->second
	      << ") is too low for our first_committed (" << first_committed
	      << ") -- bootstrap!" << dendl;
      op->mark_paxos_event("need to bootstrap");
      mon->bootstrap();
      return;
    }
    if (p->second < last_committed) {
      // share committed values
      dout(10) << " sending commit to mon." << p->first << dendl;
      MMonPaxos *commit = new MMonPaxos(mon->get_epoch(),
					MMonPaxos::OP_COMMIT,
					ceph_clock_now(g_ceph_context));
      share_state(commit, peer_first_committed[p->first], p->second);
      mon->messenger->send_message(commit, mon->monmap->get_inst(p->first));
    }
  }

  // do they accept your pn?
  if (last->pn > accepted_pn) {
    // no, try again.
    dout(10) << " they had a higher pn than us, picking a new one." << dendl;

    // cancel timeout event
    mon->timer.cancel_event(collect_timeout_event);
    collect_timeout_event = 0;

    collect(last->pn);
  } else if (last->pn == accepted_pn) {
    // yes, they accepted our pn.  great.
    num_last++;
    dout(10) << " they accepted our pn, we now have " 
	     << num_last << " peons" << dendl;

    // did this person send back an accepted but uncommitted value?
    if (last->uncommitted_pn) {
      if (last->uncommitted_pn >= uncommitted_pn &&
	  last->last_committed >= last_committed &&
	  last->last_committed + 1 >= uncommitted_v) {
	uncommitted_v = last->last_committed+1;
	uncommitted_pn = last->uncommitted_pn;
	uncommitted_value = last->values[uncommitted_v];
	dout(10) << "we learned an uncommitted value for " << uncommitted_v
		 << " pn " << uncommitted_pn
		 << " " << uncommitted_value.length() << " bytes"
		 << dendl;
      } else {
	dout(10) << "ignoring uncommitted value for " << (last->last_committed+1)
		 << " pn " << last->uncommitted_pn
		 << " " << last->values[last->last_committed+1].length() << " bytes"
		 << dendl;
      }
    }
    
    // is that everyone?
    if (num_last == mon->get_quorum().size()) {
      // cancel timeout event
      mon->timer.cancel_event(collect_timeout_event);
      collect_timeout_event = 0;
      peer_first_committed.clear();
      peer_last_committed.clear();

      // almost...

      // did we learn an old value?
      if (uncommitted_v == last_committed+1 &&
	  uncommitted_value.length()) {
	dout(10) << "that's everyone.  begin on old learned value" << dendl;
	state = STATE_UPDATING_PREVIOUS;
	begin(uncommitted_value);
      } else {
	// active!
	dout(10) << "that's everyone.  active!" << dendl;
	extend_lease();

	need_refresh = false;
	if (do_refresh()) {
	  finish_round();
	}
      }
    }
  } else {
    // no, this is an old message, discard
    dout(10) << "old pn, ignoring" << dendl;
  }

  if (need_refresh)
    (void)do_refresh();
}

void Paxos::collect_timeout()
{
  dout(1) << "collect timeout, calling fresh election" << dendl;
  collect_timeout_event = 0;
  logger->inc(l_paxos_collect_timeout);
  assert(mon->is_leader());
  mon->bootstrap();
}


// leader
void Paxos::begin(bufferlist& v)
{
  dout(10) << "begin for " << last_committed+1 << " " 
	   << v.length() << " bytes"
	   << dendl;

  assert(mon->is_leader());
  assert(is_updating() || is_updating_previous());

  // we must already have a majority for this to work.
  assert(mon->get_quorum().size() == 1 ||
	 num_last > (unsigned)mon->monmap->size()/2);
  
  // and no value, yet.
  assert(new_value.length() == 0);

  // accept it ourselves
  accepted.clear();
  accepted.insert(mon->rank);
  new_value = v;

  if (last_committed == 0) {
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    // initial base case; set first_committed too
    t->put(get_name(), "first_committed", 1);
    decode_append_transaction(t, new_value);

    bufferlist tx_bl;
    t->encode(tx_bl);

    new_value = tx_bl;
  }

  // store the proposed value in the store. IF it is accepted, we will then
  // have to decode it into a transaction and apply it.
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  t->put(get_name(), last_committed+1, new_value);

  // note which pn this pending value is for.
  t->put(get_name(), "pending_v", last_committed + 1);
  t->put(get_name(), "pending_pn", accepted_pn);

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t->dump(&f);
  f.flush(*_dout);
  MonitorDBStore::TransactionRef debug_tx(new MonitorDBStore::Transaction);
  bufferlist::iterator new_value_it = new_value.begin();
  debug_tx->decode(new_value_it);
  debug_tx->dump(&f);
  *_dout << "\nbl dump:\n";
  f.flush(*_dout);
  *_dout << dendl;

  logger->inc(l_paxos_begin);
  logger->inc(l_paxos_begin_keys, t->get_keys());
  logger->inc(l_paxos_begin_bytes, t->get_bytes());
  utime_t start = ceph_clock_now(NULL);

  get_store()->apply_transaction(t);

  utime_t end = ceph_clock_now(NULL);
  logger->tinc(l_paxos_begin_latency, end - start);

  assert(g_conf->paxos_kill_at != 3);

  if (mon->get_quorum().size() == 1) {
    // we're alone, take it easy
    commit_start();
    return;
  }

  // ask others to accept it too!
  for (set<int>::const_iterator p = mon->get_quorum().begin();
       p != mon->get_quorum().end();
       ++p) {
    if (*p == mon->rank) continue;
    
    dout(10) << " sending begin to mon." << *p << dendl;
    MMonPaxos *begin = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_BEGIN,
				     ceph_clock_now(g_ceph_context));
    begin->values[last_committed+1] = new_value;
    begin->last_committed = last_committed;
    begin->pn = accepted_pn;
    
    mon->messenger->send_message(begin, mon->monmap->get_inst(*p));
  }

  // set timeout event
  accept_timeout_event = new C_AcceptTimeout(this);
  mon->timer.add_event_after(g_conf->mon_accept_timeout_factor *
			     g_conf->mon_lease,
			     accept_timeout_event);
}

// peon
void Paxos::handle_begin(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_begin");
  MMonPaxos *begin = static_cast<MMonPaxos*>(op->get_req());
  dout(10) << "handle_begin " << *begin << dendl;

  // can we accept this?
  if (begin->pn < accepted_pn) {
    dout(10) << " we accepted a higher pn " << accepted_pn << ", ignoring" << dendl;
    op->mark_paxos_event("have higher pn, ignore");
    return;
  }
  assert(begin->pn == accepted_pn);
  assert(begin->last_committed == last_committed);
  
  assert(g_conf->paxos_kill_at != 4);

  logger->inc(l_paxos_begin);

  // set state.
  state = STATE_UPDATING;
  lease_expire = utime_t();  // cancel lease

  // yes.
  version_t v = last_committed+1;
  dout(10) << "accepting value for " << v << " pn " << accepted_pn << dendl;
  // store the accepted value onto our store. We will have to decode it and
  // apply its transaction once we receive permission to commit.
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  t->put(get_name(), v, begin->values[v]);

  // note which pn this pending value is for.
  t->put(get_name(), "pending_v", v);
  t->put(get_name(), "pending_pn", accepted_pn);

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  logger->inc(l_paxos_begin_bytes, t->get_bytes());
  utime_t start = ceph_clock_now(NULL);

  get_store()->apply_transaction(t);

  utime_t end = ceph_clock_now(NULL);
  logger->tinc(l_paxos_begin_latency, end - start);

  assert(g_conf->paxos_kill_at != 5);

  // reply
  MMonPaxos *accept = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_ACCEPT,
				    ceph_clock_now(g_ceph_context));
  accept->pn = accepted_pn;
  accept->last_committed = last_committed;
  begin->get_connection()->send_message(accept);
}

// leader
void Paxos::handle_accept(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_accept");
  MMonPaxos *accept = static_cast<MMonPaxos*>(op->get_req());
  dout(10) << "handle_accept " << *accept << dendl;
  int from = accept->get_source().num();

  if (accept->pn != accepted_pn) {
    // we accepted a higher pn, from some other leader
    dout(10) << " we accepted a higher pn " << accepted_pn << ", ignoring" << dendl;
    op->mark_paxos_event("have higher pn, ignore");
    return;
  }
  if (last_committed > 0 &&
      accept->last_committed < last_committed-1) {
    dout(10) << " this is from an old round, ignoring" << dendl;
    op->mark_paxos_event("old round, ignore");
    return;
  }
  assert(accept->last_committed == last_committed ||   // not committed
	 accept->last_committed == last_committed-1);  // committed

  assert(is_updating() || is_updating_previous());
  assert(accepted.count(from) == 0);
  accepted.insert(from);
  dout(10) << " now " << accepted << " have accepted" << dendl;

  assert(g_conf->paxos_kill_at != 6);

  // only commit (and expose committed state) when we get *all* quorum
  // members to accept.  otherwise, they may still be sharing the now
  // stale state.
  // FIXME: we can improve this with an additional lease revocation message
  // that doesn't block for the persist.
  if (accepted == mon->get_quorum()) {
    // yay, commit!
    dout(10) << " got majority, committing, done with update" << dendl;
    op->mark_paxos_event("commit_start");
    commit_start();
  }
}

void Paxos::accept_timeout()
{
  dout(1) << "accept timeout, calling fresh election" << dendl;
  accept_timeout_event = 0;
  assert(mon->is_leader());
  assert(is_updating() || is_updating_previous() || is_writing() ||
	 is_writing_previous());
  logger->inc(l_paxos_accept_timeout);
  mon->bootstrap();
}

struct C_Committed : public Context {
  Paxos *paxos;
  explicit C_Committed(Paxos *p) : paxos(p) {}
  void finish(int r) {
    assert(r >= 0);
    Mutex::Locker l(paxos->mon->lock);
    paxos->commit_finish();
  }
};

void Paxos::commit_start()
{
  dout(10) << __func__ << " " << (last_committed+1) << dendl;

  assert(g_conf->paxos_kill_at != 7);

  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);

  // commit locally
  t->put(get_name(), "last_committed", last_committed + 1);

  // decode the value and apply its transaction to the store.
  // this value can now be read from last_committed.
  decode_append_transaction(t, new_value);

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  logger->inc(l_paxos_commit);
  logger->inc(l_paxos_commit_keys, t->get_keys());
  logger->inc(l_paxos_commit_bytes, t->get_bytes());
  commit_start_stamp = ceph_clock_now(NULL);

  get_store()->queue_transaction(t, new C_Committed(this));

  if (is_updating_previous())
    state = STATE_WRITING_PREVIOUS;
  else if (is_updating())
    state = STATE_WRITING;
  else
    assert(0);

  if (mon->get_quorum().size() > 1) {
    // cancel timeout event
    mon->timer.cancel_event(accept_timeout_event);
    accept_timeout_event = 0;
  }
}

void Paxos::commit_finish()
{
  dout(20) << __func__ << " " << (last_committed+1) << dendl;
  utime_t end = ceph_clock_now(NULL);
  logger->tinc(l_paxos_commit_latency, end - commit_start_stamp);

  assert(g_conf->paxos_kill_at != 8);

  // cancel lease - it was for the old value.
  //  (this would only happen if message layer lost the 'begin', but
  //   leader still got a majority and committed with out us.)
  lease_expire = utime_t();  // cancel lease

  last_committed++;
  last_commit_time = ceph_clock_now(NULL);

  // refresh first_committed; this txn may have trimmed.
  first_committed = get_store()->get(get_name(), "first_committed");

  _sanity_check_store();

  // tell everyone
  for (set<int>::const_iterator p = mon->get_quorum().begin();
       p != mon->get_quorum().end();
       ++p) {
    if (*p == mon->rank) continue;

    dout(10) << " sending commit to mon." << *p << dendl;
    MMonPaxos *commit = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_COMMIT,
				      ceph_clock_now(g_ceph_context));
    commit->values[last_committed] = new_value;
    commit->pn = accepted_pn;
    commit->last_committed = last_committed;

    mon->messenger->send_message(commit, mon->monmap->get_inst(*p));
  }

  assert(g_conf->paxos_kill_at != 9);

  // get ready for a new round.
  new_value.clear();

  // WRITING -> REFRESH
  // among other things, this lets do_refresh() -> mon->bootstrap() know
  // it doesn't need to flush the store queue
  assert(is_writing() || is_writing_previous());
  state = STATE_REFRESH;

  if (do_refresh()) {
    commit_proposal();
    if (mon->get_quorum().size() > 1) {
      extend_lease();
    }

    finish_contexts(g_ceph_context, waiting_for_commit);

    assert(g_conf->paxos_kill_at != 10);

    finish_round();
  }
}


void Paxos::handle_commit(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_commit");
  MMonPaxos *commit = static_cast<MMonPaxos*>(op->get_req());
  dout(10) << "handle_commit on " << commit->last_committed << dendl;

  logger->inc(l_paxos_commit);

  if (!mon->is_peon()) {
    dout(10) << "not a peon, dropping" << dendl;
    assert(0);
    return;
  }

  op->mark_paxos_event("store_state");
  store_state(commit);

  if (do_refresh()) {
    finish_contexts(g_ceph_context, waiting_for_commit);
  }
}

void Paxos::extend_lease()
{
  assert(mon->is_leader());
  //assert(is_active());

  lease_expire = ceph_clock_now(g_ceph_context);
  lease_expire += g_conf->mon_lease;
  acked_lease.clear();
  acked_lease.insert(mon->rank);

  dout(7) << "extend_lease now+" << g_conf->mon_lease 
	  << " (" << lease_expire << ")" << dendl;

  // bcast
  for (set<int>::const_iterator p = mon->get_quorum().begin();
      p != mon->get_quorum().end(); ++p) {

    if (*p == mon->rank) continue;
    MMonPaxos *lease = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_LEASE,
				     ceph_clock_now(g_ceph_context));
    lease->last_committed = last_committed;
    lease->lease_timestamp = lease_expire;
    lease->first_committed = first_committed;
    mon->messenger->send_message(lease, mon->monmap->get_inst(*p));
  }

  // set timeout event.
  //  if old timeout is still in place, leave it.
  if (!lease_ack_timeout_event) {
    lease_ack_timeout_event = new C_LeaseAckTimeout(this);
    mon->timer.add_event_after(g_conf->mon_lease_ack_timeout_factor *
			       g_conf->mon_lease,
			       lease_ack_timeout_event);
  }

  // set renew event
  lease_renew_event = new C_LeaseRenew(this);
  utime_t at = lease_expire;
  at -= g_conf->mon_lease;
  at += g_conf->mon_lease_renew_interval_factor * g_conf->mon_lease;
  mon->timer.add_event_at(at, lease_renew_event);
}

void Paxos::warn_on_future_time(utime_t t, entity_name_t from)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  if (t > now) {
    utime_t diff = t - now;
    if (diff > g_conf->mon_clock_drift_allowed) {
      utime_t warn_diff = now - last_clock_drift_warn;
      if (warn_diff >
	  pow(g_conf->mon_clock_drift_warn_backoff, clock_drift_warned)) {
	mon->clog->warn() << "message from " << from << " was stamped " << diff
			 << "s in the future, clocks not synchronized";
	last_clock_drift_warn = ceph_clock_now(g_ceph_context);
	++clock_drift_warned;
      }
    }
  }

}

bool Paxos::do_refresh()
{
  bool need_bootstrap = false;

  utime_t start = ceph_clock_now(NULL);

  // make sure we have the latest state loaded up
  mon->refresh_from_paxos(&need_bootstrap);

  utime_t end = ceph_clock_now(NULL);
  logger->inc(l_paxos_refresh);
  logger->tinc(l_paxos_refresh_latency, end - start);

  if (need_bootstrap) {
    dout(10) << " doing requested bootstrap" << dendl;
    mon->bootstrap();
    return false;
  }

  return true;
}

void Paxos::commit_proposal()
{
  dout(10) << __func__ << dendl;
  assert(mon->is_leader());
  assert(is_refresh());

  list<Context*> ls;
  ls.swap(committing_finishers);
  finish_contexts(g_ceph_context, ls);
}

void Paxos::finish_round()
{
  dout(10) << __func__ << dendl;
  assert(mon->is_leader());

  // ok, now go active!
  state = STATE_ACTIVE;

  dout(20) << __func__ << " waiting_for_acting" << dendl;
  finish_contexts(g_ceph_context, waiting_for_active);
  dout(20) << __func__ << " waiting_for_readable" << dendl;
  finish_contexts(g_ceph_context, waiting_for_readable);
  dout(20) << __func__ << " waiting_for_writeable" << dendl;
  finish_contexts(g_ceph_context, waiting_for_writeable);
  
  dout(10) << __func__ << " done w/ waiters, state " << state << dendl;

  if (should_trim()) {
    trim();
  }

  if (is_active() && pending_proposal) {
    propose_pending();
  }
}


// peon
void Paxos::handle_lease(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_lease");
  MMonPaxos *lease = static_cast<MMonPaxos*>(op->get_req());
  // sanity
  if (!mon->is_peon() ||
      last_committed != lease->last_committed) {
    dout(10) << "handle_lease i'm not a peon, or they're not the leader,"
	     << " or the last_committed doesn't match, dropping" << dendl;
    op->mark_paxos_event("invalid lease, ignore");
    return;
  }

  warn_on_future_time(lease->sent_timestamp, lease->get_source());

  // extend lease
  if (lease_expire < lease->lease_timestamp) {
    lease_expire = lease->lease_timestamp;

    utime_t now = ceph_clock_now(g_ceph_context);
    if (lease_expire < now) {
      utime_t diff = now - lease_expire;
      derr << "lease_expire from " << lease->get_source_inst() << " is " << diff << " seconds in the past; mons are probably laggy (or possibly clocks are too skewed)" << dendl;
    }
  }

  state = STATE_ACTIVE;

  dout(10) << "handle_lease on " << lease->last_committed
	   << " now " << lease_expire << dendl;

  // ack
  MMonPaxos *ack = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_LEASE_ACK,
				 ceph_clock_now(g_ceph_context));
  ack->last_committed = last_committed;
  ack->first_committed = first_committed;
  ack->lease_timestamp = ceph_clock_now(g_ceph_context);
  lease->get_connection()->send_message(ack);

  // (re)set timeout event.
  reset_lease_timeout();

  // kick waiters
  finish_contexts(g_ceph_context, waiting_for_active);
  if (is_readable())
    finish_contexts(g_ceph_context, waiting_for_readable);
}

void Paxos::handle_lease_ack(MonOpRequestRef op)
{
  op->mark_paxos_event("handle_lease_ack");
  MMonPaxos *ack = static_cast<MMonPaxos*>(op->get_req());
  int from = ack->get_source().num();

  if (!lease_ack_timeout_event) {
    dout(10) << "handle_lease_ack from " << ack->get_source() 
	     << " -- stray (probably since revoked)" << dendl;
  }
  else if (acked_lease.count(from) == 0) {
    acked_lease.insert(from);
    
    if (acked_lease == mon->get_quorum()) {
      // yay!
      dout(10) << "handle_lease_ack from " << ack->get_source() 
	       << " -- got everyone" << dendl;
      mon->timer.cancel_event(lease_ack_timeout_event);
      lease_ack_timeout_event = 0;


    } else {
      dout(10) << "handle_lease_ack from " << ack->get_source() 
	       << " -- still need "
	       << mon->get_quorum().size() - acked_lease.size()
	       << " more" << dendl;
    }
  } else {
    dout(10) << "handle_lease_ack from " << ack->get_source() 
	     << " dup (lagging!), ignoring" << dendl;
  }

  warn_on_future_time(ack->sent_timestamp, ack->get_source());
}

void Paxos::lease_ack_timeout()
{
  dout(1) << "lease_ack_timeout -- calling new election" << dendl;
  assert(mon->is_leader());
  assert(is_active());
  logger->inc(l_paxos_lease_ack_timeout);
  lease_ack_timeout_event = 0;
  mon->bootstrap();
}

void Paxos::reset_lease_timeout()
{
  dout(20) << "reset_lease_timeout - setting timeout event" << dendl;
  if (lease_timeout_event)
    mon->timer.cancel_event(lease_timeout_event);
  lease_timeout_event = new C_LeaseTimeout(this);
  mon->timer.add_event_after(g_conf->mon_lease_ack_timeout_factor *
			     g_conf->mon_lease,
			     lease_timeout_event);
}

void Paxos::lease_timeout()
{
  dout(1) << "lease_timeout -- calling new election" << dendl;
  assert(mon->is_peon());
  logger->inc(l_paxos_lease_timeout);
  lease_timeout_event = 0;
  mon->bootstrap();
}

void Paxos::lease_renew_timeout()
{
  lease_renew_event = 0;
  extend_lease();
}


/*
 * trim old states
 */
void Paxos::trim()
{
  assert(should_trim());
  version_t end = MIN(get_version() - g_conf->paxos_min,
		      get_first_committed() + g_conf->paxos_trim_max);

  if (first_committed >= end)
    return;

  dout(10) << "trim to " << end << " (was " << first_committed << ")" << dendl;

  MonitorDBStore::TransactionRef t = get_pending_transaction();

  for (version_t v = first_committed; v < end; ++v) {
    dout(10) << "trim " << v << dendl;
    t->erase(get_name(), v);
  }
  t->put(get_name(), "first_committed", end);
  if (g_conf->mon_compact_on_trim) {
    dout(10) << " compacting trimmed range" << dendl;
    t->compact_range(get_name(), stringify(first_committed - 1), stringify(end));
  }

  trimming = true;
  queue_pending_finisher(new C_Trimmed(this));
}

/*
 * return a globally unique, monotonically increasing proposal number
 */
version_t Paxos::get_new_proposal_number(version_t gt)
{
  if (last_pn < gt) 
    last_pn = gt;
  
  // update. make it unique among all monitors.
  last_pn /= 100;
  last_pn++;
  last_pn *= 100;
  last_pn += (version_t)mon->rank;

  // write
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  t->put(get_name(), "last_pn", last_pn);

  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  t->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  logger->inc(l_paxos_new_pn);
  utime_t start = ceph_clock_now(NULL);

  get_store()->apply_transaction(t);

  utime_t end = ceph_clock_now(NULL);
  logger->tinc(l_paxos_new_pn_latency, end - start);

  dout(10) << "get_new_proposal_number = " << last_pn << dendl;
  return last_pn;
}


void Paxos::cancel_events()
{
  if (collect_timeout_event) {
    mon->timer.cancel_event(collect_timeout_event);
    collect_timeout_event = 0;
  }
  if (accept_timeout_event) {
    mon->timer.cancel_event(accept_timeout_event);
    accept_timeout_event = 0;
  }
  if (lease_renew_event) {
    mon->timer.cancel_event(lease_renew_event);
    lease_renew_event = 0;
  }
  if (lease_ack_timeout_event) {
    mon->timer.cancel_event(lease_ack_timeout_event);
    lease_ack_timeout_event = 0;
  }  
  if (lease_timeout_event) {
    mon->timer.cancel_event(lease_timeout_event);
    lease_timeout_event = 0;
  }
}

void Paxos::shutdown()
{
  dout(10) << __func__ << " cancel all contexts" << dendl;

  // discard pending transaction
  pending_proposal.reset();

  finish_contexts(g_ceph_context, waiting_for_writeable, -ECANCELED);
  finish_contexts(g_ceph_context, waiting_for_commit, -ECANCELED);
  finish_contexts(g_ceph_context, waiting_for_readable, -ECANCELED);
  finish_contexts(g_ceph_context, waiting_for_active, -ECANCELED);
  finish_contexts(g_ceph_context, pending_finishers, -ECANCELED);
  finish_contexts(g_ceph_context, committing_finishers, -ECANCELED);
  if (logger)
    g_ceph_context->get_perfcounters_collection()->remove(logger);
  delete logger;
}

void Paxos::leader_init()
{
  cancel_events();
  new_value.clear();

  // discard pending transaction
  pending_proposal.reset();

  finish_contexts(g_ceph_context, pending_finishers, -EAGAIN);
  finish_contexts(g_ceph_context, committing_finishers, -EAGAIN);

  logger->inc(l_paxos_start_leader);

  if (mon->get_quorum().size() == 1) {
    state = STATE_ACTIVE;
    return;
  }

  state = STATE_RECOVERING;
  lease_expire = utime_t();
  dout(10) << "leader_init -- starting paxos recovery" << dendl;
  collect(0);
}

void Paxos::peon_init()
{
  cancel_events();
  new_value.clear();

  state = STATE_RECOVERING;
  lease_expire = utime_t();
  dout(10) << "peon_init -- i am a peon" << dendl;

  // start a timer, in case the leader never manages to issue a lease
  reset_lease_timeout();

  // discard pending transaction
  pending_proposal.reset();

  // no chance to write now!
  finish_contexts(g_ceph_context, waiting_for_writeable, -EAGAIN);
  finish_contexts(g_ceph_context, waiting_for_commit, -EAGAIN);
  finish_contexts(g_ceph_context, pending_finishers, -EAGAIN);
  finish_contexts(g_ceph_context, committing_finishers, -EAGAIN);

  logger->inc(l_paxos_start_peon);
}

void Paxos::restart()
{
  dout(10) << "restart -- canceling timeouts" << dendl;
  cancel_events();
  new_value.clear();

  if (is_writing() || is_writing_previous()) {
    dout(10) << __func__ << " flushing" << dendl;
    mon->lock.Unlock();
    mon->store->flush();
    mon->lock.Lock();
    dout(10) << __func__ << " flushed" << dendl;
  }
  state = STATE_RECOVERING;

  // discard pending transaction
  pending_proposal.reset();

  finish_contexts(g_ceph_context, committing_finishers, -EAGAIN);
  finish_contexts(g_ceph_context, pending_finishers, -EAGAIN);
  finish_contexts(g_ceph_context, waiting_for_commit, -EAGAIN);
  finish_contexts(g_ceph_context, waiting_for_active, -EAGAIN);

  logger->inc(l_paxos_restart);
}


void Paxos::dispatch(MonOpRequestRef op)
{
  assert(op->is_type_paxos());
  op->mark_paxos_event("dispatch");
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  // election in progress?
  if (!mon->is_leader() && !mon->is_peon()) {
    dout(5) << "election in progress, dropping " << *m << dendl;
    return;    
  }

  // check sanity
  assert(mon->is_leader() || 
	 (mon->is_peon() && m->get_source().num() == mon->get_leader()));
  
  switch (m->get_type()) {

  case MSG_MON_PAXOS:
    {
      MMonPaxos *pm = reinterpret_cast<MMonPaxos*>(m);

      // NOTE: these ops are defined in messages/MMonPaxos.h
      switch (pm->op) {
	// learner
      case MMonPaxos::OP_COLLECT:
	handle_collect(op);
	break;
      case MMonPaxos::OP_LAST:
	handle_last(op);
	break;
      case MMonPaxos::OP_BEGIN:
	handle_begin(op);
	break;
      case MMonPaxos::OP_ACCEPT:
	handle_accept(op);
	break;		
      case MMonPaxos::OP_COMMIT:
	handle_commit(op);
	break;
      case MMonPaxos::OP_LEASE:
	handle_lease(op);
	break;
      case MMonPaxos::OP_LEASE_ACK:
	handle_lease_ack(op);
	break;
      default:
	assert(0);
      }
    }
    break;
    
  default:
    assert(0);
  }
}


// -----------------
// service interface

// -- READ --

bool Paxos::is_readable(version_t v)
{
  bool ret;
  if (v > last_committed)
    ret = false;
  else
    ret =
      (mon->is_peon() || mon->is_leader()) &&
      (is_active() || is_updating() || is_writing()) &&
      last_committed > 0 &&           // must have a value
      (mon->get_quorum().size() == 1 ||  // alone, or
       is_lease_valid()); // have lease
  dout(5) << __func__ << " = " << (int)ret
	  << " - now=" << ceph_clock_now(g_ceph_context)
	  << " lease_expire=" << lease_expire
	  << " has v" << v << " lc " << last_committed
	  << dendl;
  return ret;
}

bool Paxos::read(version_t v, bufferlist &bl)
{
  if (!get_store()->get(get_name(), v, bl))
    return false;
  return true;
}

version_t Paxos::read_current(bufferlist &bl)
{
  if (read(last_committed, bl))
    return last_committed;
  return 0;
}


bool Paxos::is_lease_valid()
{
  return ((mon->get_quorum().size() == 1)
      || (ceph_clock_now(g_ceph_context) < lease_expire));
}

// -- WRITE --

bool Paxos::is_writeable()
{
  return
    mon->is_leader() &&
    is_active() &&
    is_lease_valid();
}

void Paxos::propose_pending()
{
  assert(is_active());
  assert(pending_proposal);

  cancel_events();

  bufferlist bl;
  pending_proposal->encode(bl);

  dout(10) << __func__ << " " << (last_committed + 1)
	   << " " << bl.length() << " bytes" << dendl;
  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  pending_proposal->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  pending_proposal.reset();

  committing_finishers.swap(pending_finishers);
  state = STATE_UPDATING;
  begin(bl);
}

void Paxos::queue_pending_finisher(Context *onfinished)
{
  dout(5) << __func__ << " " << onfinished << dendl;
  assert(onfinished);
  pending_finishers.push_back(onfinished);
}

MonitorDBStore::TransactionRef Paxos::get_pending_transaction()
{
  assert(mon->is_leader());
  if (!pending_proposal) {
    pending_proposal.reset(new MonitorDBStore::Transaction);
    assert(pending_finishers.empty());
  }
  return pending_proposal;
}

bool Paxos::trigger_propose()
{
  if (is_active()) {
    dout(10) << __func__ << " active, proposing now" << dendl;
    propose_pending();
    return true;
  } else {
    dout(10) << __func__ << " not active, will propose later" << dendl;
    return false;
  }
}

bool Paxos::is_consistent()
{
  return (first_committed <= last_committed);
}

