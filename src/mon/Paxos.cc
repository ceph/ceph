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

#include "Paxos.h"
#include "Monitor.h"
#include "MonitorStore.h"

#include "messages/MMonPaxos.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_paxos
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, mon->name, mon->rank, machine_name, state, first_committed, last_committed)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, const string& name,
		        int rank, const char *machine_name, int state,
			version_t first_committed, version_t last_committed)
{
  return *_dout << "mon." << name << "@" << rank
		<< "(" << mon->get_state_name() << ")"
		<< ".paxos(" << machine_name << " " << Paxos::get_statename(state)
		<< " c " << first_committed << ".." << last_committed
		<< ") ";
}



void Paxos::init()
{
  // load paxos variables from stable storage
  last_pn = mon->store->get_int(machine_name, "last_pn");
  accepted_pn = mon->store->get_int(machine_name, "accepted_pn");
  last_committed = mon->store->get_int(machine_name, "last_committed");
  first_committed = mon->store->get_int(machine_name, "first_committed");
  bufferlist temp;
  latest_stashed = get_stashed(temp);
  slurping = mon->store->get_int(machine_name, "slurping");

  dout(10) << "init" << dendl;
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
  if (mon->store->exists_bl_sn(machine_name, last_committed+1)) {
    uncommitted_v = last_committed+1;
    uncommitted_pn = accepted_pn;
    mon->store->get_bl_sn_safe(uncommitted_value, machine_name, last_committed+1);
    assert(uncommitted_value.length());
    dout(10) << "learned uncommitted " << (last_committed+1)
	     << " (" << uncommitted_value.length() << " bytes) from myself" 
	     << dendl;
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
				       machine_id, ceph_clock_now(g_ceph_context));
    collect->last_committed = last_committed;
    collect->first_committed = first_committed;
    collect->pn = accepted_pn;
    mon->messenger->send_message(collect, mon->monmap->get_inst(*p));
  }

  // set timeout event
  collect_timeout_event = new C_CollectTimeout(this);
  mon->timer.add_event_after(g_conf->mon_accept_timeout, collect_timeout_event);
}


// peon
void Paxos::handle_collect(MMonPaxos *collect)
{
  dout(10) << "handle_collect " << *collect << dendl;

  assert(mon->is_peon()); // mon epoch filter should catch strays

  // we're recoverying, it seems!
  state = STATE_RECOVERING;

  // reply
  MMonPaxos *last = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_LAST, machine_id,
				  ceph_clock_now(g_ceph_context));
  last->last_committed = last_committed;
  last->first_committed = first_committed;
  
  // can we accept this pn?
  if (collect->pn > accepted_pn) {
    // ok, accept it
    accepted_pn = collect->pn;
    accepted_pn_from = collect->pn_from;
    dout(10) << "accepting pn " << accepted_pn << " from " 
	     << accepted_pn_from << dendl;
    mon->store->put_int(accepted_pn, machine_name, "accepted_pn");
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
  if (mon->store->exists_bl_sn(machine_name, last_committed+1)) {
    mon->store->get_bl_sn_safe(bl, machine_name, last_committed+1);
    assert(bl.length() > 0);
    dout(10) << " sharing our accepted but uncommitted value for " 
	     << last_committed+1 << " (" << bl.length() << " bytes)" << dendl;
    last->values[last_committed+1] = bl;
    last->uncommitted_pn = accepted_pn;
  }

  // send reply
  mon->messenger->send_message(last, collect->get_source_inst());
  collect->put();
}

void Paxos::share_state(MMonPaxos *m, version_t peer_first_committed,
			version_t peer_last_committed)
{
  assert(peer_last_committed < last_committed);

  dout(10) << "share_state peer has fc " << peer_first_committed 
	   << " lc " << peer_last_committed << dendl;
  version_t v = peer_last_committed + 1;

  // start with a stashed full copy?
  if (peer_last_committed + 1 < first_committed) {
    bufferlist bl;
    version_t l = get_stashed(bl);
    assert(l <= last_committed);
    dout(10) << "share_state starting with latest " << l 
	     << " (" << bl.length() << " bytes)" << dendl;
    m->latest_value.claim(bl);
    m->latest_version = l;
    v = l;
  }

  // include incrementals
  for ( ; v <= last_committed; v++) {
    if (mon->store->exists_bl_sn(machine_name, v)) {
      mon->store->get_bl_sn_safe(m->values[v], machine_name, v);
      assert(m->values[v].length());
      dout(10) << " sharing " << v << " (" 
	       << m->values[v].length() << " bytes)" << dendl;
   }
  }

  m->last_committed = last_committed;
}

void Paxos::store_state(MMonPaxos *m)
{
  map<version_t,bufferlist>::iterator start = m->values.begin();

  // stash?
  if (m->latest_version && m->latest_version > last_committed) {
    dout(10) << "store_state got stash version " 
	     << m->latest_version << ", zapping old states" << dendl;

    assert(start != m->values.end() && start->first == m->latest_version);

    // wipe out everything we had previously
    trim_to(last_committed + 1);

    stash_latest(m->latest_version, m->latest_value);

    first_committed = m->latest_version;
    last_committed = m->latest_version;
    mon->store->put_bl_sn(start->second, machine_name, m->latest_version);
    mon->store->put_int(first_committed, machine_name, "first_committed");
    mon->store->put_int(last_committed, machine_name, "last_committed");
  }

  // build map of values to store
  // we want to write the range [last_committed, m->last_committed] only.
  if (start != m->values.end() &&
      start->first > last_committed + 1) {
    // ignore everything if values start in the future.
    dout(10) << "store_state ignoring all values, they start at " << start->first
	     << " > last_committed+1" << dendl;
    start = m->values.end();
  }

  while (start != m->values.end() && start->first <= last_committed) {
    ++start;
  }

  map<version_t,bufferlist>::iterator end = start;
  while (end != m->values.end() && end->first <= m->last_committed) {
    last_committed = end->first;
    if (!first_committed)
      first_committed = last_committed;
    ++end;
  }

  if (start == end) {
    dout(10) << "store_state nothing to commit" << dendl;
  } else {
    dout(10) << "store_state [" << start->first << ".." 
	     << last_committed << "]" << dendl;

    mon->store->put_bl_sn_map(machine_name, start, end);
    mon->store->put_int(last_committed, machine_name, "last_committed");
    mon->store->put_int(first_committed, machine_name, "first_committed");
  }
}


// leader
void Paxos::handle_last(MMonPaxos *last)
{
  dout(10) << "handle_last " << *last << dendl;

  if (!mon->is_leader()) {
    dout(10) << "not leader, dropping" << dendl;
    last->put();
    return;
  }

  // note peer's last_committed, in case we learn a new commit and need to
  // push it to them.
  peer_last_committed[last->get_source().num()] = last->last_committed;

  // did we receive a committed value?
  store_state(last);
      
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
    if (last->uncommitted_pn &&
	last->uncommitted_pn > uncommitted_pn) {
      uncommitted_v = last->last_committed+1;
      uncommitted_pn = last->uncommitted_pn;
      uncommitted_value = last->values[uncommitted_v];
      dout(10) << "we learned an uncommitted value for " << uncommitted_v 
	       << " pn " << uncommitted_pn
	       << " " << uncommitted_value.length() << " bytes"
	       << dendl;
    }
    
    // is that everyone?
    if (num_last == mon->get_quorum().size()) {
      // cancel timeout event
      mon->timer.cancel_event(collect_timeout_event);
      collect_timeout_event = 0;

      // share committed values?
      for (map<int,version_t>::iterator p = peer_last_committed.begin();
	   p != peer_last_committed.end();
	   ++p) {
	if (p->second < last_committed) {
	  // share committed values
	  dout(10) << " sending commit to mon." << p->first << dendl;
	  MMonPaxos *commit = new MMonPaxos(mon->get_epoch(),
					    MMonPaxos::OP_COMMIT, machine_id,
					    ceph_clock_now(g_ceph_context));
	  share_state(commit, peer_first_committed[p->first], p->second);
	  mon->messenger->send_message(commit, mon->monmap->get_inst(p->first));
	}
      }
      peer_first_committed.clear();
      peer_last_committed.clear();

      // almost...
      state = STATE_ACTIVE;

      // did we learn an old value?
      if (uncommitted_v == last_committed+1 &&
	  uncommitted_value.length()) {
	dout(10) << "that's everyone.  begin on old learned value" << dendl;
	begin(uncommitted_value);
      } else {
	// active!
	dout(10) << "that's everyone.  active!" << dendl;
	extend_lease();

	finish_contexts(g_ceph_context, waiting_for_active);
	finish_contexts(g_ceph_context, waiting_for_readable);
	finish_contexts(g_ceph_context, waiting_for_writeable);
      }
    }
  } else {
    // no, this is an old message, discard
    dout(10) << "old pn, ignoring" << dendl;
  }

  last->put();
}

void Paxos::collect_timeout()
{
  dout(5) << "collect timeout, calling fresh election" << dendl;
  collect_timeout_event = 0;
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
  assert(is_active());
  state = STATE_UPDATING;

  // we must already have a majority for this to work.
  assert(mon->get_quorum().size() == 1 ||
	 num_last > (unsigned)mon->monmap->size()/2);
  
  // and no value, yet.
  assert(new_value.length() == 0);
  
  // accept it ourselves
  accepted.clear();
  accepted.insert(mon->rank);
  new_value = v;
  mon->store->put_bl_sn(new_value, machine_name, last_committed+1);

  if (mon->get_quorum().size() == 1) {
    // we're alone, take it easy
    commit();
    state = STATE_ACTIVE;

    finish_contexts(g_ceph_context, waiting_for_active);
    finish_contexts(g_ceph_context, waiting_for_commit);
    finish_contexts(g_ceph_context, waiting_for_readable);
    finish_contexts(g_ceph_context, waiting_for_writeable);

    return;
  }

  // ask others to accept it too!
  for (set<int>::const_iterator p = mon->get_quorum().begin();
       p != mon->get_quorum().end();
       ++p) {
    if (*p == mon->rank) continue;
    
    dout(10) << " sending begin to mon." << *p << dendl;
    MMonPaxos *begin = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_BEGIN,
				     machine_id, ceph_clock_now(g_ceph_context));
    begin->values[last_committed+1] = new_value;
    begin->last_committed = last_committed;
    begin->pn = accepted_pn;
    
    mon->messenger->send_message(begin, mon->monmap->get_inst(*p));
  }

  // set timeout event
  accept_timeout_event = new C_AcceptTimeout(this);
  mon->timer.add_event_after(g_conf->mon_accept_timeout, accept_timeout_event);
}

// peon
void Paxos::handle_begin(MMonPaxos *begin)
{
  dout(10) << "handle_begin " << *begin << dendl;

  // can we accept this?
  if (begin->pn < accepted_pn) {
    dout(10) << " we accepted a higher pn " << accepted_pn << ", ignoring" << dendl;
    begin->put();
    return;
  }
  assert(begin->pn == accepted_pn);
  assert(begin->last_committed == last_committed);
  
  // set state.
  state = STATE_UPDATING;
  lease_expire = utime_t();  // cancel lease

  // yes.
  version_t v = last_committed+1;
  dout(10) << "accepting value for " << v << " pn " << accepted_pn << dendl;
  mon->store->put_bl_sn(begin->values[v], machine_name, v);

  // reply
  MMonPaxos *accept = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_ACCEPT,
				    machine_id, ceph_clock_now(g_ceph_context));
  accept->pn = accepted_pn;
  accept->last_committed = last_committed;
  mon->messenger->send_message(accept, begin->get_source_inst());
  
  begin->put();
}

// leader
void Paxos::handle_accept(MMonPaxos *accept)
{
  dout(10) << "handle_accept " << *accept << dendl;
  int from = accept->get_source().num();

  if (accept->pn != accepted_pn) {
    // we accepted a higher pn, from some other leader
    dout(10) << " we accepted a higher pn " << accepted_pn << ", ignoring" << dendl;
    accept->put();
    return;
  }
  if (last_committed > 0 &&
      accept->last_committed < last_committed-1) {
    dout(10) << " this is from an old round, ignoring" << dendl;
    accept->put();
    return;
  }
  assert(accept->last_committed == last_committed ||   // not committed
	 accept->last_committed == last_committed-1);  // committed

  assert(state == STATE_UPDATING);
  assert(accepted.count(from) == 0);
  accepted.insert(from);
  dout(10) << " now " << accepted << " have accepted" << dendl;

  // new majority?
  if (accepted.size() == (unsigned)mon->monmap->size()/2+1) {
    // yay, commit!
    // note: this may happen before the lease is reextended (below)
    dout(10) << " got majority, committing" << dendl;
    commit();
  }

  // done?
  if (accepted == mon->get_quorum()) {
    dout(10) << " got quorum, done with update" << dendl;
    // cancel timeout event
    mon->timer.cancel_event(accept_timeout_event);
    accept_timeout_event = 0;

    // yay!
    state = STATE_ACTIVE;
    extend_lease();

    // wake people up
    finish_contexts(g_ceph_context, waiting_for_active);
    finish_contexts(g_ceph_context, waiting_for_commit);
    finish_contexts(g_ceph_context, waiting_for_readable);
    finish_contexts(g_ceph_context, waiting_for_writeable);
  }
  accept->put();
}

void Paxos::accept_timeout()
{
  dout(5) << "accept timeout, calling fresh election" << dendl;
  accept_timeout_event = 0;
  assert(mon->is_leader());
  assert(is_updating());
  mon->bootstrap();
}

void Paxos::commit()
{
  dout(10) << "commit " << last_committed+1 << dendl;

  // cancel lease - it was for the old value.
  //  (this would only happen if message layer lost the 'begin', but
  //   leader still got a majority and committed with out us.)
  lease_expire = utime_t();  // cancel lease

  // commit locally
  last_committed++;
  last_commit_time = ceph_clock_now(g_ceph_context);
  mon->store->put_int(last_committed, machine_name, "last_committed");
  if (!first_committed) {
    first_committed = last_committed;
    mon->store->put_int(last_committed, machine_name, "first_committed");
  }

  // tell everyone
  for (set<int>::const_iterator p = mon->get_quorum().begin();
       p != mon->get_quorum().end();
       ++p) {
    if (*p == mon->rank) continue;

    dout(10) << " sending commit to mon." << *p << dendl;
    MMonPaxos *commit = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_COMMIT,
				      machine_id, ceph_clock_now(g_ceph_context));
    commit->values[last_committed] = new_value;
    commit->pn = accepted_pn;
    commit->last_committed = last_committed;

    mon->messenger->send_message(commit, mon->monmap->get_inst(*p));
  }

  // get ready for a new round.
  new_value.clear();
}


void Paxos::handle_commit(MMonPaxos *commit)
{
  dout(10) << "handle_commit on " << commit->last_committed << dendl;

  if (!mon->is_peon()) {
    dout(10) << "not a peon, dropping" << dendl;
    assert(0);
    commit->put();
    return;
  }

  store_state(commit);
  
  commit->put();

  finish_contexts(g_ceph_context, waiting_for_commit);
}

void Paxos::extend_lease()
{
  assert(mon->is_leader());
  assert(is_active());

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
				     machine_id, ceph_clock_now(g_ceph_context));
    lease->last_committed = last_committed;
    lease->lease_timestamp = lease_expire;
    lease->first_committed = first_committed;
    mon->messenger->send_message(lease, mon->monmap->get_inst(*p));
  }

  // set timeout event.
  //  if old timeout is still in place, leave it.
  if (!lease_ack_timeout_event) {
    lease_ack_timeout_event = new C_LeaseAckTimeout(this);
    mon->timer.add_event_after(g_conf->mon_lease_ack_timeout, 
			       lease_ack_timeout_event);
  }

  // set renew event
  lease_renew_event = new C_LeaseRenew(this);
  utime_t at = lease_expire;
  at -= g_conf->mon_lease;
  at += g_conf->mon_lease_renew_interval;
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
	mon->clog.warn() << "message from " << from << " was stamped " << diff
			 << "s in the future, clocks not synchronized";
	last_clock_drift_warn = ceph_clock_now(g_ceph_context);
	++clock_drift_warned;
      }
    }
  }

}

// peon
void Paxos::handle_lease(MMonPaxos *lease)
{
  // sanity
  if (!mon->is_peon() ||
      last_committed != lease->last_committed) {
    dout(10) << "handle_lease i'm not a peon, or they're not the leader,"
	     << " or the last_committed doesn't match, dropping" << dendl;
    lease->put();
    return;
  }

  warn_on_future_time(lease->sent_timestamp, lease->get_source());

  // extend lease
  if (lease_expire < lease->lease_timestamp) {
    lease_expire = lease->lease_timestamp;

    // check for extreme clock skew.
    utime_t now = ceph_clock_now(g_ceph_context);
    if (lease_expire < now) {
      utime_t diff = now - lease_expire;
      derr << "lease_expire from " << lease->get_source_inst() << " is " << diff << " seconds in the past; mons are laggy or clocks are too skewed" << dendl;
    }
  }

  state = STATE_ACTIVE;

  dout(10) << "handle_lease on " << lease->last_committed
	   << " now " << lease_expire << dendl;

  // ack
  MMonPaxos *ack = new MMonPaxos(mon->get_epoch(), MMonPaxos::OP_LEASE_ACK,
				 machine_id, ceph_clock_now(g_ceph_context));
  ack->last_committed = last_committed;
  ack->first_committed = first_committed;
  ack->lease_timestamp = ceph_clock_now(g_ceph_context);
  mon->messenger->send_message(ack, lease->get_source_inst());

  // (re)set timeout event.
  if (lease_timeout_event) 
    mon->timer.cancel_event(lease_timeout_event);
  lease_timeout_event = new C_LeaseTimeout(this);
  mon->timer.add_event_after(g_conf->mon_lease_ack_timeout, lease_timeout_event);

  // trim?
  trim_to(lease->first_committed);
  
  // kick waiters
  finish_contexts(g_ceph_context, waiting_for_active);
  if (is_readable())
    finish_contexts(g_ceph_context, waiting_for_readable);

  lease->put();
}

void Paxos::handle_lease_ack(MMonPaxos *ack)
{
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

  ack->put();
}

void Paxos::lease_ack_timeout()
{
  dout(5) << "lease_ack_timeout -- calling new election" << dendl;
  assert(mon->is_leader());
  assert(is_active());

  lease_ack_timeout_event = 0;
  mon->bootstrap();
}

void Paxos::lease_timeout()
{
  dout(5) << "lease_timeout -- calling new election" << dendl;
  assert(mon->is_peon());

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

void Paxos::trim_to(version_t first, bool force)
{
  dout(10) << "trim_to " << first << " (was " << first_committed << ")"
	   << ", latest_stashed " << latest_stashed
	   << dendl;

  if (first_committed >= first)
    return;

  while (first_committed < first &&
	 (force || first_committed < latest_stashed)) {
    dout(10) << "trim " << first_committed << dendl;
    mon->store->erase_sn(machine_name, first_committed);
    for (list<string>::iterator p = extra_state_dirs.begin();
	 p != extra_state_dirs.end();
	 ++p)
      mon->store->erase_sn(p->c_str(), first_committed);
    first_committed++;
  }
  mon->store->put_int(first_committed, machine_name, "first_committed");
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
  mon->store->put_int(last_pn, machine_name, "last_pn");

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

void Paxos::shutdown() {
  dout(10) << __func__ << " cancel all contexts" << dendl;
  finish_contexts(g_ceph_context, waiting_for_writeable, -ECANCELED);
  finish_contexts(g_ceph_context, waiting_for_commit, -ECANCELED);
  finish_contexts(g_ceph_context, waiting_for_readable, -ECANCELED);
  finish_contexts(g_ceph_context, waiting_for_active, -ECANCELED);
}

void Paxos::leader_init()
{
  cancel_events();
  new_value.clear();

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

  // no chance to write now!
  finish_contexts(g_ceph_context, waiting_for_writeable, -EAGAIN);
  finish_contexts(g_ceph_context, waiting_for_commit, -EAGAIN);
}

void Paxos::restart()
{
  dout(10) << "restart -- canceling timeouts" << dendl;
  cancel_events();
  new_value.clear();

  finish_contexts(g_ceph_context, waiting_for_commit, -EAGAIN);
  finish_contexts(g_ceph_context, waiting_for_active, -EAGAIN);
}


void Paxos::dispatch(PaxosServiceMessage *m)
{
  // election in progress?
  if (!mon->is_leader() && !mon->is_peon()) {
    dout(5) << "election in progress, dropping " << *m << dendl;
    m->put();
    return;    
  }

  // check sanity
  assert(mon->is_leader() || 
	 (mon->is_peon() && m->get_source().num() == mon->get_leader()));
  
  switch (m->get_type()) {

  case MSG_MON_PAXOS:
    {
      MMonPaxos *pm = (MMonPaxos*)m;

      // NOTE: these ops are defined in messages/MMonPaxos.h
      switch (pm->op) {
	// learner
      case MMonPaxos::OP_COLLECT:
	handle_collect(pm);
	break;
      case MMonPaxos::OP_LAST:
	handle_last(pm);
	break;
      case MMonPaxos::OP_BEGIN:
	handle_begin(pm);
	break;
      case MMonPaxos::OP_ACCEPT:
	handle_accept(pm);
	break;		
      case MMonPaxos::OP_COMMIT:
	handle_commit(pm);
	break;
      case MMonPaxos::OP_LEASE:
	handle_lease(pm);
	break;
      case MMonPaxos::OP_LEASE_ACK:
	handle_lease_ack(pm);
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
  dout(1) << "is_readable now=" << ceph_clock_now(g_ceph_context) << " lease_expire=" << lease_expire
	  << " has v" << v << " lc " << last_committed << dendl;
  if (v > last_committed)
    return false;
  return 
    (mon->is_peon() || mon->is_leader()) &&
    (is_active() || is_updating()) &&
    last_committed > 0 &&           // must have a value
    (mon->get_quorum().size() == 1 ||  // alone, or
     ceph_clock_now(g_ceph_context) < lease_expire);    // have lease
}

bool Paxos::read(version_t v, bufferlist &bl)
{
  if (mon->store->get_bl_sn(bl, machine_name, v) <= 0)
    return false;
  return true;
}

version_t Paxos::read_current(bufferlist &bl)
{
  if (read(last_committed, bl))
    return last_committed;
  return 0;
}




// -- WRITE --

bool Paxos::is_writeable()
{
  if (mon->get_quorum().size() == 1) return true;
  return
    mon->is_leader() &&
    is_active() &&
    ceph_clock_now(g_ceph_context) < lease_expire;
}

bool Paxos::propose_new_value(bufferlist& bl, Context *oncommit)
{
  /*
  // writeable?
  if (!is_writeable()) {
    dout(5) << "propose_new_value " << last_committed+1 << " " << bl.length() << " bytes"
	    << " -- not writeable" << dendl;
    if (oncommit) {
      oncommit->finish(-1);
      delete oncommit;
    }
    return false;
  }
  */
  
  assert(mon->is_leader() && is_active());

  // cancel lease renewal and timeout events.
  cancel_events();

  // ok!
  dout(5) << "propose_new_value " << last_committed+1 << " " << bl.length() << " bytes" << dendl;
  if (oncommit)
    waiting_for_commit.push_back(oncommit);
  begin(bl);
  
  return true;
}

void Paxos::stash_latest(version_t v, bufferlist& bl)
{
  if (v == latest_stashed) {
    dout(10) << "stash_latest v" << v << " already stashed" << dendl;
    return;  // already stashed.
  }

  bufferlist final;
  ::encode(v, final);
  ::encode(bl, final);
  
  dout(10) << "stash_latest v" << v << " len " << bl.length() << dendl;
  mon->store->put_bl_ss(final, machine_name, "latest");

  latest_stashed = v;
}

version_t Paxos::get_stashed(bufferlist& bl)
{
  bufferlist full;
  mon->store->get_bl_ss_safe(full, machine_name, "latest");
  if (!full.length()) {
    dout(10) << "get_stashed not found" << dendl;
    return 0;
  }
  bufferlist::iterator p = full.begin();
  version_t v;
  ::decode(v, p);
  ::decode(bl, p);

  latest_stashed = v;
  dout(10) << "get_stashed v" << latest_stashed << " len " << bl.length() << dendl;
  return latest_stashed;  
}


bool Paxos::is_consistent()
{
  bool consistent = true;
  if (first_committed > 1 && first_committed > latest_stashed)
    consistent = false;
  if (first_committed > last_committed)
    consistent = false;
  if (latest_stashed > last_committed)
    consistent = false;
  if (slurping != 0)
    consistent = false;

  assert(consistent || (slurping == 1));
  return consistent;
}

void Paxos::start_slurping()
{
  if (slurping != 1) {
    slurping = 1;
    mon->store->put_int(1, machine_name, "slurping");
  }
}

void Paxos::end_slurping()
{
  if (slurping == 1) {
    slurping = 0;
    mon->store->put_int(slurping, machine_name, "slurping");
  }
  assert(is_consistent());
}
