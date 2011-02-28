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



#include "common/config.h"

#define DOUT_SUBSYS paxos
#undef dout_prefix
#define dout_prefix _prefix(mon, paxos, paxos->machine_id)
static ostream& _prefix(Monitor *mon, Paxos *paxos, int machine_id) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)")))
		<< ".paxosservice(" << get_paxos_name(machine_id) << ") ";
}

const char *PaxosService::get_machine_name()
{
  return paxos->get_machine_name();
}


bool PaxosService::dispatch(PaxosServiceMessage *m)
{
  dout(10) << "dispatch " << *m << " from " << m->get_orig_source_inst() << dendl;
  // make sure our map is readable and up to date
  if (!paxos->is_readable(m->version)) {
    dout(10) << " waiting for paxos -> readable (v" << m->version << ")" << dendl;
    paxos->wait_for_readable(new C_RetryMessage(this, m));
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
  if (!paxos->is_writeable()) {
    dout(10) << " waiting for paxos -> writeable" << dendl;
    paxos->wait_for_writeable(new C_RetryMessage(this, m));
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

bool PaxosService::should_propose(double& delay)
{
  // simple default policy: quick startup, then some damping.
  if (paxos->last_committed <= 1)
    delay = 0.0;
  else {
    utime_t now = g_clock.now();
    if ((now - paxos->last_commit_time) > g_conf.paxos_propose_interval)
      delay = (double)g_conf.paxos_min_wait;
    else
      delay = (double)(g_conf.paxos_propose_interval + paxos->last_commit_time
		       - now);
  }
  return true;
}

void PaxosService::_commit()
{
  dout(7) << "_commit" << dendl;
  update_from_paxos();   // notify service of new paxos state

  if (mon->is_leader()) {
    dout(7) << "_commit creating new pending" << dendl;
    assert(have_pending == false);
    create_pending();
    have_pending = true;

    committed();
  }
}


void PaxosService::propose_pending()
{
  dout(10) << "propose_pending" << dendl;
  assert(have_pending);
  assert(mon->is_leader() && paxos->is_active());

  if (proposal_timer) {
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  // finish and encode
  bufferlist bl;
  encode_pending(bl);
  have_pending = false;

  // apply to paxos
  paxos->wait_for_commit_front(new C_Commit(this));
  paxos->propose_new_value(bl);
}



void PaxosService::election_starting()
{
  dout(10) << "election_starting" << dendl;
  if (proposal_timer) {
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }

  on_election_start();
}

void PaxosService::election_finished()
{
  dout(10) << "election_finished" << dendl;

  if (have_pending) {
    discard_pending();
    have_pending = false;
  }

  // make sure we update our state
  if (paxos->is_active())
    _active();
  else
    paxos->wait_for_active(new C_Active(this));
}

void PaxosService::_active()
{
  dout(10) << "_active" << dendl;
  assert(paxos->is_active());

  // pull latest from paxos
  update_from_paxos();

  // create pending state?
  if (mon->is_leader()) {
    if (!have_pending) {
      create_pending();
      have_pending = true;
    }
  }

  on_active();
}


void PaxosService::shutdown()
{
  paxos->cancel_events();

  if (proposal_timer) {
    mon->timer.cancel_event(proposal_timer);
    proposal_timer = 0;
  }
}
