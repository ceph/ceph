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



#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".paxosservice(" << get_paxos_name(paxos->machine_id) << ") "
//#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << "." << get_paxos_name(paxos->machine_id) << " "




void PaxosService::dispatch(Message *m)
{
  dout(10) << "dispatch " << *m << " from " << m->get_source_inst() << endl;
  
  // make sure our map is readable and up to date
  if (!paxos->is_readable()) {
    dout(10) << " waiting for paxos -> readable" << endl;
    paxos->wait_for_readable(new C_RetryMessage(this, m));
    return;
  }

  // make sure service has latest from paxos.
  update_from_paxos();

  // preprocess
  if (preprocess_query(m)) 
    return;  // easy!

  // leader?
  if (!mon->is_leader()) {
    // fw to leader
    dout(10) << " fw to leader mon" << mon->get_leader() << endl;
    mon->messenger->send_message(m, mon->monmap->get_inst(mon->get_leader()));
    return;
  }
  
  // writeable?
  if (!paxos->is_writeable()) {
    dout(10) << " waiting for paxos -> writeable" << endl;
    paxos->wait_for_writeable(new C_RetryMessage(this, m));
    return;
  }

  // update
  if (prepare_update(m) &&
      should_propose_now()) 
    propose_pending();
}

void PaxosService::_commit()
{
  dout(7) << "_commit" << endl;
  update_from_paxos();   // notify service of new paxos state

  if (mon->is_leader()) {
    dout(7) << "_commit creating new pending" << endl;
    assert(have_pending == false);
    create_pending();
    have_pending = true;
  }
}


void PaxosService::propose_pending()
{
  dout(10) << "propose_pending" << endl;
  assert(have_pending);

  // finish and encode
  bufferlist bl;
  encode_pending(bl);
  have_pending = false;

  // apply to paxos
  paxos->wait_for_commit_front(new C_Commit(this));
  paxos->propose_new_value(bl);
}


void PaxosService::election_finished()
{
  dout(10) << "election_finished" << endl;

  if (have_pending && 
      !mon->is_leader()) {
    discard_pending();
    have_pending = false;
  }

  if (g_conf.mkfs) 
    _try_create_initial();
  
  // make sure we update our state
  if (paxos->is_active())
    _active();
  else
    paxos->wait_for_active(new C_Active(this));
}

void PaxosService::_active()
{
  dout(10) << "_active" << endl;
  update_from_paxos();

  if (mon->is_leader() &&
      !have_pending) {
    create_pending();
    have_pending = true;
  }
}

void PaxosService::_try_create_initial()
{
  if (mon->is_leader() && 
      paxos->get_version() == 0) {
    
    if (!paxos->is_writeable()) {
      dout(1) << "election_finished -- waiting for writeable to create initial state" << endl;
      paxos->wait_for_writeable(new C_CreateInitial(this));
    } else {
      // do it
      assert(have_pending == false);
      create_pending();
      have_pending = true;
      create_initial();
      propose_pending();
    }
  }
}
