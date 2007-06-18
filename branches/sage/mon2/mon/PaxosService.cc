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
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << "." << get_paxos_name(paxos->machine_id) << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << "." << get_paxos_name(paxos->machine_id) << " "



void PaxosService::dispatch(Message *m)
{
  dout(10) << "dispatch " << *m << " from " << m->get_source_inst() << endl;
  
  // make sure our map is readable and up to date
  if (!paxos->is_readable() ||
      !update_from_paxos()) {
    dout(10) << " waiting for paxos -> readable" << endl;
    paxos->wait_for_readable(new C_RetryMessage(this, m));
    return;
  }

  // preprocess
  if (preprocess_update(m)) 
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

  prepare_update(m);

  // do it now (for now!) ***
  propose_pending();
}

void PaxosService::election_finished()
{
  if (mon->is_leader() && g_conf.mkfs)
    create_initial();
}
