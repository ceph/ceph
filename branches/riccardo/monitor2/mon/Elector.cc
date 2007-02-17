// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "Elector.h"
#include "Monitor.h"

#include "common/Timer.h"

#include "messages/MMonElectionPropose.h"
#include "messages/MMonElectionAck.h"
#include "messages/MMonElectionVictory.h"

#include "config.h"
#undef dout
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".elector "
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".elector "


void Elector::start()
{
  dout(5) << "start -- can i be leader?" << endl;

  leader_acked = -1;

  // start by trying to elect me
  start_stamp = g_clock.now();
  acked_me.clear();
  acked_me.insert(whoami);
  electing_me = true;
  
  // bcast to everyone else
  for (int i=0; i<mon->monmap->num_mon; ++i) {
    if (i == whoami) continue;
    mon->messenger->send_message(new MMonElectionPropose,
				 mon->monmap->get_inst(i));
  }
  
  reset_timer();
}

void Elector::defer(int who)
{
  dout(5) << "defer to " << who << endl;

  if (electing_me) {
    acked_me.clear();
    electing_me = false;
  }

  // ack them
  leader_acked = who;
  ack_stamp = g_clock.now();
  mon->messenger->send_message(new MMonElectionAck,
			       mon->monmap->get_inst(who));
  
  // set a timer
  reset_timer(1.0);  // give the leader some extra time to declare victory
}


class C_Mon_ElectionExpire : public Context {
  Elector *elector;
public:
  C_Mon_ElectionExpire(Elector *e) : elector(e) { }
  void finish(int r) {
    elector->expire();
  }
};

void Elector::reset_timer(double plus)
{
  // set the timer
  cancel_timer();
  expire_event = new C_Mon_ElectionExpire(this);
  g_timer.add_event_after(g_conf.mon_lease + plus,
			  expire_event);
}


void Elector::cancel_timer()
{
  if (expire_event)
    g_timer.cancel_event(expire_event);
}

void Elector::expire()
{
  dout(5) << "election timer expired" << endl;
  
  // did i win?
  if (electing_me &&
      acked_me.size() > (unsigned)(mon->monmap->num_mon / 2)) {
    // i win
    victory();
  } else {
    // whoever i deferred to didn't declare victory quickly enough.
    start();
  }
}


void Elector::victory()
{
  leader_acked = -1;
  electing_me = false;

  cancel_timer();

  // tell everyone
  for (int i=0; i<mon->monmap->num_mon; ++i) {
    if (i == whoami) continue;
    mon->messenger->send_message(new MMonElectionVictory,
				 mon->monmap->get_inst(i));
  }
    
  // tell monitor
  mon->win_election(acked_me);
}


void Elector::handle_propose(MMonElectionPropose *m)
{
  dout(5) << "handle_propose from " << m->get_source() << endl;
  int from = m->get_source().num();

  if (from > whoami) {
    // wait, i should win!
    if (!electing_me)
      start();
  } else {
    // they would win over me
    if (leader_acked < 0 ||      // haven't acked anyone yet, or
	leader_acked > from ||   // they would win over who you did ack, or
	leader_acked == from) {  // this is the guy we're already deferring to
      defer(from);
    } else {
      // ignore them!
      dout(5) << "no, we already acked " << leader_acked << endl;
    }
  }
  
  delete m;
}
 
void Elector::handle_ack(MMonElectionAck *m)
{
  dout(5) << "handle_ack from " << m->get_source() << endl;
  int from = m->get_source().num();
  
  if (electing_me) {
    // thanks
    acked_me.insert(from);
    dout(5) << " so far i have " << acked_me << endl;
    
    // is that _everyone_?
    if (acked_me.size() == (unsigned)mon->monmap->num_mon) {
      // if yes, shortcut to election finish
      victory();
    }
  } else {
    // ignore, i'm deferring already.
  }
  
  delete m;
}

void Elector::handle_victory(MMonElectionVictory *m)
{
  dout(5) << "handle_victory from " << m->get_source() << endl;
  int from = m->get_source().num();
  
  if (from < whoami) {
    // ok, fine, they win
    mon->lose_election(from);
    
    // cancel my timer
    cancel_timer();	
  } else {
    // no, that makes no sense, i should win.  start over!
    start();
  }
}




void Elector::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_MON_ELECTION_ACK:
    handle_ack((MMonElectionAck*)m);
    break;
    
  case MSG_MON_ELECTION_PROPOSE:
    handle_propose((MMonElectionPropose*)m);
    break;
    
  case MSG_MON_ELECTION_VICTORY:
    handle_victory((MMonElectionVictory*)m);
    break;
    
  default:
    assert(0);
  }
}




