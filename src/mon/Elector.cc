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

#include "Elector.h"
#include "Monitor.h"

#include "common/Timer.h"
#include "MonitorDBStore.h"
#include "MonmapMonitor.h"
#include "messages/MMonElection.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, epoch)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, epoch_t epoch) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").elector(" << epoch << ") ";
}


void Elector::init()
{
  epoch = mon->store->get(Monitor::MONITOR_NAME, "election_epoch");
  if (!epoch)
    epoch = 1;
  dout(1) << "init, last seen epoch " << epoch << dendl;
}

void Elector::shutdown()
{
  if (expire_event)
    mon->timer.cancel_event(expire_event);
}

void Elector::bump_epoch(epoch_t e) 
{
  dout(10) << "bump_epoch " << epoch << " to " << e << dendl;
  assert(epoch <= e);
  epoch = e;
  MonitorDBStore::Transaction t;
  t.put(Monitor::MONITOR_NAME, "election_epoch", epoch);
  mon->store->apply_transaction(t);

  // clear up some state
  electing_me = false;
  acked_me.clear();
}


void Elector::start()
{
  if (!participating) {
    dout(0) << "not starting new election -- not participating" << dendl;
    return;
  }
  dout(5) << "start -- can i be leader?" << dendl;

  acked_me.clear();
  init();
  
  // start by trying to elect me
  if (epoch % 2 == 0) 
    bump_epoch(epoch+1);  // odd == election cycle
  start_stamp = ceph_clock_now(g_ceph_context);
  electing_me = true;
  acked_me[mon->rank] = CEPH_FEATURES_ALL;
  leader_acked = -1;

  // bcast to everyone else
  for (unsigned i=0; i<mon->monmap->size(); ++i) {
    if ((int)i == mon->rank) continue;
    Message *m = new MMonElection(MMonElection::OP_PROPOSE, epoch, mon->monmap);
    mon->messenger->send_message(m, mon->monmap->get_inst(i));
  }
  
  reset_timer();
}

void Elector::defer(int who)
{
  dout(5) << "defer to " << who << dendl;

  if (electing_me) {
    // drop out
    acked_me.clear();
    electing_me = false;
  }

  // ack them
  leader_acked = who;
  ack_stamp = ceph_clock_now(g_ceph_context);
  mon->messenger->send_message(new MMonElection(MMonElection::OP_ACK, epoch, mon->monmap),
			       mon->monmap->get_inst(who));
  
  // set a timer
  reset_timer(1.0);  // give the leader some extra time to declare victory
}


void Elector::reset_timer(double plus)
{
  // set the timer
  cancel_timer();
  expire_event = new C_ElectionExpire(this);
  mon->timer.add_event_after(g_conf->mon_lease + plus,
			     expire_event);
}


void Elector::cancel_timer()
{
  if (expire_event) {
    mon->timer.cancel_event(expire_event);
    expire_event = 0;
  }
}

void Elector::expire()
{
  dout(5) << "election timer expired" << dendl;
  
  // did i win?
  if (electing_me &&
      acked_me.size() > (unsigned)(mon->monmap->size() / 2)) {
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

  unsigned features = CEPH_FEATURES_ALL;
  set<int> quorum;
  for (map<int,unsigned>::iterator p = acked_me.begin(); p != acked_me.end(); ++p) {
    quorum.insert(p->first);
    features &= p->second;
  }    
  
  cancel_timer();
  
  assert(epoch % 2 == 1);  // election
  bump_epoch(epoch+1);     // is over!
  
  // tell everyone
  for (set<int>::iterator p = quorum.begin();
       p != quorum.end();
       ++p) {
    if (*p == mon->rank) continue;
    MMonElection *m = new MMonElection(MMonElection::OP_VICTORY, epoch, mon->monmap);
    m->quorum = quorum;
    mon->messenger->send_message(m, mon->monmap->get_inst(*p));
  }
    
  // tell monitor
  mon->win_election(epoch, quorum, features);
}


void Elector::handle_propose(MMonElection *m)
{
  dout(5) << "handle_propose from " << m->get_source() << dendl;
  int from = m->get_source().num();

  assert(m->epoch % 2 == 1); // election
  if (m->epoch > epoch) {
    bump_epoch(m->epoch);
  }
  else if (m->epoch < epoch) {
    // got an "old" propose,
    if (epoch % 2 == 0 &&    // in a non-election cycle
	mon->quorum.count(from) == 0) {  // from someone outside the quorum
      // a mon just started up, call a new election so they can rejoin!
      dout(5) << " got propose from old epoch, quorum is " << mon->quorum 
	      << ", " << m->get_source() << " must have just started" << dendl;
      // we may be active; make sure we reset things in the monitor appropriately.
      mon->reset();
      mon->start_election();
    } else {
      dout(5) << " ignoring old propose" << dendl;
      m->put();
      return;
    }
  }

  if (mon->rank < from) {
    // i would win over them.
    if (leader_acked >= 0) {        // we already acked someone
      assert(leader_acked < from);  // and they still win, of course
      dout(5) << "no, we already acked " << leader_acked << dendl;
    } else {
      // wait, i should win!
      if (!electing_me) {
	mon->reset();
	mon->start_election();
      }
    }
  } else {
    // they would win over me
    if (leader_acked < 0 ||      // haven't acked anyone yet, or
	leader_acked > from ||   // they would win over who you did ack, or
	leader_acked == from) {  // this is the guy we're already deferring to
      defer(from);
    } else {
      // ignore them!
      dout(5) << "no, we already acked " << leader_acked << dendl;
    }
  }
  
  m->put();
}
 
void Elector::handle_ack(MMonElection *m)
{
  dout(5) << "handle_ack from " << m->get_source() << dendl;
  int from = m->get_source().num();

  assert(m->epoch % 2 == 1); // election
  if (m->epoch > epoch) {
    dout(5) << "woah, that's a newer epoch, i must have rebooted.  bumping and re-starting!" << dendl;
    bump_epoch(m->epoch);
    start();
    m->put();
    return;
  }
  assert(m->epoch == epoch);
  
  if (electing_me) {
    // thanks
    acked_me[from] = m->get_connection()->get_features();
    dout(5) << " so far i have " << acked_me << dendl;
    
    // is that _everyone_?
    if (acked_me.size() == mon->monmap->size()) {
      // if yes, shortcut to election finish
      victory();
    }
  } else {
    // ignore, i'm deferring already.
    assert(leader_acked >= 0);
  }
  
  m->put();
}


void Elector::handle_victory(MMonElection *m)
{
  dout(5) << "handle_victory from " << m->get_source() << " quorum_features " << m->quorum_features << dendl;
  int from = m->get_source().num();

  assert(from < mon->rank);
  assert(m->epoch % 2 == 0);  

  leader_acked = -1;

  // i should have seen this election if i'm getting the victory.
  if (m->epoch != epoch + 1) { 
    dout(5) << "woah, that's a funny epoch, i must have rebooted.  bumping and re-starting!" << dendl;
    bump_epoch(m->epoch);
    start();
    m->put();
    return;
  }

  bump_epoch(m->epoch);
  
  // they win
  mon->lose_election(epoch, m->quorum, from, m->quorum_features);
  
  // cancel my timer
  cancel_timer();	
  m->put();
}




void Elector::dispatch(Message *m)
{
  switch (m->get_type()) {
    
  case MSG_MON_ELECTION:
    {
      if (!participating) {
        m->put();
        return;
      }
      if (m->get_source().num() >= mon->monmap->size()) {
	dout(5) << " ignoring bogus election message with bad mon rank " 
		<< m->get_source() << dendl;
	m->put();
	return;
      }

      MMonElection *em = (MMonElection*)m;

      // assume an old message encoding would have matched
      if (em->fsid != mon->monmap->fsid) {
	dout(0) << " ignoring election msg fsid " 
		<< em->fsid << " != " << mon->monmap->fsid << dendl;
	m->put();
	return;
      }

      if (!mon->monmap->contains(m->get_source_addr())) {
	dout(1) << "discarding election message: " << m->get_source_addr()
		<< " not in my monmap " << *mon->monmap << dendl;
	m->put();
	return;
      }

      MonMap *peermap = new MonMap;
      peermap->decode(em->monmap_bl);
      if (peermap->epoch > mon->monmap->epoch) {
	dout(0) << m->get_source_inst() << " has newer monmap epoch " << peermap->epoch
		<< " > my epoch " << mon->monmap->epoch 
		<< ", taking it"
		<< dendl;
	mon->monmap->decode(em->monmap_bl);
        MonitorDBStore::Transaction t;
        t.put("monmap", mon->monmap->epoch, em->monmap_bl);
        t.put("monmap", "last_committed", mon->monmap->epoch);
        mon->store->apply_transaction(t);
	//mon->monmon()->paxos->stash_latest(mon->monmap->epoch, em->monmap_bl);
	mon->bootstrap();
	m->put();
	delete peermap;
	return;
      }
      if (peermap->epoch < mon->monmap->epoch) {
	dout(0) << m->get_source_inst() << " has older monmap epoch " << peermap->epoch
		<< " < my epoch " << mon->monmap->epoch 
		<< dendl;
      } 
      delete peermap;

      switch (em->op) {
      case MMonElection::OP_PROPOSE:
	handle_propose(em);
	return;
      }

      if (em->epoch < epoch) {
	dout(5) << "old epoch, dropping" << dendl;
	em->put();
	break;
      }

      switch (em->op) {
      case MMonElection::OP_ACK:
	handle_ack(em);
	return;
      case MMonElection::OP_VICTORY:
	handle_victory(em);
	return;
      default:
	assert(0);
      }
    }
    break;
    
  default: 
    assert(0);
  }
}

void Elector::start_participating()
{
  if (!participating) {
    participating = true;
    call_election();
  }
}
