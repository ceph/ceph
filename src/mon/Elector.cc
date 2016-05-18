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
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  t->put(Monitor::MONITOR_NAME, "election_epoch", epoch);
  mon->store->apply_transaction(t);

  mon->join_election();

  // clear up some state
  electing_me = false;
  acked_me.clear();
  classic_mons.clear();
}


void Elector::start()
{
  if (!participating) {
    dout(0) << "not starting new election -- not participating" << dendl;
    return;
  }
  dout(5) << "start -- can i be leader?" << dendl;

  acked_me.clear();
  classic_mons.clear();
  init();
  
  // start by trying to elect me
  if (epoch % 2 == 0) {
    bump_epoch(epoch+1);  // odd == election cycle
  } else {
    // do a trivial db write just to ensure it is writeable.
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    t->put(Monitor::MONITOR_NAME, "election_writeable_test", rand());
    int r = mon->store->apply_transaction(t);
    assert(r >= 0);
  }
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
    classic_mons.clear();
    electing_me = false;
  }

  // ack them
  leader_acked = who;
  ack_stamp = ceph_clock_now(g_ceph_context);
  MMonElection *m = new MMonElection(MMonElection::OP_ACK, epoch, mon->monmap);
  m->sharing_bl = mon->get_supported_commands_bl();
  mon->messenger->send_message(m, mon->monmap->get_inst(who));
  
  // set a timer
  reset_timer(1.0);  // give the leader some extra time to declare victory
}


void Elector::reset_timer(double plus)
{
  // set the timer
  cancel_timer();
  expire_event = new C_ElectionExpire(this);
  mon->timer.add_event_after(g_conf->mon_election_timeout + plus,
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
    if (mon->has_ever_joined)
      start();
    else
      mon->bootstrap();
  }
}


void Elector::victory()
{
  leader_acked = -1;
  electing_me = false;

  uint64_t features = CEPH_FEATURES_ALL;
  set<int> quorum;
  for (map<int, uint64_t>::iterator p = acked_me.begin(); p != acked_me.end();
       ++p) {
    quorum.insert(p->first);
    features &= p->second;
  }

  // decide what command set we're supporting
  bool use_classic_commands = !classic_mons.empty();
  // keep a copy to share with the monitor; we clear classic_mons in bump_epoch
  set<int> copy_classic_mons = classic_mons;
  
  cancel_timer();
  
  assert(epoch % 2 == 1);  // election
  bump_epoch(epoch+1);     // is over!

  // decide my supported commands for peons to advertise
  const bufferlist *cmds_bl = NULL;
  const MonCommand *cmds;
  int cmdsize;
  if (use_classic_commands) {
    mon->get_classic_monitor_commands(&cmds, &cmdsize);
    cmds_bl = &mon->get_classic_commands_bl();
  } else {
    mon->get_locally_supported_monitor_commands(&cmds, &cmdsize);
    cmds_bl = &mon->get_supported_commands_bl();
  }
  
  // tell everyone!
  for (set<int>::iterator p = quorum.begin();
       p != quorum.end();
       ++p) {
    if (*p == mon->rank) continue;
    MMonElection *m = new MMonElection(MMonElection::OP_VICTORY, epoch, mon->monmap);
    m->quorum = quorum;
    m->quorum_features = features;
    m->sharing_bl = *cmds_bl;
    mon->messenger->send_message(m, mon->monmap->get_inst(*p));
  }
    
  // tell monitor
  mon->win_election(epoch, quorum, features, cmds, cmdsize, &copy_classic_mons);
}


void Elector::handle_propose(MonOpRequestRef op)
{
  op->mark_event("elector:handle_propose");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_propose from " << m->get_source() << dendl;
  int from = m->get_source().num();

  assert(m->epoch % 2 == 1); // election
  uint64_t required_features = mon->get_required_features();
  dout(10) << __func__ << " required features " << required_features
           << ", peer features " << m->get_connection()->get_features()
           << dendl;
  if ((required_features ^ m->get_connection()->get_features()) &
      required_features) {
    dout(5) << " ignoring propose from mon" << from
	    << " without required features" << dendl;
    nak_old_peer(op);
    return;
  } else if (m->epoch > epoch) {
    bump_epoch(m->epoch);
  } else if (m->epoch < epoch) {
    // got an "old" propose,
    if (epoch % 2 == 0 &&    // in a non-election cycle
	mon->quorum.count(from) == 0) {  // from someone outside the quorum
      // a mon just started up, call a new election so they can rejoin!
      dout(5) << " got propose from old epoch, quorum is " << mon->quorum 
	      << ", " << m->get_source() << " must have just started" << dendl;
      // we may be active; make sure we reset things in the monitor appropriately.
      mon->start_election();
    } else {
      dout(5) << " ignoring old propose" << dendl;
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
}
 
void Elector::handle_ack(MonOpRequestRef op)
{
  op->mark_event("elector:handle_ack");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(5) << "handle_ack from " << m->get_source() << dendl;
  int from = m->get_source().num();

  assert(m->epoch % 2 == 1); // election
  if (m->epoch > epoch) {
    dout(5) << "woah, that's a newer epoch, i must have rebooted.  bumping and re-starting!" << dendl;
    bump_epoch(m->epoch);
    start();
    return;
  }
  assert(m->epoch == epoch);
  uint64_t required_features = mon->get_required_features();
  if ((required_features ^ m->get_connection()->get_features()) &
      required_features) {
    dout(5) << " ignoring ack from mon" << from
	    << " without required features" << dendl;
    return;
  }
  
  if (electing_me) {
    // thanks
    acked_me[from] = m->get_connection()->get_features();
    if (!m->sharing_bl.length())
      classic_mons.insert(from);
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
}


void Elector::handle_victory(MonOpRequestRef op)
{
  op->mark_event("elector:handle_victory");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
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
    return;
  }

  bump_epoch(m->epoch);
  
  // they win
  mon->lose_election(epoch, m->quorum, from, m->quorum_features);
  
  // cancel my timer
  cancel_timer();

  // stash leader's commands
  if (m->sharing_bl.length()) {
    MonCommand *new_cmds;
    int cmdsize;
    bufferlist::iterator bi = m->sharing_bl.begin();
    MonCommand::decode_array(&new_cmds, &cmdsize, bi);
    mon->set_leader_supported_commands(new_cmds, cmdsize);
  } else { // they are a legacy monitor; use known legacy command set
    const MonCommand *new_cmds;
    int cmdsize;
    mon->get_classic_monitor_commands(&new_cmds, &cmdsize);
    mon->set_leader_supported_commands(new_cmds, cmdsize);
  }
}

void Elector::nak_old_peer(MonOpRequestRef op)
{
  op->mark_event("elector:nak_old_peer");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  uint64_t supported_features = m->get_connection()->get_features();

  if (supported_features & CEPH_FEATURE_OSDMAP_ENC) {
    uint64_t required_features = mon->get_required_features();
    dout(10) << "sending nak to peer " << m->get_source()
	     << " that only supports " << supported_features
	     << " of the required " << required_features << dendl;
    
    MMonElection *reply = new MMonElection(MMonElection::OP_NAK, m->epoch,
					   mon->monmap);
    reply->quorum_features = required_features;
    mon->features.encode(reply->sharing_bl);
    m->get_connection()->send_message(reply);
  }
}

void Elector::handle_nak(MonOpRequestRef op)
{
  op->mark_event("elector:handle_nak");
  MMonElection *m = static_cast<MMonElection*>(op->get_req());
  dout(1) << "handle_nak from " << m->get_source()
	  << " quorum_features " << m->quorum_features << dendl;

  CompatSet other;
  bufferlist::iterator bi = m->sharing_bl.begin();
  other.decode(bi);
  CompatSet diff = Monitor::get_supported_features().unsupported(other);
  
  derr << "Shutting down because I do not support required monitor features: { "
       << diff << " }" << dendl;
  
  exit(0);
  // the end!
}

void Elector::dispatch(MonOpRequestRef op)
{
  op->mark_event("elector:dispatch");
  assert(op->is_type_election());

  switch (op->get_req()->get_type()) {
    
  case MSG_MON_ELECTION:
    {
      if (!participating) {
        return;
      }
      if (op->get_req()->get_source().num() >= mon->monmap->size()) {
	dout(5) << " ignoring bogus election message with bad mon rank " 
		<< op->get_req()->get_source() << dendl;
	return;
      }

      MMonElection *em = static_cast<MMonElection*>(op->get_req());

      // assume an old message encoding would have matched
      if (em->fsid != mon->monmap->fsid) {
	dout(0) << " ignoring election msg fsid " 
		<< em->fsid << " != " << mon->monmap->fsid << dendl;
	return;
      }

      if (!mon->monmap->contains(em->get_source_addr())) {
	dout(1) << "discarding election message: " << em->get_source_addr()
		<< " not in my monmap " << *mon->monmap << dendl;
	return;
      }

      MonMap peermap;
      peermap.decode(em->monmap_bl);
      if (peermap.epoch > mon->monmap->epoch) {
	dout(0) << em->get_source_inst() << " has newer monmap epoch " << peermap.epoch
		<< " > my epoch " << mon->monmap->epoch 
		<< ", taking it"
		<< dendl;
	mon->monmap->decode(em->monmap_bl);
        MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
        t->put("monmap", mon->monmap->epoch, em->monmap_bl);
        t->put("monmap", "last_committed", mon->monmap->epoch);
        mon->store->apply_transaction(t);
	//mon->monmon()->paxos->stash_latest(mon->monmap->epoch, em->monmap_bl);
	cancel_timer();
	mon->bootstrap();
	return;
      }
      if (peermap.epoch < mon->monmap->epoch) {
	dout(0) << em->get_source_inst() << " has older monmap epoch " << peermap.epoch
		<< " < my epoch " << mon->monmap->epoch 
		<< dendl;
      } 

      switch (em->op) {
      case MMonElection::OP_PROPOSE:
	handle_propose(op);
	return;
      }

      if (em->epoch < epoch) {
	dout(5) << "old epoch, dropping" << dendl;
	break;
      }

      switch (em->op) {
      case MMonElection::OP_ACK:
	handle_ack(op);
	return;
      case MMonElection::OP_VICTORY:
	handle_victory(op);
	return;
      case MMonElection::OP_NAK:
	handle_nak(op);
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
