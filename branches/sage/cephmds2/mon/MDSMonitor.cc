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


#include "MDSMonitor.h"
#include "Monitor.h"
#include "MonitorStore.h"
#include "OSDMonitor.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSGetMap.h"
#include "messages/MMDSBeacon.h"

#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"

#include "messages/MGenericMessage.h"


#include "common/Timer.h"

#include <sstream>

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".mds e" << mdsmap.get_epoch() << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".mds e" << mdsmap.get_epoch() << " "



// my methods

void MDSMonitor::print_map(MDSMap &m)
{
  dout(7) << "print_map epoch " << m.get_epoch() << " target_num " << m.target_num << endl;
  entity_inst_t blank;
  set<int> all;
  m.get_mds_set(all);
  for (set<int>::iterator p = all.begin();
       p != all.end();
       ++p) {
    dout(7) << " mds" << *p << "." << m.mds_inc[*p]
	    << " : " << MDSMap::get_state_name(m.get_state(*p))
	    << " : " << (m.have_inst(*p) ? m.get_inst(*p) : blank)
	    << endl;
  }
}



// service methods

void MDSMonitor::create_initial()
{
  dout(10) << "create_initial" << endl;
  pending_mdsmap.target_num = g_conf.num_mds;
  pending_mdsmap.created = g_clock.now();
  print_map(pending_mdsmap);
}

bool MDSMonitor::update_from_paxos()
{
  assert(paxos->is_active());

  version_t paxosv = paxos->get_version();
  dout(10) << "update_from_paxos paxosv " << paxosv 
	   << ", my e " << mdsmap.epoch << endl;

  if (paxosv == mdsmap.epoch) return true;
  assert(paxosv >= mdsmap.epoch);

  // read and decode
  mdsmap_bl.clear();
  bool success = paxos->read(paxosv, mdsmap_bl);
  assert(success);
  dout(10) << "update_from_paxos  got " << paxosv << endl;
  mdsmap.decode(mdsmap_bl);

  // new map
  print_map(mdsmap);

  // bcast map to mds, waiters
  if (mon->is_leader())
    bcast_latest_mds();
  send_to_waiting();

  // hackish: did all mds's shut down?
  if (mon->is_leader() &&
      g_conf.mon_stop_with_last_mds &&
      mdsmap.get_epoch() > 1 &&
      mdsmap.is_stopped()) 
    mon->messenger->send_message(new MGenericMessage(MSG_SHUTDOWN), 
				 mon->monmap->get_inst(mon->whoami));

  return true;
}

void MDSMonitor::create_pending()
{
  pending_mdsmap = mdsmap;
  pending_mdsmap.epoch++;
  dout(10) << "create_pending e" << pending_mdsmap.epoch << endl;
}

void MDSMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending e" << pending_mdsmap.epoch << endl;
  
  print_map(pending_mdsmap);

  // apply to paxos
  assert(paxos->get_version() + 1 == pending_mdsmap.epoch);
  pending_mdsmap.encode(bl);
}


bool MDSMonitor::preprocess_query(Message *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_source_inst() << endl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return preprocess_beacon((MMDSBeacon*)m);
    
  case MSG_MDS_GETMAP:
    send_full(m->get_source_inst());
    return true;

  case MSG_MON_COMMAND:
    return false;

  default:
    assert(0);
    delete m;
    return true;
  }
}


bool MDSMonitor::preprocess_beacon(MMDSBeacon *m)
{
  dout(12) << "preprocess_beacon " << *m
	   << " from " << m->get_mds_inst()
	   << endl;

  // fw to leader?
  if (!mon->is_leader()) {
    dout(10) << "fw to leader" << endl;
    mon->messenger->send_message(m, mon->monmap->get_inst(mon->get_leader()));
    return true;
  }

  // let's see.
  int from = m->get_mds_inst().name.num();
  int state = m->get_state();
  version_t seq = m->get_seq();

  // can i handle this query without a map update?
  
  // boot?
  if (state == MDSMap::STATE_BOOT) {
    // already booted?
    int already = mdsmap.get_addr_rank(m->get_mds_inst().addr);
    if (already < 0) 
      return false;  // need to update map
    
    // already booted.  just reply to beacon, as per usual.
    from = already;
  }

  // reply to beacon
  if (mdsmap.mds_state_seq[from] > seq) {
    dout(7) << "mds_beacon " << *m << " has old seq, ignoring" << endl;
    delete m;
    return true;
  }
  
  // reply to beacon?
  if (state != MDSMap::STATE_STOPPED) {
    last_beacon[from] = g_clock.now();  // note time
    mon->messenger->send_message(new MMDSBeacon(m->get_mds_inst(), state, seq), 
				 m->get_mds_inst());
  }
  
  // is there a state change here?
  if (mdsmap.mds_state.count(from) == 0 ||
      mdsmap.mds_state[from] != state)
    return false;  // yep, need to update map.
  
  // we're done.
  delete m;
  return true;
}


bool MDSMonitor::prepare_update(Message *m)
{
  dout(10) << "prepare_update " << *m << endl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return handle_beacon((MMDSBeacon*)m);
    
  case MSG_MON_COMMAND:
    return handle_command((MMonCommand*)m);

  default:
    assert(0);
    delete m;
  }

  return true;
}

bool MDSMonitor::should_propose_now()
{
  return true;
}


bool MDSMonitor::handle_beacon(MMDSBeacon *m)
{
  // -- this is an update --
  dout(12) << "handle_beacon " << *m
	   << " from " << m->get_mds_inst()
	   << endl;
  int from = m->get_mds_inst().name.num();
  int state = m->get_state();
  version_t seq = m->get_seq();

  assert(state != mdsmap.get_state(from));

  // boot?
  if (state == MDSMap::STATE_BOOT) {
    // assign a name.
    if (from >= 0) {
      // wants to be (or already is) a specific MDS. 
      if (mdsmap.is_failed(from)) {
	dout(10) << "mds_beacon boot: mds" << from << " was failed, replaying" << endl;
	state = MDSMap::STATE_REPLAY;
      } else if (mdsmap.is_stopped(from)) {
	dout(10) << "mds_beacon boot: mds" << from << " was stopped, starting" << endl;
	state = MDSMap::STATE_STARTING;
      } else if (!mdsmap.have_inst(from) || mdsmap.get_inst(from) != m->get_mds_inst()) {
	dout(10) << "mds_beacon boot: mds" << from << " is someone else" << endl;
	from = -1;
      }
    }
    if (from < 0) {
      from = pending_mdsmap.get_addr_rank(m->get_mds_inst().addr);
      if (from >= 0) {
	state = pending_mdsmap.mds_state[from];
	dout(10) << "mds_beacon boot: already pending mds" << from
		 << " " << MDSMap::get_state_name(state) << endl;
	delete m;
	return false;
      }
    }
    if (from < 0) {
      // pick a failed mds?
      set<int> failed;
      pending_mdsmap.get_failed_mds_set(failed);
      if (!failed.empty()) {
	from = *failed.begin();
	dout(10) << "mds_beacon boot: assigned failed mds" << from << endl;
	state = MDSMap::STATE_REPLAY;
      }
    }
    if (from < 0) {
      // ok, just pick any unused mds id.
      for (from=0; ; ++from) {
	if (pending_mdsmap.is_dne(from)) {
	  dout(10) << "mds_beacon boot: assigned new mds" << from << endl;
	  state = MDSMap::STATE_CREATING;
	  break;
	} else if (pending_mdsmap.is_stopped(from)) {
	  dout(10) << "mds_beacon boot: assigned stopped mds" << from << endl;
	  state = MDSMap::STATE_STARTING;
	  break;
	}
      }
    }
    
    assert(state == MDSMap::STATE_CREATING ||
	   state == MDSMap::STATE_STARTING ||
	   state == MDSMap::STATE_REPLAY);
    
    // put it in the map.
    pending_mdsmap.mds_inst[from].addr = m->get_mds_inst().addr;
    pending_mdsmap.mds_inst[from].name = MSG_ADDR_MDS(from);
    pending_mdsmap.mds_inc[from]++;
    
    // someone (new) has joined the cluster.
    pending_mdsmap.same_inst_since = pending_mdsmap.epoch;

    // reset the beacon timer
    last_beacon[from] = g_clock.now();
  }

  // created?
  if (state == MDSMap::STATE_ACTIVE && 
      mdsmap.is_creating(from)) {
    pending_mdsmap.mds_created.insert(from);
    dout(10) << "mds_beacon created mds" << from << endl;
  }
  
  // if starting|creating and degraded|full, go to standby
  if ((state == MDSMap::STATE_STARTING || 
       state == MDSMap::STATE_CREATING ||
       mdsmap.is_starting(from) ||
       mdsmap.is_creating(from)) &&
      (pending_mdsmap.is_degraded() || 
       pending_mdsmap.is_full())) {
    dout(10) << "mds_beacon cluster degraded|full, mds" << from << " will be standby" << endl;
    state = MDSMap::STATE_STANDBY;
  }

  // update the map
  dout(10) << "mds_beacon mds" << from << " " << MDSMap::get_state_name(mdsmap.mds_state[from])
	   << " -> " << MDSMap::get_state_name(state)
	   << endl;

  // did someone leave the cluster?
  if (state == MDSMap::STATE_STOPPED && 
      !mdsmap.is_stopped(from))
    pending_mdsmap.same_inst_since = pending_mdsmap.epoch;
  
  // change the state
  pending_mdsmap.mds_state[from] = state;
  if (pending_mdsmap.is_up(from))
    pending_mdsmap.mds_state_seq[from] = seq;
  else
    pending_mdsmap.mds_state_seq.erase(from);
  
  dout(7) << "pending map now:" << endl;
  print_map(pending_mdsmap);

  paxos->wait_for_commit(new C_Updated(this, from, m));

  return true;
}


void MDSMonitor::_updated(int from, MMDSBeacon *m)
{
  if (m->get_state() == MDSMap::STATE_BOOT) {
    dout(10) << "_updated (booted) mds" << from << " " << *m << endl;
    mon->osdmon->send_latest(mdsmap.get_inst(from));
  } else {
    dout(10) << "_updated mds" << from << " " << *m << endl;
  }
  if (m->get_state() == MDSMap::STATE_STOPPED) {
    // send the map manually (they're out of the map, so they won't get it automatic)
    send_latest(m->get_mds_inst());
  }
  delete m;
}



bool MDSMonitor::handle_command(MMonCommand *m)
{
  int r = -EINVAL;
  stringstream ss;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stop" && m->cmd.size() > 2) {
      int who = atoi(m->cmd[2].c_str());
      if (mdsmap.is_active(who)) {
	r = 0;
	ss << "telling mds" << who << " to stop";
	pending_mdsmap.mds_state[who] = MDSMap::STATE_STOPPING;
      } else {
	r = -EEXIST;
	ss << "mds" << who << " not active (" << mdsmap.get_state_name(mdsmap.get_state(who)) << ")";
      }
    }
    else if (m->cmd[1] == "set_target_num" && m->cmd.size() > 2) {
      pending_mdsmap.target_num = atoi(m->cmd[2].c_str());
      r = 0;
      ss << "target_num = " << pending_mdsmap.target_num << endl;
    }
  }
  if (r == -EINVAL) {
    ss << "unrecognized command";
  } 

  // reply
  string rs;
  getline(ss,rs);
  mon->messenger->send_message(new MMonCommandAck(r, rs), m->get_source_inst());
  delete m;
  return r >= 0;
}



void MDSMonitor::bcast_latest_mds()
{
  dout(10) << "bcast_latest_mds " << mdsmap.get_epoch() << endl;
  
  // tell mds
  set<int> up;
  mdsmap.get_up_mds_set(up);
  for (set<int>::iterator p = up.begin();
       p != up.end();
       p++) 
    send_full(mdsmap.get_inst(*p));
}

void MDSMonitor::send_full(entity_inst_t dest)
{
  dout(11) << "send_full to " << dest << endl;
  mon->messenger->send_message(new MMDSMap(&mdsmap), dest);
}

void MDSMonitor::send_to_waiting()
{
  dout(10) << "send_to_waiting " << mdsmap.get_epoch() << endl;
  for (list<entity_inst_t>::iterator i = waiting_for_map.begin();
       i != waiting_for_map.end();
       i++) 
    send_full(*i);
  waiting_for_map.clear();
}

void MDSMonitor::send_latest(entity_inst_t dest)
{
  if (paxos->is_readable()) 
    send_full(dest);
  else
    waiting_for_map.push_back(dest);
}


void MDSMonitor::tick()
{
  // make sure mds's are still alive
  utime_t now = g_clock.now();

  // ...if i am an active leader
  if (!mon->is_leader()) return;
  if (!paxos->is_active()) return;

  if (now > g_conf.mds_beacon_grace) {
    utime_t cutoff = now;
    cutoff -= g_conf.mds_beacon_grace;
    
    bool changed = false;
    
    set<int> up;
    mdsmap.get_up_mds_set(up);

    for (set<int>::iterator p = up.begin();
	 p != up.end();
	 ++p) {
      if (last_beacon.count(*p)) {
	if (last_beacon[*p] < cutoff) {

	  // failure!
	  int newstate;
	  switch (mdsmap.get_state(*p)) {
	  case MDSMap::STATE_STANDBY:
	    if (mdsmap.has_created(*p))
	      newstate = MDSMap::STATE_STOPPED;
	    else
	      newstate = MDSMap::STATE_DNE;
	    break;

	  case MDSMap::STATE_CREATING:
	    // didn't finish creating
	    newstate = MDSMap::STATE_DNE;
	    break;

	  case MDSMap::STATE_STARTING:
	    newstate = MDSMap::STATE_STOPPED;
	    break;

	  case MDSMap::STATE_REPLAY:
	  case MDSMap::STATE_RESOLVE:
	  case MDSMap::STATE_REJOIN:
	  case MDSMap::STATE_ACTIVE:
	  case MDSMap::STATE_STOPPING:
	    newstate = MDSMap::STATE_FAILED;
	    break;

	  default:
	    assert(0);
	  }
	  
	  dout(10) << "no beacon from mds" << *p << " since " << last_beacon[*p]
		   << ", marking " << mdsmap.get_state_name(newstate)
		   << endl;
	  
	  // update map
	  pending_mdsmap.mds_state[*p] = newstate;
	  pending_mdsmap.mds_state_seq.erase(*p);
	  changed = true;
	}
      } else {
	dout(10) << "no beacons from mds" << *p << ", assuming one " << now << endl;
	last_beacon[*p] = now;
      }
    }

    if (changed) 
      propose_pending();
  }
}


void MDSMonitor::do_stop()
{
  // hrm...
  if (!mon->is_leader() ||
      !paxos->is_active()) {
    dout(-10) << "do_stop can't stop right now, mdsmap not writeable" << endl;
    return;
  }

  dout(10) << "do_stop stopping active mds nodes" << endl;
  
  print_map(mdsmap);
  for (map<int,int>::iterator p = mdsmap.mds_state.begin();
       p != mdsmap.mds_state.end();
       ++p) 
    if (mdsmap.is_active(p->first))
      pending_mdsmap.mds_state[p->first] = MDSMap::STATE_STOPPING;

  propose_pending();
}
