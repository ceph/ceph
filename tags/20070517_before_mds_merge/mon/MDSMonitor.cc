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


#include "MDSMonitor.h"
#include "Monitor.h"
#include "MonitorStore.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSGetMap.h"
#include "messages/MMDSBeacon.h"

#include "common/Timer.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".mds e" << mdsmap.get_epoch() << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".mds e" << mdsmap.get_epoch() << " "



/********* MDS map **************/

void MDSMonitor::dispatch(Message *m)
{
  switch (m->get_type()) {

  case MSG_MDS_BEACON:
    handle_mds_beacon((MMDSBeacon*)m);
    break;
    
  case MSG_MDS_GETMAP:
    handle_mds_getmap((MMDSGetMap*)m);
    break;
    
  default:
    assert(0);
  }  
}



void MDSMonitor::election_finished()
{
  if (mon->is_leader()) {

    // FIXME be smarter later.

    if (g_conf.mkfs) {
      create_initial();
      save_map();
    } else {
      load_map();
    }
  }
}


void MDSMonitor::create_initial()
{
  mdsmap.epoch = 0;  // until everyone boots
  mdsmap.ctime = g_clock.now();

  mdsmap.encode(encoded_map);

  print_map();
}

void MDSMonitor::load_map()
{
  int r = mon->store->get_bl_ss(encoded_map, "mdsmap", "current");
  assert(r > 0);
  mdsmap.decode(encoded_map);
  dout(7) << "load_map epoch " << mdsmap.get_epoch() << endl;
}

void MDSMonitor::save_map()
{
  dout(7) << "save_map epoch " << mdsmap.get_epoch() << endl;
  
  int r = mon->store->put_bl_ss(encoded_map, "mdsmap", "current");
  assert(r>=0);
}

void MDSMonitor::print_map()
{
  dout(7) << "print_map epoch " << mdsmap.get_epoch() << endl;
  entity_inst_t blank;
  set<int> all;
  mdsmap.get_mds_set(all);
  for (set<int>::iterator p = all.begin();
       p != all.end();
       ++p) {
    dout(7) << " mds" << *p << "." << mdsmap.mds_inc[*p]
	    << " : " << MDSMap::get_state_name(mdsmap.get_state(*p))
	    << " : " << (mdsmap.have_inst(*p) ? mdsmap.get_inst(*p) : blank)
	    << endl;
  }
}


void MDSMonitor::issue_map()
{
  mdsmap.inc_epoch();
  encoded_map.clear();
  mdsmap.encode(encoded_map);

  dout(7) << "issue_map epoch " << mdsmap.get_epoch() << endl;
  
  save_map();
  print_map();
  
  // bcast map
  bcast_latest_mds();
  send_current();
}


void MDSMonitor::handle_mds_beacon(MMDSBeacon *m)
{
  dout(7) << "mds_beacon " << *m
	  << " from " << m->get_source()
	  << " " << m->get_source_inst()
	  << endl;
  int from = m->get_source().num();
  int state = m->get_state();
  version_t seq = m->get_seq();

  // initial boot?
  bool booted = false;
  
  // choose an MDS id
  if (from >= 0) {
    // wants to be (or already is) a specific MDS. 
    if (mdsmap.is_down(from)) {
      dout(10) << "mds_beacon assigning requested mds" << from << endl;
      booted = true;
    } else if (mdsmap.get_inst(from) != m->get_source_inst()) {
      dout(10) << "mds_beacon not assigning requested mds" << from 
	       << ", that mds is up and someone else" << endl;
      from = -1;
    }
  }
  if (from < 0) {
    // pick a failed mds?
    set<int> failed;
    mdsmap.get_failed_mds_set(failed);
    if (!failed.empty()) {
      from = *failed.begin();
      dout(10) << "mds_beacon assigned failed mds" << from << endl;
      booted = true;
    }
  }
  if (from < 0) {
    // ok, just pick any unused mds id.
    for (from=0; ; ++from) {
      if (mdsmap.is_dne(from) ||
	  mdsmap.is_out(from)) {
	dout(10) << "mds_beacon assigned out|dne mds" << from << endl;
	booted = true;
	break;
      }
    }
  }
  

  // old beacon?
  if (mdsmap.mds_state_seq[from] > seq) {
    dout(7) << "mds_beacon " << *m << " has old seq, ignoring" << endl;
    delete m;
    return;
  }
  
  // reply to beacon?
  if (state != MDSMap::STATE_OUT) {
    last_beacon[from] = g_clock.now();  // note time
    messenger->send_message(new MMDSBeacon(state, seq), 
			    m->get_source_inst());
  }


  // make sure it's in the map
  if (booted) {
    mdsmap.mds_inst[from].addr = m->get_source_addr();
    mdsmap.mds_inst[from].name = MSG_ADDR_MDS(from);
    mdsmap.mds_inc[from]++;

    // starting -> creating|starting|replay
    if (mdsmap.is_degraded() &&
	!mdsmap.is_failed(from)) {
      dout(10) << "mds_beacon currently degraded, mds" << from << " will be standby" << endl;
      state = MDSMap::STATE_STANDBY;
    }
    else if (state == MDSMap::STATE_STARTING) {
      if (mdsmap.is_failed(from)) {
	dout(10) << "mds_beacon will recover mds" << from << endl;
	state = MDSMap::STATE_REPLAY;
      } 
      else if (mdsmap.is_out(from)) {
	dout(10) << "mds_beacon will start mds" << from << endl;
	state = MDSMap::STATE_STARTING;
      } 
      else {
	dout(10) << "mds_beacon will create mds" << from << endl;
	state = MDSMap::STATE_CREATING;
      } 
    }
  }

  // if creating -> active, go to standby instead
  if (state == MDSMap::STATE_ACTIVE && mdsmap.is_creating(from)) {
    mdsmap.mds_created.insert(from);
    dout(10) << "mds_beacon created mds" << from << endl;
    
    if (mdsmap.is_degraded()) {
      dout(10) << "mds_beacon current degraded, marking mds" << from << " as standby" << endl;
      state = MDSMap::STATE_STANDBY;
    }
  }


  // did we update the map?
  if (mdsmap.mds_state.count(from) == 0 ||
      mdsmap.mds_state[from] != state) {
    // update mds state
    dout(10) << "mds_beacon mds" << from << " " << MDSMap::get_state_name(mdsmap.mds_state[from])
	     << " -> " << MDSMap::get_state_name(state)
	     << endl;
    mdsmap.mds_state[from] = state;
    if (mdsmap.is_up(from))
      mdsmap.mds_state_seq[from] = seq;
    else
      mdsmap.mds_state_seq.erase(from);
    
    issue_map();
  }

  delete m;
}


void MDSMonitor::handle_mds_getmap(MMDSGetMap *m)
{
  dout(7) << "mds_getmap from " << m->get_source() << " " << m->get_source_inst() << endl;
  if (mdsmap.get_epoch() > 0)
    send_full(m->get_source_inst());
  else
    awaiting_map.push_back( m->get_source_inst() );
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
  messenger->send_message(new MMDSMap(&mdsmap), dest);
}

void MDSMonitor::send_current()
{
  dout(10) << "mds_send_current " << mdsmap.get_epoch() << endl;
  for (list<entity_inst_t>::iterator i = awaiting_map.begin();
       i != awaiting_map.end();
       i++) 
    send_full(*i);
  awaiting_map.clear();
}

void MDSMonitor::send_latest(entity_inst_t dest)
{
  // FIXME: check if we're locked, etc.
  if (mdsmap.get_epoch() > 0)
    send_full(dest);
  else
    awaiting_map.push_back(dest);
}


void MDSMonitor::tick()
{
  // make sure mds's are still alive
  utime_t now = g_clock.now();
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
	  case MDSMap::STATE_CREATING:
	    // didn't finish creating
	    newstate = MDSMap::STATE_DNE;
	    break;

	  case MDSMap::STATE_STANDBY:
	    if (mdsmap.has_created(*p))
	      newstate = MDSMap::STATE_OUT;
	    else
	      newstate = MDSMap::STATE_DNE;
	    break;

	  case MDSMap::STATE_REPLAY:
	  case MDSMap::STATE_REJOIN:
	  case MDSMap::STATE_ACTIVE:
	  case MDSMap::STATE_STOPPING:
	    newstate = MDSMap::STATE_FAILED;
	    break;

	  case MDSMap::STATE_STARTING:
	  case MDSMap::STATE_STOPPED:
	    newstate = MDSMap::STATE_OUT;
	    break;

	  default:
	    assert(0);
	  }
	  
	  dout(10) << "no beacon from mds" << *p << " since " << last_beacon[*p]
		   << ", marking " << mdsmap.get_state_name(newstate)
		   << endl;
	  
	  // update map
	  mdsmap.mds_state[*p] = newstate;
	  mdsmap.mds_state_seq.erase(*p);
	  changed = true;
	}
      } else {
	dout(10) << "no beacons from mds" << *p << ", assuming one " << now << endl;
	last_beacon[*p] = now;
      }
    }

    if (changed) {
      issue_map();
    }
  }
}
